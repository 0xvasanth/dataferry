//! The observer hook the verifier (A8) checksums through and `expect:`
//! (B8) accumulates source-side aggregates through: what it sees, when it
//! is told, and what happens when recording fails.

mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use arrow_array::RecordBatch;
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use async_trait::async_trait;
use common::*;
use ferry_core::testkit::{MemSink, MemSource};
use ferry_core::{FerryError, Lease, Result, TransformOutput, WriteMode};
use ferry_registry::state::{PartitionRows, RunId};
use ferry_runtime::{Flow, Input, PartitionObserver, ReadObserver, Runner, WriteObserver};
use ferry_state::FileStateStore;

/// Logs what it is shown. `fail_first_finish` makes the first `finish`
/// fail, as a full disk would.
#[derive(Clone, Default)]
struct Recorder {
    log: Log,
    fail_first_finish: Arc<AtomicBool>,
    flows: Arc<Mutex<Vec<(RunId, String)>>>,
}

#[async_trait]
impl WriteObserver for Recorder {
    async fn prepared(&self, run: &RunId, flow: &Flow) -> Result<()> {
        self.flows
            .lock()
            .unwrap()
            .push((run.clone(), flow.name.clone()));
        self.log.push(Event::Prepared(flow.name.clone()));
        Ok(())
    }

    fn begin(&self, _: &RunId, _: &str, lease: &Lease) -> Box<dyn PartitionObserver> {
        Box::new(Attempt {
            recorder: self.clone(),
            lease: lease.clone(),
        })
    }
}

struct Attempt {
    recorder: Recorder,
    lease: Lease,
}

#[async_trait]
impl PartitionObserver for Attempt {
    fn output(&mut self, output: &TransformOutput) -> Result<()> {
        let rows = output.targets.iter().map(|t| t.batch.num_rows()).sum();
        self.recorder
            .log
            .push(Event::Observed(self.lease.clone(), rows));
        Ok(())
    }

    async fn finish(self: Box<Self>, rows: &PartitionRows) -> Result<()> {
        if self
            .recorder
            .fail_first_finish
            .swap(false, Ordering::SeqCst)
        {
            return Err(FerryError::Connector {
                connector: "recorder".into(),
                message: "no space left on device".into(),
            });
        }
        self.recorder
            .log
            .push(Event::Finished(self.lease.clone(), *rows));
        Ok(())
    }
}

fn position(events: &[Event], f: impl Fn(&Event) -> bool) -> Option<usize> {
    events.iter().position(f)
}

#[tokio::test]
async fn sees_every_output_and_finishes_after_recording_rows_and_before_the_commit() {
    let dir = tempfile::tempdir().unwrap();
    let log = Log::default();
    let store = SpyStore::new(Arc::new(FileStateStore::new(dir.path())), log.clone());
    let sink = mem_sink();
    let spy = SpySink::new(Arc::new(sink.clone()), log.clone());
    let recorder = Recorder {
        log: log.clone(),
        ..Default::default()
    };
    let flow = identity_flow("orders", orders_source(300, 30), "orders", Arc::new(spy));
    let runner = Runner::new(Arc::new(store), vec![flow], settings())
        .unwrap()
        .with_observer(Arc::new(recorder.clone()));

    let report = runner.start(new_run()).await.unwrap();
    assert!(report.is_complete(), "{report:?}");

    let events = log.events();
    // Told once, before any partition was leased.
    assert_eq!(
        recorder.flows.lock().unwrap().clone(),
        vec![(report.run.clone(), "orders".to_string())]
    );
    let prepared = position(&events, |e| matches!(e, Event::Prepared(_))).unwrap();
    let first_lease = position(&events, |e| matches!(e, Event::Leased(_))).unwrap();
    assert!(prepared < first_lease);

    let leases: Vec<Lease> = events
        .iter()
        .filter_map(|e| match e {
            Event::Leased(l) => Some(l.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(leases.len(), 3);
    for lease in leases {
        let mine = |l: &Lease| *l == lease;
        // Every row of the partition went past the observer, in outputs
        // of the source's batch size, before the last write.
        let observed: Vec<usize> = events
            .iter()
            .filter_map(|e| match e {
                Event::Observed(l, n) if mine(l) => Some(*n),
                _ => None,
            })
            .collect();
        assert_eq!(observed, vec![30, 30, 30, 10], "{lease:?}");
        let rows = position(&events, |e| matches!(e, Event::RecordRows(l, _) if mine(l))).unwrap();
        let finished = position(
            &events,
            |e| matches!(e, Event::Finished(l, r) if mine(l) && r.read == 100),
        )
        .unwrap();
        let commit = position(&events, |e| matches!(e, Event::Commit(l) if mine(l))).unwrap();
        assert!(rows < finished && finished < commit, "{lease:?}");
    }
}

#[tokio::test]
async fn a_failed_finish_aborts_the_partition_and_its_retry_finishes_at_a_higher_epoch() {
    let dir = tempfile::tempdir().unwrap();
    let log = Log::default();
    let store = SpyStore::new(Arc::new(FileStateStore::new(dir.path())), log.clone());
    let sink: MemSink = mem_sink();
    let spy = SpySink::new(Arc::new(sink.clone()), log.clone());
    let recorder = Recorder {
        log: log.clone(),
        fail_first_finish: Arc::new(AtomicBool::new(true)),
        ..Default::default()
    };
    let mut settings = settings();
    settings.workers = 1;
    let flow = identity_flow("orders", orders_source(200, 50), "orders", Arc::new(spy));
    let runner = Runner::new(Arc::new(store), vec![flow], settings)
        .unwrap()
        .with_observer(Arc::new(recorder.clone()));

    let report = runner.start(new_run()).await.unwrap();

    // No partition commits without its record: the first one's
    // transaction was aborted and the partition marked failed.
    assert_eq!(report.failures.len(), 1, "{report:?}");
    assert!(report.failures[0].error.contains("no space left on device"));
    let failed = report.failures[0].partition.clone();
    let events = log.events();
    let first = events
        .iter()
        .find_map(|e| match e {
            Event::Leased(l) if l.partition == failed => Some(l.clone()),
            _ => None,
        })
        .unwrap();
    assert!(events.contains(&Event::Abort(first.clone())));
    assert!(!events.contains(&Event::Commit(first.clone())));
    assert!(sink.rows("orders").is_empty());

    let report = runner
        .retry_failed(&report.run, HASH, ENGINE)
        .await
        .unwrap();
    assert!(report.is_complete(), "{report:?}");
    assert_eq!(ids(&sink, "orders"), (0..200).collect::<Vec<_>>());
    // The retry was told the flow is prepared again, and finished the
    // failed partition under a newer lease.
    assert_eq!(recorder.flows.lock().unwrap().len(), 2);
    let retried = log
        .events()
        .into_iter()
        .find_map(|e| match e {
            Event::Finished(l, _) if l.partition == failed => Some(l),
            _ => None,
        })
        .unwrap();
    assert!(retried.epoch > first.epoch);
}

/// What a [`Reads`] observer saw, in the order it saw it.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Seen {
    /// The ids of a driver batch, and the columns it had.
    Read(Lease, Vec<i64>, Vec<String>),
    /// The ids of the records an output accounted for.
    Output(Lease, Vec<i64>),
}

/// Watches reads as well as outputs. `fail_first_read` makes the first
/// driver batch of the first attempt fail.
#[derive(Clone, Default)]
struct Reads {
    seen: Arc<Mutex<Vec<Seen>>>,
    fail_first_read: Arc<AtomicBool>,
}

impl Reads {
    fn seen(&self) -> Vec<Seen> {
        self.seen.lock().unwrap().clone()
    }
}

#[async_trait]
impl WriteObserver for Reads {
    async fn prepared(&self, _: &RunId, _: &Flow) -> Result<()> {
        Ok(())
    }

    fn begin(&self, _: &RunId, _: &str, lease: &Lease) -> Box<dyn PartitionObserver> {
        Box::new(ReadAttempt {
            reads: self.clone(),
            lease: lease.clone(),
            asked: false,
        })
    }
}

struct ReadAttempt {
    reads: Reads,
    lease: Lease,
    asked: bool,
}

fn ids_of(batch: &RecordBatch) -> Vec<i64> {
    batch
        .column_by_name("id")
        .unwrap()
        .as_primitive::<Int64Type>()
        .values()
        .to_vec()
}

#[async_trait]
impl PartitionObserver for ReadAttempt {
    fn reader(&mut self) -> Option<Box<dyn ReadObserver>> {
        assert!(!self.asked, "asked for a reader twice");
        self.asked = true;
        Some(Box::new(ReadAttempt {
            reads: self.reads.clone(),
            lease: self.lease.clone(),
            asked: true,
        }))
    }

    fn output(&mut self, output: &TransformOutput) -> Result<()> {
        let ids = output
            .records
            .iter()
            .map(|r| match r.0.as_slice() {
                [ferry_core::KeyValue::Int(id)] => *id,
                other => panic!("{other:?}"),
            })
            .collect();
        self.reads
            .seen
            .lock()
            .unwrap()
            .push(Seen::Output(self.lease.clone(), ids));
        Ok(())
    }

    async fn finish(self: Box<Self>, _: &PartitionRows) -> Result<()> {
        Ok(())
    }
}

impl ReadObserver for ReadAttempt {
    fn batch(&mut self, batch: &RecordBatch) -> Result<()> {
        if self.reads.fail_first_read.swap(false, Ordering::SeqCst) {
            return Err(FerryError::Connector {
                connector: "reads".into(),
                message: "the accumulator gave up".into(),
            });
        }
        let columns = batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        self.reads.seen.lock().unwrap().push(Seen::Read(
            self.lease.clone(),
            ids_of(batch),
            columns,
        ));
        Ok(())
    }
}

#[tokio::test]
async fn the_reader_sees_every_driver_batch_before_the_outputs_that_account_for_it() {
    let dir = tempfile::tempdir().unwrap();
    let source = Arc::new(
        MemSource::new()
            .with_entity(orders_entity(), orders(0..500))
            .unwrap()
            .with_entity(items_entity(), items(0..500))
            .unwrap()
            .with_batch_rows(40),
    );
    // A merge, so there is a joined input the reader must not be shown.
    let flow = Flow {
        name: "merged".into(),
        inputs: vec![
            Input {
                source: source.clone(),
                entity: orders_entity(),
                co_partition: None,
            },
            Input {
                source,
                entity: items_entity(),
                co_partition: Some("order_id".into()),
            },
        ],
        plan: Arc::new(MergePlan::default()),
        sink: Arc::new(mem_sink()),
        target: split_target(),
        mode: WriteMode::Insert,
        depends_on: Vec::new(),
        finalizer: None,
    };
    let reads = Reads::default();
    let runner = Runner::new(
        Arc::new(FileStateStore::new(dir.path())),
        vec![flow],
        settings(),
    )
    .unwrap()
    .with_observer(Arc::new(reads.clone()));
    let report = runner.start(new_run()).await.unwrap();
    assert!(report.is_complete(), "{report:?}");

    let seen = reads.seen();
    let mut read_ids: Vec<i64> = Vec::new();
    let mut partitions = std::collections::BTreeSet::new();
    for (i, s) in seen.iter().enumerate() {
        match s {
            Seen::Read(lease, ids, columns) => {
                // Driver rows only, in the source's batches.
                assert_eq!(columns, &["id", "name"], "{s:?}");
                assert!(ids.len() <= 40, "{s:?}");
                read_ids.extend(ids);
                partitions.insert(lease.partition.clone());
            }
            Seen::Output(lease, ids) => {
                // Every record an output accounts for was read before it,
                // by the same attempt.
                for id in ids {
                    let read_before = seen[..i].iter().any(
                        |r| matches!(r, Seen::Read(l, read, _) if l == lease && read.contains(id)),
                    );
                    assert!(read_before, "order {id} was output before it was read");
                }
            }
        }
    }
    read_ids.sort_unstable();
    assert_eq!(read_ids, (0..500).collect::<Vec<_>>());
    assert_eq!(partitions.len(), 5);
}

#[tokio::test]
async fn a_failing_reader_fails_the_partition_and_its_retry_reads_again() {
    let dir = tempfile::tempdir().unwrap();
    let sink = mem_sink();
    let reads = Reads {
        fail_first_read: Arc::new(AtomicBool::new(true)),
        ..Default::default()
    };
    let mut settings = settings();
    settings.workers = 1;
    let flow = identity_flow(
        "orders",
        orders_source(200, 50),
        "orders",
        Arc::new(sink.clone()),
    );
    let runner = Runner::new(
        Arc::new(FileStateStore::new(dir.path())),
        vec![flow],
        settings,
    )
    .unwrap()
    .with_observer(Arc::new(reads.clone()));

    let report = runner.start(new_run()).await.unwrap();
    assert_eq!(report.failures.len(), 1, "{report:?}");
    assert!(
        report.failures[0].error.contains("the accumulator gave up"),
        "{report:?}"
    );
    let failed = report.failures[0].partition.clone();
    // Nothing of the failed partition was shown as an output.
    let output_of_failed = |seen: &[Seen]| {
        seen.iter()
            .filter(|s| matches!(s, Seen::Output(l, _) if l.partition == failed))
            .count()
    };
    assert_eq!(output_of_failed(&reads.seen()), 0);

    let report = runner
        .retry_failed(&report.run, HASH, ENGINE)
        .await
        .unwrap();
    assert!(report.is_complete(), "{report:?}");
    assert_eq!(ids(&sink, "orders"), (0..200).collect::<Vec<_>>());
    // The retry got a reader of its own and was shown every row again.
    let mut retried: Vec<i64> = reads
        .seen()
        .into_iter()
        .filter_map(|s| match s {
            Seen::Read(l, ids, _) if l.partition == failed => Some(ids),
            _ => None,
        })
        .flatten()
        .collect();
    retried.sort_unstable();
    assert_eq!(retried.len(), 100);
    assert!(output_of_failed(&reads.seen()) > 0);
}

#[tokio::test]
async fn every_observer_sees_every_partition_and_finishes_before_the_commit() {
    let dir = tempfile::tempdir().unwrap();
    let log = Log::default();
    let store = SpyStore::new(Arc::new(FileStateStore::new(dir.path())), log.clone());
    let spy = SpySink::new(Arc::new(mem_sink()), log.clone());
    let first = Recorder {
        log: log.clone(),
        ..Default::default()
    };
    let second = Recorder {
        log: log.clone(),
        ..Default::default()
    };
    let flow = identity_flow("orders", orders_source(300, 30), "orders", Arc::new(spy));
    let runner = Runner::new(Arc::new(store), vec![flow], settings())
        .unwrap()
        .with_observer(Arc::new(first.clone()))
        .with_observer(Arc::new(second.clone()));

    let report = runner.start(new_run()).await.unwrap();
    assert!(report.is_complete(), "{report:?}");
    assert_eq!(first.flows.lock().unwrap().len(), 1);
    assert_eq!(second.flows.lock().unwrap().len(), 1);
    let events = log.events();
    assert_eq!(
        log.count(|e| matches!(e, Event::Prepared(_))),
        2,
        "both were told the flow is prepared"
    );
    for lease in events.iter().filter_map(|e| match e {
        Event::Leased(l) => Some(l.clone()),
        _ => None,
    }) {
        let finished: Vec<usize> = events
            .iter()
            .enumerate()
            .filter(|(_, e)| matches!(e, Event::Finished(l, _) if *l == lease))
            .map(|(i, _)| i)
            .collect();
        let observed = log.count(|e| matches!(e, Event::Observed(l, _) if *l == lease));
        assert_eq!(finished.len(), 2, "{lease:?}");
        assert_eq!(observed, 8, "each observer saw the 4 outputs: {lease:?}");
        let commit = position(&events, |e| matches!(e, Event::Commit(l) if *l == lease)).unwrap();
        assert!(finished.iter().all(|&f| f < commit), "{lease:?}");
    }
}
