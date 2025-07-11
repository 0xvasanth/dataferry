//! The write observer hook the verifier (A8) checksums through: what it
//! sees, when it is told, and what happens when recording fails.

mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use common::*;
use ferry_core::testkit::MemSink;
use ferry_core::{FerryError, Lease, Result, TransformOutput};
use ferry_registry::state::{PartitionRows, RunId};
use ferry_runtime::{Flow, PartitionObserver, Runner, WriteObserver};
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
