//! The runner against `MemSource`, `MemSink` and the file state store:
//! the per-partition protocol, failures and retry, resume, heartbeats and
//! lease loss, write order, batching and merges.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use common::*;
use ferry_core::testkit::{MemSink, MemSinkFaults, MemSource};
use ferry_core::{
    FerryError, IdempotencyStrategy, IdentityPlan, Lease, PartitionId, TargetSchema, WriteMode,
};
use ferry_registry::state::{PartitionCounts, RunId};
use ferry_registry::{FinalizeProblem, Finalized, Finalizer, Orphans, StateStore};
use ferry_runtime::{Flow, Input, RunError, Runner};
use ferry_state::{FileStateStore, ResumeError};
use futures::FutureExt;

struct Fixture {
    dir: tempfile::TempDir,
    log: Log,
    store: SpyStore,
    sink: MemSink,
    spy: SpySink,
}

impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let log = Log::default();
        let store = SpyStore::new(Arc::new(FileStateStore::new(dir.path())), log.clone());
        let sink = mem_sink();
        let spy = SpySink::new(Arc::new(sink.clone()), log.clone());
        Self {
            dir,
            log,
            store,
            sink,
            spy,
        }
    }

    fn store(&self) -> Arc<dyn StateStore> {
        Arc::new(self.store.clone())
    }

    /// A store handle as a restarted process gets it: same directory,
    /// nothing cached.
    fn reopened(&self) -> Arc<dyn StateStore> {
        Arc::new(SpyStore::new(
            Arc::new(FileStateStore::new(self.dir.path())),
            self.log.clone(),
        ))
    }

    fn orders_flow(&self, n: i64) -> Flow {
        identity_flow(
            "orders",
            orders_source(n, 25),
            "orders",
            Arc::new(self.spy.clone()),
        )
    }
}

fn ids_of(events: &[Event], f: impl Fn(&Event) -> Option<&PartitionId>) -> Vec<String> {
    events.iter().filter_map(f).map(|p| p.0.clone()).collect()
}

#[tokio::test]
async fn copies_every_row_once_across_flows_that_read_the_same_entity() {
    let fx = Fixture::new();
    let source = orders_source(1000, 30);
    let orders = identity_flow("orders", source.clone(), "orders", Arc::new(fx.spy.clone()));
    let mut copy = identity_flow(
        "orders_copy",
        source,
        "orders_copy",
        Arc::new(fx.spy.clone()),
    );
    copy.depends_on = vec!["orders".into()];
    let runner = Runner::new(fx.store(), vec![copy, orders], settings()).unwrap();

    let report = runner.start(new_run()).await.unwrap();

    assert!(report.is_complete(), "{report:?}");
    assert_eq!(report.counts.done, 20);
    assert_eq!(report.totals.read, 2000);
    assert_eq!(report.totals.written.get("orders"), Some(&1000));
    assert_eq!(report.totals.written.get("orders_copy"), Some(&1000));
    let all: Vec<i64> = (0..1000).collect();
    assert_eq!(ids(&fx.sink, "orders"), all);
    assert_eq!(ids(&fx.sink, "orders_copy"), all);

    // Both flows planned the source's `orders-0..9`; stored per flow, the
    // ids don't clash (the store refuses an id in two flows of a run).
    let stored = fx
        .store
        .load_partitions(&report.run, "orders_copy")
        .await
        .unwrap();
    assert_eq!(stored.len(), 10);
    assert!(
        stored
            .iter()
            .all(|p| p.id.0.starts_with("orders_copy/orders-"))
    );

    // depends_on: the copy's first lease comes after the last of `orders`
    // was done.
    let events = fx.log.events();
    let last_done = events
        .iter()
        .rposition(|e| matches!(e, Event::MarkDone(l) if l.partition.0.starts_with("orders/")))
        .unwrap();
    let first_copy = events
        .iter()
        .position(|e| matches!(e, Event::Leased(l) if l.partition.0.starts_with("orders_copy/")))
        .unwrap();
    assert!(last_done < first_copy);
}

#[tokio::test]
async fn records_rows_before_the_commit_and_marks_done_after_it() {
    let fx = Fixture::new();
    let runner = Runner::new(fx.store(), vec![fx.orders_flow(300)], settings()).unwrap();
    let report = runner.start(new_run()).await.unwrap();
    assert!(report.is_complete());

    let events = fx.log.events();
    for partition in ids_of(&events, |e| match e {
        Event::Leased(l) => Some(&l.partition),
        _ => None,
    }) {
        let at = |f: &dyn Fn(&Event) -> bool| {
            let found: Vec<usize> = events
                .iter()
                .enumerate()
                .filter(|(_, e)| f(e))
                .map(|(i, _)| i)
                .collect();
            assert_eq!(found.len(), 1, "{partition}: {events:#?}");
            found[0]
        };
        let mine = |l: &ferry_core::Lease| l.partition.0 == partition;
        let rows = at(&|e| matches!(e, Event::RecordRows(l, r) if mine(l) && r.read == 100));
        let commit = at(&|e| matches!(e, Event::Commit(l) if mine(l)));
        let done = at(&|e| matches!(e, Event::MarkDone(l) if mine(l)));
        let last_write = events
            .iter()
            .rposition(|e| matches!(e, Event::Write { lease, .. } if mine(lease)))
            .unwrap();
        assert!(
            last_write < rows && rows < commit && commit < done,
            "{partition}"
        );
    }
}

#[tokio::test]
async fn a_failed_partition_stops_the_run_and_retry_failed_finishes_it() {
    let fx = Fixture::new();
    let flow = || fx.orders_flow(1000);
    let mut settings = settings();
    settings.workers = 1;
    // A partition's four source batches are coalesced into one write, so
    // the second write is the second partition's.
    fx.sink.inject(MemSinkFaults {
        fail_write_number: Some(2),
        ..Default::default()
    });
    let runner = Runner::new(fx.store(), vec![flow()], settings.clone()).unwrap();
    let report = runner.start(new_run()).await.unwrap();

    assert_eq!(report.failures.len(), 1, "{report:?}");
    assert_eq!(report.failures[0].partition.0, "orders/orders-1");
    assert!(report.failures[0].error.contains("injected failure"));
    // Fail fast: nothing was leased after the failure.
    assert_eq!(
        report.counts,
        PartitionCounts {
            pending: 8,
            done: 1,
            failed: 1,
            leased: 0
        }
    );
    assert!(fx.log.count(|e| matches!(e, Event::Abort(_))) == 1);

    // `resume` continues the pending partitions but leaves the failed one.
    let runner = Runner::new(fx.reopened(), vec![flow()], settings.clone()).unwrap();
    let report = runner.resume(&report.run, HASH, ENGINE).await.unwrap();
    assert_eq!(report.counts.done, 9);
    assert_eq!(report.counts.failed, 1);
    assert!(report.failures.is_empty());
    assert!(!report.is_complete());

    let report = runner
        .retry_failed(&report.run, HASH, ENGINE)
        .await
        .unwrap();
    assert!(report.is_complete(), "{report:?}");
    assert_eq!(ids(&fx.sink, "orders"), (0..1000).collect::<Vec<_>>());
    assert_eq!(report.totals.read, 1000);
}

#[tokio::test]
async fn a_rejected_record_fails_its_partition() {
    let fx = Fixture::new();
    let mut flow = fx.orders_flow(200);
    flow.plan = Arc::new(RejectingPlan {
        inner: IdentityPlan::new(orders_entity(), "orders"),
        bad: vec![150],
    });
    let runner = Runner::new(fx.store(), vec![flow], settings()).unwrap();
    let report = runner.start(new_run()).await.unwrap();
    assert_eq!(report.counts.failed, 1);
    assert_eq!(report.failures[0].partition.0, "orders/orders-1");
    assert!(
        report.failures[0].error.contains("[Int(150)]") && report.failures[0].error.contains("qty"),
        "{}",
        report.failures[0].error
    );
    // The good partition is untouched by the bad one.
    assert_eq!(ids(&fx.sink, "orders"), (0..100).collect::<Vec<_>>());
}

#[tokio::test]
async fn resume_reads_back_the_stored_partitions_after_a_crash() {
    let fx = Fixture::new();
    let counting = Arc::new(CountingSource::new(orders_source(1000, 25)));
    let flow = || {
        identity_flow(
            "orders",
            counting.clone(),
            "orders",
            Arc::new(fx.spy.clone()),
        )
    };
    // From the 4th on, commits never return: the process "dies" once every
    // worker is stuck in one, holding leases it will never renew or release.
    let workers = settings().workers;
    let commits = Arc::new(AtomicUsize::new(0));
    let stalled = Arc::new(tokio::sync::Notify::new());
    let (seen, all_stuck) = (Arc::clone(&commits), Arc::clone(&stalled));
    fx.spy.on_commit(move |_| {
        let n = seen.fetch_add(1, Ordering::SeqCst) + 1;
        if n == 3 + workers {
            all_stuck.notify_one();
        }
        async move {
            if n >= 4 {
                std::future::pending::<()>().await;
            }
        }
        .boxed()
    });
    let runner = Runner::new(fx.store(), vec![flow()], settings()).unwrap();
    let run = runner.create(new_run()).await.unwrap();
    tokio::select! {
        ended = runner.execute(&run) => panic!("the run should hang on its 4th commit: {ended:?}"),
        () = stalled.notified() => {}
    }
    drop(runner);
    fx.store.crash().await;
    assert_eq!(fx.reopened().counts(&run).await.unwrap().done, 3);

    fx.spy.on_commit(|_| async {}.boxed());
    // A bigger partition size on resume changes nothing: partitions were
    // stored when the run was planned.
    let mut resumed = settings();
    resumed.partition_rows = 7;
    let runner = Runner::new(fx.reopened(), vec![flow()], resumed).unwrap();
    let report = runner.resume(&run, HASH, ENGINE).await.unwrap();

    assert!(report.is_complete(), "{report:?}");
    assert_eq!(report.counts.done, 10);
    assert_eq!(*counting.plans.lock().unwrap(), 1);
    assert_eq!(ids(&fx.sink, "orders"), (0..1000).collect::<Vec<_>>());
    // The partitions the dead process held were leased again once their
    // leases expired, at a higher epoch.
    let leases: Vec<_> = fx
        .log
        .events()
        .into_iter()
        .filter_map(|e| match e {
            Event::Leased(l) => Some(l),
            _ => None,
        })
        .collect();
    let releases: Vec<_> = leases
        .iter()
        .filter(|l| {
            leases
                .iter()
                .any(|o| o.partition == l.partition && o.epoch < l.epoch)
        })
        .collect();
    assert_eq!(releases.len(), 4, "{leases:#?}");
}

#[tokio::test]
async fn resume_refuses_an_unknown_run_or_a_changed_config() {
    let fx = Fixture::new();
    let runner = Runner::new(fx.store(), vec![fx.orders_flow(10)], settings()).unwrap();
    let report = runner.start(new_run()).await.unwrap();

    let missing = runner.resume(&RunId("nope".into()), HASH, ENGINE).await;
    assert_eq!(
        missing,
        Err(RunError::Resume(ResumeError::NotFound("nope".into())))
    );
    let changed = runner
        .retry_failed(&report.run, "sha256:other", ENGINE)
        .await;
    assert!(
        matches!(
            changed,
            Err(RunError::Resume(ResumeError::ConfigChanged { .. }))
        ),
        "{changed:?}"
    );
    let engine = runner.resume(&report.run, HASH, "9.9.9").await;
    assert!(matches!(
        engine,
        Err(RunError::Resume(ResumeError::EngineChanged { .. }))
    ));
}

/// Per lease, when each of its renewals was asked for, counted from when
/// the lease was.
fn renewals(log: &Log) -> Vec<(Lease, Vec<Duration>)> {
    let timeline = log.timeline();
    timeline
        .iter()
        .filter_map(|(leased, e)| match e {
            Event::Leased(lease) => Some((lease.clone(), *leased)),
            _ => None,
        })
        .map(|(lease, leased)| {
            let renewed = timeline
                .iter()
                .filter(|(_, e)| *e == Event::Renewed(lease.clone()))
                .map(|(at, _)| *at - leased)
                .collect();
            (lease, renewed)
        })
        .collect()
}

/// Every heartbeat that falls `within` this long after the lease was asked
/// for.
fn every_heartbeat(within: Duration) -> Vec<Duration> {
    let heartbeat = settings().heartbeat;
    (1..=within.as_nanos() / heartbeat.as_nanos())
        .map(|k| heartbeat * k as u32)
        .collect()
}

// On tokio's paused clock: the heartbeat schedule is exact, whatever else
// runs on the machine (the store's own I/O takes no time on it).
#[tokio::test(start_paused = true)]
async fn heartbeats_keep_a_slow_partition_leased() {
    let fx = Fixture::new();
    // Each write takes longer than the lease, and ends between two
    // heartbeats: only renewals keep it.
    let s = settings();
    let write = s.lease_ttl + s.heartbeat / 2;
    fx.spy.on_write(move |_| tokio::time::sleep(write).boxed());
    let mut flow = fx.orders_flow(200);
    flow.inputs[0].source = orders_source(200, 100);
    let runner = Runner::new(fx.store(), vec![flow], settings()).unwrap();
    let report = runner.start(new_run()).await.unwrap();

    assert!(report.is_complete(), "{report:?}");
    assert_eq!(report.leases_lost, 0);
    let renewed = renewals(&fx.log);
    assert_eq!(renewed.len(), 2, "{renewed:?}");
    for (lease, at) in renewed {
        assert_eq!(at, every_heartbeat(write), "{lease:?}");
    }
}

/// Runs two partitions whose writes outlast the lease, against a store that
/// takes `latency(event, log)` to answer each lease and renewal. Returns
/// how long each lease was held for, and when each of its renewals was
/// asked for, counted from the lease's.
async fn renewals_with_a_store_that_answers_after(
    latency: impl Fn(&Event, &Log) -> Duration + Send + Sync + 'static,
) -> Vec<(Lease, Duration, Vec<Duration>)> {
    let fx = Fixture::new();
    let s = settings();
    let log = fx.log.clone();
    fx.store.reply_after(move |event| latency(event, &log));
    let write = s.lease_ttl + s.heartbeat / 2;
    fx.spy.on_write(move |_| tokio::time::sleep(write).boxed());
    let mut flow = fx.orders_flow(200);
    flow.inputs[0].source = orders_source(200, 100);
    let runner = Runner::new(fx.store(), vec![flow], settings()).unwrap();
    let report = runner.start(new_run()).await.unwrap();

    assert!(report.is_complete(), "{report:?}");
    assert_eq!(report.leases_lost, 0);
    let timeline = fx.log.timeline();
    let renewed = renewals(&fx.log);
    assert_eq!(renewed.len(), 2, "{renewed:?}");
    renewed
        .into_iter()
        .map(|(lease, at)| {
            // Held from the lease's ask until its commit.
            let at_event = |want: &Event| {
                timeline
                    .iter()
                    .find_map(|(t, e)| (e == want).then_some(*t))
                    .unwrap()
            };
            let held =
                at_event(&Event::Commit(lease.clone())) - at_event(&Event::Leased(lease.clone()));
            (lease, held, at)
        })
        .collect()
}

#[tokio::test(start_paused = true)]
async fn renewals_keep_to_the_heartbeat_however_slowly_the_store_answers() {
    // The schedule counts from when the lease was asked for, and doesn't
    // slip by the time each renewal takes: renewing `latency` late every
    // time would lose a heartbeat of margin every few renewals.
    let latency = settings().heartbeat * 3 / 10;
    for (lease, held, at) in renewals_with_a_store_that_answers_after(move |_, _| latency).await {
        assert_eq!(at, every_heartbeat(held), "{lease:?}");
    }
}

#[tokio::test(start_paused = true)]
async fn a_renewal_slower_than_the_heartbeat_is_followed_by_the_next_at_once() {
    // Every lease and renewal takes longer than the heartbeat: the next
    // renewal is asked for the moment the one before comes back (the first
    // the moment the lease does), so two are never further apart than the
    // slow one took.
    let latency = settings().heartbeat * 3 / 2;
    for (lease, held, at) in renewals_with_a_store_that_answers_after(move |_, _| latency).await {
        let expected: Vec<Duration> = (1..)
            .map(|k| latency * k)
            .take_while(|at| *at < held)
            .collect();
        assert!(expected.len() >= 4, "{expected:?}");
        assert_eq!(at, expected, "{lease:?}");
    }
}

#[tokio::test(start_paused = true)]
async fn after_one_slow_renewal_the_heartbeat_keeps_its_schedule() {
    // Each lease's 2nd renewal takes 2.5 heartbeats: the 3rd is asked for
    // the moment it returns, and the rest fall back on the schedule counted
    // from the lease, rather than a new one counted from the late renewal.
    let heartbeat = settings().heartbeat;
    let slow = heartbeat * 5 / 2;
    let renewed = renewals_with_a_store_that_answers_after(move |event, log| match event {
        Event::Renewed(lease) if log.count(|e| *e == Event::Renewed(lease.clone())) == 2 => slow,
        _ => Duration::ZERO,
    })
    .await;
    for (lease, held, at) in renewed {
        let back = heartbeat * 2 + slow;
        let expected: Vec<Duration> = [heartbeat, heartbeat * 2, back]
            .into_iter()
            .chain(every_heartbeat(held).into_iter().filter(|at| *at > back))
            .collect();
        assert_eq!(at, expected, "{lease:?}");
    }
}

#[tokio::test]
async fn a_superseded_lease_aborts_the_transaction() {
    let fx = Fixture::new();
    // Partition 1's first lease is taken over while its write is stuck.
    fx.store.on_renew(|lease| {
        (lease.partition.0 == "orders/orders-1" && lease.epoch.0 == 2).then(|| {
            Err(FerryError::StaleLease {
                partition: lease.partition.0.clone(),
                epoch: 2,
                current: 99,
            })
        })
    });
    fx.spy.on_write(|lease| {
        let stuck = lease.partition.0 == "orders/orders-1" && lease.epoch.0 == 2;
        async move {
            if stuck {
                std::future::pending::<()>().await;
            }
        }
        .boxed()
    });
    let runner = Runner::new(fx.store(), vec![fx.orders_flow(300)], settings()).unwrap();
    let report = runner.start(new_run()).await.unwrap();

    assert_eq!(report.leases_lost, 1);
    let first = lease("orders/orders-1", 2);
    assert!(fx.log.events().contains(&Event::Abort(first.clone())));
    assert!(!fx.log.events().contains(&Event::Commit(first.clone())));
    assert!(!fx.log.events().contains(&Event::MarkFailed(first)));
    // Its real lease expired and the runner took the partition again.
    assert!(report.is_complete(), "{report:?}");
    assert_eq!(ids(&fx.sink, "orders"), (0..300).collect::<Vec<_>>());
}

#[tokio::test(start_paused = true)]
async fn renewals_that_keep_failing_give_the_lease_up_before_it_expires() {
    let fx = Fixture::new();
    let s = settings();
    fx.store.on_renew(|lease| {
        (lease.partition.0 == "orders/orders-0" && lease.epoch.0 == 1).then(|| {
            Err(FerryError::Connector {
                connector: "state:file".into(),
                message: "disk unavailable".into(),
            })
        })
    });
    // The lease comes back late: its expiry still counts from when it was
    // asked for, as the store's does.
    let latency = s.heartbeat * 3 / 10;
    fx.store.reply_after(move |_| latency);
    let aborted_at = Arc::new(std::sync::Mutex::new(None));
    let at = Arc::clone(&aborted_at);
    fx.spy.on_write(move |lease| {
        let stuck = lease.partition.0 == "orders/orders-0" && lease.epoch.0 == 1;
        let at = Arc::clone(&at);
        async move {
            if stuck {
                // Dropped when the runner gives the lease up.
                struct Mark(Arc<std::sync::Mutex<Option<tokio::time::Instant>>>);
                impl Drop for Mark {
                    fn drop(&mut self) {
                        *self.0.lock().unwrap() = Some(tokio::time::Instant::now());
                    }
                }
                let _mark = Mark(at);
                std::future::pending::<()>().await;
            }
        }
        .boxed()
    });
    let mut flow = fx.orders_flow(100);
    flow.inputs[0].source = orders_source(100, 100);
    let runner = Runner::new(fx.store(), vec![flow], settings()).unwrap();
    let report = runner.start(new_run()).await.unwrap();

    assert_eq!(report.leases_lost, 1);
    assert!(report.is_complete(), "{report:?}");
    // Given up at the first failed renewal once the lease's ttl had passed
    // since it was asked for, not at the first failure (one heartbeat in).
    let leased = fx
        .log
        .timeline()
        .into_iter()
        .find_map(|(at, e)| (e == Event::Leased(lease("orders/orders-0", 1))).then_some(at))
        .expect("the stuck lease");
    let gave_up = aborted_at
        .lock()
        .unwrap()
        .expect("the stuck write was dropped")
        - leased;
    assert_eq!(gave_up, s.lease_ttl);
}

#[tokio::test]
async fn writes_parents_before_children_in_batches_sized_by_the_budget() {
    let fx = Fixture::new();
    let flow = |batch_rows, sink: Arc<dyn ferry_core::Sink>| Flow {
        name: "split".into(),
        inputs: vec![Input {
            source: orders_source(4000, batch_rows),
            entity: orders_entity(),
            co_partition: None,
        }],
        plan: Arc::new(SplitPlan::default()),
        sink,
        target: split_target(),
        mode: WriteMode::Insert,
        finalizer: None,
        depends_on: Vec::new(),
    };

    // Small source batches are coalesced: one write per target per
    // partition of 1000 rows.
    let mut big = settings();
    big.partition_rows = 1000;
    let runner = Runner::new(fx.store(), vec![flow(50, Arc::new(fx.spy.clone()))], big).unwrap();
    assert!(runner.start(new_run()).await.unwrap().is_complete());
    let writes = |log: &Log| {
        log.events()
            .into_iter()
            .filter_map(|e| match e {
                Event::Write {
                    lease,
                    target,
                    rows,
                    bytes,
                } => Some((lease, target, rows, bytes)),
                _ => None,
            })
            .collect::<Vec<_>>()
    };
    let w = writes(&fx.log);
    assert_eq!(w.len(), 8, "{w:#?}");
    for pair in w.chunks(2) {
        assert_eq!((pair[0].1.as_str(), pair[0].2), ("orders", 1000));
        assert_eq!((pair[1].1.as_str(), pair[1].2), ("order_items", 2000));
        assert_eq!(pair[0].0, pair[1].0);
    }
    assert_eq!(ids(&fx.sink, "order_items"), (0..8000).collect::<Vec<_>>());

    // A small budget cuts the same rows into many writes, each about the
    // batch size, still parents first within every flush.
    let small = MemSink::new(vec![IdempotencyStrategy::DeletePartitionThenInsert]);
    let log = Log::default();
    let spy = SpySink::new(Arc::new(small.clone()), log.clone());
    let mut tight = settings();
    tight.partition_rows = 4000;
    tight.workers = 1;
    tight.memory_budget = 8 * 64 * 1024;
    let batch_bytes = ferry_runtime::Budget::new(tight.memory_budget, 1).batch_bytes() as usize;
    let dir = tempfile::tempdir().unwrap();
    let runner = Runner::new(
        Arc::new(FileStateStore::new(dir.path())),
        vec![flow(4000, Arc::new(spy))],
        tight,
    )
    .unwrap();
    assert!(runner.start(new_run()).await.unwrap().is_complete());
    let w = writes(&log);
    // 4000 orders (~87 KB) and 8000 items (128 KB) in 64 KiB writes.
    let count = |t: &str| w.iter().filter(|w| w.1 == t).count();
    assert_eq!((count("orders"), count("order_items")), (2, 2), "{w:#?}");
    for (_, target, rows, bytes) in &w {
        assert!(*rows > 0);
        assert!(
            *bytes <= batch_bytes,
            "a {bytes}-byte write to {target} over {batch_bytes}"
        );
    }
    assert_eq!(ids(&small, "orders"), (0..4000).collect::<Vec<_>>());
    assert_eq!(ids(&small, "order_items"), (0..8000).collect::<Vec<_>>());
    // Every items write comes after at least as many order rows as it has
    // parents for: never a child before its parent.
    let (mut parents, mut children) = (0usize, 0usize);
    for (_, target, rows, _) in &w {
        if target == "orders" {
            parents += rows;
        } else {
            children += rows;
            assert!(
                children <= parents * 2,
                "{children} items before {parents} orders"
            );
        }
    }
}

#[tokio::test]
async fn a_merge_reads_each_driver_partition_with_its_co_partitioned_children() {
    let fx = Fixture::new();
    let source = Arc::new(
        MemSource::new()
            .with_entity(orders_entity(), orders(0..500))
            .unwrap()
            .with_entity(items_entity(), items(0..500))
            .unwrap()
            .with_batch_rows(40),
    );
    let plan = Arc::new(MergePlan::default());
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
        plan: plan.clone(),
        sink: Arc::new(fx.spy.clone()),
        target: split_target(),
        mode: WriteMode::Insert,
        finalizer: None,
        depends_on: Vec::new(),
    };
    let runner = Runner::new(fx.store(), vec![flow], settings()).unwrap();
    let report = runner.start(new_run()).await.unwrap();

    assert!(report.is_complete(), "{report:?}");
    assert_eq!(report.counts.done, 5);
    // Only the driver's partitions are stored, and only its rows count as
    // read.
    assert_eq!(report.totals.read, 500);
    assert!(plan.stray_items.lock().unwrap().is_empty());
    assert_eq!(ids(&fx.sink, "orders"), (0..500).collect::<Vec<_>>());
    assert_eq!(ids(&fx.sink, "order_items"), (0..1000).collect::<Vec<_>>());
}

#[tokio::test]
async fn refuses_flows_that_do_not_fit_together() {
    let fx = Fixture::new();
    let invalid = |flows: Vec<Flow>| match Runner::new(fx.store(), flows, settings()) {
        Err(RunError::Invalid(reason)) => reason,
        Err(e) => panic!("{e:?}"),
        Ok(_) => panic!("accepted"),
    };
    let a = || fx.orders_flow(10);
    let named = |name: &str, deps: &[&str]| {
        let mut f = a();
        f.name = name.into();
        f.depends_on = deps.iter().map(|d| d.to_string()).collect();
        f
    };
    assert!(invalid(vec![a(), a()]).contains("same name"));
    assert!(invalid(vec![named("x", &["y"]), named("y", &["x"])]).contains("depends on itself"));
    assert!(invalid(vec![named("x", &["nope"])]).contains("unknown flow `nope`"));
    let mut unplanned = a();
    unplanned.target = TargetSchema::new(vec![table("elsewhere", order_fields(), "id")]).unwrap();
    assert!(invalid(vec![unplanned]).contains("isn't a target"));
    let mut none = a();
    none.inputs.clear();
    none.plan = Arc::new(NoInputs);
    assert!(invalid(vec![none]).contains("reads no entity"));
    let mut two = a();
    two.inputs.push(two.inputs[0].clone());
    assert!(invalid(vec![two]).contains("plan reads"));

    // A merge opens both its streams at once: a limit of one can't hold.
    let mut merge = a();
    merge.inputs.push(Input {
        co_partition: Some("id".into()),
        ..merge.inputs[0].clone()
    });
    merge.plan = Arc::new(MergePlan::default());
    merge.inputs[1].entity = items_entity();
    merge.target = split_target();
    let mut one = settings();
    one.source_max_connections = Some(1);
    assert!(matches!(
        Runner::new(fx.store(), vec![merge], one),
        Err(RunError::Invalid(r)) if r.contains("more than source_max_connections (1)")
    ));

    let mut bad = settings();
    bad.heartbeat = bad.lease_ttl;
    assert!(matches!(
        Runner::new(fx.store(), vec![a()], bad),
        Err(RunError::Invalid(r)) if r.contains("heartbeat")
    ));
}

#[tokio::test]
async fn refuses_a_sink_with_no_safe_strategy_for_the_mode() {
    let fx = Fixture::new();
    // UpsertByKey can't make an insert-mode re-run safe (0001).
    let sink = MemSink::new(vec![IdempotencyStrategy::UpsertByKey]);
    let flow = identity_flow("orders", orders_source(10, 10), "orders", Arc::new(sink));
    let runner = Runner::new(fx.store(), vec![flow], settings()).unwrap();
    let err = runner.start(new_run()).await.unwrap_err();
    assert!(
        matches!(err, RunError::NoStrategy { ref flow, .. } if flow == "orders"),
        "{err}"
    );
}

#[tokio::test]
async fn a_flow_after_a_failed_one_is_blocked_until_the_failure_is_retried() {
    let fx = Fixture::new();
    fx.sink.inject(MemSinkFaults {
        fail_next_commits: 1,
        ..Default::default()
    });
    let flows = || {
        let mut first = fx.orders_flow(100);
        first.inputs[0].source = orders_source(100, 100);
        let spy = Arc::new(fx.spy.clone());
        let mut after = identity_flow("after", orders_source(10, 10), "after", spy.clone());
        after.depends_on = vec!["orders".into()];
        let other = identity_flow("other", orders_source(10, 10), "other", spy);
        vec![first, after, other]
    };
    let mut one = settings();
    one.workers = 1;
    let runner = Runner::new(fx.store(), flows(), one).unwrap();
    let report = runner.start(new_run()).await.unwrap();
    assert_eq!(report.counts.failed, 1);

    // Resumed, `orders` is finished but failed: `after` can't start, and
    // the independent flow still runs.
    let runner = Runner::new(fx.reopened(), flows(), settings()).unwrap();
    let report = runner.resume(&report.run, HASH, ENGINE).await.unwrap();
    assert_eq!(report.blocked, vec!["after".to_owned()]);
    assert_eq!(ids(&fx.sink, "other"), (0..10).collect::<Vec<_>>());
    let after_leases = || {
        fx.log
            .count(|e| matches!(e, Event::Leased(l) if l.partition.0.starts_with("after/")))
    };
    assert_eq!(after_leases(), 0);

    // Retried, the dependency finishes and the blocked flow runs.
    let report = runner
        .retry_failed(&report.run, HASH, ENGINE)
        .await
        .unwrap();
    assert!(report.is_complete(), "{report:?}");
    assert!(report.blocked.is_empty());
    assert_eq!(after_leases(), 1);
    assert_eq!(ids(&fx.sink, "after"), (0..10).collect::<Vec<_>>());
}

/// A plan with no inputs at all.
struct NoInputs;

impl ferry_core::TransformPlan for NoInputs {
    fn inputs(&self) -> &[String] {
        &[]
    }

    fn write_order(&self) -> &[String] {
        &[]
    }

    fn execute(
        &self,
        _: ferry_core::PartitionInputs,
    ) -> ferry_core::Result<ferry_core::TransformStream> {
        unreachable!("never run")
    }
}

/// Panics part-way through reading `orders-1`.
struct Panicking(Arc<dyn ferry_core::Source>);

#[async_trait::async_trait]
impl ferry_core::Source for Panicking {
    async fn discover(&self) -> ferry_core::Result<Vec<ferry_core::Entity>> {
        self.0.discover().await
    }
    async fn plan_partitions(
        &self,
        e: &ferry_core::Entity,
        hint: &ferry_core::PartitionHint,
    ) -> ferry_core::Result<Vec<ferry_core::Partition>> {
        self.0.plan_partitions(e, hint).await
    }
    async fn read(
        &self,
        e: &ferry_core::Entity,
        p: &ferry_core::Partition,
    ) -> ferry_core::Result<ferry_core::BatchStream> {
        use futures::StreamExt;
        let poisoned = p.id.0 == "orders-1";
        let mut n = 0;
        Ok(self
            .0
            .read(e, p)
            .await?
            .map(move |b| {
                n += 1;
                assert!(!(poisoned && n == 2), "the source driver crashed");
                b
            })
            .boxed())
    }
    async fn capabilities(&self) -> ferry_core::Result<ferry_core::SourceCaps> {
        self.0.capabilities().await
    }
}

#[tokio::test]
async fn a_reader_that_panics_fails_its_partition_instead_of_committing_part_of_it() {
    let fx = Fixture::new();
    let mut flow = fx.orders_flow(200);
    flow.inputs[0].source = Arc::new(Panicking(orders_source(200, 25)));
    let mut one = settings();
    one.workers = 1;
    let runner = Runner::new(fx.store(), vec![flow], one).unwrap();
    let report = runner.start(new_run()).await.unwrap();

    assert_eq!(report.failures.len(), 1, "{report:?}");
    assert_eq!(report.failures[0].partition.0, "orders/orders-1");
    assert!(
        report.failures[0].error.contains("panic"),
        "{}",
        report.failures[0].error
    );
    assert!(
        !fx.log
            .events()
            .iter()
            .any(|e| matches!(e, Event::Commit(l) if l.partition.0 == "orders/orders-1"))
    );
    // Only the partition read in full is in the target.
    assert_eq!(ids(&fx.sink, "orders"), (0..100).collect::<Vec<_>>());
}

#[tokio::test]
async fn a_failure_blocks_every_flow_down_the_dependency_chain() {
    let fx = Fixture::new();
    fx.sink.inject(MemSinkFaults {
        fail_next_commits: 1,
        ..Default::default()
    });
    // Listed against dependency order, so one pass over them isn't enough.
    let flows = || {
        let spy = Arc::new(fx.spy.clone());
        let mut c = identity_flow("c", orders_source(10, 10), "c", spy.clone());
        c.depends_on = vec!["b".into()];
        let mut b = identity_flow("b", orders_source(10, 10), "b", spy.clone());
        b.depends_on = vec!["a".into()];
        let a = identity_flow("a", orders_source(10, 10), "a", spy);
        vec![c, b, a]
    };
    let runner = Runner::new(fx.store(), flows(), settings()).unwrap();
    let report = runner.start(new_run()).await.unwrap();
    assert_eq!(report.counts.failed, 1);

    let runner = Runner::new(fx.reopened(), flows(), settings()).unwrap();
    let report = runner.resume(&report.run, HASH, ENGINE).await.unwrap();
    assert_eq!(report.blocked, vec!["c".to_owned(), "b".to_owned()]);
    assert_eq!(report.counts.pending, 2);

    let report = runner
        .retry_failed(&report.run, HASH, ENGINE)
        .await
        .unwrap();
    assert!(report.is_complete(), "{report:?}");
    assert_eq!(ids(&fx.sink, "c"), (0..10).collect::<Vec<_>>());
}

/// Records which tables it was asked to finalize, and answers with
/// `problems`, or fails if `fail` is set.
#[derive(Default)]
struct SpyFinalizer {
    calls: std::sync::Mutex<Vec<Vec<String>>>,
    problems: Vec<FinalizeProblem>,
    fail: bool,
}

#[async_trait::async_trait]
impl Finalizer for SpyFinalizer {
    async fn finalize(&self, target: &TargetSchema) -> ferry_core::Result<Finalized> {
        self.calls
            .lock()
            .unwrap()
            .push(target.tables().iter().map(|t| t.name.clone()).collect());
        if self.fail {
            return Err(FerryError::Connector {
                connector: "spy".into(),
                message: "the sink is gone".into(),
            });
        }
        Ok(Finalized {
            restored: vec!["orders.orders_name_idx".into()],
            problems: self.problems.clone(),
        })
    }
}

#[tokio::test]
async fn a_flow_is_finalized_only_once_every_partition_is_done_and_again_on_resume() {
    let fx = Fixture::new();
    let orphan = FinalizeProblem {
        table: "orders".into(),
        object: "orders_customer_fkey".into(),
        message: "1 rows reference no parent row".into(),
        orphans: Some(Orphans {
            count: 1,
            keys: vec!["id=7".into()],
        }),
    };
    let finalizer = Arc::new(SpyFinalizer {
        problems: vec![orphan.clone()],
        ..Default::default()
    });
    let flow = || {
        let mut flow = fx.orders_flow(1000);
        flow.finalizer = Some(finalizer.clone());
        flow
    };
    let mut settings = settings();
    settings.workers = 1;
    fx.sink.inject(MemSinkFaults {
        fail_write_number: Some(2),
        ..Default::default()
    });

    // A failed partition: indexes and keys stay off, the flow isn't done.
    let runner = Runner::new(fx.store(), vec![flow()], settings.clone()).unwrap();
    let report = runner.start(new_run()).await.unwrap();
    assert_eq!(report.counts.failed, 1);
    assert!(report.finalized.is_empty());
    let report = runner.resume(&report.run, HASH, ENGINE).await.unwrap();
    assert_eq!(report.counts.failed, 1);
    assert!(report.finalized.is_empty());
    assert!(finalizer.calls.lock().unwrap().is_empty());

    // Once every partition is done, the flow's targets are finalized and
    // what finalize couldn't restore is in the report.
    let report = runner
        .retry_failed(&report.run, HASH, ENGINE)
        .await
        .unwrap();
    assert!(report.is_complete());
    assert_eq!(*finalizer.calls.lock().unwrap(), vec![vec!["orders"]]);
    assert_eq!(
        report.finalized["orders"].restored,
        vec!["orders.orders_name_idx"]
    );
    assert_eq!(
        report.finalize_problems().collect::<Vec<_>>(),
        vec![("orders", &orphan)]
    );

    // A process that died between finalize's start and its end left the
    // flow done but not finalized: every later execution finalizes again.
    let runner = Runner::new(fx.reopened(), vec![flow()], settings).unwrap();
    let report = runner.resume(&report.run, HASH, ENGINE).await.unwrap();
    assert!(report.finalized.contains_key("orders"));
    assert_eq!(finalizer.calls.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn a_finalizer_that_fails_fails_the_run_after_every_partition_committed() {
    let fx = Fixture::new();
    let mut flow = fx.orders_flow(200);
    flow.finalizer = Some(Arc::new(SpyFinalizer {
        fail: true,
        ..Default::default()
    }));
    let runner = Runner::new(fx.store(), vec![flow], settings()).unwrap();
    let err = runner.start(new_run()).await.unwrap_err();
    assert!(
        matches!(&err, RunError::Flow { flow, .. } if flow == "orders")
            && err.to_string().contains("the sink is gone"),
        "{err}"
    );
    assert_eq!(ids(&fx.sink, "orders"), (0..200).collect::<Vec<_>>());
}
