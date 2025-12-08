//! `backend: file`: the shared suite, plus what only a file store has to
//! get right: state surviving a restart, and files it didn't write.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use ferry_core::{Lease, Partition, PartitionId, PartitionScope, Result, WriteSummary};
use ferry_registry::StateStore;
use ferry_registry::state::{NewRun, PartitionCounts, PartitionRows, RunId, RunRecord, RunTotals};
use ferry_state::FileStateStore;
use ferry_state::testkit::check_state_store;

fn new_run() -> NewRun {
    NewRun {
        config_hash: "sha256:file".into(),
        engine_version: "0.0.1".into(),
        connections: vec![("warehouse".into(), "postgres".into())],
    }
}

fn partitions(n: usize) -> Vec<Partition> {
    (0..n)
        .map(|i| Partition {
            id: PartitionId(format!("orders-{i}")),
            entity: "orders".into(),
            scope: PartitionScope::Whole,
        })
        .collect()
}

#[tokio::test]
async fn the_file_store_passes_the_state_store_suite() {
    let dir = tempfile::tempdir().unwrap();
    // One handle: a file store's cache is the truth while its process runs,
    // so two live handles on one directory are not supported.
    let store: Arc<dyn StateStore> = Arc::new(FileStateStore::new(dir.path()));
    let violations = check_state_store(&|| Arc::clone(&store)).await;
    assert!(violations.is_empty(), "{violations:#?}");
}

#[tokio::test]
async fn state_survives_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let (run, done, leased) = {
        let store = FileStateStore::new(dir.path().join("state"));
        let run = store.create_run(new_run()).await.unwrap();
        store
            .store_partitions(&run, "orders", &partitions(3))
            .await
            .unwrap();
        let done = store
            .lease_next(&run, "orders", Duration::from_secs(600))
            .await
            .unwrap()
            .unwrap();
        store
            .record_rows(
                &run,
                &done,
                &PartitionRows {
                    read: 4,
                    dead_lettered: 1,
                },
            )
            .await
            .unwrap();
        let summary = WriteSummary {
            rows_written: [("orders".to_owned(), 3)].into(),
        };
        store.mark_done(&run, &done, &summary).await.unwrap();
        let leased = store
            .lease_next(&run, "orders", Duration::from_secs(600))
            .await
            .unwrap()
            .unwrap();
        (run, done, leased)
    };

    // The process restarted: a new store on the same directory.
    let store = FileStateStore::new(dir.path().join("state"));
    let record = store.load_run(&run).await.unwrap().unwrap();
    assert_eq!(record.config_hash, "sha256:file");
    assert_eq!(
        store.load_partitions(&run, "orders").await.unwrap(),
        partitions(3)
    );
    assert!(store.is_done(&run, &done.partition).await.unwrap());
    assert_eq!(
        store.counts(&run).await.unwrap(),
        PartitionCounts {
            pending: 1,
            leased: 1,
            done: 1,
            failed: 0,
        }
    );
    assert_eq!(store.totals(&run).await.unwrap().read, 4);

    // The live lease from before the restart still fences: its holder can
    // renew it, and the next lease skips its partition and gets a higher
    // epoch than any issued before.
    store
        .renew(&run, &leased, Duration::from_secs(600))
        .await
        .unwrap();
    let next = store
        .lease_next(&run, "orders", Duration::from_secs(600))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(next.partition, PartitionId("orders-2".into()));
    assert!(next.epoch > leased.epoch);
}

#[tokio::test]
async fn a_corrupt_or_foreign_run_file_is_an_error_not_an_empty_run() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileStateStore::new(dir.path());
    let run = store.create_run(new_run()).await.unwrap();
    let path = dir.path().join(format!("{}.json", run.0));
    assert!(path.exists(), "one file per run, named by its id");
    std::fs::write(&path, "{ not json").unwrap();

    let restarted = FileStateStore::new(dir.path());
    let err = restarted.load_run(&run).await.unwrap_err().to_string();
    assert!(err.contains("is not a run file"), "{err}");

    // A file renamed to another run's id is refused rather than adopted.
    let other = store.create_run(new_run()).await.unwrap();
    std::fs::copy(dir.path().join(format!("{}.json", other.0)), &path).unwrap();
    let err = FileStateStore::new(dir.path())
        .load_run(&run)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains(&other.0), "{err}");
}

#[tokio::test]
async fn a_failed_write_leaves_the_last_good_state() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileStateStore::new(dir.path());
    let run = store.create_run(new_run()).await.unwrap();
    store
        .store_partitions(&run, "orders", &partitions(1))
        .await
        .unwrap();

    // The temp file the store writes through is a directory now, so the
    // next write fails before it can replace the run file.
    let tmp = dir.path().join(format!("{}.json.tmp", run.0));
    std::fs::create_dir(&tmp).unwrap();
    assert!(
        store
            .lease_next(&run, "orders", Duration::from_secs(600))
            .await
            .is_err()
    );
    // Neither the cache nor the file took the lease.
    assert_eq!(store.counts(&run).await.unwrap().pending, 1);
    let restarted = FileStateStore::new(dir.path());
    assert_eq!(restarted.counts(&run).await.unwrap().pending, 1);
}

/// How long a write the tests slow down takes.
const SLOW_WRITE: Duration = Duration::from_millis(600);

#[tokio::test(flavor = "current_thread")]
async fn a_slow_write_does_not_stall_the_async_thread() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileStateStore::new(dir.path());
    let run = store.create_run(new_run()).await.unwrap();
    store
        .store_partitions(&run, "orders", &partitions(1))
        .await
        .unwrap();
    let runtime_thread = std::thread::current().id();
    let (tx, rx) = std::sync::mpsc::channel();
    store.before_rename(move || {
        tx.send(std::thread::current().id()).unwrap();
        std::thread::sleep(SLOW_WRITE);
    });

    // A timer on the same (only) thread, like a heartbeat, scheduled while
    // the write is in flight.
    let started = tokio::time::Instant::now();
    let timer = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        started.elapsed()
    });
    let lease = store
        .lease_next(&run, "orders", Duration::from_secs(600))
        .await
        .unwrap();
    let wrote = started.elapsed();

    assert!(lease.is_some());
    assert!(
        wrote >= SLOW_WRITE,
        "the hook ran inside the write: {wrote:?}"
    );
    assert!(timer.is_finished(), "the timer waited for the write");
    let fired = timer.await.unwrap();
    assert!(
        fired < SLOW_WRITE / 3,
        "the timer fired {fired:?} in, while the write took {wrote:?}"
    );
    assert_ne!(
        rx.recv().unwrap(),
        runtime_thread,
        "the write ran on the runtime's thread"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn a_slow_write_does_not_stall_other_calls_waiting_on_the_store() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(FileStateStore::new(dir.path()));
    let run = store.create_run(new_run()).await.unwrap();
    store
        .store_partitions(&run, "orders", &partitions(2))
        .await
        .unwrap();
    let first = store
        .lease_next(&run, "orders", Duration::from_secs(600))
        .await
        .unwrap()
        .unwrap();
    store.before_rename(|| std::thread::sleep(SLOW_WRITE));

    // A renewal queues behind a slow lease: the timer still fires while
    // both are in flight, and both writes land.
    let started = tokio::time::Instant::now();
    let timer = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        started.elapsed()
    });
    let (leased, renewed) = tokio::join!(
        store.lease_next(&run, "orders", Duration::from_secs(600)),
        store.renew(&run, &first, Duration::from_secs(600)),
    );
    let fired = timer.await.unwrap();

    assert!(leased.unwrap().is_some());
    renewed.unwrap();
    assert!(
        started.elapsed() >= SLOW_WRITE * 2,
        "the writes ran one at a time"
    );
    assert!(fired < SLOW_WRITE / 3, "the timer fired {fired:?} in");
    assert_eq!(store.counts(&run).await.unwrap().leased, 2);
}

#[tokio::test]
async fn a_call_dropped_mid_write_still_finishes_it() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileStateStore::new(dir.path());
    let run = store.create_run(new_run()).await.unwrap();
    store
        .store_partitions(&run, "orders", &partitions(1))
        .await
        .unwrap();
    store.before_rename(|| std::thread::sleep(SLOW_WRITE));

    // The caller gives up on the lease while its write is in flight.
    let gave_up = tokio::time::timeout(
        Duration::from_millis(50),
        store.lease_next(&run, "orders", Duration::from_secs(600)),
    )
    .await;
    assert!(gave_up.is_err());

    // `settled` waits for the write nobody waits for any more: the file
    // has the lease once it returns.
    let started = std::time::Instant::now();
    store.settled().await;
    assert!(
        started.elapsed() >= SLOW_WRITE / 2,
        "settled before the write"
    );
    let restarted = FileStateStore::new(dir.path());
    assert_eq!(restarted.counts(&run).await.unwrap().leased, 1);
    // And the cache took it with the file.
    store.before_rename(|| {});
    assert_eq!(store.counts(&run).await.unwrap().leased, 1);
}

#[tokio::test]
async fn a_dropped_call_and_the_next_one_see_the_same_state() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileStateStore::new(dir.path());
    let run = store.create_run(new_run()).await.unwrap();
    store
        .store_partitions(&run, "orders", &partitions(1))
        .await
        .unwrap();
    store.before_rename(|| std::thread::sleep(SLOW_WRITE));
    let gave_up = tokio::time::timeout(
        Duration::from_millis(50),
        store.lease_next(&run, "orders", Duration::from_secs(600)),
    )
    .await;
    assert!(gave_up.is_err());

    // The next call waits for the dropped one rather than seeing the state
    // from before it: the partition is leased, so there is none to lease.
    store.before_rename(|| {});
    assert_eq!(
        store
            .lease_next(&run, "orders", Duration::from_secs(600))
            .await
            .unwrap(),
        None
    );
}

/// A store that skips fencing: `mark_done` accepts any lease the partition
/// ever had. The suite has to catch it.
struct Unfenced(FileStateStore);

#[async_trait]
impl StateStore for Unfenced {
    async fn create_run(&self, run: NewRun) -> Result<RunId> {
        self.0.create_run(run).await
    }
    async fn load_run(&self, run: &RunId) -> Result<Option<RunRecord>> {
        self.0.load_run(run).await
    }
    async fn store_partitions(&self, run: &RunId, flow: &str, p: &[Partition]) -> Result<()> {
        self.0.store_partitions(run, flow, p).await
    }
    async fn load_partitions(&self, run: &RunId, flow: &str) -> Result<Vec<Partition>> {
        self.0.load_partitions(run, flow).await
    }
    async fn lease_next(&self, run: &RunId, flow: &str, ttl: Duration) -> Result<Option<Lease>> {
        self.0.lease_next(run, flow, ttl).await
    }
    async fn renew(&self, run: &RunId, lease: &Lease, ttl: Duration) -> Result<()> {
        self.0.renew(run, lease, ttl).await
    }
    async fn mark_done(&self, run: &RunId, lease: &Lease, summary: &WriteSummary) -> Result<()> {
        match self.0.mark_done(run, lease, summary).await {
            Err(ferry_core::FerryError::StaleLease { current, .. }) => {
                let current = Lease {
                    partition: lease.partition.clone(),
                    epoch: ferry_core::Epoch(current),
                };
                self.0.mark_done(run, &current, summary).await
            }
            other => other,
        }
    }
    async fn mark_failed(&self, run: &RunId, lease: &Lease, error: &str) -> Result<()> {
        self.0.mark_failed(run, lease, error).await
    }
    async fn record_rows(&self, run: &RunId, lease: &Lease, rows: &PartitionRows) -> Result<()> {
        self.0.record_rows(run, lease, rows).await
    }
    async fn is_done(&self, run: &RunId, partition: &PartitionId) -> Result<bool> {
        self.0.is_done(run, partition).await
    }
    async fn counts(&self, run: &RunId) -> Result<PartitionCounts> {
        self.0.counts(run).await
    }
    async fn flow_counts(&self, run: &RunId, flow: &str) -> Result<PartitionCounts> {
        self.0.flow_counts(run, flow).await
    }
    async fn reset_failed(&self, run: &RunId) -> Result<u64> {
        self.0.reset_failed(run).await
    }
    async fn totals(&self, run: &RunId) -> Result<RunTotals> {
        self.0.totals(run).await
    }
}

#[tokio::test]
async fn the_suite_catches_a_store_without_fencing() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn StateStore> = Arc::new(Unfenced(FileStateStore::new(dir.path())));
    let violations = check_state_store(&|| Arc::clone(&store)).await;
    assert!(
        violations
            .iter()
            .any(|v| v.contains("mark_done under a superseded lease succeeded")),
        "{violations:#?}"
    );
    assert!(
        violations
            .iter()
            .any(|v| v.contains("worker A's superseded commit marked the partition done")),
        "{violations:#?}"
    );
}
