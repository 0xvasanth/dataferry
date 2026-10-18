//! `source_rows_per_sec` and `source_max_connections`, observed at the
//! source: when each batch was pulled, and how many read streams were open
//! at once.

mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use common::*;
use dataferry_core::{BatchStream, Entity, Partition, PartitionHint, Result, Source, SourceCaps};
use dataferry_runtime::Runner;
use dataferry_state::FileStateStore;
use futures::StreamExt;
use tokio::time::Instant;

/// Records when each batch is pulled from the inner source, and how many
/// streams are open.
struct Observed {
    inner: Arc<dyn Source>,
    /// How long each batch takes to read.
    delay: Duration,
    pulls: Arc<Mutex<Vec<(Instant, usize)>>>,
    open: Arc<Mutex<(usize, usize)>>,
}

impl Observed {
    fn new(inner: Arc<dyn Source>) -> Arc<Self> {
        Self::slow(inner, Duration::ZERO)
    }

    fn slow(inner: Arc<dyn Source>, delay: Duration) -> Arc<Self> {
        Arc::new(Self {
            inner,
            delay,
            pulls: Arc::default(),
            open: Arc::default(),
        })
    }

    fn max_open(&self) -> usize {
        self.open.lock().unwrap().1
    }
}

struct Open(Arc<Mutex<(usize, usize)>>);

impl Drop for Open {
    fn drop(&mut self) {
        self.0.lock().unwrap().0 -= 1;
    }
}

#[async_trait]
impl Source for Observed {
    async fn discover(&self) -> Result<Vec<Entity>> {
        self.inner.discover().await
    }

    async fn plan_partitions(&self, e: &Entity, hint: &PartitionHint) -> Result<Vec<Partition>> {
        self.inner.plan_partitions(e, hint).await
    }

    async fn read(&self, e: &Entity, p: &Partition) -> Result<BatchStream> {
        {
            let mut open = self.open.lock().unwrap();
            open.0 += 1;
            open.1 = open.1.max(open.0);
        }
        let guard = Open(Arc::clone(&self.open));
        let pulls = Arc::clone(&self.pulls);
        let inner = self.inner.read(e, p).await?;
        let delay = self.delay;
        Ok(inner
            .then(move |batch| {
                let _open = &guard;
                let pulls = Arc::clone(&pulls);
                async move {
                    if !delay.is_zero() {
                        tokio::time::sleep(delay).await;
                    }
                    if let Ok(b) = &batch {
                        pulls.lock().unwrap().push((Instant::now(), b.num_rows()));
                    }
                    batch
                }
            })
            .boxed())
    }

    async fn capabilities(&self) -> Result<SourceCaps> {
        self.inner.capabilities().await
    }
}

/// Reads 30 000 rows in 10 partitions on four workers, and returns when
/// each batch was pulled from the source.
async fn read(rows_per_sec: Option<u64>) -> Vec<(Instant, usize)> {
    let dir = tempfile::tempdir().unwrap();
    let source = Observed::new(orders_source(30_000, 100));
    let flow = identity_flow("orders", source.clone(), "orders", Arc::new(mem_sink()));
    let mut settings = settings();
    settings.partition_rows = 3000;
    settings.source_rows_per_sec = rows_per_sec;
    let store = Arc::new(FileStateStore::new(dir.path()));
    let runner = Runner::new(store, vec![flow], settings).unwrap();
    let report = runner.start(new_run()).await.unwrap();
    assert!(report.is_complete(), "{report:?}");
    assert_eq!(report.totals.read, 30_000);
    source.pulls.lock().unwrap().clone()
}

/// Rows per second between the first pull and the last. The first batch
/// opens the window, so it doesn't count.
fn rate(pulls: &[(Instant, usize)]) -> f64 {
    let rows: usize = pulls[1..].iter().map(|(_, n)| n).sum();
    rows as f64 / (pulls.last().unwrap().0 - pulls[0].0).as_secs_f64()
}

// Both tests run on tokio's paused clock, which the throttle and the slow
// source sleep on: a loaded machine can't stretch a second or keep the
// workers' reads from overlapping (the store's own I/O takes no time on it).

#[tokio::test(start_paused = true)]
async fn the_read_rate_stays_within_ten_percent_of_source_rows_per_sec() {
    const RATE: u64 = 10_000;
    let within = |rate: f64| (rate - RATE as f64).abs() <= RATE as f64 * 0.10;

    let pulls = read(Some(RATE)).await;
    let overall = rate(&pulls);
    assert!(within(overall), "{overall:.0} rows/s");
    // And in every whole second of it, not just on average: no burst
    // followed by a stall.
    let first = pulls[0].0;
    let seconds = (pulls.last().unwrap().0 - first).as_secs();
    assert!(seconds >= 2, "{seconds}s");
    for s in 0..seconds {
        let from = first + Duration::from_secs(s);
        let to = from + Duration::from_secs(1);
        let rows: usize = pulls
            .iter()
            .filter(|(at, _)| *at >= from && *at < to)
            .map(|(_, n)| n)
            .sum();
        assert!(within(rows as f64), "second {s}: {rows} rows");
    }

    // The control: unthrottled, the same run reads far faster, so the
    // limit is what held it to the rate.
    let unthrottled = rate(&read(None).await);
    assert!(unthrottled > 3.0 * RATE as f64, "{unthrottled:.0} rows/s");
}

#[tokio::test(start_paused = true)]
async fn no_more_read_streams_are_open_than_source_max_connections() {
    let run = |limit: Option<usize>| async move {
        let dir = tempfile::tempdir().unwrap();
        // 20 partitions of 10 batches, 2ms a batch: reads overlap.
        let source = Observed::slow(orders_source(2000, 10), Duration::from_millis(2));
        let flow = identity_flow("orders", source.clone(), "orders", Arc::new(mem_sink()));
        let mut settings = settings();
        settings.workers = 6;
        settings.source_max_connections = limit;
        let store = Arc::new(FileStateStore::new(dir.path()));
        let runner = Runner::new(store, vec![flow], settings).unwrap();
        assert!(runner.start(new_run()).await.unwrap().is_complete());
        source.max_open()
    };
    assert_eq!(run(Some(2)).await, 2);
    // The control: without the limit, the six workers read at once.
    assert_eq!(run(None).await, 6);
}
