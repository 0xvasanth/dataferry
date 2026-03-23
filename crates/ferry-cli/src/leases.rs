//! Releasing a run's leases when Ctrl-C stops it (S3), so `ferry resume`
//! can take its partitions at once instead of waiting for them to expire.
//!
//! The runner holds its leases inside the run future, which Ctrl-C drops.
//! [`Tracked`] wraps the state store the runner is given and remembers
//! every lease it grants until the partition is marked done or failed, or
//! the lease runs out without a renewal. [`Tracked::release`] then renews
//! each lease still held for no time at all: its partition is pending again
//! (a lease is expired once its expiry is reached), with no new epoch and
//! no change to the store's API. `renew` is fenced, so a lease that was
//! superseded, or whose partition committed under `TxnWithState` (where the
//! sink's ledger, not this store, writes the done-record), is left alone.
//!
//! This is best effort:
//!
//! - A lease granted while Ctrl-C arrives, before the runner saw it, isn't
//!   known here and expires on its own, as every lease of a process killed
//!   with SIGKILL does.
//! - Dropping the run aborts its heartbeats, but a renewal already on its
//!   way to the store can land after the release and extend the lease
//!   again; a resume then waits for that one to expire.
//! - Under `TxnWithState` the store never hears that a partition is done,
//!   so a lease stays here until its time runs out. Releasing checks each
//!   partition first and skips the done ones.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use ferry_core::{Lease, Partition, PartitionId, Result, WriteSummary};
use ferry_registry::StateStore;
use ferry_registry::state::{NewRun, PartitionCounts, PartitionRows, RunId, RunRecord, RunTotals};
use futures::StreamExt;

/// How long releasing may take before the process exits anyway.
const RELEASE_TIMEOUT: Duration = Duration::from_secs(5);
/// Leases released at once, so a long list doesn't take the whole pool.
const RELEASE_CONCURRENCY: usize = 8;

/// A state store that remembers the leases it granted.
pub(crate) struct Tracked {
    inner: Arc<dyn StateStore>,
    /// By partition: the run, the lease, and when it expires.
    held: Mutex<HashMap<PartitionId, (RunId, Lease, Instant)>>,
}

impl Tracked {
    pub fn new(inner: Arc<dyn StateStore>) -> Self {
        Self {
            inner,
            held: Mutex::default(),
        }
    }

    fn forget(&self, lease: &Lease) {
        let mut held = self.held.lock().expect("leases lock");
        if held
            .get(&lease.partition)
            .is_some_and(|(_, l, _)| l.epoch == lease.epoch)
        {
            held.remove(&lease.partition);
        }
    }

    /// Expires every lease still held. Returns how many were released.
    pub async fn release(&self) -> usize {
        let now = Instant::now();
        let held: Vec<(RunId, Lease)> = self
            .held
            .lock()
            .expect("leases lock")
            .drain()
            .filter(|(_, (_, _, expires))| *expires > now)
            .map(|(_, (run, lease, _))| (run, lease))
            .collect();
        let releases = futures::stream::iter(held.iter().map(|(run, lease)| async move {
            match self.inner.is_done(run, &lease.partition).await {
                // A renewal of a lease that isn't held fails harmlessly, so
                // only a partition known to be done is skipped.
                Ok(true) => false,
                Ok(false) | Err(_) => self.inner.renew(run, lease, Duration::ZERO).await.is_ok(),
            }
        }))
        .buffer_unordered(RELEASE_CONCURRENCY)
        .collect::<Vec<bool>>();
        tokio::time::timeout(RELEASE_TIMEOUT, releases)
            .await
            .map_or(0, |released| released.into_iter().filter(|ok| *ok).count())
    }
}

#[async_trait]
impl StateStore for Tracked {
    async fn create_run(&self, run: NewRun) -> Result<RunId> {
        self.inner.create_run(run).await
    }

    async fn load_run(&self, run: &RunId) -> Result<Option<RunRecord>> {
        self.inner.load_run(run).await
    }

    async fn store_partitions(
        &self,
        run: &RunId,
        flow: &str,
        partitions: &[Partition],
    ) -> Result<()> {
        self.inner.store_partitions(run, flow, partitions).await
    }

    async fn load_partitions(&self, run: &RunId, flow: &str) -> Result<Vec<Partition>> {
        self.inner.load_partitions(run, flow).await
    }

    async fn lease_next(&self, run: &RunId, flow: &str, ttl: Duration) -> Result<Option<Lease>> {
        let asked = Instant::now();
        let lease = self.inner.lease_next(run, flow, ttl).await?;
        if let Some(lease) = &lease {
            let mut held = self.held.lock().expect("leases lock");
            // Leases that ran out on their own need no release.
            let now = Instant::now();
            held.retain(|_, (_, _, expires)| *expires > now);
            held.insert(
                lease.partition.clone(),
                (run.clone(), lease.clone(), asked + ttl),
            );
        }
        Ok(lease)
    }

    async fn renew(&self, run: &RunId, lease: &Lease, ttl: Duration) -> Result<()> {
        let asked = Instant::now();
        self.inner.renew(run, lease, ttl).await?;
        if let Some((_, l, expires)) = self
            .held
            .lock()
            .expect("leases lock")
            .get_mut(&lease.partition)
            && l.epoch == lease.epoch
        {
            *expires = asked + ttl;
        }
        Ok(())
    }

    async fn mark_done(&self, run: &RunId, lease: &Lease, summary: &WriteSummary) -> Result<()> {
        let done = self.inner.mark_done(run, lease, summary).await;
        if done.is_ok() {
            self.forget(lease);
        }
        done
    }

    async fn mark_failed(&self, run: &RunId, lease: &Lease, error: &str) -> Result<()> {
        let failed = self.inner.mark_failed(run, lease, error).await;
        if failed.is_ok() {
            self.forget(lease);
        }
        failed
    }

    async fn record_rows(&self, run: &RunId, lease: &Lease, rows: &PartitionRows) -> Result<()> {
        self.inner.record_rows(run, lease, rows).await
    }

    async fn is_done(&self, run: &RunId, partition: &PartitionId) -> Result<bool> {
        self.inner.is_done(run, partition).await
    }

    async fn counts(&self, run: &RunId) -> Result<PartitionCounts> {
        self.inner.counts(run).await
    }

    async fn flow_counts(&self, run: &RunId, flow: &str) -> Result<PartitionCounts> {
        self.inner.flow_counts(run, flow).await
    }

    async fn reset_failed(&self, run: &RunId) -> Result<u64> {
        self.inner.reset_failed(run).await
    }

    async fn totals(&self, run: &RunId) -> Result<RunTotals> {
        self.inner.totals(run).await
    }
}
