//! The `runtime.yaml` keys the runner reads (0003 §1, §4), plus the lease
//! timings, which are not user settings in v0.1.

use std::time::Duration;

/// How the runner spends the process: workers, memory and source load.
///
/// None of these change what gets written, so none are in the config hash
/// (0003 §10.5): a run that ran out of memory can be resumed with a bigger
/// `memory_budget`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Settings {
    /// Partitions processed at once (`workers`). Default: one per core.
    pub workers: usize,
    /// Bytes of row data the runner may hold at once (`memory_budget`),
    /// across every worker. It sizes read-ahead and write batches; see
    /// [`Budget`](crate::Budget). Default 1 GiB.
    pub memory_budget: u64,
    /// `PartitionHint::target_rows` when a flow is planned
    /// (`partition_rows`). Default 100 000.
    pub partition_rows: u64,
    /// Source read streams open at once, across every flow
    /// (`source_max_connections`). `None` is unlimited.
    pub source_max_connections: Option<usize>,
    /// Rows read per second, across every flow (`source_rows_per_sec`).
    /// `None` is unlimited.
    pub source_rows_per_sec: Option<u64>,
    /// How long a lease lasts without a heartbeat.
    pub lease_ttl: Duration,
    /// How often a worker renews its lease. Must be well under `lease_ttl`.
    pub heartbeat: Duration,
    /// How often to look again when a flow's last partitions are leased by
    /// another worker (a crashed run's leases, until they expire).
    pub poll: Duration,
}

pub const DEFAULT_MEMORY_BUDGET: u64 = 1 << 30;
pub const DEFAULT_PARTITION_ROWS: u64 = 100_000;

impl Default for Settings {
    fn default() -> Self {
        Self {
            workers: std::thread::available_parallelism().map_or(1, |n| n.get()),
            memory_budget: DEFAULT_MEMORY_BUDGET,
            partition_rows: DEFAULT_PARTITION_ROWS,
            source_max_connections: None,
            source_rows_per_sec: None,
            lease_ttl: Duration::from_secs(60),
            heartbeat: Duration::from_secs(20),
            poll: Duration::from_secs(1),
        }
    }
}

impl Settings {
    pub(crate) fn check(&self) -> Result<(), String> {
        if self.workers == 0 {
            return Err("workers must be at least 1".into());
        }
        if self.partition_rows == 0 {
            return Err("partition_rows must be at least 1".into());
        }
        if self.source_max_connections == Some(0) {
            return Err("source_max_connections must be at least 1".into());
        }
        if self.source_rows_per_sec == Some(0) {
            return Err("source_rows_per_sec must be at least 1".into());
        }
        if self.heartbeat.is_zero() || self.heartbeat >= self.lease_ttl {
            return Err(format!(
                "the heartbeat ({:?}) must be shorter than the lease ttl ({:?})",
                self.heartbeat, self.lease_ttl
            ));
        }
        Ok(())
    }
}
