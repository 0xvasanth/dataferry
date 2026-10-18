//! State-capable connectors: one transaction for the sink and the state
//! store (0004 §8).
//!
//! `TxnWithState` is the only exactly-once strategy: a partition's rows and
//! its done-record commit in one transaction. A connector that can host the
//! state store implements [`StateCapable`] and is registered together with a
//! [`StateBackend`] that keeps state in its database, through
//! [`ConnectorRegistry::register_state_capable`](crate::ConnectorRegistry::register_state_capable).
//! The compiler checks there that both sides name the same driver handle
//! types; after that the runner only sees `Arc<dyn Sink>` and
//! `Arc<dyn StateStore>`. No `Any`, no downcasts.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use dataferry_core::{Lease, Partition, PartitionId, Result, WriteSummary};

use crate::connector::{BuildContext, BuildError, Connector, Endpoints, Roles};

/// Identifies one run in the state store.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RunId(pub String);

/// What a run is pinned to when it is created.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewRun {
    /// The config hash (B1c): the parsed model before `${ENV}`
    /// substitution. Stored as an opaque string.
    pub config_hash: String,
    pub engine_version: String,
    /// `(name, type)` of every connection, never its fields (0004 §4).
    pub connections: Vec<(String, String)>,
}

/// A run as the store keeps it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunRecord {
    pub id: RunId,
    pub config_hash: String,
    pub engine_version: String,
    pub connections: Vec<(String, String)>,
}

/// How a run's partitions stand.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PartitionCounts {
    /// Planned and never leased, or leased and expired.
    pub pending: u64,
    /// Held by a live lease.
    pub leased: u64,
    pub done: u64,
    pub failed: u64,
}

/// Row counts the runner reports for one partition, besides what the sink
/// wrote (which the done-record carries as a [`WriteSummary`]).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PartitionRows {
    pub read: u64,
    pub dead_lettered: u64,
}

/// Row counts over a run's done partitions, for the report. A partition
/// that re-ran counts once, with its last attempt's numbers.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RunTotals {
    pub read: u64,
    /// Per target table.
    pub written: BTreeMap<String, u64>,
    pub dead_lettered: u64,
}

/// The state store as the runner sees it: runs, plans, leases, done-records.
///
/// **A4 owns these methods.** They live here because connectors (through
/// [`StateBackend`]) and the runner both name the trait. This is the minimal
/// set the runner needs under 0001 (one commit per partition, epoch-fenced
/// leases), and A4 may change it through review from both tracks.
/// `dataferry-state` implements it for the file backend and each database
/// backend.
#[async_trait]
pub trait StateStore: Send + Sync {
    /// Creates a run pinned to `run`'s config hash and engine version.
    async fn create_run(&self, run: NewRun) -> Result<RunId>;

    /// `None` if no such run exists.
    async fn load_run(&self, run: &RunId) -> Result<Option<RunRecord>>;

    /// Stores the planned partitions of one flow, once per run. Resume reads
    /// them back with [`load_partitions`](StateStore::load_partitions)
    /// instead of re-planning.
    async fn store_partitions(
        &self,
        run: &RunId,
        flow: &str,
        partitions: &[Partition],
    ) -> Result<()>;

    async fn load_partitions(&self, run: &RunId, flow: &str) -> Result<Vec<Partition>>;

    /// Leases a pending partition of `flow` for `ttl`, with an epoch strictly
    /// higher than any earlier lease on it. `None` when nothing is pending.
    async fn lease_next(&self, run: &RunId, flow: &str, ttl: Duration) -> Result<Option<Lease>>;

    /// Extends `lease` by `ttl`. Fails with `FerryError::StaleLease` if a
    /// newer lease exists for the partition.
    async fn renew(&self, run: &RunId, lease: &Lease, ttl: Duration) -> Result<()>;

    /// Records the partition done, for every strategy except `TxnWithState`
    /// (where the sink's commit does it through the [`Ledger`]). The runner
    /// calls it after the sink's commit returns. Fails with `StaleLease`
    /// like `renew`.
    async fn mark_done(&self, run: &RunId, lease: &Lease, summary: &WriteSummary) -> Result<()>;

    /// Records the partition failed, with a message for the report.
    async fn mark_failed(&self, run: &RunId, lease: &Lease, error: &str) -> Result<()>;

    /// Records what the partition read and dead-lettered under `lease`,
    /// replacing an earlier attempt's numbers. The runner calls it before
    /// the sink commits, so it holds under every strategy. Fails with
    /// `StaleLease` like `renew`.
    async fn record_rows(&self, run: &RunId, lease: &Lease, rows: &PartitionRows) -> Result<()>;

    /// Whether a done-record exists for the partition.
    async fn is_done(&self, run: &RunId, partition: &PartitionId) -> Result<bool>;

    async fn counts(&self, run: &RunId) -> Result<PartitionCounts>;

    /// [`counts`](StateStore::counts) over the partitions planned under
    /// `flow` only. The runner uses it to tell a flow with nothing left to
    /// lease from one whose partitions are still leased by another worker.
    async fn flow_counts(&self, run: &RunId, flow: &str) -> Result<PartitionCounts>;

    /// Puts every failed partition of `run` back to pending, for `dataferry
    /// retry --failed`. Done partitions are left alone, and a reset
    /// partition's next lease gets a higher epoch than its failed attempt.
    /// Returns how many partitions were reset.
    async fn reset_failed(&self, run: &RunId) -> Result<u64>;

    /// Row counts over the run's done partitions.
    async fn totals(&self, run: &RunId) -> Result<RunTotals>;
}

/// The one piece of state store work that runs inside a sink's transaction.
///
/// It is opened per state connection, not per run, and `record_done` takes
/// no run id (0004 §8). How a done-record finds its run (partition ids
/// unique across runs, or a ledger bound to a run) is A4's call.
#[async_trait]
pub trait Ledger<Conn: Send + 'static>: Send + Sync {
    /// Fails with `FerryError::StaleLease` if a newer lease exists for the
    /// partition, otherwise writes its done-record. Runs on `conn` inside the
    /// sink's open transaction, so the rows and the record commit together
    /// or not at all.
    async fn record_done(
        &self,
        conn: &mut Conn,
        lease: &Lease,
        summary: &WriteSummary,
    ) -> Result<()>;
}

/// A connector whose sink can carry the state store's done-record.
pub trait StateCapable: Connector {
    /// Driver-level handles the backend works with. They are types from the
    /// driver crate, not from this connector, so a backend can name them
    /// without depending on the connector. Postgres: the pool type A1/A3
    /// settle on, and `tokio_postgres::Client`.
    type Pool: Clone + Send + Sync + 'static;
    type Conn: Send + 'static;

    /// The connection's pool. Connects lazily: no I/O, like `build`.
    fn pool(
        &self,
        ctx: &BuildContext<'_>,
        options: &Self::Options,
    ) -> Result<Self::Pool, BuildError>;

    /// `build` on `pool`, with the sink bound to `ledger`. Only a sink built
    /// here lists `TxnWithState` in `capabilities()`. Under that strategy its
    /// commit calls `ledger.record_done` on the transaction's own connection,
    /// after the last write and before COMMIT.
    fn build_with_ledger(
        &self,
        ctx: &BuildContext<'_>,
        options: Self::Options,
        wants: Roles,
        pool: Self::Pool,
        ledger: Arc<dyn Ledger<Self::Conn>>,
    ) -> Result<Endpoints, BuildError>;
}

/// A state store kept in a connector's database (A4, in `dataferry-state`).
pub trait StateBackend: Send + Sync + 'static {
    type Pool: Clone + Send + Sync + 'static;
    type Conn: Send + 'static;

    /// No I/O. The store creates its tables on first use.
    fn open(&self, pool: Self::Pool) -> (Arc<dyn StateStore>, Arc<dyn Ledger<Self::Conn>>);
}
