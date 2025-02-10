//! The contract every sink connector implements.
//!
//! # Commit granularity
//!
//! In v0.1 the unit of commit is one partition: the runner opens a
//! [`SinkTxn`] per leased partition, writes every target batch for it, and
//! commits once. Partitions are sized so that this transaction stays short.
//!
//! # Idempotency
//!
//! A partition can be processed more than once (crash before the state store
//! records it as done, or an expired lease). Every sink therefore declares
//! the [`IdempotencyStrategy`]s it supports, and the planner refuses a flow
//! whose write mode has no compatible strategy on its sink.

use std::collections::BTreeMap;

use arrow_array::RecordBatch;
use async_trait::async_trait;

use crate::error::Result;
use crate::lease::Lease;
use crate::partition::Partition;
use crate::target::TargetSchema;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteMode {
    /// Target rows are created by this run.
    Insert,
    /// Target rows may already exist and are replaced by key.
    Upsert,
}

/// How re-processing a partition is made safe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdempotencyStrategy {
    /// Data and the partition's "done" record commit in one transaction, so
    /// a partition is either fully applied and recorded, or neither. The sink
    /// rejects a commit whose lease epoch is no longer current.
    TxnWithState,
    /// Rows are written as upserts on the target key; writing the same rows
    /// again changes nothing.
    UpsertByKey,
    /// The partition's previous rows are deleted before inserting, in the
    /// same transaction.
    DeletePartitionThenInsert,
    /// Rows load into a staging table that replaces the target on success.
    StagingSwap,
}

impl IdempotencyStrategy {
    /// Strategies in the order the planner prefers them.
    pub const PREFERENCE: [IdempotencyStrategy; 4] = [
        IdempotencyStrategy::TxnWithState,
        IdempotencyStrategy::DeletePartitionThenInsert,
        IdempotencyStrategy::UpsertByKey,
        IdempotencyStrategy::StagingSwap,
    ];

    pub fn is_compatible_with(self, mode: WriteMode) -> bool {
        match self {
            IdempotencyStrategy::TxnWithState => true,
            IdempotencyStrategy::DeletePartitionThenInsert => true,
            // An insert-mode re-run has no key conflict to collapse onto.
            IdempotencyStrategy::UpsertByKey => mode == WriteMode::Upsert,
            // Replacing the whole target would drop rows an upsert keeps.
            IdempotencyStrategy::StagingSwap => mode == WriteMode::Insert,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SinkCaps {
    pub strategies: Vec<IdempotencyStrategy>,
    /// Can bulk-load faster than row inserts (e.g. Postgres `COPY`).
    pub bulk_copy: bool,
}

impl SinkCaps {
    /// The strategy the planner will use for `mode`, or `None` if re-running
    /// a partition on this sink could duplicate or lose rows.
    pub fn strategy_for(&self, mode: WriteMode) -> Option<IdempotencyStrategy> {
        IdempotencyStrategy::PREFERENCE
            .into_iter()
            .find(|s| self.strategies.contains(s) && s.is_compatible_with(mode))
    }
}

/// Rows durably written per target table by one committed transaction.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WriteSummary {
    pub rows_written: BTreeMap<String, u64>,
}

#[async_trait]
pub trait Sink: Send + Sync {
    /// Creates or validates the target tables (DDL rendered by the sink).
    async fn prepare(&self, target: &TargetSchema, mode: WriteMode) -> Result<()>;

    /// Opens the write scope for one leased partition.
    async fn begin(
        &self,
        partition: &Partition,
        lease: &Lease,
        strategy: IdempotencyStrategy,
    ) -> Result<Box<dyn SinkTxn>>;

    async fn capabilities(&self) -> Result<SinkCaps>;
}

/// The writes of one partition, across every target table it produces.
/// Either all of them become durable on [`commit`](SinkTxn::commit) or, on
/// [`abort`](SinkTxn::abort) or drop, none of them do.
#[async_trait]
pub trait SinkTxn: Send {
    async fn write(&mut self, target: &str, batch: RecordBatch) -> Result<()>;

    /// Fails with [`FerryError::StaleLease`](crate::FerryError::StaleLease)
    /// if the sink can see that a newer lease exists for this partition.
    async fn commit(self: Box<Self>) -> Result<WriteSummary>;

    async fn abort(self: Box<Self>) -> Result<()>;
}
