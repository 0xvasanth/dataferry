//! The runner (A5): a single-process Tokio runner on the state store's
//! lease interface.
//!
//! Each partition goes through: lease → [`Source::read`] (every input of
//! the flow) → [`TransformPlan`] → [`SinkTxn::write`] per target in the
//! plan's write order → commit, recorded per the flow's idempotency
//! strategy (0001). Under `TxnWithState` the sink's commit writes the
//! done-record through the state store's ledger (0004 §8); under every
//! other strategy the runner marks the partition done after the commit
//! returns, and a crash in between re-runs a partition the strategy makes
//! safe to re-run. The rows a partition read are recorded before the
//! commit, under every strategy, and so is what each [`WriteObserver`] (the
//! verifier's write-time checksums, A8; source-side aggregates for
//! `expect:`, B8) saw the partition read and write.
//!
//! A record the plan rejects (or a source can't read as its entity) fails
//! its partition under `on_error: fail`. Under `on_error: dead_letter` it
//! is written to [`DLQ_TABLE`] in the partition's own transaction, so a
//! re-run replaces its dead letters instead of adding to them, and none of
//! its rows land in any target (A7). `max_errors` is counted over the done
//! partitions in the state store.
//!
//! Once every partition of a flow is done, the runner calls its sink's
//! [`Finalizer`], if it has one, to restore what `prepare` loosened for the
//! load (A6), and reports what couldn't be restored.
//!
//! While a partition is processed its lease is renewed every
//! [`Settings::heartbeat`]. If the store says the lease was superseded, or
//! renewals keep failing until it would have expired, the transaction is
//! aborted and the partition is left to whoever holds it.
//!
//! Reads go through bounded, byte-sized read-ahead channels and writes are
//! coalesced into batches, both sized from `memory_budget` ([`Budget`]).
//! `source_max_connections` and `source_rows_per_sec` throttle reads across
//! every flow of the process.
//!
//! The entry points for `dataferry run`, `dataferry resume <run-id>` and `dataferry
//! retry --failed <run-id>` are [`Runner::start`] (or [`Runner::create`]
//! then [`Runner::execute`]), [`Runner::resume`] and
//! [`Runner::retry_failed`]. The CLI wiring is the CLI's.
//!
//! [`Source::read`]: dataferry_core::Source::read
//! [`TransformPlan`]: dataferry_core::TransformPlan
//! [`SinkTxn::write`]: dataferry_core::SinkTxn::write
//! [`Finalizer`]: dataferry_registry::Finalizer

mod budget;
mod dlq;
mod error;
mod flow;
mod observe;
mod runner;
mod settings;
#[cfg(feature = "testkit")]
pub mod testkit;
mod throttle;
mod work;

pub use budget::Budget;
pub use dlq::{DLQ_TABLE, dlq_table};
pub use error::RunError;
pub use flow::{Flow, Input};
pub use observe::{PartitionObserver, ReadObserver, WriteObserver};
pub use runner::{PartitionFailure, RunReport, Runner};
pub use settings::{DEFAULT_MEMORY_BUDGET, DEFAULT_PARTITION_ROWS, OnError, Settings};
