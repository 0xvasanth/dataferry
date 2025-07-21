//! A hook that sees what each partition writes, for the verifier (A8).
//!
//! The verifier checksums rows at write time and compares the sum with a
//! read-back of the target. It needs every transform output a partition
//! writes, and a point before the commit to record what it saw, so the
//! record holds under every strategy, exactly like the rows the runner
//! records in the state store.

use async_trait::async_trait;
use ferry_core::{Lease, Result, TransformOutput};
use ferry_registry::state::{PartitionRows, RunId};

use crate::flow::Flow;

/// Watches the runner's writes. Set with
/// [`Runner::with_observer`](crate::Runner::with_observer).
#[async_trait]
pub trait WriteObserver: Send + Sync {
    /// Called once per flow each time a run is executed (so again on
    /// resume and retry), after the sink prepared the flow's targets and
    /// before any of its partitions is processed. An error stops the run.
    async fn prepared(&self, run: &RunId, flow: &Flow) -> Result<()>;

    /// A new attempt at the partition `lease` names, in `flow`. Called
    /// after the sink's transaction is open.
    fn begin(&self, run: &RunId, flow: &str, lease: &Lease) -> Box<dyn PartitionObserver>;
}

/// One attempt at one partition. Dropped without
/// [`finish`](PartitionObserver::finish) when the attempt is abandoned.
#[async_trait]
pub trait PartitionObserver: Send {
    /// Each transform output, after it passed
    /// [`TransformOutput::validate`], in order, before its rows are
    /// written. An error fails the partition.
    fn output(&mut self, output: &TransformOutput) -> Result<()>;

    /// After every row was written and `rows` recorded in the state store,
    /// before the commit. A later attempt at the same partition (with a
    /// higher lease epoch) replaces what this one recorded. An error aborts
    /// the transaction and fails the partition, so no partition commits
    /// without its record; `FerryError::StaleLease` gives the partition up
    /// like a lost lease.
    async fn finish(self: Box<Self>, rows: &PartitionRows) -> Result<()>;
}
