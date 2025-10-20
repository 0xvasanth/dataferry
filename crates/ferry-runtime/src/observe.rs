//! A hook that sees what each partition reads and writes, for the
//! verifier (A8) and `expect:` assertions (B8).
//!
//! The verifier checksums rows at write time and compares the sum with a
//! read-back of the target. It needs every transform output a partition
//! writes, and a point before the commit to record what it saw, so the
//! record holds under every strategy, exactly like the rows the runner
//! records in the state store. Source-side aggregates
//! (`source.sum(total * 100)`) also need the driver rows as the source
//! returned them, which a [`ReadObserver`] is shown.

use arrow_array::RecordBatch;
use async_trait::async_trait;
use ferry_core::{Lease, Result, TransformOutput};
use ferry_registry::state::{PartitionRows, RunId};

use crate::flow::Flow;

/// Watches the runner's reads and writes. Set with
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
    /// What sees this attempt's driver rows as they are read, if anything.
    /// Asked once, before the partition's inputs are opened.
    fn reader(&mut self) -> Option<Box<dyn ReadObserver>> {
        None
    }

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

/// Sees one attempt's driver rows at read time: every batch the driver's
/// source returned, in order, before the transform plan sees it. So a batch
/// is always shown before any [`TransformOutput`] that accounts for its
/// records. Joined inputs of a merge are not shown. Runs on the task that
/// reads the source, not the one that writes.
pub trait ReadObserver: Send {
    /// One driver batch. An error ends the driver's stream with it, which
    /// fails the partition.
    fn batch(&mut self, batch: &RecordBatch) -> Result<()>;
}
