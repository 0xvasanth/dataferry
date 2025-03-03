use std::collections::BTreeMap;

use arrow_array::RecordBatch;
use async_trait::async_trait;

use crate::error::{FerryError, Result};
use crate::lease::{Epoch, Lease};
use crate::partition::Partition;
use crate::sink::{IdempotencyStrategy, Sink, WriteMode};
use crate::target::TargetSchema;

/// Lets the suite observe what a sink made durable. Implemented by each
/// connector's tests (e.g. by querying the target database directly).
#[async_trait]
pub trait SinkProbe: Send + Sync {
    async fn row_count(&self, target: &str) -> Result<u64>;
}

/// One partition's worth of writes to run through the suite. Use at least two
/// targets so the suite can check they commit and abort together.
pub struct SinkFixture {
    pub schema: TargetSchema,
    pub mode: WriteMode,
    pub partition: Partition,
    pub batches: Vec<(String, RecordBatch)>,
}

/// Checks the guarantees every [`Sink`] makes, starting from empty targets:
///
/// - the sink supports a safe strategy for the fixture's write mode;
/// - aborting leaves no rows in any target;
/// - committing makes every row durable and the summary reports them;
/// - re-running the committed partition under a newer lease leaves the same
///   rows (no duplicates);
/// - with [`IdempotencyStrategy::TxnWithState`], committing under a lease
///   older than the last commit fails with [`FerryError::StaleLease`] and
///   changes nothing.
pub async fn check_sink(
    sink: &dyn Sink,
    probe: &dyn SinkProbe,
    fixture: &SinkFixture,
) -> Vec<String> {
    match run(sink, probe, fixture).await {
        Ok(violations) => violations,
        Err(e) => vec![format!("sink operation failed: {e}")],
    }
}

async fn run(sink: &dyn Sink, probe: &dyn SinkProbe, f: &SinkFixture) -> Result<Vec<String>> {
    let mut violations = Vec::new();
    let caps = sink.capabilities().await?;
    let Some(strategy) = caps.strategy_for(f.mode) else {
        return Ok(vec![format!(
            "no idempotency strategy for {:?} among {:?}",
            f.mode, caps.strategies
        )]);
    };
    sink.prepare(&f.schema, f.mode).await?;

    let mut expected: BTreeMap<String, u64> = BTreeMap::new();
    for (target, batch) in &f.batches {
        *expected.entry(target.clone()).or_default() += batch.num_rows() as u64;
    }
    let lease = |epoch| Lease {
        partition: f.partition.id.clone(),
        epoch: Epoch(epoch),
    };

    let mut txn = sink.begin(&f.partition, &lease(1), strategy).await?;
    for (target, batch) in &f.batches {
        txn.write(target, batch.clone()).await?;
    }
    txn.abort().await?;
    for target in expected.keys() {
        let n = probe.row_count(target).await?;
        if n != 0 {
            violations.push(format!("after abort, `{target}` has {n} rows instead of 0"));
        }
    }

    let summary = write_partition(sink, f, &lease(2), strategy).await?;
    if summary.rows_written != expected {
        violations.push(format!(
            "commit summary reported {:?}, expected {expected:?}",
            summary.rows_written
        ));
    }
    check_counts(probe, &expected, "after commit", &mut violations).await?;

    write_partition(sink, f, &lease(3), strategy).await?;
    check_counts(
        probe,
        &expected,
        "after re-running the partition",
        &mut violations,
    )
    .await?;

    if strategy == IdempotencyStrategy::TxnWithState {
        match write_partition(sink, f, &lease(2), strategy).await {
            Err(FerryError::StaleLease { .. }) => {}
            Ok(_) => violations.push("commit under a superseded lease was accepted".into()),
            Err(e) => violations.push(format!(
                "stale commit failed with {e} instead of StaleLease"
            )),
        }
        check_counts(probe, &expected, "after a stale commit", &mut violations).await?;
    }
    Ok(violations)
}

async fn write_partition(
    sink: &dyn Sink,
    f: &SinkFixture,
    lease: &Lease,
    strategy: IdempotencyStrategy,
) -> Result<crate::sink::WriteSummary> {
    let mut txn = sink.begin(&f.partition, lease, strategy).await?;
    for (target, batch) in &f.batches {
        txn.write(target, batch.clone()).await?;
    }
    txn.commit().await
}

async fn check_counts(
    probe: &dyn SinkProbe,
    expected: &BTreeMap<String, u64>,
    when: &str,
    violations: &mut Vec<String>,
) -> Result<()> {
    for (target, want) in expected {
        let n = probe.row_count(target).await?;
        if n != *want {
            violations.push(format!("{when}, `{target}` has {n} rows instead of {want}"));
        }
    }
    Ok(())
}
