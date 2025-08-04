//! One unit of work: a leased partition, from `begin` to its done-record.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow_array::RecordBatch;
use ferry_core::{
    Entity, FerryError, IdempotencyStrategy, Lease, Partition, PartitionId, PartitionInputs,
    Result, SinkTxn, Source, TargetBatch, WriteSummary,
};
use ferry_registry::StateStore;
use ferry_registry::state::{PartitionRows, RunId};
use futures::StreamExt;
use tokio::sync::{OwnedSemaphorePermit, watch};
use tokio::task::JoinSet;
use tokio::time::{Instant, MissedTickBehavior};

use crate::budget::{AheadSender, Budget, batch_size, read_ahead};
use crate::flow::Flow;
use crate::observe::{PartitionObserver, WriteObserver};
use crate::settings::Settings;
use crate::throttle::Throttle;

/// What every worker of one run shares.
pub(crate) struct Shared {
    pub store: Arc<dyn StateStore>,
    pub run: RunId,
    pub settings: Settings,
    pub budget: Budget,
    pub throttle: Throttle,
    pub observer: Option<Arc<dyn WriteObserver>>,
}

/// A flow as it runs: its strategy, and what each stored partition reads.
pub(crate) struct FlowRun {
    pub flow: Flow,
    pub strategy: IdempotencyStrategy,
    /// By stored (namespaced) partition id.
    pub units: HashMap<PartitionId, Unit>,
}

pub(crate) struct Unit {
    /// As stored, and as the sink and the lease name it.
    pub partition: Partition,
    /// One per input, as each source planned it.
    pub reads: Vec<Partition>,
}

pub(crate) enum Outcome {
    Done,
    /// Marked failed in the store, with this message.
    Failed(String),
    /// Another worker holds the partition now, or this one could no longer
    /// prove it held it: its transaction was aborted, or (if the commit had
    /// already happened) the strategy makes the new owner's re-run safe.
    LeaseLost,
}

/// Processes one leased partition, whose lease was asked for at `asked`.
/// An `Err` is a state store failure that leaves the partition's state
/// unknown, and stops the run.
pub(crate) async fn process(
    shared: Arc<Shared>,
    flow: Arc<FlowRun>,
    lease: Lease,
    asked: Instant,
) -> Result<Outcome> {
    let Some(unit) = flow.units.get(&lease.partition) else {
        return Err(FerryError::Connector {
            connector: "runner".into(),
            message: format!(
                "the state store leased partition `{}`, which flow `{}` didn't plan",
                lease.partition.0, flow.flow.name
            ),
        });
    };
    let (lost_tx, mut lost) = watch::channel(false);
    let heartbeat = tokio::spawn(heartbeat(
        Arc::clone(&shared),
        lease.clone(),
        asked,
        lost_tx,
    ));
    let _heartbeat = AbortOnDrop(heartbeat);

    let fail = |error: FerryError| mark_failed(&shared, &lease, error.to_string());

    let begun = until_lost(
        &mut lost,
        flow.flow.sink.begin(&unit.partition, &lease, flow.strategy),
    )
    .await;
    let mut txn = match begun {
        None => return Ok(Outcome::LeaseLost),
        Some(Err(e)) => return fail(e).await,
        Some(Ok(txn)) => txn,
    };
    let mut observer = shared
        .observer
        .as_ref()
        .map(|o| o.begin(&shared.run, &flow.flow.name, &lease));
    let written = until_lost(
        &mut lost,
        write_unit(&shared, &flow, unit, txn.as_mut(), observer.as_mut()),
    )
    .await;
    let rows = match written {
        None => {
            let _ = txn.abort().await;
            return Ok(Outcome::LeaseLost);
        }
        Some(Err(e)) => {
            let _ = txn.abort().await;
            return fail(e).await;
        }
        Some(Ok(rows)) => rows,
    };
    if *lost.borrow() {
        let _ = txn.abort().await;
        return Ok(Outcome::LeaseLost);
    }
    // Before the commit, so the numbers hold under every strategy (A4).
    match shared.store.record_rows(&shared.run, &lease, &rows).await {
        Ok(()) => {}
        Err(FerryError::StaleLease { .. }) => {
            let _ = txn.abort().await;
            return Ok(Outcome::LeaseLost);
        }
        Err(e) => {
            let _ = txn.abort().await;
            return Err(e);
        }
    }
    // Also before the commit, so no partition commits without its record.
    if let Some(observer) = observer {
        match until_lost(&mut lost, observer.finish(&rows)).await {
            Some(Ok(())) => {}
            None | Some(Err(FerryError::StaleLease { .. })) => {
                let _ = txn.abort().await;
                return Ok(Outcome::LeaseLost);
            }
            Some(Err(e)) => {
                let _ = txn.abort().await;
                return fail(e).await;
            }
        }
    }
    // Under `TxnWithState` the sink's commit writes the done-record through
    // the ledger, and refuses if the lease was superseded (0004 §8). The
    // heartbeat keeps running until the commit returns.
    let summary = match txn.commit().await {
        Ok(summary) => summary,
        Err(FerryError::StaleLease { .. }) => return Ok(Outcome::LeaseLost),
        Err(e) => return fail(e).await,
    };
    if flow.strategy != IdempotencyStrategy::TxnWithState {
        return mark_done(&shared, &lease, &summary).await;
    }
    Ok(Outcome::Done)
}

async fn mark_done(shared: &Shared, lease: &Lease, summary: &WriteSummary) -> Result<Outcome> {
    // A crash before this line re-runs a partition whose rows committed;
    // the strategy makes that safe.
    match shared.store.mark_done(&shared.run, lease, summary).await {
        Ok(()) => Ok(Outcome::Done),
        Err(FerryError::StaleLease { .. }) => Ok(Outcome::LeaseLost),
        Err(e) => Err(e),
    }
}

async fn mark_failed(shared: &Shared, lease: &Lease, error: String) -> Result<Outcome> {
    match shared.store.mark_failed(&shared.run, lease, &error).await {
        Ok(()) => Ok(Outcome::Failed(error)),
        Err(FerryError::StaleLease { .. }) => Ok(Outcome::LeaseLost),
        Err(e) => Err(e),
    }
}

/// `None` if the lease is lost before `work` finishes; `work` is dropped.
async fn until_lost<T>(
    lost: &mut watch::Receiver<bool>,
    work: impl Future<Output = T>,
) -> Option<T> {
    tokio::select! {
        biased;
        // An `Err` means the heartbeat died: the lease can't be vouched for.
        _ = lost.wait_for(|lost| *lost) => None,
        out = work => Some(out),
    }
}

/// Renews the lease every `heartbeat`, counted from `asked`, when the lease
/// was asked for. Reports it lost when the store says a newer lease exists,
/// or when renewals kept failing until the lease would have expired.
async fn heartbeat(shared: Arc<Shared>, lease: Lease, asked: Instant, lost: watch::Sender<bool>) {
    let ttl = shared.settings.lease_ttl;
    // The store set the expiry no earlier than it was asked to: counting
    // from when the lease came back would believe in it for too long.
    let mut expires = asked + ttl;
    // Every heartbeat from `asked`, so a renewal that takes a while doesn't
    // push the next one back by that long. One slower than the heartbeat is
    // followed by the next as soon as it returns, and the rest stay on the
    // schedule: two renewals are never further apart than the heartbeat or
    // the slow one, whichever is longer.
    let heartbeat = shared.settings.heartbeat;
    let mut ticks = tokio::time::interval_at(asked + heartbeat, heartbeat);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        ticks.tick().await;
        let asked = Instant::now();
        match shared.store.renew(&shared.run, &lease, ttl).await {
            Ok(()) => expires = asked + ttl,
            Err(FerryError::StaleLease { .. }) => break,
            Err(_) if Instant::now() >= expires => break,
            Err(_) => {}
        }
    }
    lost.send_replace(true);
}

struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Reads every input, runs the plan, and writes its output in write order.
/// Returns the rows read from the driver: each is either written or
/// dead-lettered, which is what the verifier (A8) checks.
async fn write_unit(
    shared: &Shared,
    flow: &FlowRun,
    unit: &Unit,
    txn: &mut dyn SinkTxn,
    mut observer: Option<&mut Box<dyn PartitionObserver>>,
) -> Result<PartitionRows> {
    let mut connections = shared.throttle.connections(unit.reads.len()).await;
    let read_ahead_bytes = shared.budget.read_ahead(unit.reads.len());
    let driver_rows = Arc::new(AtomicU64::new(0));
    let mut readers = JoinSet::new();
    let mut streams = Vec::with_capacity(unit.reads.len());
    for (i, (input, partition)) in flow.flow.inputs.iter().zip(&unit.reads).enumerate() {
        let (tx, rx) = read_ahead(read_ahead_bytes);
        let connection = match &mut connections {
            Some(permits) if permits.num_permits() > 1 => permits.split(1),
            other => other.take(),
        };
        readers.spawn(read_input(Reader {
            source: Arc::clone(&input.source),
            entity: input.entity.clone(),
            partition: partition.clone(),
            tx,
            throttle: shared.throttle.clone(),
            counted: (i == 0).then(|| Arc::clone(&driver_rows)),
            _connection: connection,
        }));
        streams.push((input.entity.name().to_owned(), rx.boxed()));
    }

    let write_order = flow.flow.plan.write_order();
    let mut outputs = flow.flow.plan.execute(PartitionInputs::new(streams))?;
    let mut batcher = Batcher::new(write_order, shared.budget.batch_bytes());
    while let Some(output) = outputs.next().await {
        let output = output?;
        output.validate(write_order)?;
        if let Some((id, rejected)) = output.rejected_records().next() {
            // `on_error: fail` (0003 §4). Dead-lettering is A7's.
            return Err(FerryError::InvalidTransformOutput {
                reason: format!(
                    "record {:?} was rejected{}: {}",
                    id.0,
                    rejected
                        .target
                        .as_ref()
                        .map(|t| format!(" for `{t}`"))
                        .unwrap_or_default(),
                    rejected.error
                ),
            });
        }
        if let Some(observer) = observer.as_mut() {
            observer.output(&output)?;
        }
        batcher.push(output.targets, txn).await?;
    }
    batcher.flush(txn).await?;
    drop(outputs);
    // A reader that panicked ended its stream early, which looks like the
    // end of the partition: fail it rather than commit what was read.
    while let Some(joined) = readers.join_next().await {
        if let Err(e) = joined {
            return Err(FerryError::Connector {
                connector: "runner".into(),
                message: format!("reading the partition failed: {e}"),
            });
        }
    }
    Ok(PartitionRows {
        read: driver_rows.load(Ordering::Relaxed),
        dead_lettered: 0,
    })
}

struct Reader {
    source: Arc<dyn Source>,
    entity: Entity,
    partition: Partition,
    tx: AheadSender,
    throttle: Throttle,
    /// Counts the rows read, for the driver.
    counted: Option<Arc<AtomicU64>>,
    /// The stream's share of `source_max_connections`, held until it ends.
    _connection: Option<OwnedSemaphorePermit>,
}

async fn read_input(reader: Reader) {
    let mut stream = match reader.source.read(&reader.entity, &reader.partition).await {
        Ok(stream) => stream,
        Err(e) => return reader.tx.fail(e).await,
    };
    while let Some(batch) = stream.next().await {
        match batch {
            Ok(batch) => {
                reader.throttle.rows(batch.num_rows()).await;
                if let Some(counted) = &reader.counted {
                    counted.fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
                }
                if !reader.tx.send(batch).await {
                    return;
                }
            }
            Err(e) => return reader.tx.fail(e).await,
        }
    }
}

/// Coalesces transformed rows into writes of about `limit` bytes per
/// target. A flush writes every target in write order, so a record's
/// parent rows never land after its children.
struct Batcher<'a> {
    order: &'a [String],
    pending: Vec<Vec<RecordBatch>>,
    bytes: u64,
    limit: u64,
}

impl<'a> Batcher<'a> {
    fn new(order: &'a [String], limit: u64) -> Self {
        Self {
            order,
            pending: vec![Vec::new(); order.len()],
            bytes: 0,
            limit,
        }
    }

    async fn push(&mut self, targets: Vec<TargetBatch>, txn: &mut dyn SinkTxn) -> Result<()> {
        for t in targets {
            if t.batch.num_rows() == 0 {
                continue;
            }
            let i = self
                .order
                .iter()
                .position(|w| *w == t.target)
                .expect("validated against the write order");
            self.bytes += batch_size(&t.batch);
            self.pending[i].push(t.batch);
        }
        if self.bytes >= self.limit {
            self.flush(txn).await?;
        }
        Ok(())
    }

    async fn flush(&mut self, txn: &mut dyn SinkTxn) -> Result<()> {
        for (target, pending) in self.order.iter().zip(&mut self.pending) {
            let batches = std::mem::take(pending);
            let batch = match batches.len() {
                0 => continue,
                1 => batches.into_iter().next().expect("one batch"),
                _ => arrow_select::concat::concat_batches(&batches[0].schema(), &batches).map_err(
                    |e| FerryError::InvalidTransformOutput {
                        reason: format!("batches for `{target}` don't share a schema: {e}"),
                    },
                )?,
            };
            for slice in split(batch, self.limit) {
                txn.write(target, slice).await?;
            }
        }
        self.bytes = 0;
        Ok(())
    }
}

/// Cuts `batch` into row ranges of about `limit` bytes each.
fn split(batch: RecordBatch, limit: u64) -> Vec<RecordBatch> {
    let bytes = batch_size(&batch);
    let rows = batch.num_rows();
    if bytes <= limit || rows <= 1 {
        return vec![batch];
    }
    let parts = bytes.div_ceil(limit.max(1)) as usize;
    let per = rows.div_ceil(parts).max(1);
    (0..rows)
        .step_by(per)
        .map(|offset| batch.slice(offset, per.min(rows - offset)))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Int64Array};

    use super::*;

    fn batch(n: i64) -> RecordBatch {
        let id: ArrayRef = Arc::new(Int64Array::from_iter_values(0..n));
        RecordBatch::try_from_iter([("id", id)]).unwrap()
    }

    #[test]
    fn a_big_batch_is_cut_into_slices_that_keep_every_row() {
        let b = batch(10_000);
        let limit = batch_size(&b) / 4;
        let slices = split(b, limit);
        assert!(slices.len() >= 4, "{} slices", slices.len());
        assert_eq!(slices.iter().map(|s| s.num_rows()).sum::<usize>(), 10_000);
        let first: &Int64Array = slices[1].column(0).as_any().downcast_ref().unwrap();
        assert_eq!(first.value(0), slices[0].num_rows() as i64);
        // Small batches and single rows are written as they are.
        assert_eq!(split(batch(10), u64::MAX).len(), 1);
        assert_eq!(split(batch(1), 1).len(), 1);
    }
}
