use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use arrow_array::RecordBatch;
use async_trait::async_trait;

use super::SinkProbe;
use crate::error::{FerryError, Result};
use crate::lease::{Epoch, Lease};
use crate::partition::{Partition, PartitionId};
use crate::sink::{IdempotencyStrategy, Sink, SinkCaps, SinkTxn, WriteMode, WriteSummary};
use crate::target::{TargetSchema, TargetTable};
use crate::types::CanonicalField;

/// Faults a [`MemSink`] injects, for testing how callers recover.
#[derive(Debug, Clone, Default)]
pub struct MemSinkFaults {
    /// Fail the n-th `write` call (1-based, counted across transactions).
    pub fail_write_number: Option<usize>,
    /// Fail this many upcoming commits without applying them.
    pub fail_next_commits: usize,
}

/// A [`Sink`] that keeps committed rows in memory.
///
/// Supports [`IdempotencyStrategy::TxnWithState`] and
/// [`IdempotencyStrategy::DeletePartitionThenInsert`]: a commit replaces
/// everything the same partition committed before. Under `TxnWithState` it
/// also records the committing epoch and rejects commits from older leases.
///
/// Writes are checked against the prepared target schema, so a transform
/// that produces the wrong columns, types or nulls fails here.
#[derive(Clone)]
pub struct MemSink {
    strategies: Vec<IdempotencyStrategy>,
    state: Arc<Mutex<State>>,
}

#[derive(Default)]
struct State {
    tables: HashMap<String, TargetTable>,
    committed: BTreeMap<PartitionId, (Epoch, Vec<(String, RecordBatch)>)>,
    faults: MemSinkFaults,
    writes_seen: usize,
}

impl MemSink {
    pub fn new(strategies: Vec<IdempotencyStrategy>) -> Self {
        Self {
            strategies,
            state: Arc::default(),
        }
    }

    pub fn inject(&self, faults: MemSinkFaults) {
        self.state.lock().unwrap().faults = faults;
    }

    /// Every committed row of `target`, ordered by partition.
    pub fn rows(&self, target: &str) -> Vec<RecordBatch> {
        let state = self.state.lock().unwrap();
        state
            .committed
            .values()
            .flat_map(|(_, writes)| writes.iter())
            .filter(|(t, _)| t == target)
            .map(|(_, b)| b.clone())
            .collect()
    }
}

fn sink_error(message: String) -> FerryError {
    FerryError::Connector {
        connector: "mem".into(),
        message,
    }
}

#[async_trait]
impl Sink for MemSink {
    async fn prepare(&self, target: &TargetSchema, _mode: WriteMode) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        for t in target.tables() {
            state.tables.insert(t.name.clone(), t.clone());
        }
        Ok(())
    }

    async fn begin(
        &self,
        partition: &Partition,
        lease: &Lease,
        strategy: IdempotencyStrategy,
    ) -> Result<Box<dyn SinkTxn>> {
        if !self.strategies.contains(&strategy) {
            return Err(sink_error(format!(
                "strategy {strategy:?} is not supported"
            )));
        }
        Ok(Box::new(MemTxn {
            state: Arc::clone(&self.state),
            partition: partition.id.clone(),
            epoch: lease.epoch,
            strategy,
            writes: Vec::new(),
        }))
    }

    async fn capabilities(&self) -> Result<SinkCaps> {
        Ok(SinkCaps {
            strategies: self.strategies.clone(),
            bulk_copy: false,
        })
    }
}

#[async_trait]
impl SinkProbe for MemSink {
    async fn row_count(&self, target: &str) -> Result<u64> {
        Ok(self.rows(target).iter().map(|b| b.num_rows() as u64).sum())
    }
}

struct MemTxn {
    state: Arc<Mutex<State>>,
    partition: PartitionId,
    epoch: Epoch,
    strategy: IdempotencyStrategy,
    writes: Vec<(String, RecordBatch)>,
}

#[async_trait]
impl SinkTxn for MemTxn {
    async fn write(&mut self, target: &str, batch: RecordBatch) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        state.writes_seen += 1;
        if state.faults.fail_write_number == Some(state.writes_seen) {
            return Err(sink_error(format!(
                "injected failure on write {}",
                state.writes_seen
            )));
        }
        let table = state
            .tables
            .get(target)
            .ok_or_else(|| sink_error(format!("target `{target}` was not prepared")))?;
        check_batch(table, &batch)?;
        drop(state);
        self.writes.push((target.to_owned(), batch));
        Ok(())
    }

    async fn commit(self: Box<Self>) -> Result<WriteSummary> {
        let mut state = self.state.lock().unwrap();
        if state.faults.fail_next_commits > 0 {
            state.faults.fail_next_commits -= 1;
            return Err(sink_error("injected commit failure".into()));
        }
        if self.strategy == IdempotencyStrategy::TxnWithState
            && let Some((current, _)) = state.committed.get(&self.partition)
            && *current > self.epoch
        {
            return Err(FerryError::StaleLease {
                partition: self.partition.0.clone(),
                epoch: self.epoch.0,
                current: current.0,
            });
        }
        let mut summary = WriteSummary::default();
        for (target, batch) in &self.writes {
            *summary.rows_written.entry(target.clone()).or_default() += batch.num_rows() as u64;
        }
        state
            .committed
            .insert(self.partition.clone(), (self.epoch, self.writes));
        Ok(summary)
    }

    async fn abort(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}

fn check_batch(table: &TargetTable, batch: &RecordBatch) -> Result<()> {
    let schema = batch.schema();
    let names: Vec<_> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    let expected: Vec<_> = table.fields.iter().map(|f| f.name.as_str()).collect();
    if names != expected {
        return Err(sink_error(format!(
            "`{}` expects columns {expected:?}, got {names:?}",
            table.name
        )));
    }
    for (field, column) in table.fields.iter().zip(batch.columns()) {
        let want = CanonicalField::to_arrow(field);
        if column.data_type() != want.data_type() {
            return Err(sink_error(format!(
                "`{}.{}` expects {}, got {}",
                table.name,
                field.name,
                want.data_type(),
                column.data_type()
            )));
        }
        if !field.nullable && column.null_count() > 0 {
            return Err(sink_error(format!(
                "`{}.{}` is not nullable but the batch has {} nulls",
                table.name,
                field.name,
                column.null_count()
            )));
        }
    }
    Ok(())
}
