//! Accumulating source-side aggregates at read time, through the runner's
//! observer hook.
//!
//! Each driver batch is evaluated as it is read (the [`ReadObserver`]):
//! every source-side expression of the flow, per record, held by the
//! record's id. When a transform output accounts for a record, its values
//! join the aggregates if the record was written (it has a row in a
//! target) and are dropped if it was filtered or rejected (0003 §5.9). So
//! the aggregates cover exactly the rows the run read and wrote, whatever
//! the source holds by the time the run is verified.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};

use arrow_array::{Array as _, RecordBatch};
use async_trait::async_trait;
use dataferry_core::{Entity, Lease, Result, SourceRecordId, TransformOutput};
use dataferry_registry::state::{PartitionRows, RunId};
use dataferry_runtime::{Flow, PartitionObserver, ReadObserver, WriteObserver};

use super::check::SourceTerm;
use super::log::{SourceLog, SourceRecord, expect_error};
use super::parse::Agg;
use super::value::{Accumulated, Value, values};

/// The runner observer that feeds every flow's source-side aggregates and
/// records them per partition attempt in a [`SourceLog`] before the
/// commit. Built by [`Expectations::observer`](super::Expectations::observer).
pub struct SourceAggregates {
    log: Arc<dyn SourceLog>,
    /// By flow name; flows with none aren't here.
    terms: HashMap<String, Arc<[SourceTerm]>>,
    /// The driver entity of each flow, as the runner runs it.
    drivers: Mutex<HashMap<String, Arc<Entity>>>,
}

impl SourceAggregates {
    pub(crate) fn new(log: Arc<dyn SourceLog>, terms: HashMap<String, Arc<[SourceTerm]>>) -> Self {
        Self {
            log,
            terms,
            drivers: Mutex::default(),
        }
    }
}

#[async_trait]
impl WriteObserver for SourceAggregates {
    async fn prepared(&self, _: &RunId, flow: &Flow) -> Result<()> {
        if !self.terms.contains_key(&flow.name) {
            return Ok(());
        }
        let driver = flow
            .inputs
            .first()
            .ok_or_else(|| expect_error(format!("flow `{}` has no inputs", flow.name)))?;
        self.drivers
            .lock()
            .expect("not poisoned")
            .insert(flow.name.clone(), Arc::new(driver.entity.clone()));
        Ok(())
    }

    fn begin(&self, run: &RunId, flow: &str, lease: &Lease) -> Box<dyn PartitionObserver> {
        let Some(terms) = self.terms.get(flow) else {
            return Box::new(Nothing);
        };
        let driver = self
            .drivers
            .lock()
            .expect("not poisoned")
            .get(flow)
            .cloned();
        let state = State {
            pending: HashMap::new(),
            aggregates: terms.iter().map(|t| Accumulated::empty(t.agg)).collect(),
            error: driver
                .is_none()
                .then(|| format!("flow `{flow}` wasn't prepared before its partitions ran")),
        };
        Box::new(Attempt {
            log: Arc::clone(&self.log),
            run: run.clone(),
            flow: flow.to_owned(),
            lease: lease.clone(),
            terms: Arc::clone(terms),
            driver,
            state: Arc::new(Mutex::new(state)),
        })
    }
}

/// A flow without source-side aggregates.
struct Nothing;

#[async_trait]
impl PartitionObserver for Nothing {
    fn output(&mut self, _: &TransformOutput) -> Result<()> {
        Ok(())
    }

    async fn finish(self: Box<Self>, _: &PartitionRows) -> Result<()> {
        Ok(())
    }
}

/// One record's value of each term, or the error its expression gave.
type Row = Vec<std::result::Result<Value, String>>;

struct State {
    /// Records read, not yet accounted for by an output. A queue per id,
    /// so a source that returns the same key twice is still matched up.
    pending: HashMap<SourceRecordId, VecDeque<Row>>,
    /// One per term, over the records written so far.
    aggregates: Vec<Accumulated>,
    /// Set when read and written records couldn't be matched, which makes
    /// every aggregate of the attempt unknown. The partition itself still
    /// runs: the assertion reports it can't be evaluated.
    error: Option<String>,
}

impl State {
    fn fail(&mut self, error: String) {
        self.error.get_or_insert(error);
    }
}

struct Attempt {
    log: Arc<dyn SourceLog>,
    run: RunId,
    flow: String,
    lease: Lease,
    terms: Arc<[SourceTerm]>,
    driver: Option<Arc<Entity>>,
    state: Arc<Mutex<State>>,
}

struct Reader {
    terms: Arc<[SourceTerm]>,
    driver: Arc<Entity>,
    state: Arc<Mutex<State>>,
}

impl ReadObserver for Reader {
    fn batch(&mut self, batch: &RecordBatch) -> Result<()> {
        // Evaluated before the lock: the writer only waits for the insert.
        let rows = self.rows(batch);
        let mut state = self.state.lock().expect("not poisoned");
        match rows {
            Ok(rows) => {
                for (id, row) in rows {
                    state.pending.entry(id).or_default().push_back(row);
                }
            }
            Err(e) => state.fail(e),
        }
        Ok(())
    }
}

impl Reader {
    /// Every record of `batch` with its value of each term.
    fn rows(&self, batch: &RecordBatch) -> std::result::Result<Vec<(SourceRecordId, Row)>, String> {
        let ids = self.driver.record_ids(batch).map_err(|e| e.to_string())?;
        let mut rows: Vec<(SourceRecordId, Row)> =
            ids.into_iter().map(|id| (id, Vec::new())).collect();
        for term in self.terms.iter() {
            let input = project(batch, term)?;
            let (array, errors) = term.expr.evaluate_rows(&input);
            let mut failed: HashMap<usize, String> =
                errors.into_iter().map(|e| (e.row, e.message)).collect();
            let values = match term.agg {
                // A count needs to know only which values are null.
                Agg::Count => (0..array.len())
                    .map(|i| {
                        if array.is_valid(i) {
                            Value::int(1)
                        } else {
                            Value::Null
                        }
                    })
                    .collect(),
                _ => values(&array).map_err(|e| format!("`{}`: {e}", term.key))?,
            };
            for (i, (row, value)) in rows.iter_mut().zip(values).enumerate() {
                row.1.push(match failed.remove(&i) {
                    Some(message) => {
                        Err(format!("`{}` failed on {:?}: {message}", term.key, row.0))
                    }
                    None => Ok(value),
                });
            }
        }
        Ok(rows)
    }
}

/// The columns of `batch` the term's scope reads, in its order.
fn project(batch: &RecordBatch, term: &SourceTerm) -> std::result::Result<RecordBatch, String> {
    let schema = term.expr.input_schema();
    let indices = schema
        .fields()
        .iter()
        .map(|f| {
            batch.schema().index_of(f.name()).map_err(|_| {
                format!(
                    "`{}`: the driver batch has no column `{}`",
                    term.key,
                    f.name()
                )
            })
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    batch
        .project(&indices)
        .map_err(|e| format!("`{}`: {e}", term.key))
}

#[async_trait]
impl PartitionObserver for Attempt {
    fn reader(&mut self) -> Option<Box<dyn ReadObserver>> {
        Some(Box::new(Reader {
            terms: Arc::clone(&self.terms),
            driver: Arc::clone(self.driver.as_ref()?),
            state: Arc::clone(&self.state),
        }))
    }

    fn output(&mut self, output: &TransformOutput) -> Result<()> {
        let rejected: HashSet<u32> = output.rejected.iter().map(|r| r.index).collect();
        let written: HashSet<u32> = output
            .targets
            .iter()
            .flat_map(|t| t.source_index.iter().copied())
            .filter(|i| !rejected.contains(i))
            .collect();
        let mut state = self.state.lock().expect("not poisoned");
        let state = &mut *state;
        for (i, id) in output.records.iter().enumerate() {
            let row = state.pending.get_mut(id).and_then(VecDeque::pop_front);
            if state.pending.get(id).is_some_and(VecDeque::is_empty) {
                state.pending.remove(id);
            }
            let Some(row) = row else {
                state.fail(format!(
                    "record {:?} was transformed but not seen when it was read",
                    id.0
                ));
                continue;
            };
            if written.contains(&(i as u32)) {
                for (aggregate, value) in state.aggregates.iter_mut().zip(&row) {
                    aggregate.add(value.as_ref().map_err(String::as_str));
                }
            }
        }
        Ok(())
    }

    async fn finish(self: Box<Self>, _: &PartitionRows) -> Result<()> {
        let record = {
            let state = self.state.lock().expect("not poisoned");
            let aggregates: BTreeMap<String, Accumulated> = self
                .terms
                .iter()
                .zip(&state.aggregates)
                .map(|(term, aggregate)| {
                    let aggregate = match &state.error {
                        Some(error) => Accumulated::Failed {
                            error: error.clone(),
                        },
                        None => aggregate.clone(),
                    };
                    (term.key.clone(), aggregate)
                })
                .collect();
            SourceRecord {
                flow: self.flow.clone(),
                partition: self.lease.partition.0.clone(),
                epoch: self.lease.epoch.0,
                aggregates,
            }
        };
        self.log.record(&self.run, &record).await
    }
}
