//! Write-time checksums: the runner's [`WriteObserver`], recording what
//! each partition hands the sink.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use ferry_core::{Lease, Result, TransformOutput};
use ferry_registry::state::{PartitionRows, RunId};
use ferry_runtime::{Flow, PartitionObserver, WriteObserver};

use crate::checksum::{ChecksumSpec, MICROS};
use crate::log::{PartitionRecord, RecordCounts, TargetWrite, WriteLog};
use crate::target::Target;
use crate::verifier_error;

/// Checksums every row the runner writes, per target table, and records
/// the result with the partition's record counts in a [`WriteLog`]
/// before the commit. Set it with `Runner::with_observer`.
pub struct ChecksumObserver {
    log: Arc<dyn WriteLog>,
    target: Arc<dyn Target>,
    /// By flow name: each target table's spec, with the target's
    /// timestamp precision, resolved once the sink prepared the tables.
    specs: Mutex<HashMap<String, Arc<BTreeMap<String, ChecksumSpec>>>>,
}

impl ChecksumObserver {
    pub fn new(log: Arc<dyn WriteLog>, target: Arc<dyn Target>) -> Self {
        Self {
            log,
            target,
            specs: Mutex::default(),
        }
    }
}

/// The spec each of `flow`'s target tables is checksummed with, on both
/// sides.
pub(crate) async fn table_specs(
    target: &dyn Target,
    tables: &[ferry_core::TargetTable],
) -> Result<BTreeMap<String, ChecksumSpec>> {
    let mut specs = BTreeMap::new();
    for table in tables {
        let mut spec = ChecksumSpec::new(table);
        for (column, digits) in target.timestamp_digits(table).await? {
            spec = spec.with_timestamp_digits(&column, digits)?;
        }
        specs.insert(table.name.clone(), spec);
    }
    Ok(specs)
}

#[async_trait]
impl WriteObserver for ChecksumObserver {
    async fn prepared(&self, _: &RunId, flow: &Flow) -> Result<()> {
        let specs = table_specs(self.target.as_ref(), flow.target.tables()).await?;
        self.specs
            .lock()
            .expect("not poisoned")
            .insert(flow.name.clone(), Arc::new(specs));
        Ok(())
    }

    fn begin(&self, run: &RunId, flow: &str, lease: &Lease) -> Box<dyn PartitionObserver> {
        let specs = self.specs.lock().expect("not poisoned").get(flow).cloned();
        let targets = specs
            .iter()
            .flat_map(|s| s.iter())
            .map(|(table, spec)| {
                let timestamp_digits = spec
                    .columns()
                    .iter()
                    .filter(|c| c.timestamp_digits < MICROS)
                    .map(|c| (c.name.clone(), c.timestamp_digits))
                    .collect();
                (
                    table.clone(),
                    TargetWrite {
                        timestamp_digits,
                        ..Default::default()
                    },
                )
            })
            .collect();
        Box::new(Attempt {
            log: Arc::clone(&self.log),
            run: run.clone(),
            specs,
            record: PartitionRecord {
                flow: flow.to_owned(),
                partition: lease.partition.0.clone(),
                epoch: lease.epoch.0,
                read: 0,
                records: RecordCounts::default(),
                targets,
            },
        })
    }
}

struct Attempt {
    log: Arc<dyn WriteLog>,
    run: RunId,
    /// `None` if the flow was never prepared, which the runner rules out.
    specs: Option<Arc<BTreeMap<String, ChecksumSpec>>>,
    record: PartitionRecord,
}

#[async_trait]
impl PartitionObserver for Attempt {
    fn output(&mut self, output: &TransformOutput) -> Result<()> {
        let Some(specs) = &self.specs else {
            return Err(verifier_error(format!(
                "flow `{}` wasn't prepared before its partitions ran",
                self.record.flow
            )));
        };
        let rejected: HashSet<u32> = output.rejected.iter().map(|r| r.index).collect();
        let mut with_rows: HashSet<u32> = HashSet::new();
        for t in &output.targets {
            let (Some(spec), Some(write)) =
                (specs.get(&t.target), self.record.targets.get_mut(&t.target))
            else {
                return Err(verifier_error(format!(
                    "flow `{}` wrote `{}`, which isn't one of its targets",
                    self.record.flow, t.target
                )));
            };
            write.checksum.add_batch(spec, &t.batch)?;
            with_rows.extend(&t.source_index);
        }
        // Counted, not subtracted: `validate` rules out a rejected record
        // with rows, but each record still lands in exactly one bucket if
        // an output breaks that. Rejection wins over rows.
        let counts = &mut self.record.records;
        for i in 0..output.records.len() as u32 {
            if rejected.contains(&i) {
                counts.rejected += 1;
            } else if with_rows.contains(&i) {
                counts.written += 1;
            } else {
                counts.filtered += 1;
            }
        }
        Ok(())
    }

    async fn finish(mut self: Box<Self>, rows: &PartitionRows) -> Result<()> {
        self.record.read = rows.read;
        self.log.record(&self.run, &self.record).await
    }
}
