//! The seam between planning (Track B) and execution (Track A).
//!
//! The planner compiles a flow into a [`TransformPlan`]; the runner feeds it
//! one partition's input streams and writes what comes out. Every output row
//! records which source record it came from, which is what lets the runner
//! enforce, and the dead-letter sink rely on, one rule:
//!
//! **A source record is either written in full or rejected in full.** If any
//! row derived from a record fails (say one bad child of a split), the plan
//! rejects the whole record and emits none of its rows, so a parent never
//! lands without its children or vice versa.

use std::sync::Arc;

use arrow_array::RecordBatch;
use futures::StreamExt;
use futures::stream::BoxStream;

use crate::entity::Entity;
use crate::error::{FerryError, Result};
use crate::partition::KeyValue;
use crate::source::BatchStream;

/// Identifies a driver record by its primary-key values.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SourceRecordId(pub Vec<KeyValue>);

/// Rows for one target table.
#[derive(Debug, Clone)]
pub struct TargetBatch {
    pub target: String,
    pub batch: RecordBatch,
    /// For each row of `batch`, the index into [`TransformOutput::records`]
    /// of the record it was derived from.
    pub source_index: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RejectedRecord {
    /// Index into [`TransformOutput::records`].
    pub index: u32,
    /// The target whose row failed, if the failure was target-specific.
    pub target: Option<String>,
    pub error: String,
}

/// The result of transforming a run of driver records.
#[derive(Debug, Clone, Default)]
pub struct TransformOutput {
    /// Every driver record this output accounts for, written or rejected.
    pub records: Vec<SourceRecordId>,
    /// Target batches in the plan's write order.
    pub targets: Vec<TargetBatch>,
    pub rejected: Vec<RejectedRecord>,
}

impl TransformOutput {
    /// Each rejected record paired with its id, for dead-lettering.
    pub fn rejected_records(&self) -> impl Iterator<Item = (&SourceRecordId, &RejectedRecord)> {
        self.rejected
            .iter()
            .map(|r| (&self.records[r.index as usize], r))
    }

    /// Checks the invariants every plan must uphold: lineage is in range and
    /// one entry per row, targets follow `write_order`, and no rejected
    /// record has rows in any target.
    pub fn validate(&self, write_order: &[String]) -> Result<()> {
        let invalid = |reason: String| Err(FerryError::InvalidTransformOutput { reason });
        let n = self.records.len();
        let mut last_position = None;
        for t in &self.targets {
            let Some(position) = write_order.iter().position(|w| *w == t.target) else {
                return invalid(format!("target `{}` is not in the write order", t.target));
            };
            if last_position.is_some_and(|last| position <= last) {
                return invalid(format!(
                    "target `{}` is out of write order {write_order:?}",
                    t.target
                ));
            }
            last_position = Some(position);
            if t.source_index.len() != t.batch.num_rows() {
                return invalid(format!(
                    "lineage has {} entries for {} rows of `{}`",
                    t.source_index.len(),
                    t.batch.num_rows(),
                    t.target
                ));
            }
            if let Some(i) = t.source_index.iter().find(|&&i| i as usize >= n) {
                return invalid(format!(
                    "`{}` row lineage index {i} is out of range for {n} records",
                    t.target
                ));
            }
        }
        for r in &self.rejected {
            if r.index as usize >= n {
                return invalid(format!(
                    "rejected index {} is out of range for {n} records",
                    r.index
                ));
            }
            if let Some(t) = self
                .targets
                .iter()
                .find(|t| t.source_index.contains(&r.index))
            {
                return invalid(format!(
                    "record {:?} is rejected but has rows in `{}`",
                    self.records[r.index as usize], t.target
                ));
            }
        }
        Ok(())
    }
}

pub type TransformStream = BoxStream<'static, Result<TransformOutput>>;

/// One partition's input streams, keyed by entity name.
pub struct PartitionInputs {
    streams: Vec<(String, BatchStream)>,
}

impl PartitionInputs {
    pub fn new(streams: Vec<(String, BatchStream)>) -> Self {
        Self { streams }
    }

    /// Removes and returns the stream for `entity`.
    pub fn take(&mut self, entity: &str) -> Result<BatchStream> {
        let pos = self
            .streams
            .iter()
            .position(|(name, _)| name == entity)
            .ok_or_else(|| FerryError::MissingInput {
                entity: entity.to_owned(),
            })?;
        Ok(self.streams.swap_remove(pos).1)
    }
}

pub trait TransformPlan: Send + Sync {
    /// Entities this plan reads. The first is the driver: its records are
    /// the ones outputs are attributed to.
    fn inputs(&self) -> &[String];

    /// Target tables in the order they must be written: parents first.
    fn write_order(&self) -> &[String];

    fn execute(&self, inputs: PartitionInputs) -> Result<TransformStream>;
}

/// Copies one entity's declared fields to one target unchanged. Lets the
/// runner be built and tested before the DSL compiles real plans.
pub struct IdentityPlan {
    entity: Arc<Entity>,
    inputs: Vec<String>,
    write_order: Vec<String>,
}

impl IdentityPlan {
    pub fn new(entity: Entity, target: impl Into<String>) -> Self {
        Self {
            inputs: vec![entity.name().to_owned()],
            write_order: vec![target.into()],
            entity: Arc::new(entity),
        }
    }
}

impl TransformPlan for IdentityPlan {
    fn inputs(&self) -> &[String] {
        &self.inputs
    }

    fn write_order(&self) -> &[String] {
        &self.write_order
    }

    fn execute(&self, mut inputs: PartitionInputs) -> Result<TransformStream> {
        let input = inputs.take(self.entity.name())?;
        let entity = Arc::clone(&self.entity);
        let target = self.write_order[0].clone();
        Ok(input
            .map(move |batch| {
                let batch = batch?;
                let records = entity.record_ids(&batch)?;
                let declared = entity
                    .fields()
                    .iter()
                    .map(|f| batch.schema().index_of(&f.name))
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .and_then(|columns| batch.project(&columns))
                    .map_err(|e| FerryError::InvalidTransformOutput {
                        reason: format!("input batch does not match entity: {e}"),
                    })?;
                Ok(TransformOutput {
                    targets: vec![TargetBatch {
                        target: target.clone(),
                        source_index: (0..records.len() as u32).collect(),
                        batch: declared,
                    }],
                    records,
                    rejected: Vec::new(),
                })
            })
            .boxed())
    }
}
