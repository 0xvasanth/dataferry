use arrow_array::{BooleanArray, RecordBatch};
use async_trait::async_trait;
use futures::StreamExt;
use futures::stream;

use crate::entity::Entity;
use crate::error::{FerryError, Result};
use crate::partition::{KeyRange, Partition, PartitionHint, PartitionScope};
use crate::source::{BatchStream, Source, SourceCaps};

/// A [`Source`] over Arrow batches held in memory.
///
/// Entities with a single integer primary key are split into key ranges of
/// about [`PartitionHint::target_rows`] rows; others become one
/// [`PartitionScope::Whole`] partition.
pub struct MemSource {
    entities: Vec<(Entity, RecordBatch)>,
    batch_rows: usize,
    fail_after_batches: Option<usize>,
}

impl MemSource {
    pub fn new() -> Self {
        Self {
            entities: Vec::new(),
            batch_rows: 1024,
            fail_after_batches: None,
        }
    }

    /// Adds an entity whose rows are `rows` (which must match its schema).
    pub fn with_entity(mut self, entity: Entity, rows: RecordBatch) -> Result<Self> {
        if rows.schema().fields() != entity.arrow_schema().fields() {
            return Err(FerryError::InvalidEntity {
                entity: entity.name().to_owned(),
                reason: "rows do not match the entity's schema".into(),
            });
        }
        self.entities.push((entity, rows));
        Ok(self)
    }

    /// Maximum rows per emitted batch.
    pub fn with_batch_rows(mut self, n: usize) -> Self {
        self.batch_rows = n.max(1);
        self
    }

    /// Makes every read fail after emitting `n` batches.
    pub fn failing_after_batches(mut self, n: usize) -> Self {
        self.fail_after_batches = Some(n);
        self
    }

    fn rows(&self, entity: &str) -> Result<&RecordBatch> {
        self.entities
            .iter()
            .find(|(e, _)| e.name() == entity)
            .map(|(_, rows)| rows)
            .ok_or_else(|| connector_error(format!("unknown entity `{entity}`")))
    }
}

impl Default for MemSource {
    fn default() -> Self {
        Self::new()
    }
}

fn connector_error(message: String) -> FerryError {
    FerryError::Connector {
        connector: "mem".into(),
        message,
    }
}

#[async_trait]
impl Source for MemSource {
    async fn discover(&self) -> Result<Vec<Entity>> {
        Ok(self.entities.iter().map(|(e, _)| e.clone()).collect())
    }

    async fn plan_partitions(
        &self,
        entity: &Entity,
        hint: &PartitionHint,
    ) -> Result<Vec<Partition>> {
        let rows = self.rows(entity.name())?;
        let whole = || {
            vec![Partition {
                id: crate::PartitionId(format!("{}-0", entity.name())),
                entity: entity.name().to_owned(),
                scope: PartitionScope::Whole,
            }]
        };
        let [pk] = entity.primary_key() else {
            return Ok(whole());
        };
        let mut keys = Vec::with_capacity(rows.num_rows());
        let column = rows
            .column_by_name(pk)
            .expect("validated against the schema");
        for row in 0..rows.num_rows() {
            match crate::KeyValue::from_array(column.as_ref(), row)? {
                crate::KeyValue::Int(k) => keys.push(k),
                _ => return Ok(whole()),
            }
        }
        keys.sort_unstable();
        keys.dedup();

        // Every `target_rows`-th key starts a new range; the outermost ranges
        // are unbounded so keys inserted later still land somewhere.
        let step = usize::try_from(hint.target_rows.max(1)).unwrap_or(usize::MAX);
        let starts: Vec<i64> = keys.iter().copied().step_by(step).skip(1).collect();
        let mut bounds = vec![None];
        bounds.extend(starts.into_iter().map(Some));
        bounds.push(None);
        bounds
            .windows(2)
            .enumerate()
            .map(|(i, w)| {
                let range = KeyRange::new(
                    pk.clone(),
                    w[0].map(crate::KeyValue::Int),
                    w[1].map(crate::KeyValue::Int),
                )?;
                Ok(Partition {
                    id: crate::PartitionId(format!("{}-{i}", entity.name())),
                    entity: entity.name().to_owned(),
                    scope: PartitionScope::KeyRange(range),
                })
            })
            .collect()
    }

    async fn read(&self, entity: &Entity, partition: &Partition) -> Result<BatchStream> {
        let rows = self.rows(entity.name())?;
        let selected = match &partition.scope {
            PartitionScope::Whole => rows.clone(),
            PartitionScope::KeyRange(range) => {
                let column = rows
                    .column_by_name(range.column())
                    .ok_or_else(|| connector_error(format!("no column `{}`", range.column())))?;
                let mask = (0..rows.num_rows())
                    .map(|row| {
                        crate::KeyValue::from_array(column.as_ref(), row)
                            .map(|k| range.contains(&k))
                    })
                    .collect::<Result<BooleanArray>>()?;
                arrow_select::filter::filter_record_batch(rows, &mask)
                    .map_err(|e| connector_error(e.to_string()))?
            }
        };
        let mut batches: Vec<Result<RecordBatch>> = (0..selected.num_rows())
            .step_by(self.batch_rows)
            .map(|offset| {
                let len = self.batch_rows.min(selected.num_rows() - offset);
                Ok(selected.slice(offset, len))
            })
            .collect();
        if let Some(n) = self.fail_after_batches {
            batches.truncate(n);
            batches.push(Err(connector_error(format!(
                "injected read failure after {n} batches"
            ))));
        }
        Ok(stream::iter(batches).boxed())
    }

    async fn capabilities(&self) -> Result<SourceCaps> {
        Ok(SourceCaps {
            range_scan: true,
            predicate_pushdown: false,
            consistent_snapshot: true,
        })
    }
}
