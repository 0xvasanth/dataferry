//! Logical entities: what a source exposes (a table, a collection, a file glob).

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};

use crate::error::{FerryError, Result};
use crate::missing::missing_paths_field;
use crate::partition::KeyValue;
use crate::transform::SourceRecordId;
use crate::types::CanonicalField;

/// Column names starting with this prefix are reserved for the engine
/// (e.g. `_ferry_missing`, `_ferry_extra`).
pub const RESERVED_PREFIX: &str = "_ferry_";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entity {
    name: String,
    fields: Vec<CanonicalField>,
    primary_key: Vec<String>,
    natural_keys: Vec<Vec<String>>,
    ordering_column: Option<String>,
    tracks_missing: bool,
}

impl Entity {
    /// An entity must have a primary key: without one it can't be
    /// partitioned by range or re-run idempotently.
    pub fn new(
        name: impl Into<String>,
        fields: Vec<CanonicalField>,
        primary_key: Vec<String>,
    ) -> Result<Self> {
        let entity = Self {
            name: name.into(),
            fields,
            primary_key,
            natural_keys: Vec::new(),
            ordering_column: None,
            tracks_missing: false,
        };
        for (i, field) in entity.fields.iter().enumerate() {
            if field.name.starts_with(RESERVED_PREFIX) {
                return Err(entity.invalid(format!(
                    "field `{}` uses the reserved prefix `{RESERVED_PREFIX}`",
                    field.name
                )));
            }
            if entity.fields[..i].iter().any(|f| f.name == field.name) {
                return Err(entity.invalid(format!("field `{}` is declared twice", field.name)));
            }
        }
        if entity.primary_key.is_empty() {
            return Err(entity
                .invalid("a primary key is required to partition and re-run idempotently".into()));
        }
        entity.check_columns_exist(&entity.primary_key)?;
        Ok(entity)
    }

    pub fn with_natural_key(mut self, columns: Vec<String>) -> Result<Self> {
        self.check_columns_exist(&columns)?;
        self.natural_keys.push(columns);
        Ok(self)
    }

    /// The column whose value orders rows for checkpoints within a partition.
    pub fn with_ordering_column(mut self, column: impl Into<String>) -> Result<Self> {
        let column = column.into();
        self.check_columns_exist(std::slice::from_ref(&column))?;
        self.ordering_column = Some(column);
        Ok(self)
    }

    fn check_columns_exist(&self, columns: &[String]) -> Result<()> {
        match columns.iter().find(|c| self.field(c).is_none()) {
            Some(c) => Err(self.invalid(format!("key column `{c}` is not a field"))),
            None => Ok(()),
        }
    }

    fn invalid(&self, reason: String) -> FerryError {
        FerryError::InvalidEntity {
            entity: self.name.clone(),
            reason,
        }
    }

    /// Marks this entity as coming from a source that can omit fields, so its
    /// batches carry the missing-paths column.
    pub fn with_missing_tracking(mut self) -> Self {
        self.tracks_missing = true;
        self
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn fields(&self) -> &[CanonicalField] {
        &self.fields
    }

    pub fn primary_key(&self) -> &[String] {
        &self.primary_key
    }

    pub fn natural_keys(&self) -> &[Vec<String>] {
        &self.natural_keys
    }

    pub fn ordering_column(&self) -> Option<&str> {
        self.ordering_column.as_deref()
    }

    pub fn tracks_missing(&self) -> bool {
        self.tracks_missing
    }

    pub fn field(&self, name: &str) -> Option<&CanonicalField> {
        self.fields.iter().find(|f| f.name == name)
    }

    /// The id of every row in `batch`, from its primary-key columns.
    pub fn record_ids(&self, batch: &RecordBatch) -> Result<Vec<SourceRecordId>> {
        let columns = self
            .primary_key
            .iter()
            .map(|c| {
                batch
                    .column_by_name(c)
                    .ok_or_else(|| FerryError::InvalidKey {
                        reason: format!("batch has no primary key column `{c}`"),
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        (0..batch.num_rows())
            .map(|row| {
                columns
                    .iter()
                    .map(|col| KeyValue::from_array(col.as_ref(), row))
                    .collect::<Result<_>>()
                    .map(SourceRecordId)
            })
            .collect()
    }

    /// The Arrow schema of batches read from this entity.
    pub fn arrow_schema(&self) -> SchemaRef {
        let mut fields: Vec<_> = self.fields.iter().map(CanonicalField::to_arrow).collect();
        if self.tracks_missing {
            fields.push(missing_paths_field());
        }
        Arc::new(Schema::new(fields))
    }
}
