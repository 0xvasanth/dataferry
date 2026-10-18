//! Source rows that don't fit their entity (A7).
//!
//! The [`Source`] contract has no per-row error channel: a read yields
//! batches matching [`Entity::arrow_schema`], or fails. A schemaless source
//! (Mongo) can meet a document that doesn't fit the schema it inferred at
//! discovery, and failing the read would fail the whole partition over one
//! document. Instead the source emits a *rejected batch*: one row per such
//! document, holding the entity's primary-key columns, why the document
//! doesn't fit, and the document itself as JSON. A metadata key on the
//! schema marks it, so the runner tells it apart from data without
//! reserving a column name.
//!
//! The runner never hands a rejected batch to a plan. Under `on_error:
//! dead_letter` each row is dead-lettered like a record the plan rejected;
//! under `on_error: fail` the first one fails the partition. Only the
//! driver input of a flow may yield them, since only driver records can be
//! dead-lettered.
//!
//! [`Source`]: dataferry_core::Source

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use dataferry_core::{Entity, FerryError, Result, SourceRecordId};

/// The schema metadata key that marks a rejected batch.
pub const REJECTED_METADATA: &str = "ferry.rejected";
/// Why the row's record doesn't fit.
pub const ERROR_COLUMN: &str = "_ferry_error";
/// The record as JSON, or null if it couldn't be rendered.
pub const RECORD_COLUMN: &str = "_ferry_record";

/// One record a source couldn't read as its entity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RejectedRow {
    pub id: SourceRecordId,
    pub error: String,
    pub record: Option<String>,
}

fn invalid(entity: &Entity, reason: String) -> FerryError {
    FerryError::InvalidEntity {
        entity: entity.name().to_owned(),
        reason,
    }
}

/// A rejected batch of `entity`: `keys` holds one column per primary-key
/// column, in order, typed as the entity types it; `errors` and `records`
/// one entry per row.
pub fn rejected_batch(
    entity: &Entity,
    keys: Vec<ArrayRef>,
    errors: Vec<String>,
    records: Vec<Option<String>>,
) -> Result<RecordBatch> {
    let mut fields = Vec::new();
    for name in entity.primary_key() {
        let field = entity
            .field(name)
            .ok_or_else(|| invalid(entity, format!("key column `{name}` is not a field")))?;
        fields.push(field.to_arrow());
    }
    fields.push(Field::new(ERROR_COLUMN, DataType::Utf8, false));
    fields.push(Field::new(RECORD_COLUMN, DataType::Utf8, true));
    let schema = Schema::new(fields).with_metadata(HashMap::from([(
        REJECTED_METADATA.to_owned(),
        "true".to_owned(),
    )]));
    let mut columns = keys;
    columns.push(Arc::new(StringArray::from(errors)));
    columns.push(Arc::new(StringArray::from(records)));
    RecordBatch::try_new(Arc::new(schema), columns)
        .map_err(|e| invalid(entity, format!("rejected batch: {e}")))
}

/// Whether `batch` is a rejected batch rather than rows of its entity.
pub fn is_rejected(batch: &RecordBatch) -> bool {
    batch
        .schema_ref()
        .metadata()
        .get(REJECTED_METADATA)
        .is_some_and(|v| v == "true")
}

/// The rows of a rejected batch of `entity`.
pub fn rejected_rows(entity: &Entity, batch: &RecordBatch) -> Result<Vec<RejectedRow>> {
    let column = |name: &str| {
        batch
            .column_by_name(name)
            .and_then(|c| c.as_string_opt::<i32>())
            .ok_or_else(|| {
                invalid(
                    entity,
                    format!("rejected batch has no text column `{name}`"),
                )
            })
    };
    let errors = column(ERROR_COLUMN)?;
    let records = column(RECORD_COLUMN)?;
    let ids = entity.record_ids(batch)?;
    Ok(ids
        .into_iter()
        .enumerate()
        .map(|(row, id)| RejectedRow {
            id,
            error: errors.value(row).to_owned(),
            record: records.is_valid(row).then(|| records.value(row).to_owned()),
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use arrow_array::Int64Array;
    use dataferry_core::KeyValue;
    use dataferry_core::types::{CanonicalField, CanonicalType};

    use super::*;

    fn readings() -> Entity {
        Entity::new(
            "readings",
            vec![
                CanonicalField::new("id", CanonicalType::Int64, false),
                CanonicalField::new("n", CanonicalType::Int32, true),
            ],
            vec!["id".into()],
        )
        .unwrap()
    }

    #[test]
    fn a_rejected_batch_round_trips_and_is_told_apart_from_data() {
        let e = readings();
        let batch = rejected_batch(
            &e,
            vec![Arc::new(Int64Array::from(vec![100, 101]))],
            vec![
                "`n` holds String".into(),
                "field `extra` is not in the schema".into(),
            ],
            vec![Some(r#"{"_id":100,"n":"late"}"#.into()), None],
        )
        .unwrap();
        assert!(is_rejected(&batch));
        let rows = rejected_rows(&e, &batch).unwrap();
        assert_eq!(
            rows,
            vec![
                RejectedRow {
                    id: SourceRecordId(vec![KeyValue::Int(100)]),
                    error: "`n` holds String".into(),
                    record: Some(r#"{"_id":100,"n":"late"}"#.into()),
                },
                RejectedRow {
                    id: SourceRecordId(vec![KeyValue::Int(101)]),
                    error: "field `extra` is not in the schema".into(),
                    record: None,
                },
            ]
        );

        // Data of the entity, even with columns of the same names, isn't.
        let data = RecordBatch::new_empty(e.arrow_schema());
        assert!(!is_rejected(&data));
    }

    #[test]
    fn key_columns_must_match_the_entity() {
        let e = readings();
        let err = rejected_batch(
            &e,
            vec![Arc::new(StringArray::from(vec!["100"]))],
            vec!["bad".into()],
            vec![None],
        )
        .unwrap_err();
        assert!(err.to_string().contains("rejected batch"), "{err}");
    }
}
