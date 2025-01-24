//! Distinguishing a *missing* field from an explicit `null`.
//!
//! Document sources can omit a field entirely or set it to `null`, and a
//! mapping may treat those differently. Arrow has only one null, so batches
//! from such sources carry one reserved column, [`MISSING_PATHS_COLUMN`]: a
//! `List<Utf8>` holding, per row, the dotted paths that were absent. For a
//! listed path the value column is null; a null value whose path is *not*
//! listed was an explicit `null` in the source.
//!
//! Batches without the column (relational sources) never have missing fields.

use std::sync::Arc;

use arrow_array::builder::{ListBuilder, StringBuilder};
use arrow_array::{Array, ArrayRef, ListArray, RecordBatch, StringArray};
use arrow_schema::{DataType, Field};

pub const MISSING_PATHS_COLUMN: &str = "_ferry_missing";

pub fn missing_paths_field() -> Field {
    Field::new(
        MISSING_PATHS_COLUMN,
        DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, false))),
        false,
    )
}

/// Builds the [`MISSING_PATHS_COLUMN`] array one row at a time.
pub struct MissingPathsBuilder {
    inner: ListBuilder<StringBuilder>,
}

impl Default for MissingPathsBuilder {
    fn default() -> Self {
        // The item field must match `missing_paths_field` exactly, or a batch
        // built from this array fails schema validation.
        let item = Field::new_list_field(DataType::Utf8, false);
        Self {
            inner: ListBuilder::new(StringBuilder::new()).with_field(item),
        }
    }
}

impl MissingPathsBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Records the paths absent from the next row (empty if none were).
    pub fn append_row<S: AsRef<str>>(&mut self, missing: &[S]) {
        for path in missing {
            self.inner.values().append_value(path.as_ref());
        }
        self.inner.append(true);
    }

    pub fn finish(&mut self) -> ArrayRef {
        Arc::new(self.inner.finish())
    }
}

/// Whether `path` was absent (rather than null or present) in `row`.
pub fn is_missing(batch: &RecordBatch, row: usize, path: &str) -> bool {
    let Some(column) = batch.column_by_name(MISSING_PATHS_COLUMN) else {
        return false;
    };
    let Some(lists) = column.as_any().downcast_ref::<ListArray>() else {
        return false;
    };
    if lists.is_null(row) {
        return false;
    }
    let paths = lists.value(row);
    let Some(paths) = paths.as_any().downcast_ref::<StringArray>() else {
        return false;
    };
    paths.iter().any(|p| p == Some(path))
}
