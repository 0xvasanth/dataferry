use std::sync::Arc;

use arrow_array::{Array, ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use dataferry_core::missing::{MissingPathsBuilder, is_missing, missing_paths_field};

/// Three orders: `deleted_at` explicitly null, absent, and set.
fn orders_batch() -> RecordBatch {
    let deleted_at: ArrayRef = Arc::new(StringArray::from(vec![None, None, Some("2026-01-01")]));
    let mut missing = MissingPathsBuilder::new();
    missing.append_row::<&str>(&[]);
    missing.append_row(&["deleted_at", "customer.email"]);
    missing.append_row::<&str>(&[]);

    let schema = Schema::new(vec![
        Field::new("deleted_at", DataType::Utf8, true),
        missing_paths_field(),
    ]);
    RecordBatch::try_new(Arc::new(schema), vec![deleted_at, missing.finish()]).unwrap()
}

#[test]
fn missing_and_explicit_null_are_distinguishable() {
    let batch = orders_batch();
    let values = batch.column(0);

    // Both rows are null in the value column...
    assert!(values.is_null(0) && values.is_null(1));
    // ...but only row 1 was absent from the source document.
    assert!(!is_missing(&batch, 0, "deleted_at"));
    assert!(is_missing(&batch, 1, "deleted_at"));
    assert!(!is_missing(&batch, 2, "deleted_at"));
}

#[test]
fn nested_paths_are_tracked_by_full_path() {
    let batch = orders_batch();
    assert!(is_missing(&batch, 1, "customer.email"));
    assert!(
        !is_missing(&batch, 1, "customer"),
        "a parent path is not missing just because a child is"
    );
    assert!(
        !is_missing(&batch, 1, "email"),
        "matching is on the full path, not a suffix"
    );
}

#[test]
fn batches_without_the_column_have_no_missing_fields() {
    let col: ArrayRef = Arc::new(StringArray::from(vec![None::<&str>]));
    let schema = Schema::new(vec![Field::new("deleted_at", DataType::Utf8, true)]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![col]).unwrap();
    assert!(!is_missing(&batch, 0, "deleted_at"));
}
