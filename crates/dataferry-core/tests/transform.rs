use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use dataferry_core::missing::{MISSING_PATHS_COLUMN, MissingPathsBuilder};
use dataferry_core::types::{CanonicalField, CanonicalType};
use dataferry_core::{
    BatchStream, Entity, FerryError, IdentityPlan, KeyValue, PartitionInputs, RejectedRecord,
    SourceRecordId, TargetBatch, TransformOutput, TransformPlan,
};
use futures::{StreamExt, TryStreamExt, stream};

fn users(tracks_missing: bool) -> Entity {
    let e = Entity::new(
        "users",
        vec![
            CanonicalField::new("id", CanonicalType::Int64, false),
            CanonicalField::new("email", CanonicalType::Utf8, true),
        ],
        vec!["id".into()],
    )
    .unwrap();
    if tracks_missing {
        e.with_missing_tracking()
    } else {
        e
    }
}

fn users_batch(entity: &Entity, ids: &[i64]) -> RecordBatch {
    let id: ArrayRef = Arc::new(Int64Array::from(ids.to_vec()));
    let email: ArrayRef = Arc::new(StringArray::from(
        ids.iter()
            .map(|i| format!("u{i}@x.dev"))
            .collect::<Vec<_>>(),
    ));
    let mut columns = vec![id, email];
    if entity.tracks_missing() {
        let mut m = MissingPathsBuilder::new();
        for _ in ids {
            m.append_row::<&str>(&[]);
        }
        columns.push(m.finish());
    }
    RecordBatch::try_new(entity.arrow_schema(), columns).unwrap()
}

fn inputs(entity: &Entity, batches: Vec<RecordBatch>) -> PartitionInputs {
    let s: BatchStream = stream::iter(batches.into_iter().map(Ok)).boxed();
    PartitionInputs::new(vec![(entity.name().to_owned(), s)])
}

async fn run(plan: &IdentityPlan, inputs: PartitionInputs) -> Vec<TransformOutput> {
    plan.execute(inputs).unwrap().try_collect().await.unwrap()
}

fn id(v: i64) -> SourceRecordId {
    SourceRecordId(vec![KeyValue::Int(v)])
}

#[tokio::test]
async fn identity_copies_every_row_to_its_one_target() {
    let entity = users(false);
    let plan = IdentityPlan::new(entity.clone(), "customers");
    let out = run(
        &plan,
        inputs(
            &entity,
            vec![users_batch(&entity, &[1, 2, 3]), users_batch(&entity, &[4])],
        ),
    )
    .await;

    assert_eq!(out.len(), 2, "one output per input batch");
    assert_eq!(out[0].records, [id(1), id(2), id(3)]);
    assert_eq!(out[1].records, [id(4)]);
    for o in &out {
        assert!(o.rejected.is_empty());
        assert_eq!(o.targets.len(), 1);
        assert_eq!(o.targets[0].target, "customers");
        o.validate(plan.write_order()).unwrap();
    }
    assert_eq!(out[0].targets[0].source_index, [0, 1, 2]);
    assert_eq!(out[0].targets[0].batch, users_batch(&entity, &[1, 2, 3]));
}

#[tokio::test]
async fn identity_drops_the_missing_paths_column() {
    let entity = users(true);
    let plan = IdentityPlan::new(entity.clone(), "customers");
    let out = run(&plan, inputs(&entity, vec![users_batch(&entity, &[1])])).await;

    let schema = out[0].targets[0].batch.schema();
    let names: Vec<_> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(names, ["id", "email"]);
    assert!(schema.field_with_name(MISSING_PATHS_COLUMN).is_err());
}

#[test]
fn identity_without_its_input_stream_fails() {
    let plan = IdentityPlan::new(users(false), "customers");
    match plan.execute(PartitionInputs::new(vec![])) {
        Err(FerryError::MissingInput { entity }) => assert_eq!(entity, "users"),
        Err(e) => panic!("expected MissingInput, got {e}"),
        Ok(_) => panic!("expected MissingInput, got a stream"),
    }
}

/// An output for records 1..=3 of orders where record 2 failed.
fn output_with_rejection(item_lineage: Vec<u32>) -> TransformOutput {
    let entity = users(false);
    let order_rows = users_batch(&entity, &[1, 3]);
    let item_rows = users_batch(&entity, &vec![9; item_lineage.len()]);
    TransformOutput {
        records: vec![id(1), id(2), id(3)],
        targets: vec![
            TargetBatch {
                target: "orders".into(),
                batch: order_rows,
                source_index: vec![0, 2],
            },
            TargetBatch {
                target: "order_items".into(),
                batch: item_rows,
                source_index: item_lineage,
            },
        ],
        rejected: vec![RejectedRecord {
            index: 1,
            target: Some("order_items".into()),
            error: "qty is not an integer".into(),
        }],
    }
}

fn order() -> Vec<String> {
    vec!["orders".into(), "order_items".into()]
}

fn invalid_reason(out: &TransformOutput, write_order: &[String]) -> String {
    match out.validate(write_order) {
        Err(FerryError::InvalidTransformOutput { reason }) => reason,
        other => panic!("expected InvalidTransformOutput, got {other:?}"),
    }
}

#[test]
fn rejected_records_keep_their_source_id() {
    let out = output_with_rejection(vec![0, 0, 2]);
    out.validate(&order()).unwrap();

    let rejected: Vec<_> = out.rejected_records().collect();
    assert_eq!(rejected.len(), 1);
    assert_eq!(rejected[0].0, &id(2));
    assert_eq!(rejected[0].1.error, "qty is not an integer");
}

#[test]
fn a_rejected_record_with_rows_in_any_target_breaks_atomicity() {
    // Record 2 (index 1) is rejected but still has a child row.
    let out = output_with_rejection(vec![0, 1, 2]);
    let reason = invalid_reason(&out, &order());
    assert!(
        reason.contains("rejected") && reason.contains("order_items"),
        "{reason}"
    );
}

#[test]
fn lineage_must_have_one_in_range_entry_per_row() {
    let reason = invalid_reason(&output_with_rejection(vec![0, 7, 2]), &order());
    assert!(reason.contains("7"), "{reason}");

    let reason = invalid_reason(
        &output_with_rejection(vec![0, 2]).with_extra_item_row(),
        &order(),
    );
    assert!(reason.contains("lineage"), "{reason}");
}

#[test]
fn targets_must_follow_the_write_order() {
    let out = output_with_rejection(vec![0, 2]);
    let reversed = vec!["order_items".to_string(), "orders".to_string()];
    let reason = invalid_reason(&out, &reversed);
    assert!(reason.contains("order"), "{reason}");

    let reason = invalid_reason(&out, &["orders".to_string()]);
    assert!(
        reason.contains("order_items"),
        "unknown target should be named: {reason}"
    );
}

#[test]
fn rejected_index_must_be_in_range() {
    let mut out = output_with_rejection(vec![0, 2]);
    out.rejected[0].index = 3;
    let reason = invalid_reason(&out, &order());
    assert!(reason.contains("3"), "{reason}");
}

trait ExtraRow {
    fn with_extra_item_row(self) -> Self;
}

impl ExtraRow for TransformOutput {
    /// Adds a row to `order_items` without adding a lineage entry for it.
    fn with_extra_item_row(mut self) -> Self {
        let entity = users(false);
        let items = &mut self.targets[1];
        let n = items.batch.num_rows() + 1;
        items.batch = users_batch(&entity, &vec![9; n]);
        self
    }
}
