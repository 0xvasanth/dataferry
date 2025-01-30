use arrow_schema::DataType;
use ferry_core::missing::MISSING_PATHS_COLUMN;
use ferry_core::types::{CanonicalField, CanonicalType};
use ferry_core::{Entity, FerryError};

fn orders_fields() -> Vec<CanonicalField> {
    vec![
        CanonicalField::new("_id", CanonicalType::Utf8, false),
        CanonicalField::new("order_no", CanonicalType::Int64, false),
        CanonicalField::new("created_at", CanonicalType::Timestamp { tz: None }, true),
    ]
}

fn reason(err: FerryError) -> String {
    match err {
        FerryError::InvalidEntity { reason, .. } => reason,
        other => panic!("expected InvalidEntity, got {other:?}"),
    }
}

#[test]
fn entity_exposes_keys_and_ordering_column() {
    let e = Entity::new("orders", orders_fields(), vec!["_id".into()])
        .unwrap()
        .with_natural_key(vec!["order_no".into()])
        .unwrap()
        .with_ordering_column("created_at")
        .unwrap();

    assert_eq!(e.name(), "orders");
    assert_eq!(e.primary_key(), ["_id"]);
    assert_eq!(e.natural_keys(), [vec!["order_no".to_string()]]);
    assert_eq!(e.ordering_column(), Some("created_at"));
}

#[test]
fn entity_without_primary_key_is_rejected() {
    let err = Entity::new("orders", orders_fields(), vec![]).unwrap_err();
    assert!(reason(err).contains("primary key"));
}

#[test]
fn keys_must_name_existing_columns() {
    let err = Entity::new("orders", orders_fields(), vec!["id".into()]).unwrap_err();
    assert!(reason(err).contains("`id`"));

    let e = Entity::new("orders", orders_fields(), vec!["_id".into()]).unwrap();
    let err = e.clone().with_natural_key(vec!["sku".into()]).unwrap_err();
    assert!(reason(err).contains("`sku`"));
    let err = e.with_ordering_column("updated_at").unwrap_err();
    assert!(reason(err).contains("`updated_at`"));
}

#[test]
fn duplicate_field_names_are_rejected() {
    let mut fields = orders_fields();
    fields.push(CanonicalField::new("order_no", CanonicalType::Utf8, true));
    let err = Entity::new("orders", fields, vec!["_id".into()]).unwrap_err();
    assert!(reason(err).contains("`order_no`"));
}

#[test]
fn fields_using_the_reserved_prefix_are_rejected() {
    let mut fields = orders_fields();
    fields.push(CanonicalField::new(
        "_ferry_extra",
        CanonicalType::Json,
        true,
    ));
    let err = Entity::new("orders", fields, vec!["_id".into()]).unwrap_err();
    assert!(reason(err).contains("reserved"));
}

#[test]
fn arrow_schema_adds_missing_paths_column_only_when_tracking_missing() {
    let relational = Entity::new("orders", orders_fields(), vec!["_id".into()]).unwrap();
    let names: Vec<_> = relational
        .arrow_schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(names, ["_id", "order_no", "created_at"]);

    let document = relational.with_missing_tracking();
    let schema = document.arrow_schema();
    let missing = schema.field_with_name(MISSING_PATHS_COLUMN).unwrap();
    assert!(
        matches!(missing.data_type(), DataType::List(item) if item.data_type() == &DataType::Utf8)
    );
    assert_eq!(schema.fields().len(), 4);
}
