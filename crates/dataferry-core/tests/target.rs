use dataferry_core::types::{CanonicalField, CanonicalType};
use dataferry_core::{FerryError, ForeignKey, TargetSchema, TargetTable};

fn table(name: &str, cols: &[&str], pk: &[&str], fks: Vec<ForeignKey>) -> TargetTable {
    TargetTable {
        name: name.into(),
        fields: cols
            .iter()
            .map(|c| CanonicalField::new(*c, CanonicalType::Utf8, true))
            .collect(),
        primary_key: pk.iter().map(|c| c.to_string()).collect(),
        foreign_keys: fks,
    }
}

fn fk(cols: &[&str], table: &str, refs: &[&str]) -> ForeignKey {
    ForeignKey {
        columns: cols.iter().map(|c| c.to_string()).collect(),
        references_table: table.into(),
        references_columns: refs.iter().map(|c| c.to_string()).collect(),
    }
}

fn orders() -> TargetTable {
    table("orders", &["id", "status"], &["id"], vec![])
}

fn invalid(tables: Vec<TargetTable>) -> (String, String) {
    match TargetSchema::new(tables) {
        Err(FerryError::InvalidTargetSchema { table, reason }) => (table, reason),
        other => panic!("expected InvalidTargetSchema, got {other:?}"),
    }
}

#[test]
fn valid_parent_and_child_tables_are_accepted() {
    let items = table(
        "order_items",
        &["id", "order_id"],
        &["id"],
        vec![fk(&["order_id"], "orders", &["id"])],
    );
    let schema = TargetSchema::new(vec![orders(), items]).unwrap();
    assert_eq!(schema.table("order_items").unwrap().foreign_keys.len(), 1);
}

#[test]
fn foreign_key_to_unknown_table_is_rejected() {
    let items = table(
        "order_items",
        &["id", "order_id"],
        &["id"],
        vec![fk(&["order_id"], "order", &["id"])],
    );
    let (t, reason) = invalid(vec![orders(), items]);
    assert_eq!(t, "order_items");
    assert!(reason.contains("`order`"), "{reason}");
}

#[test]
fn foreign_key_to_unknown_column_is_rejected() {
    let items = table(
        "order_items",
        &["id", "order_id"],
        &["id"],
        vec![fk(&["order_id"], "orders", &["uuid"])],
    );
    let (_, reason) = invalid(vec![orders(), items]);
    assert!(reason.contains("`uuid`"), "{reason}");
}

#[test]
fn foreign_key_column_count_must_match() {
    let items = table(
        "order_items",
        &["id", "order_id"],
        &["id"],
        vec![fk(&["order_id"], "orders", &["id", "status"])],
    );
    let (_, reason) = invalid(vec![orders(), items]);
    assert!(reason.contains("columns"), "{reason}");
}

#[test]
fn primary_key_must_name_existing_columns() {
    let (t, reason) = invalid(vec![table("orders", &["id"], &["order_id"], vec![])]);
    assert_eq!(t, "orders");
    assert!(reason.contains("`order_id`"), "{reason}");
}

#[test]
fn duplicate_table_names_are_rejected() {
    let (t, _) = invalid(vec![orders(), orders()]);
    assert_eq!(t, "orders");
}
