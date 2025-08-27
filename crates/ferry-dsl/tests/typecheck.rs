//! The type checker (B3, 0XT-260): flows checked against discovered
//! entities, the lossy-cast table and its opt-in, nearest-field
//! suggestions, and the errors for the most common mistakes.

mod common;

use std::path::Path;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, ArrayRef, RecordBatch, StringArray};
use common::golden;
use ferry_core::Entity;
use ferry_core::types::{CanonicalField, CanonicalType};
use ferry_dsl::config::{ConfigErrors, ErrorKind, load};
use ferry_dsl::expr::convert::to_canonical;
use ferry_dsl::typecheck::cast::LOSSY;
use ferry_dsl::typecheck::{Catalog, CheckedFlow, ColumnValue, check};
use ferry_dsl::types;

/// A `ferry/` directory with one flow file, `flows/f.yaml`.
fn project(flows: &str) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let write = |path: &str, text: &str| {
        let path = dir.path().join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, text).unwrap();
    };
    write("ferry.yaml", "version: 1\nname: t\n");
    write(
        "connections.yaml",
        "connections:\n  - name: shop\n    type: mongo\n    url: x\n  - name: warehouse\n    type: postgres\n    url: y\n",
    );
    write("flows/f.yaml", flows);
    dir
}

fn golden_catalog() -> Catalog {
    Catalog::new().with("shop", golden::entity())
}

fn check_dir(dir: &Path, catalog: &Catalog) -> Result<Vec<CheckedFlow>, ConfigErrors> {
    let config = load(dir).unwrap_or_else(|e| panic!("{e}"));
    check(&config, catalog)
}

fn check_flows(flows: &str, catalog: &Catalog) -> Result<Vec<CheckedFlow>, ConfigErrors> {
    let dir = project(flows);
    check_dir(dir.path(), catalog)
}

/// A flow from the golden entity into `orders` with these columns.
fn orders_flow(extra: &str, columns: &str) -> String {
    format!(
        "flows:\n  - name: orders\n    from: shop.orders\n    sink: warehouse\n{extra}    to:\n      orders:\n        columns:\n{columns}"
    )
}

fn errors_of(result: Result<Vec<CheckedFlow>, ConfigErrors>) -> ConfigErrors {
    match result {
        Ok(_) => panic!("expected type errors"),
        Err(e) => e,
    }
}

// ---------------------------------------------------------------- the examples

#[test]
fn the_golden_config_type_checks() {
    let flows = check_dir(&common::repo("examples/golden/ferry"), &golden_catalog())
        .unwrap_or_else(|e| panic!("{e}"));
    let [flow] = flows.as_slice() else { panic!() };
    assert_eq!(flow.filter.len(), 1);
    assert_eq!(flow.filter[0].name.as_deref(), Some("not_deleted"));
    let tables: Vec<&str> = flow.targets.iter().map(|t| t.table.as_str()).collect();
    assert_eq!(tables, ["orders", "order_items"], "parents first");

    let shape = |table: usize| -> Vec<(String, String, bool)> {
        flow.targets[table]
            .columns
            .iter()
            .map(|c| {
                (
                    c.field.name.clone(),
                    types::name(&c.field.ty),
                    c.field.nullable,
                )
            })
            .collect()
    };
    let owned = |v: &[(&str, &str, bool)]| -> Vec<(String, String, bool)> {
        v.iter()
            .map(|(a, b, n)| (a.to_string(), b.to_string(), *n))
            .collect()
    };
    assert_eq!(
        shape(0),
        owned(&[
            ("id", "utf8", false),
            ("customer_id", "utf8", true),
            ("status", "utf8", true),
            ("placed_at", "timestamp(UTC)", true),
            ("total_cents", "int64", true),
        ])
    );
    assert_eq!(
        shape(1),
        owned(&[
            ("order_id", "utf8", false),
            ("line_no", "int64", false),
            ("sku", "utf8", true),
            ("qty", "int64", true),
            ("unit_cents", "int64", true),
        ])
    );
}

#[test]
fn the_golden_orders_columns_compute_the_expected_rows() {
    let flows = check_dir(&common::repo("examples/golden/ferry"), &golden_catalog()).unwrap();
    let flow = &flows[0];
    let batch = golden::batch();
    let keep = flow.filter[0].expr.evaluate(&batch).unwrap();
    let keep = keep.as_boolean();
    let docs = golden::documents();
    let rows: Vec<usize> = (0..batch.num_rows())
        .filter(|&i| keep.value(i) && golden::id(&docs[i]) != golden::DEAD_LETTERED)
        .collect();

    let orders = &flow.targets[0];
    let value = |name: &str| -> ArrayRef {
        match &orders.column(name).unwrap().value {
            ColumnValue::Expr(e) => e.evaluate(&batch).unwrap(),
            other => panic!("{other:?}"),
        }
    };
    let (ids, customers, statuses, placed, cents) = (
        value("id"),
        value("customer_id"),
        value("status"),
        value("placed_at"),
        value("total_cents"),
    );
    let text =
        std::fs::read_to_string(common::repo("fixtures/golden/expected/orders.json")).unwrap();
    let expected: Vec<serde_json::Value> = serde_json::from_str(&text).unwrap();
    assert_eq!(rows.len(), expected.len());
    for (&i, want) in rows.iter().zip(&expected) {
        let text_at =
            |a: &ArrayRef| (!a.is_null(i)).then(|| a.as_string::<i32>().value(i).to_owned());
        assert_eq!(text_at(&ids).as_deref(), want["id"].as_str());
        assert_eq!(text_at(&customers).as_deref(), want["customer_id"].as_str());
        assert_eq!(text_at(&statuses).as_deref(), want["status"].as_str());
        assert_eq!(
            cents.as_primitive::<Int64Type>().value(i),
            want["total_cents"].as_i64().unwrap()
        );
        assert_eq!(
            placed.is_null(i),
            want["placed_at"].is_null(),
            "{}",
            want["id"]
        );
    }
}

fn pg_merge_catalog() -> Catalog {
    let utc = CanonicalType::Timestamp {
        tz: Some("UTC".into()),
    };
    let f = |n: &str, t: CanonicalType, null: bool| CanonicalField::new(n, t, null);
    let customers = Entity::new(
        "public.customers",
        vec![
            f("id", CanonicalType::Int64, false),
            f("name", CanonicalType::Utf8, false),
            f("email", CanonicalType::Utf8, true),
        ],
        vec!["id".into()],
    )
    .unwrap();
    let orders = Entity::new(
        "public.orders",
        vec![
            f("id", CanonicalType::Int64, false),
            f("customer_id", CanonicalType::Int64, true),
            f("status", CanonicalType::Utf8, false),
            f("placed_at", utc, false),
            f("total_cents", CanonicalType::Int64, false),
        ],
        vec!["id".into()],
    )
    .unwrap();
    let items = Entity::new(
        "public.order_items",
        vec![
            f("order_id", CanonicalType::Int64, false),
            f("line_no", CanonicalType::Int32, false),
            f("sku", CanonicalType::Utf8, false),
            f("qty", CanonicalType::Int32, false),
            f("unit_cents", CanonicalType::Int64, false),
        ],
        vec!["order_id".into(), "line_no".into()],
    )
    .unwrap();
    Catalog::new()
        .with("app", customers)
        .with("app", orders)
        .with("app", items)
}

#[test]
fn the_pg_merge_config_type_checks_joins_and_nests() {
    let flows = check_dir(
        &common::repo("examples/pg-merge/ferry"),
        &pg_merge_catalog(),
    )
    .unwrap_or_else(|e| panic!("{e}"));
    let summaries = flows.iter().find(|f| f.name == "order_summaries").unwrap();
    let target = &summaries.targets[0];
    assert_eq!(
        types::name(&target.column("customer_name").unwrap().field.ty),
        "utf8"
    );
    let items = target.column("items").unwrap();
    assert_eq!(
        types::name(&items.field.ty),
        "list(struct(sku: utf8, qty: int32, unit_cents: int64))"
    );
    assert!(!items.field.nullable, "no matches is [], not null");
    let ColumnValue::Nest {
        alias,
        order_by,
        filter,
        ..
    } = &items.value
    else {
        panic!()
    };
    assert_eq!((alias.as_str(), order_by.len(), filter.len()), ("i", 1, 1));

    // `columns: "*"` copies the entity's fields.
    let customers = flows.iter().find(|f| f.name == "customers").unwrap();
    assert_eq!(customers.targets[0].columns.len(), 3);
    assert!(matches!(
        customers.targets[0].columns[0].value,
        ColumnValue::Copy
    ));
}

// ---------------------------------------------------------------- lossy casts

/// A one-entity catalog whose only field `x` has type `ty`.
fn catalog_of(ty: &CanonicalType) -> Catalog {
    let entity = Entity::new(
        "t",
        vec![
            CanonicalField::new("id", CanonicalType::Int64, false),
            CanonicalField::new("x", ty.clone(), true),
        ],
        vec!["id".into()],
    )
    .unwrap();
    Catalog::new().with("shop", entity)
}

fn cast_flow(to: &str, opt_in: Option<bool>) -> String {
    let opt_in = opt_in.map_or(String::new(), |b| format!(", allow_lossy: {b}"));
    format!(
        "flows:\n  - name: f\n    from: shop.t\n    sink: warehouse\n    to:\n      t:\n        columns:\n          y: {{expr: x, type: \"{to}\"{opt_in}}}\n"
    )
}

/// A value of `from` that converts to `to`, written as text.
fn sample(from: &str, to: &str) -> &'static str {
    match (from, to) {
        ("utf8", "int64") => "42",
        ("utf8", "uuid") => "01234567-89ab-cdef-0123-456789abcdef",
        ("utf8", "json") => r#"{"a": [1, 2]}"#,
        ("binary", _) => "ok",
        ("timestamp(UTC)" | "timestamp", _) => "2026-03-01T10:00:00",
        (f, _) if f.starts_with("int") || f.starts_with("uint") => "7",
        _ => "7.25",
    }
}

#[test]
fn every_lossy_cast_is_rejected_by_default_and_accepted_with_allow_lossy() {
    for row in LOSSY {
        let from = types::parse(row.from).unwrap();
        let catalog = catalog_of(&from);
        let what = format!("{} → {}", row.from, row.to);

        for opt_in in [None, Some(false)] {
            let errors = errors_of(check_flows(&cast_flow(row.to, opt_in), &catalog));
            let [error] = errors.0.as_slice() else {
                panic!("{what}: {errors}")
            };
            assert_eq!(error.kind, ErrorKind::Type, "{what}");
            assert_eq!(error.path, "flows[f].to.t.columns.y", "{what}");
            assert!(
                error.message.contains(&format!("`x` is {}", row.from))
                    && error.message.contains(&format!("declared {}", row.to)),
                "{what}: inferred and expected type: {error}"
            );
            assert!(
                error.help.as_deref().unwrap().contains("allow_lossy: true"),
                "{what}"
            );
        }

        let flows = check_flows(&cast_flow(row.to, Some(true)), &catalog)
            .unwrap_or_else(|e| panic!("{what}: {e}"));
        let column = &flows[0].targets[0].columns[0];
        assert_eq!(types::name(&column.field.ty), row.to, "{what}");

        // The opted-in conversion runs: a value that fits converts.
        let ColumnValue::Expr(compiled) = &column.value else {
            panic!()
        };
        let text: ArrayRef = Arc::new(StringArray::from(vec![sample(row.from, row.to)]));
        let x = to_canonical(&text, &from).unwrap();
        assert!(!x.is_null(0), "{what}: bad sample");
        let batch = RecordBatch::try_new(
            compiled.input_schema().clone(),
            vec![Arc::new(arrow_array::Int64Array::from(vec![1])), x],
        )
        .unwrap();
        let out = compiled
            .evaluate(&batch)
            .unwrap_or_else(|e| panic!("{what}: {e}"));
        assert_eq!(
            out.data_type(),
            &types::parse(row.to).unwrap().to_arrow(),
            "{what}"
        );
        assert!(!out.is_null(0), "{what}");
    }
}

#[test]
fn opted_in_conversions_round_half_up_and_fail_out_of_range_values() {
    let decimal = types::parse("decimal(12,4)").unwrap();
    let flows = check_flows(
        &cast_flow("decimal(12,2)", Some(true)),
        &catalog_of(&decimal),
    )
    .unwrap();
    let ColumnValue::Expr(compiled) = &flows[0].targets[0].columns[0].value else {
        panic!()
    };
    let x: ArrayRef = Arc::new(
        arrow_array::Decimal128Array::from(vec![12_350, -12_350, 12_349])
            .with_precision_and_scale(12, 4)
            .unwrap(),
    );
    let ids: ArrayRef = Arc::new(arrow_array::Int64Array::from(vec![1, 2, 3]));
    let batch =
        RecordBatch::try_new(compiled.input_schema().clone(), vec![ids.clone(), x]).unwrap();
    let out = compiled.evaluate(&batch).unwrap();
    let out = out.as_primitive::<arrow_array::types::Decimal128Type>();
    assert_eq!(
        out.values().to_vec(),
        [124, -124, 123],
        "1.2350 → 1.24, 1.2349 → 1.23"
    );

    let flows = check_flows(
        &cast_flow("int32", Some(true)),
        &catalog_of(&CanonicalType::Int64),
    )
    .unwrap();
    let ColumnValue::Expr(compiled) = &flows[0].targets[0].columns[0].value else {
        panic!()
    };
    let x: ArrayRef = Arc::new(arrow_array::Int64Array::from(vec![7, 1 << 40, -3]));
    let batch = RecordBatch::try_new(compiled.input_schema().clone(), vec![ids, x]).unwrap();
    let (values, errors) = compiled.evaluate_rows(&batch);
    assert_eq!(errors.iter().map(|e| e.row).collect::<Vec<_>>(), [1]);
    assert_eq!(
        values
            .as_primitive::<arrow_array::types::Int32Type>()
            .value(2),
        -3
    );
}

#[test]
fn safe_conversions_need_no_opt_in() {
    for (from, to) in [
        ("int32", "int64"),
        ("decimal(10,2)", "decimal(12,4)"),
        ("int64", "utf8"),
    ] {
        let catalog = catalog_of(&types::parse(from).unwrap());
        check_flows(&cast_flow(to, None), &catalog)
            .unwrap_or_else(|e| panic!("{from} → {to}: {e}"));
    }
}

#[test]
fn a_conversion_written_in_the_expression_is_explicit() {
    let catalog = catalog_of(&CanonicalType::Utf8);
    let flow = "flows:\n  - name: f\n    from: shop.t\n    sink: warehouse\n    to:\n      t:\n        columns:\n          y: {expr: int(x), type: int64}\n          z: int(x)\n";
    let flows = check_flows(flow, &catalog).unwrap_or_else(|e| panic!("{e}"));
    let columns = &flows[0].targets[0].columns;
    assert!(columns.iter().all(|c| c.field.ty == CanonicalType::Int64));
}

#[test]
fn an_impossible_conversion_is_rejected_even_with_allow_lossy() {
    let catalog = catalog_of(&CanonicalType::Bool);
    let errors = errors_of(check_flows(
        &cast_flow("timestamp(UTC)", Some(true)),
        &catalog,
    ));
    assert!(
        errors.0[0].message.contains("can't be converted"),
        "{errors}"
    );
}

#[test]
fn an_unknown_type_name_suggests_one() {
    let errors = errors_of(check_flows(
        &cast_flow("in64", None),
        &catalog_of(&CanonicalType::Int64),
    ));
    let error = &errors.0[0];
    assert_eq!(error.path, "flows[f].to.t.columns.y.type");
    assert_eq!(error.help.as_deref(), Some("did you mean `int64`?"));
}

// ---------------------------------------------------------------- paths and scopes

#[test]
fn a_path_that_does_not_exist_fails_with_the_nearest_field() {
    for (column, message, help) in [
        (
            "lower(stauts)",
            "no field `stauts` in entity `shop.orders`",
            "did you mean `status`?",
        ),
        (
            "customer.emial",
            "`customer` has no field `emial`",
            "did you mean `customer.email`?",
        ),
        (
            "cardinality(items[*].skus)",
            "`items[*]` has no field `skus`",
            "did you mean `items[*].sku`?",
        ),
    ] {
        let flows = orders_flow("", &format!("          c: \"{column}\"\n"));
        let errors = errors_of(check_flows(&flows, &golden_catalog()));
        let error = &errors.0[0];
        assert_eq!(error.kind, ErrorKind::Expr, "{column}");
        assert!(error.message.starts_with(message), "{column}: {error}");
        assert_eq!(error.help.as_deref(), Some(help), "{column}");
        assert_eq!(error.path, "flows[orders].to.orders.columns.c");
        let at = error.location.as_ref().unwrap();
        assert_eq!((&*at.file, at.line), ("flows/f.yaml", 8), "{column}");
    }
}

#[test]
fn a_child_sees_the_mapped_parent_row() {
    let flows = "flows:\n  - name: orders\n    from: shop.orders\n    sink: warehouse\n    to:\n      orders:\n        columns:\n          id: string(_id)\n          total_cents: int(round(total * 100))\n      order_items:\n        parent: orders\n        unnest: items\n        columns:\n          order_id: $parent.id\n          total: $parent.totl_cents\n";
    let errors = errors_of(check_flows(flows, &golden_catalog()));
    let error = &errors.0[0];
    assert_eq!(
        error.message,
        "`$parent` has no field `totl_cents`\n   | $parent.totl_cents\n   |         ^^^^^^^^^^"
    );
    assert_eq!(
        error.help.as_deref(),
        Some("did you mean `$parent.total_cents`?")
    );
    let at = error.location.as_ref().unwrap();
    assert_eq!((at.line, at.column), (15, 26), "points at `total` itself");
}

#[test]
fn unnest_needs_an_array() {
    let flows = "flows:\n  - name: orders\n    from: shop.orders\n    sink: warehouse\n    to:\n      orders:\n        columns:\n          id: string(_id)\n      bad:\n        parent: orders\n        unnest: status\n        columns:\n          i: $index\n";
    let errors = errors_of(check_flows(flows, &golden_catalog()));
    assert_eq!(
        errors.0[0].message,
        "`unnest: status` needs an array, but it is utf8"
    );
    assert_eq!(errors.0[0].path, "flows[orders].to.bad.unnest");
}

#[test]
fn key_columns_must_be_columns_and_are_not_null() {
    let flows = "flows:\n  - name: orders\n    from: shop.orders\n    sink: warehouse\n    to:\n      orders:\n        key: [idd]\n        columns:\n          id: string(_id)\n";
    let errors = errors_of(check_flows(flows, &golden_catalog()));
    assert_eq!(
        errors.0[0].message,
        "key column `idd` is not a column of `orders`"
    );
    assert_eq!(errors.0[0].help.as_deref(), Some("did you mean `id`?"));
}

#[test]
fn an_entity_missing_from_discovery_is_named() {
    let flows = "flows:\n  - name: orders\n    from: shop.order\n    sink: warehouse\n    to:\n      orders:\n        columns: \"*\"\n";
    let errors = errors_of(check_flows(flows, &golden_catalog()));
    assert_eq!(
        errors.0[0].help.as_deref(),
        Some("did you mean `shop.orders`?")
    );
}

#[test]
fn join_keys_must_exist_and_compare() {
    let flows = |keys: &str| {
        format!(
            "flows:\n  - name: s\n    from:\n      - entity: app.public.orders\n        as: o\n      - entity: app.public.customers\n        as: c\n        join:\n          cardinality: one\n          keys: {{{keys}}}\n    sink: warehouse\n    to:\n      s:\n        columns:\n          n: c.name\n"
        )
    };
    let dir = project(&flows("idd: o.customer_id"));
    std::fs::write(
        dir.path().join("connections.yaml"),
        "connections:\n  - name: app\n    type: postgres\n    url: x\n  - name: warehouse\n    type: postgres\n    url: y\n",
    )
    .unwrap();
    let errors = errors_of(check_dir(dir.path(), &pg_merge_catalog()));
    assert_eq!(errors.0[0].help.as_deref(), Some("did you mean `id`?"));

    std::fs::write(dir.path().join("flows/f.yaml"), flows("id: o.status")).unwrap();
    let errors = errors_of(check_dir(dir.path(), &pg_merge_catalog()));
    assert!(errors.0[0].message.contains("comparable types"), "{errors}");

    std::fs::write(dir.path().join("flows/f.yaml"), flows("id: o.customer_id")).unwrap();
    check_dir(dir.path(), &pg_merge_catalog()).unwrap_or_else(|e| panic!("{e}"));
}

#[test]
fn every_error_of_a_flow_is_reported_at_once() {
    let flows = orders_flow(
        "    where: deleted_at\n",
        "          a: lower(stauts)\n          b: {expr: status, type: int64}\n",
    );
    let errors = errors_of(check_flows(&flows, &golden_catalog()));
    assert_eq!(errors.len(), 3, "{errors}");
}

// ---------------------------------------------------------------- the five most common mistakes

fn snapshot_errors(flows: &str) -> String {
    errors_of(check_flows(flows, &golden_catalog())).to_string()
}

#[test]
fn mistake_misspelled_field() {
    insta::assert_snapshot!(snapshot_errors(&orders_flow(
        "",
        "          status: lower(stauts)\n"
    )));
}

#[test]
fn mistake_lossy_narrowing_without_opt_in() {
    insta::assert_snapshot!(snapshot_errors(&orders_flow(
        "",
        "          total_cents: {expr: \"int(round(total * 100))\", type: int32}\n"
    )));
}

#[test]
fn mistake_text_declared_as_a_number() {
    insta::assert_snapshot!(snapshot_errors(&orders_flow(
        "",
        "          status_code: {expr: status, type: int64}\n"
    )));
}

#[test]
fn mistake_where_that_is_not_a_condition() {
    insta::assert_snapshot!(snapshot_errors(&orders_flow(
        "    where:\n      not_deleted: deleted_at\n",
        "          id: string(_id)\n"
    )));
}

/// Caught without a schema, when the config is loaded (`ferry validate`).
#[test]
fn mistake_reserved_name_without_its_sigil() {
    let dir = project(
        "flows:\n  - name: orders\n    from: shop.orders\n    sink: warehouse\n    to:\n      orders:\n        columns:\n          id: string(_id)\n      order_items:\n        parent: orders\n        unnest: items\n        columns:\n          line_no: index\n",
    );
    let errors = load(dir.path()).unwrap_err();
    insta::assert_snapshot!(errors.to_string());
}

#[test]
fn a_nested_unnest_resolves_in_its_parents_element() {
    let options = CanonicalType::List(Box::new(CanonicalField::new(
        "item",
        CanonicalType::Utf8,
        true,
    )));
    let item = CanonicalType::Struct(vec![
        CanonicalField::new("sku", CanonicalType::Utf8, true),
        CanonicalField::new("options", options, true),
    ]);
    let entity = Entity::new(
        "orders",
        vec![
            CanonicalField::new("_id", CanonicalType::Utf8, false),
            CanonicalField::new(
                "items",
                CanonicalType::List(Box::new(CanonicalField::new("item", item, true))),
                true,
            ),
        ],
        vec!["_id".into()],
    )
    .unwrap();
    let catalog = Catalog::new().with("shop", entity);
    let flows = |grandchild_unnest: &str| {
        format!(
            "flows:\n  - name: orders\n    from: shop.orders\n    sink: warehouse\n    to:\n      orders:\n        columns:\n          id: _id\n      order_items:\n        parent: orders\n        unnest: items\n        columns:\n          order_id: $parent.id\n          sku: sku\n      item_options:\n        parent: order_items\n        unnest: {grandchild_unnest}\n        columns:\n          sku: $parent.sku\n          position: $index\n          option: $item\n"
        )
    };
    let checked = check_flows(&flows("options"), &catalog).unwrap_or_else(|e| panic!("{e}"));
    let options = &checked[0].targets[2];
    assert_eq!(options.table, "item_options");
    let shape: Vec<String> = options
        .columns
        .iter()
        .map(|c| types::name(&c.field.ty))
        .collect();
    assert_eq!(shape, ["utf8", "int64", "utf8"]);

    // `options` is a field of an item, not of the order.
    let errors = errors_of(check_flows(&flows("optons"), &catalog));
    assert_eq!(
        errors.0[0].message.lines().next().unwrap(),
        "no field `optons` in an element of `items`"
    );
    assert_eq!(errors.0[0].help.as_deref(), Some("did you mean `options`?"));
    assert_eq!(errors.0[0].path, "flows[orders].to.item_options.unnest");
}
