//! The compiler (B4, 0XT-262): columns, split targets, `where:` with the
//! propagation rule, `key:` for upsert, write order, and error rows that
//! reject the whole source record, against the golden dataset.

mod common;

use std::collections::BTreeSet;

use arrow_array::cast::AsArray;
use common::golden;
use common::plan::{
    compile_dir, compile_one, expected, golden_catalog, golden_flow, project, sorted, target_rows,
};
use dataferry_core::partition::KeyValue;
use dataferry_core::transform::TransformPlan;
use dataferry_core::types::{CanonicalField, CanonicalType};
use dataferry_core::{ForeignKey, WriteMode};
use dataferry_dsl::compile::{NestOrder, Strategy, Transformed, compile};
use dataferry_dsl::config::load;
use dataferry_dsl::config::model::Mode;
use dataferry_dsl::typecheck::check;
use serde_json::{Value, json};

fn run_golden() -> Transformed {
    golden_flow()
        .plan
        .transform_batch(&golden::batch())
        .unwrap()
}

/// The `_id` of record `index`.
fn record_id(out: &Transformed, index: u32) -> String {
    match out.output.records[index as usize].0.as_slice() {
        [KeyValue::Str(id)] => id.clone(),
        other => panic!("unexpected record id {other:?}"),
    }
}

fn ids(rows: &[Value], column: &str) -> BTreeSet<String> {
    rows.iter()
        .map(|r| r[column].as_str().unwrap().to_owned())
        .collect()
}

/// A flow over the golden entity with `to:` as given.
fn flow(extra: &str, to: &str) -> String {
    format!(
        "flows:\n  - name: orders\n    from: shop.orders\n    sink: warehouse\n{extra}    to:\n{to}"
    )
}

// ---------------------------------------------------------------- the golden data

#[test]
fn golden_orders_are_exactly_the_expected_rows() {
    let out = run_golden();
    assert_eq!(
        sorted(target_rows(&out, "orders"), &["id"]),
        sorted(expected("orders"), &["id"])
    );
}

#[test]
fn the_soft_deleted_order_is_filtered_with_its_items() {
    let out = run_golden();
    let deleted = "650000000000000000000004";
    assert!(!ids(&target_rows(&out, "orders"), "id").contains(deleted));
    assert!(!ids(&target_rows(&out, "order_items"), "order_id").contains(deleted));
    // Filtered, not rejected, and reported by the predicate's name.
    let filtered: Vec<Value> = out
        .filtered
        .iter()
        .map(|f| json!({ "_id": record_id(&out, f.index), "reason": f.reason }))
        .collect();
    assert_eq!(filtered, expected("filtered"));
    assert!(
        out.output
            .rejected
            .iter()
            .all(|r| record_id(&out, r.index) != deleted)
    );
}

#[test]
fn a_document_with_one_bad_item_is_rejected_whole() {
    let out = run_golden();
    let bad = golden::DEAD_LETTERED;
    // Its first item is fine, yet neither the order nor any item is written.
    assert!(!ids(&target_rows(&out, "orders"), "id").contains(bad));
    assert!(!ids(&target_rows(&out, "order_items"), "order_id").contains(bad));

    let rejected: Vec<(String, Option<String>, String)> = out
        .output
        .rejected_records()
        .map(|(id, r)| {
            let [KeyValue::Str(id)] = id.0.as_slice() else {
                panic!("{id:?}")
            };
            (id.clone(), r.target.clone(), r.error.clone())
        })
        .collect();
    let want: Vec<String> = expected("dead_letter")
        .iter()
        .map(|d| d["_id"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(
        rejected.iter().map(|r| r.0.clone()).collect::<Vec<_>>(),
        want
    );
    let (_, target, error) = &rejected[0];
    assert_eq!(target.as_deref(), Some("order_items"));
    // Which column, which element, and why, for the dead-letter row (A7).
    assert_eq!(
        error,
        r#"order_items.qty = `int(qty)`: int(): `"three"` is not an integer (at items[1])"#
    );
}

#[test]
fn every_record_is_written_filtered_or_rejected_exactly_once() {
    let out = run_golden();
    out.output
        .validate(golden_flow().plan.write_order())
        .unwrap();
    let docs = golden::documents();
    assert_eq!(out.output.records.len(), docs.len());
    let written = ids(&target_rows(&out, "orders"), "id");
    for (i, doc) in docs.iter().enumerate() {
        let id = golden::id(doc);
        assert_eq!(record_id(&out, i as u32), id, "records are in input order");
        let outcomes = [
            written.contains(&id),
            out.filtered.iter().any(|f| f.index == i as u32),
            out.output.rejected.iter().any(|r| r.index == i as u32),
        ];
        assert_eq!(
            outcomes.iter().filter(|o| **o).count(),
            1,
            "{id}: {outcomes:?}"
        );
    }
}

#[test]
fn every_row_carries_the_identity_of_its_source_record() {
    let out = run_golden();
    for target in &out.output.targets {
        let column = match target.target.as_str() {
            "orders" => "id",
            "order_items" => "order_id",
            other => panic!("{other}"),
        };
        let values = target
            .batch
            .column_by_name(column)
            .unwrap()
            .as_string::<i32>();
        for (row, &index) in target.source_index.iter().enumerate() {
            assert_eq!(values.value(row), record_id(&out, index), "{column}");
        }
    }
}

#[test]
fn the_target_schema_has_the_columns_keys_and_the_parent_foreign_key() {
    let compiled = golden_flow();
    assert_eq!(compiled.mode, WriteMode::Insert);
    assert!(compiled.warnings.is_empty(), "{:?}", compiled.warnings);
    let tables: Vec<&str> = compiled
        .target
        .tables()
        .iter()
        .map(|t| t.name.as_str())
        .collect();
    assert_eq!(tables, ["orders", "order_items"]);
    let orders = compiled.target.table("orders").unwrap();
    assert_eq!(orders.primary_key, ["id"]);
    assert!(orders.foreign_keys.is_empty());
    let items = compiled.target.table("order_items").unwrap();
    assert_eq!(items.primary_key, ["order_id", "line_no"]);
    assert_eq!(
        items.fields[0],
        CanonicalField::new("id", CanonicalType::Uuid, false)
    );
    // `order_id: $parent.id` maps the parent's whole key.
    assert_eq!(
        items.foreign_keys,
        [ForeignKey {
            columns: vec!["order_id".into()],
            references_table: "orders".into(),
            references_columns: vec!["id".into()],
        }]
    );
}

// ---------------------------------------------------------------- key: and mode

const SPLIT: &str = "      orders:\n        key: [id]\n        columns:\n          id: string(_id)\n      order_status:\n        columns:\n          order_id: string(_id)\n          status: lower(status)\n";

#[test]
fn an_upsert_split_target_without_key_is_a_plan_error_naming_it() {
    // The loader refuses this too; the planner checks it on its own.
    let dir = project(&flow("", SPLIT));
    let mut config = load(dir.path()).unwrap();
    config.flows[0].mode = Mode::Upsert;
    let catalog = golden_catalog();
    let checked = check(&config, &catalog).unwrap();
    let errors = compile(&config.flows[0], &checked[0], &catalog)
        .err()
        .expect("a plan error");
    let [error] = errors.0.as_slice() else {
        panic!("{errors}")
    };
    assert_eq!(
        error.message,
        "flow `orders` is `mode: upsert`, so target `order_status` needs `key:`"
    );
    assert_eq!(error.path, "flows[orders].to.order_status.key");
    assert!(error.location.is_some());

    // The same split, every target keyed, compiles to an upsert plan.
    let keyed = SPLIT.replace(
        "      order_status:\n",
        "      order_status:\n        key: [order_id]\n",
    );
    let compiled = compile_one(&flow("    mode: upsert\n", &keyed), &golden_catalog());
    assert_eq!(compiled.mode, WriteMode::Upsert);
    assert_eq!(
        compiled.target.table("order_status").unwrap().primary_key,
        ["order_id"]
    );
}

#[test]
fn an_insert_child_without_key_is_a_warning() {
    let compiled = compile_one(
        &flow(
            "",
            "      orders:\n        columns:\n          id: string(_id)\n      order_items:\n        parent: orders\n        unnest: items\n        columns:\n          sku: sku\n",
        ),
        &golden_catalog(),
    );
    assert_eq!(compiled.warnings.len(), 1, "{:?}", compiled.warnings);
    assert!(compiled.warnings[0].contains("`order_items` has no `key:`"));
}

// ---------------------------------------------------------------- where: and propagation

#[test]
fn a_target_where_drops_its_rows_their_descendants_and_nothing_else() {
    // `orders` keeps paid and shipped orders. `order_items` hangs off it and
    // `item_notes` off that; `order_status` is a sibling with no `parent:`.
    let to = "      orders:\n        where:\n          not_pending: status != 'PENDING'\n        columns:\n          id: string(_id)\n      order_items:\n        parent: orders\n        unnest: items\n        columns:\n          order_id: $parent.id\n          line_no: $index\n          sku: sku\n      item_notes:\n        parent: order_items\n        columns:\n          order_id: $parent.order_id\n          sku: $parent.sku\n      order_status:\n        columns:\n          order_id: string(_id)\n";
    let compiled = compile_one(&flow("", to), &golden_catalog());
    let out = compiled.plan.transform_batch(&golden::batch()).unwrap();

    // …0003 (no items) and …000a (one item) are pending.
    let pending = ["650000000000000000000003", "65000000000000000000000a"];
    let orders = ids(&target_rows(&out, "orders"), "id");
    let items = ids(&target_rows(&out, "order_items"), "order_id");
    let notes = ids(&target_rows(&out, "item_notes"), "order_id");
    let status = ids(&target_rows(&out, "order_status"), "order_id");
    for id in pending {
        assert!(!orders.contains(id) && !items.contains(id) && !notes.contains(id));
        assert!(status.contains(id), "the sibling keeps {id}");
    }
    assert_eq!(items, notes, "grandchildren follow their parents");
    // No flow `where:` here, and `int(qty)` isn't mapped: every document
    // has a status row.
    assert_eq!(status.len(), 10);
    assert_eq!(orders.len(), 8);
    // A filtered row is not an error and not a filtered record.
    assert!(out.output.rejected.is_empty());
    assert!(out.filtered.is_empty());
    let skus: Vec<Value> = target_rows(&out, "item_notes")
        .into_iter()
        .filter(|r| r["order_id"] == "650000000000000000000001")
        .map(|r| r["sku"].clone())
        .collect();
    assert_eq!(
        skus,
        [json!("SKU-1"), json!("SKU-2")],
        "$parent is the mapped row"
    );
}

#[test]
fn a_child_where_drops_only_child_rows() {
    let to = "      orders:\n        columns:\n          id: string(_id)\n      order_items:\n        parent: orders\n        unnest: items\n        where: sku != 'SKU-1'\n        columns:\n          order_id: $parent.id\n          sku: sku\n";
    let out = compile_one(&flow("", to), &golden_catalog())
        .plan
        .transform_batch(&golden::batch())
        .unwrap();
    assert_eq!(target_rows(&out, "orders").len(), 10);
    let skus: BTreeSet<String> = ids(&target_rows(&out, "order_items"), "sku");
    assert!(!skus.contains("SKU-1"));
    assert!(skus.contains("SKU-2"));
    // …0009 had only SKU-1: its order stays, with no items.
    assert!(ids(&target_rows(&out, "orders"), "id").contains("650000000000000000000009"));
    assert!(
        !ids(&target_rows(&out, "order_items"), "order_id").contains("650000000000000000000009")
    );
}

#[test]
fn a_flow_where_that_errors_rejects_the_record_instead_of_filtering_it() {
    // `int()` of a decimal with a fraction is an error, not a truncation.
    let to = "      orders:\n        columns:\n          id: string(_id)\n";
    let out = compile_one(
        &flow("    where:\n      whole_total: int(total) >= 0\n", to),
        &golden_catalog(),
    )
    .plan
    .transform_batch(&golden::batch())
    .unwrap();
    let fractional: BTreeSet<String> = golden::documents()
        .iter()
        .filter(|d| {
            let total = d["total"]["$numberDecimal"].as_str().unwrap();
            !total.ends_with(".00")
        })
        .map(golden::id)
        .collect();
    let rejected: BTreeSet<String> = out
        .output
        .rejected
        .iter()
        .map(|r| record_id(&out, r.index))
        .collect();
    assert_eq!(rejected, fractional);
    assert!(out.output.rejected.iter().all(|r| r.target.is_none()));
    assert!(
        out.output.rejected[0]
            .error
            .starts_with("where `whole_total`: "),
        "{}",
        out.output.rejected[0].error
    );
    assert!(out.filtered.is_empty());
    assert_eq!(target_rows(&out, "orders").len(), 10 - fractional.len());
}

#[test]
fn the_first_failing_predicate_is_the_reason() {
    let to = "      orders:\n        columns:\n          id: string(_id)\n";
    let out = compile_one(
        &flow(
            "    where:\n      paid: status == 'PAID'\n      not_deleted: deleted_at == null\n",
            to,
        ),
        &golden_catalog(),
    )
    .plan
    .transform_batch(&golden::batch())
    .unwrap();
    let reasons: Vec<(String, String)> = out
        .filtered
        .iter()
        .map(|f| (record_id(&out, f.index), f.reason.clone()))
        .collect();
    // …0004 is paid and deleted; the others aren't paid.
    assert_eq!(
        reasons,
        [
            ("650000000000000000000003".to_owned(), "paid".to_owned()),
            (
                "650000000000000000000004".to_owned(),
                "not_deleted".to_owned()
            ),
            ("650000000000000000000008".to_owned(), "paid".to_owned()),
            ("65000000000000000000000a".to_owned(), "paid".to_owned()),
        ]
    );
    // An unnamed `where:` is reported by its text.
    let out = compile_one(
        &flow("    where: deleted_at == null\n", to),
        &golden_catalog(),
    )
    .plan
    .transform_batch(&golden::batch())
    .unwrap();
    assert_eq!(out.filtered.len(), 1);
    assert_eq!(out.filtered[0].reason, "deleted_at == null");
}

// ---------------------------------------------------------------- error rows

#[test]
fn a_null_in_a_key_column_rejects_the_record() {
    let to = "      customers:\n        key: [customer_id]\n        columns:\n          customer_id: customer.id\n          order_id: string(_id)\n";
    let out = compile_one(&flow("", to), &golden_catalog())
        .plan
        .transform_batch(&golden::batch())
        .unwrap();
    let [rejected] = out.output.rejected.as_slice() else {
        panic!("{:?}", out.output.rejected)
    };
    // …000a has `customer: null`.
    assert_eq!(record_id(&out, rejected.index), "65000000000000000000000a");
    assert_eq!(
        rejected.error,
        "customers.customer_id can't be null, but `customer.id` is null"
    );
    assert_eq!(target_rows(&out, "customers").len(), 9);
}

#[test]
fn an_error_in_a_later_target_removes_the_rows_of_earlier_ones() {
    // `orders` comes first in write order and maps …0006 fine; its item
    // fails afterwards, and the order row has to go too.
    let out = run_golden();
    let orders = &out.output.targets[0];
    assert_eq!(orders.target, "orders");
    let rejected = out.output.rejected[0].index;
    assert!(!orders.source_index.contains(&rejected));
}

// ---------------------------------------------------------------- write order and shapes

#[test]
fn write_order_puts_parents_first_whatever_their_names_and_yaml_order() {
    let to = "      b_child:\n        parent: z_parent\n        unnest: items\n        columns:\n          sku: sku\n      z_parent:\n        key: [id]\n        columns:\n          id: string(_id)\n      a_root:\n        columns:\n          id: string(_id)\n";
    let compiled = compile_one(&flow("", to), &golden_catalog());
    assert_eq!(
        compiled.plan.write_order(),
        ["a_root", "z_parent", "b_child"]
    );
    let tables: Vec<&str> = compiled
        .target
        .tables()
        .iter()
        .map(|t| t.name.as_str())
        .collect();
    assert_eq!(tables, ["a_root", "z_parent", "b_child"]);
    let out = compiled.plan.transform_batch(&golden::batch()).unwrap();
    let order: Vec<&str> = out
        .output
        .targets
        .iter()
        .map(|t| t.target.as_str())
        .collect();
    assert_eq!(order, ["a_root", "z_parent", "b_child"]);
    out.output.validate(compiled.plan.write_order()).unwrap();
}

#[test]
fn columns_star_copies_the_entity_unchanged() {
    let to = "      orders_raw:\n        columns: \"*\"\n";
    let compiled = compile_one(&flow("", to), &golden_catalog());
    let batch = golden::batch();
    let out = compiled.plan.transform_batch(&batch).unwrap();
    let [copy] = out.output.targets.as_slice() else {
        panic!()
    };
    let entity = golden::entity();
    assert_eq!(copy.batch.num_columns(), entity.fields().len());
    for field in entity.fields() {
        assert_eq!(
            copy.batch.column_by_name(&field.name).unwrap().as_ref(),
            batch.column_by_name(&field.name).unwrap().as_ref(),
            "{}",
            field.name
        );
    }
    assert_eq!(copy.source_index, (0..10).collect::<Vec<u32>>());
}

#[test]
fn the_plan_reads_the_driver_entity_and_streams_each_batch() {
    use dataferry_core::source::BatchStream;
    use dataferry_core::transform::PartitionInputs;
    use futures::StreamExt;

    let compiled = golden_flow();
    assert_eq!(compiled.plan.inputs(), ["orders"]);
    let batch = golden::batch();
    // Two batches in, two outputs out, the same rows as one batch.
    let parts: BatchStream =
        futures::stream::iter(vec![Ok(batch.slice(0, 5)), Ok(batch.slice(5, 5))]).boxed();
    let inputs = PartitionInputs::new(vec![("orders".into(), parts)]);
    let outputs: Vec<_> =
        futures::executor::block_on(compiled.plan.execute(inputs).unwrap().collect::<Vec<_>>());
    assert_eq!(outputs.len(), 2);
    let mut orders = Vec::new();
    for output in outputs {
        let output = output.unwrap();
        output.validate(compiled.plan.write_order()).unwrap();
        for t in output.targets.iter().filter(|t| t.target == "orders") {
            orders.extend(common::plan::json_rows(&t.batch));
        }
    }
    assert_eq!(sorted(orders, &["id"]), sorted(expected("orders"), &["id"]));
}

#[test]
fn the_pg_merge_example_compiles_with_a_strategy_per_join() {
    let flows = compile_dir(
        &common::repo("examples/pg-merge/ferry"),
        &common::plan::pg_merge_catalog(),
    )
    .unwrap_or_else(|e| panic!("{e}"));
    let merge = flows
        .iter()
        .find(|f| f.name == "order_summaries")
        .expect("the merge flow");
    // The customer lookup is on `o.customer_id`, not the driver's key, so
    // `auto` stages it; the items pin co-partitioning on `order_id`.
    let joins: Vec<_> = merge
        .plan
        .joins()
        .map(|j| (j.alias.as_str(), j.strategy.clone(), j.reason.as_str()))
        .collect();
    assert_eq!(
        joins,
        [
            (
                "c",
                Strategy::Stage,
                "auto: staged, because `o.customer_id` isn't the driver's key `o.id`"
            ),
            (
                "i",
                Strategy::CoPartition {
                    column: "order_id".into()
                },
                "pinned by `strategy: co_partition`"
            ),
        ]
    );
    assert_eq!(
        merge.plan.inputs(),
        ["public.orders", "public.order_items"],
        "the driver, then the co-partitioned input; the staged one isn't read by the runner"
    );
    assert_eq!(
        merge.plan.nest_orders(),
        [NestOrder {
            target: "order_summaries".into(),
            column: "items".into(),
            alias: "i".into(),
            order_by: vec!["line_no".into(), "order_id".into()],
        }]
    );
    let items = merge
        .target
        .table("order_summaries")
        .unwrap()
        .fields
        .iter()
        .find(|f| f.name == "items")
        .unwrap();
    assert!(
        !items.nullable,
        "a driver with no items gets [], never null"
    );
}
