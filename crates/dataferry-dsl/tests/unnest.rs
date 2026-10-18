//! `unnest:` (B5, 0XT-263): child rows per array element, `$index`,
//! `$item`, `$parent` as the mapped parent row, deterministic `$key`s, and
//! empty, missing, null and non-array values (0003 §5.4, §5.7).

mod common;

use std::collections::BTreeMap;

use arrow_array::RecordBatch;
use common::golden;
use common::plan::{
    batch_of, compile_one, expected, golden_catalog, golden_flow, list_of, sorted, target_rows,
};
use dataferry_core::Entity;
use dataferry_core::partition::KeyValue;
use dataferry_core::transform::SourceRecordId;
use dataferry_core::types::{CanonicalField, CanonicalType};
use dataferry_dsl::compile::key::{Step, child_key};
use dataferry_dsl::config::model::EntityRef;
use dataferry_dsl::typecheck::Catalog;
use serde_json::{Value, json};

fn golden_items(batch: &RecordBatch) -> Vec<Value> {
    let out = golden_flow().plan.transform_batch(batch).unwrap();
    sorted(target_rows(&out, "order_items"), &["order_id", "line_no"])
}

fn step(path: &str, index: u64) -> Step {
    Step {
        path: path.into(),
        index,
    }
}

fn str_id(id: &str) -> SourceRecordId {
    SourceRecordId(vec![KeyValue::Str(id.into())])
}

// ---------------------------------------------------------------- the golden data

#[test]
fn golden_order_items_are_exactly_the_expected_rows() {
    // The expected `id`s were computed outside Rust from the encoding in
    // `compile::key`, so this also pins the key format.
    assert_eq!(
        golden_items(&golden::batch()),
        sorted(expected("order_items"), &["order_id", "line_no"])
    );
}

#[test]
fn empty_and_missing_item_arrays_write_the_order_and_no_items() {
    let out = golden_flow()
        .plan
        .transform_batch(&golden::batch())
        .unwrap();
    let orders: Vec<String> = target_rows(&out, "orders")
        .iter()
        .map(|r| r["id"].as_str().unwrap().to_owned())
        .collect();
    let items = target_rows(&out, "order_items");
    for id in ["650000000000000000000002", "650000000000000000000003"] {
        assert!(orders.contains(&id.to_owned()), "{id} is written");
        assert!(
            items.iter().all(|i| i["order_id"] != id),
            "{id} has no items"
        );
    }
}

#[test]
fn every_golden_key_is_the_documented_uuidv5() {
    for item in expected("order_items") {
        let record = str_id(item["order_id"].as_str().unwrap());
        let index = item["line_no"].as_u64().unwrap();
        assert_eq!(
            child_key(&record, &[step("items", index)]).to_string(),
            item["id"].as_str().unwrap()
        );
    }
}

#[test]
fn the_same_input_gives_the_same_keys_however_it_is_batched() {
    let batch = golden::batch();
    let whole = golden_items(&batch);
    // A second compile, the records in reverse and three at a time.
    let reversed = arrow_array::UInt32Array::from_iter_values((0..batch.num_rows() as u32).rev());
    let reversed = arrow_select::take::take_record_batch(&batch, &reversed).unwrap();
    let plan = golden_flow().plan;
    let mut pieces = Vec::new();
    for offset in (0..reversed.num_rows()).step_by(3) {
        let len = 3.min(reversed.num_rows() - offset);
        let out = plan.transform_batch(&reversed.slice(offset, len)).unwrap();
        pieces.extend(target_rows(&out, "order_items"));
    }
    assert_eq!(sorted(pieces, &["order_id", "line_no"]), whole);
    let keys: std::collections::BTreeSet<&str> =
        whole.iter().map(|r| r["id"].as_str().unwrap()).collect();
    assert_eq!(keys.len(), whole.len(), "no two items share a key");
}

#[test]
fn a_target_without_parent_unnests_the_driver_record() {
    let to = "      skus:\n        columns:\n          id: $key\n          line: $index\n          sku: sku\n        unnest: items\n";
    let flow = format!(
        "flows:\n  - name: skus\n    from: shop.orders\n    sink: warehouse\n    to:\n{to}"
    );
    let out = compile_one(&flow, &golden_catalog())
        .plan
        .transform_batch(&golden::batch())
        .unwrap();
    let rows = target_rows(&out, "skus");
    let docs = golden::documents();
    let total: usize = docs
        .iter()
        .map(|d| d["items"].as_array().map_or(0, Vec::len))
        .sum();
    assert_eq!(rows.len(), total);
    // Lineage still points at the document each element came from.
    let lineage = &out.output.targets[0].source_index;
    for (row, &record) in rows.iter().zip(lineage) {
        let id = &out.output.records[record as usize];
        let line = row["line"].as_u64().unwrap();
        assert_eq!(
            row["id"].as_str().unwrap(),
            child_key(id, &[step("items", line)]).to_string()
        );
    }
}

// ---------------------------------------------------------------- empty, null, missing, non-arrays

fn tags_catalog(tags: CanonicalType) -> Catalog {
    let entity = Entity::new(
        "posts",
        vec![
            CanonicalField::new("id", CanonicalType::Int64, false),
            CanonicalField::new("tags", tags, true),
        ],
        vec!["id".into()],
    )
    .unwrap()
    .with_missing_tracking();
    Catalog::new().with("shop", entity)
}

const TAGS_FLOW: &str = "flows:\n  - name: posts\n    from: shop.posts\n    sink: warehouse\n    to:\n      posts:\n        key: [id]\n        columns:\n          id: id\n      post_tags:\n        parent: posts\n        unnest: tags\n        key: [post_id, pos]\n        columns:\n          post_id: $parent.id\n          pos: $index\n          tag: $item\n          tag_id: $key\n";

#[test]
fn empty_null_and_missing_arrays_give_no_children_and_keep_the_parent() {
    let catalog = tags_catalog(list_of(CanonicalType::Utf8));
    let entity = catalog
        .get(&EntityRef::parse("shop.posts").unwrap())
        .unwrap()
        .clone();
    let batch = batch_of(
        &entity,
        &[
            json!({"id": 1, "tags": ["a", null, "c"]}),
            json!({"id": 2, "tags": null}),
            json!({"id": 3}),
            json!({"id": 4, "tags": []}),
        ],
        &[&[], &[], &["tags"], &[]],
    );
    let out = compile_one(TAGS_FLOW, &catalog)
        .plan
        .transform_batch(&batch)
        .unwrap();
    assert!(out.output.rejected.is_empty(), "{:?}", out.output.rejected);
    assert_eq!(target_rows(&out, "posts").len(), 4);
    let tags: Vec<(Value, Value, Value)> = target_rows(&out, "post_tags")
        .into_iter()
        .map(|r| (r["post_id"].clone(), r["pos"].clone(), r["tag"].clone()))
        .collect();
    // A null element is still an element: it has a row, with a null $item.
    assert_eq!(
        tags,
        [
            (json!(1), json!(0), json!("a")),
            (json!(1), json!(1), Value::Null),
            (json!(1), json!(2), json!("c")),
        ]
    );
}

#[test]
fn a_value_that_is_not_an_array_rejects_the_record() {
    // A Mongo path that is an array in some documents and not in others is
    // inferred as a `json` variant; only the arrays can be unnested.
    let catalog = tags_catalog(CanonicalType::Json);
    let entity = catalog
        .get(&EntityRef::parse("shop.posts").unwrap())
        .unwrap()
        .clone();
    let batch = batch_of(
        &entity,
        &[
            json!({"id": 1, "tags": r#"["a", {"b": 1}]"#}),
            json!({"id": 2, "tags": r#""x""#}),
            json!({"id": 3, "tags": null}),
            json!({"id": 4, "tags": r#"{"k": 1}"#}),
            json!({"id": 5, "tags": "[]"}),
        ],
        &[],
    );
    let out = compile_one(TAGS_FLOW, &catalog)
        .plan
        .transform_batch(&batch)
        .unwrap();
    let rejected: Vec<(&SourceRecordId, &str)> = out
        .output
        .rejected_records()
        .map(|(id, r)| (id, r.error.as_str()))
        .collect();
    let int = |i| SourceRecordId(vec![KeyValue::Int(i)]);
    assert_eq!(
        rejected,
        [
            (
                &int(2),
                "post_tags: `unnest: tags` needs an array, but the value is a string"
            ),
            (
                &int(4),
                "post_tags: `unnest: tags` needs an array, but the value is an object"
            ),
        ]
    );
    let posts: Vec<Value> = target_rows(&out, "posts")
        .into_iter()
        .map(|r| r["id"].clone())
        .collect();
    assert_eq!(
        posts,
        [json!(1), json!(3), json!(5)],
        "the parent of a rejected record goes too"
    );
    let tags: Vec<Value> = target_rows(&out, "post_tags")
        .into_iter()
        .map(|r| r["tag"].clone())
        .collect();
    assert_eq!(
        tags,
        [json!(r#""a""#), json!(r#"{"b":1}"#)],
        "elements as JSON"
    );
}

// ---------------------------------------------------------------- nesting

fn nested_catalog() -> Catalog {
    let option = CanonicalType::Struct(vec![
        CanonicalField::new("name", CanonicalType::Utf8, true),
        CanonicalField::new("extra", CanonicalType::Utf8, true),
    ]);
    let item = CanonicalType::Struct(vec![
        CanonicalField::new("sku", CanonicalType::Utf8, true),
        CanonicalField::new("options", list_of(option), true),
    ]);
    let entity = Entity::new(
        "orders",
        vec![
            CanonicalField::new("_id", CanonicalType::Utf8, false),
            CanonicalField::new("items", list_of(item), true),
        ],
        vec!["_id".into()],
    )
    .unwrap()
    .with_missing_tracking();
    Catalog::new().with("shop", entity)
}

const NESTED_FLOW: &str = "flows:\n  - name: orders\n    from: shop.orders\n    sink: warehouse\n    to:\n      orders:\n        key: [id]\n        columns:\n          id: _id\n      order_items:\n        parent: orders\n        unnest: items\n        key: [item_id]\n        columns:\n          item_id: $key\n          order_id: $parent.id\n          line: $index\n          sku: sku\n          no_options: missing(options)\n      item_options:\n        parent: order_items\n        unnest: options\n        key: [option_id]\n        columns:\n          option_id: $key\n          item_id: $parent.item_id\n          sku: $parent.sku\n          pos: $index\n          name: name\n          no_extra: missing(extra)\n";

#[test]
fn nested_unnests_index_the_innermost_array_and_key_the_whole_path() {
    let catalog = nested_catalog();
    let entity = catalog
        .get(&EntityRef::parse("shop.orders").unwrap())
        .unwrap()
        .clone();
    let batch = batch_of(
        &entity,
        &[
            json!({"_id": "o1", "items": [
                {"sku": "A", "options": [{"name": "x", "extra": "e"}, {"name": "y"}]},
                {"sku": "B", "options": [{"name": "x"}]},
            ]}),
            json!({"_id": "o2", "items": [{"sku": "C"}]}),
        ],
        // Mongo-style paths: the array index is a path segment.
        &[
            &["items.0.options.1.extra", "items.1.options.0.extra"],
            &["items.0.options"],
        ],
    );
    let compiled = compile_one(NESTED_FLOW, &catalog);
    assert_eq!(
        compiled.target.table("item_options").unwrap().foreign_keys[0].references_table,
        "order_items"
    );
    let out = compiled.plan.transform_batch(&batch).unwrap();
    assert!(out.output.rejected.is_empty(), "{:?}", out.output.rejected);
    let o1 = str_id("o1");

    let items = target_rows(&out, "order_items");
    let item_key = |i: u64| child_key(&o1, &[step("items", i)]).to_string();
    assert_eq!(
        items,
        [
            json!({"item_id": item_key(0), "order_id": "o1", "line": 0, "sku": "A", "no_options": false}),
            json!({"item_id": item_key(1), "order_id": "o1", "line": 1, "sku": "B", "no_options": false}),
            json!({"item_id": child_key(&str_id("o2"), &[step("items", 0)]).to_string(),
                   "order_id": "o2", "line": 0, "sku": "C", "no_options": true}),
        ]
    );

    let option_key =
        |i: u64, j: u64| child_key(&o1, &[step("items", i), step("options", j)]).to_string();
    let options = target_rows(&out, "item_options");
    assert_eq!(
        options,
        [
            json!({"option_id": option_key(0, 0), "item_id": item_key(0), "sku": "A", "pos": 0, "name": "x", "no_extra": false}),
            json!({"option_id": option_key(0, 1), "item_id": item_key(0), "sku": "A", "pos": 1, "name": "y", "no_extra": true}),
            json!({"option_id": option_key(1, 0), "item_id": item_key(1), "sku": "B", "pos": 0, "name": "x", "no_extra": true}),
        ]
    );
    // Same `$index`, different outer index: different keys.
    assert_ne!(option_key(0, 0), option_key(1, 0));
    assert_ne!(option_key(0, 0), item_key(0));
}

#[test]
fn an_error_deep_in_a_nested_unnest_rejects_the_whole_document() {
    let catalog = nested_catalog();
    let entity = catalog
        .get(&EntityRef::parse("shop.orders").unwrap())
        .unwrap()
        .clone();
    let batch = batch_of(
        &entity,
        &[
            json!({"_id": "o1", "items": [{"sku": "A", "options": [{"name": "x"}, {"name": "not a number"}]}]}),
            json!({"_id": "o2", "items": [{"sku": "B", "options": [{"name": "7"}]}]}),
        ],
        &[],
    );
    let flow = NESTED_FLOW.replace("          name: name\n", "          name: int(name)\n");
    let out = compile_one(&flow, &catalog)
        .plan
        .transform_batch(&batch)
        .unwrap();
    let [rejected] = out.output.rejected.as_slice() else {
        panic!("{:?}", out.output.rejected)
    };
    assert_eq!(out.output.records[rejected.index as usize], str_id("o1"));
    assert_eq!(rejected.target.as_deref(), Some("item_options"));
    assert!(
        rejected.error.ends_with("(at items[0].options[0])"),
        "the first failing element is named: {}",
        rejected.error
    );
    for t in &out.output.targets {
        assert!(
            !t.source_index.contains(&rejected.index),
            "{} has o1",
            t.target
        );
    }
    let names: BTreeMap<String, Value> = target_rows(&out, "item_options")
        .into_iter()
        .map(|r| (r["sku"].as_str().unwrap().to_owned(), r["name"].clone()))
        .collect();
    assert_eq!(names, BTreeMap::from([("B".to_owned(), json!(7))]));
}
