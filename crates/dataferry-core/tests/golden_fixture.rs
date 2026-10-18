//! The golden dataset is only trustworthy if it is internally consistent:
//! every source document is accounted for exactly once, no expected child is
//! orphaned, and every record is documented.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use serde_json::Value;

fn golden(path: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/golden")
        .join(path)
}

fn load(path: &str) -> Vec<Value> {
    let text = std::fs::read_to_string(golden(path)).unwrap();
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("{path}: {e}"))
}

fn source_ids() -> Vec<String> {
    load("source/orders.json")
        .iter()
        .map(|d| {
            d["_id"]["$oid"]
                .as_str()
                .expect("_id is an ObjectId")
                .to_owned()
        })
        .collect()
}

fn ids(path: &str, key: &str) -> Vec<String> {
    load(path)
        .iter()
        .map(|r| r[key].as_str().unwrap().to_owned())
        .collect()
}

#[test]
fn every_source_document_has_exactly_one_outcome() {
    let mut outcomes: BTreeMap<String, Vec<&str>> = BTreeMap::new();
    for (file, key, outcome) in [
        ("expected/orders.json", "id", "written"),
        ("expected/filtered.json", "_id", "filtered"),
        ("expected/dead_letter.json", "_id", "dead-lettered"),
    ] {
        for id in ids(file, key) {
            outcomes.entry(id).or_default().push(outcome);
        }
    }
    let sources = source_ids();
    assert_eq!(
        sources.iter().collect::<BTreeSet<_>>().len(),
        sources.len(),
        "duplicate source _id"
    );
    for id in &sources {
        match outcomes.get(id).map(Vec::as_slice) {
            Some([_]) => {}
            other => panic!("source {id} has outcomes {other:?}, expected exactly one"),
        }
    }
    let unknown: Vec<_> = outcomes.keys().filter(|id| !sources.contains(id)).collect();
    assert!(
        unknown.is_empty(),
        "expected files mention unknown ids {unknown:?}"
    );
}

#[test]
fn every_expected_item_belongs_to_a_written_order() {
    let written: BTreeSet<_> = ids("expected/orders.json", "id").into_iter().collect();
    for item in load("expected/order_items.json") {
        let order_id = item["order_id"].as_str().unwrap();
        assert!(written.contains(order_id), "item {item} is orphaned");
    }
}

#[test]
fn written_orders_keep_every_source_item_in_order() {
    let sources = load("source/orders.json");
    let items = load("expected/order_items.json");
    for order_id in ids("expected/orders.json", "id") {
        let doc = sources
            .iter()
            .find(|d| d["_id"]["$oid"] == order_id.as_str())
            .unwrap();
        let source_skus: Vec<_> = doc["items"]
            .as_array()
            .map(|a| a.iter().map(|i| i["sku"].clone()).collect())
            .unwrap_or_default();
        let expected: Vec<_> = items
            .iter()
            .filter(|i| i["order_id"] == order_id.as_str())
            .collect();
        let skus: Vec<_> = expected.iter().map(|i| i["sku"].clone()).collect();
        let lines: Vec<_> = expected
            .iter()
            .map(|i| i["line_no"].as_u64().unwrap())
            .collect();
        assert_eq!(skus, source_skus, "items of {order_id}");
        assert_eq!(
            lines,
            (0..lines.len() as u64).collect::<Vec<_>>(),
            "line_no of {order_id}"
        );
    }
}

#[test]
fn every_source_document_is_documented_in_the_readme() {
    let readme = std::fs::read_to_string(golden("README.md")).unwrap();
    for id in source_ids() {
        assert!(
            readme.contains(&format!("| `{id}` |")),
            "README has no row for {id}"
        );
    }
}
