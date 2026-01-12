//! Merges (B6, 0XT-264): `one` and `many` joins compiled from a `from:`
//! list, run by the runner (A5) from in-memory sources into `MemSink`,
//! under both strategies (0002): co-partitioned streaming and staging.
//!
//! - a duplicate on a `one` join rejects exactly the driver records it
//!   would fan out, which then go to `_ferry_dlq` (A7) or fail their
//!   partition, per `on_error`;
//! - a driver with no children gets `items: []`;
//! - the same input under any partitioning, batch size and strategy gives
//!   identical nested arrays, ties in `order_by` broken by the nested
//!   entity's primary key;
//! - `unnest: <many alias>`, lookup chains, composite and widened keys, and
//!   which strategy each join gets, and why.

mod common;

use std::sync::Arc;
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_array::cast::AsArray;
use common::plan::{batch_of, compile_one, json_rows, project, runtime_flow, sorted};
use ferry_core::testkit::MemSource;
use ferry_core::types::{CanonicalField, CanonicalType};
use ferry_core::{Entity, IdempotencyStrategy, Source, TransformPlan};
use ferry_dsl::compile::{CompiledFlow, Staging, Strategy, compile};
use ferry_dsl::config::load;
use ferry_dsl::typecheck::{Catalog, check};
use ferry_registry::state::NewRun;
use ferry_runtime::{DLQ_TABLE, OnError, RunReport, Runner, Settings};
use ferry_state::FileStateStore;
use futures::StreamExt;
use serde_json::{Value, json};

// ---------------------------------------------------------------- the data

fn field(name: &str, ty: CanonicalType, nullable: bool) -> CanonicalField {
    CanonicalField::new(name, ty, nullable)
}

fn orders() -> Entity {
    use CanonicalType::*;
    Entity::new(
        "orders",
        vec![
            field("id", Int64, false),
            field("customer_id", Int64, true),
            field("status", Utf8, false),
        ],
        vec!["id".into()],
    )
    .unwrap()
}

/// Keyed by a surrogate `row_id`, so a customer `id` can repeat.
fn customers() -> Entity {
    use CanonicalType::*;
    Entity::new(
        "customers",
        vec![
            field("row_id", Int64, false),
            field("id", Int64, false),
            field("name", Utf8, false),
            field("region_id", Int64, true),
        ],
        vec!["row_id".into()],
    )
    .unwrap()
}

fn regions() -> Entity {
    use CanonicalType::*;
    Entity::new(
        "regions",
        vec![field("id", Int64, false), field("name", Utf8, false)],
        vec!["id".into()],
    )
    .unwrap()
}

fn items() -> Entity {
    use CanonicalType::*;
    Entity::new(
        "items",
        vec![
            field("item_id", Int64, false),
            field("order_id", Int64, false),
            field("line_no", Int32, false),
            field("sku", Utf8, false),
            field("qty", Int64, false),
        ],
        vec!["item_id".into()],
    )
    .unwrap()
}

fn catalog() -> Catalog {
    Catalog::new()
        .with("shop", orders())
        .with("shop", customers())
        .with("shop", regions())
        .with("shop", items())
}

fn rows(entity: &Entity, rows: Value) -> RecordBatch {
    batch_of(entity, rows.as_array().unwrap(), &[])
}

/// Order 8 is cancelled (filtered), 4 has no customer, 5 a customer that
/// doesn't exist, 7 one with no region.
fn order_rows() -> RecordBatch {
    rows(
        &orders(),
        json!([
            {"id": 1, "customer_id": 1, "status": "paid"},
            {"id": 2, "customer_id": 2, "status": "paid"},
            {"id": 3, "customer_id": 2, "status": "paid"},
            {"id": 4, "customer_id": null, "status": "pending"},
            {"id": 5, "customer_id": 9, "status": "paid"},
            {"id": 6, "customer_id": 1, "status": "paid"},
            {"id": 7, "customer_id": 3, "status": "paid"},
            {"id": 8, "customer_id": 1, "status": "cancelled"},
        ]),
    )
}

fn customer_rows(duplicate_bo: bool) -> RecordBatch {
    let mut customers = vec![
        json!({"row_id": 10, "id": 1, "name": "Ana", "region_id": 1}),
        json!({"row_id": 11, "id": 2, "name": "Bo", "region_id": 2}),
        json!({"row_id": 12, "id": 3, "name": "Cy", "region_id": null}),
    ];
    if duplicate_bo {
        customers.push(json!({"row_id": 13, "id": 2, "name": "Bo again", "region_id": 2}));
    }
    rows(&self::customers(), Value::Array(customers))
}

fn region_rows() -> RecordBatch {
    rows(
        &regions(),
        json!([{"id": 1, "name": "North"}, {"id": 2, "name": "South"}]),
    )
}

/// Items in no particular order. Order 1's have tied `qty`s, 6's only item
/// has `qty: 0` (dropped by the nest's `where:`), 3 and 5 have none.
fn item_rows() -> RecordBatch {
    rows(
        &items(),
        json!([
            {"item_id": 105, "order_id": 1, "line_no": 1, "sku": "A", "qty": 2},
            {"item_id": 108, "order_id": 4, "line_no": 0, "sku": "H", "qty": 1},
            {"item_id": 101, "order_id": 1, "line_no": 0, "sku": "B", "qty": 5},
            {"item_id": 107, "order_id": 7, "line_no": 1, "sku": "G", "qty": 3},
            {"item_id": 102, "order_id": 2, "line_no": 0, "sku": "D", "qty": 1},
            {"item_id": 104, "order_id": 6, "line_no": 0, "sku": "E", "qty": 0},
            {"item_id": 103, "order_id": 1, "line_no": 2, "sku": "C", "qty": 2},
            {"item_id": 106, "order_id": 7, "line_no": 0, "sku": "F", "qty": 3},
        ]),
    )
}

fn source(duplicate_bo: bool, batch_rows: usize) -> Arc<MemSource> {
    Arc::new(
        MemSource::new()
            .with_entity(orders(), order_rows())
            .unwrap()
            .with_entity(customers(), customer_rows(duplicate_bo))
            .unwrap()
            .with_entity(regions(), region_rows())
            .unwrap()
            .with_entity(items(), item_rows())
            .unwrap()
            .with_batch_rows(batch_rows),
    )
}

/// Orders with their customer, the customer's region, and their items
/// nested by `qty` descending: `item_id` breaks the ties.
fn summaries_flow(items_strategy: &str) -> String {
    format!(
        "flows:
  - name: summaries
    from:
      - entity: shop.orders
        as: o
      - entity: shop.customers
        as: c
        join:
          cardinality: one
          keys: {{id: o.customer_id}}
      - entity: shop.regions
        as: r
        join:
          cardinality: one
          keys: {{id: c.region_id}}
      - entity: shop.items
        as: i
        join:
          cardinality: many
          keys: {{order_id: o.id}}
          strategy: {items_strategy}
    sink: warehouse
    where: status != 'cancelled'
    to:
      summaries:
        key: [order_id]
        columns:
          order_id: id
          customer: c.name
          region: r.name
          items:
            nest: i
            order_by: [qty desc]
            where: qty > 0
            columns:
              sku: sku
              qty: qty
"
    )
}

fn expected_summaries() -> Vec<Value> {
    let item = |sku: &str, qty: i64| json!({"sku": sku, "qty": qty});
    vec![
        json!({"order_id": 1, "customer": "Ana", "region": "North",
               "items": [item("B", 5), item("C", 2), item("A", 2)]}),
        json!({"order_id": 2, "customer": "Bo", "region": "South", "items": [item("D", 1)]}),
        json!({"order_id": 3, "customer": "Bo", "region": "South", "items": []}),
        json!({"order_id": 4, "customer": null, "region": null, "items": [item("H", 1)]}),
        json!({"order_id": 5, "customer": null, "region": null, "items": []}),
        json!({"order_id": 6, "customer": "Ana", "region": "North", "items": []}),
        json!({"order_id": 7, "customer": "Cy", "region": null,
               "items": [item("F", 3), item("G", 3)]}),
    ]
}

// ---------------------------------------------------------------- running

fn settings(partition_rows: u64, on_error: OnError) -> Settings {
    Settings {
        workers: 2,
        partition_rows,
        on_error,
        lease_ttl: Duration::from_secs(30),
        heartbeat: Duration::from_secs(5),
        poll: Duration::from_millis(20),
        ..Settings::default()
    }
}

async fn run(
    compiled: &CompiledFlow,
    source: Arc<MemSource>,
    settings: Settings,
) -> (RunReport, ferry_core::testkit::MemSink) {
    let sink =
        ferry_core::testkit::MemSink::new(vec![IdempotencyStrategy::DeletePartitionThenInsert]);
    let flow = runtime_flow(
        compiled,
        |_| Arc::clone(&source) as Arc<dyn Source>,
        "shop",
        Arc::new(sink.clone()),
        Staging::DEFAULT_MEMORY,
    );
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(FileStateStore::new(dir.path()));
    let runner = Runner::new(store, vec![flow], settings).unwrap();
    let report = runner
        .start(NewRun {
            config_hash: "sha256:merge".into(),
            engine_version: "0.0.1-test".into(),
            connections: vec![("shop".into(), "mem".into())],
        })
        .await
        .unwrap();
    (report, sink)
}

fn committed(sink: &ferry_core::testkit::MemSink, target: &str, key: &str) -> Vec<Value> {
    sorted(
        sink.rows(target).iter().flat_map(json_rows).collect(),
        &[key],
    )
}

/// `(source_id, error)` of every dead letter, sorted.
fn dead_letters(sink: &ferry_core::testkit::MemSink) -> Vec<(String, String)> {
    let mut rows = Vec::new();
    for b in sink.rows(DLQ_TABLE) {
        let text = |name: &str| b.column_by_name(name).unwrap().as_string::<i32>().clone();
        let (id, error) = (text("source_id"), text("error"));
        for i in 0..b.num_rows() {
            rows.push((id.value(i).to_owned(), error.value(i).to_owned()));
        }
    }
    rows.sort();
    rows
}

// ---------------------------------------------------------------- one joins

#[tokio::test]
async fn a_duplicate_customer_dead_letters_exactly_the_orders_that_reference_it() {
    let compiled = compile_one(&summaries_flow("auto"), &catalog());
    let (report, sink) = run(
        &compiled,
        source(true, 64),
        settings(2, OnError::DeadLetter),
    )
    .await;
    assert!(report.is_complete(), "the run carries on: {report:?}");
    let error = "join `c` is `cardinality: one`, but 2 rows of `shop.customers` match id = 2";
    assert_eq!(
        dead_letters(&sink),
        [("[2]".into(), error.into()), ("[3]".into(), error.into())],
        "orders 2 and 3 are Bo's; nothing is fanned out"
    );
    let written: Vec<Value> = expected_summaries()
        .into_iter()
        .filter(|s| ![2, 3].contains(&s["order_id"].as_i64().unwrap()))
        .collect();
    assert_eq!(committed(&sink, "summaries", "order_id"), written);
    assert_eq!(report.totals.dead_lettered, 2);
}

#[tokio::test]
async fn under_on_error_fail_a_duplicate_fails_its_partition_and_stops_the_run() {
    let compiled = compile_one(&summaries_flow("auto"), &catalog());
    // One order per partition: orders 2 and 3 are partitions 1 and 2.
    let (report, sink) = run(&compiled, source(true, 64), settings(1, OnError::Fail)).await;
    assert!(!report.is_complete());
    assert!(report.counts.failed >= 1, "{report:?}");
    for failure in &report.failures {
        assert!(
            ["summaries/orders-1", "summaries/orders-2"].contains(&failure.partition.0.as_str()),
            "{failure:?}"
        );
        assert!(
            failure
                .error
                .contains("2 rows of `shop.customers` match id = 2"),
            "{}",
            failure.error
        );
    }
    // Whatever committed before the run stopped is whole and correct; no
    // row of Bo's orders exists, joined to either Bo.
    let expected = expected_summaries();
    let written = committed(&sink, "summaries", "order_id");
    assert!(!written.is_empty());
    for row in &written {
        assert!(![2, 3].contains(&row["order_id"].as_i64().unwrap()));
        assert!(expected.contains(row), "{row}");
    }
    assert!(sink.rows(DLQ_TABLE).is_empty());
}

#[tokio::test]
async fn a_duplicate_on_a_co_partitioned_one_join_is_caught_too() {
    // `details` is 1:1 on the driver's key, so `auto` streams it; order 2
    // has two rows.
    let details = Entity::new(
        "details",
        vec![
            field("detail_id", CanonicalType::Int64, false),
            field("order_id", CanonicalType::Int64, false),
            field("note", CanonicalType::Utf8, false),
        ],
        vec!["detail_id".into()],
    )
    .unwrap();
    let detail_rows = rows(
        &details,
        json!([
            {"detail_id": 1, "order_id": 1, "note": "first"},
            {"detail_id": 2, "order_id": 2, "note": "second"},
            {"detail_id": 3, "order_id": 2, "note": "second again"},
        ]),
    );
    let flow = "flows:
  - name: notes
    from:
      - entity: shop.orders
        as: o
      - entity: shop.details
        as: d
        join:
          cardinality: one
          keys: {order_id: o.id}
    sink: warehouse
    to:
      notes:
        columns:
          order_id: id
          note: d.note
";
    let compiled = compile_one(flow, &catalog().with("shop", details.clone()));
    let [join] = compiled.plan.joins().collect::<Vec<_>>()[..] else {
        panic!()
    };
    assert_eq!(
        join.strategy,
        Strategy::CoPartition {
            column: "order_id".into()
        }
    );
    let source = Arc::new(
        MemSource::new()
            .with_entity(orders(), order_rows())
            .unwrap()
            .with_entity(details, detail_rows)
            .unwrap(),
    );
    let (report, sink) = run(&compiled, source, settings(3, OnError::DeadLetter)).await;
    assert!(report.is_complete(), "{report:?}");
    assert_eq!(
        dead_letters(&sink),
        [(
            "[2]".into(),
            "join `d` is `cardinality: one`, but 2 rows of `shop.details` match order_id = 2"
                .into()
        )]
    );
    let notes = committed(&sink, "notes", "order_id");
    assert_eq!(notes.len(), 7);
    assert_eq!(notes[0], json!({"order_id": 1, "note": "first"}));
    assert_eq!(
        notes[1],
        json!({"order_id": 3, "note": null}),
        "no match: null columns"
    );
}

// ---------------------------------------------------------------- nest

#[tokio::test]
async fn an_order_with_no_items_gets_an_empty_array_under_both_strategies() {
    for strategy in ["co_partition", "stage"] {
        let compiled = compile_one(&summaries_flow(strategy), &catalog());
        let (report, sink) = run(&compiled, source(false, 64), settings(2, OnError::Fail)).await;
        assert!(report.is_complete(), "{strategy}: {report:?}");
        let summaries = committed(&sink, "summaries", "order_id");
        assert_eq!(summaries, expected_summaries(), "{strategy}");
        // 3 and 5 have no items, 6 only one the nest's `where:` drops.
        for order in [3, 5, 6] {
            let row = summaries.iter().find(|r| r["order_id"] == order).unwrap();
            assert_eq!(row["items"], json!([]), "{strategy}: order {order}");
        }
    }
}

#[tokio::test]
async fn the_same_input_gives_identical_nested_arrays_under_any_partitioning() {
    let expected = expected_summaries();
    for strategy in ["co_partition", "stage"] {
        let compiled = compile_one(&summaries_flow(strategy), &catalog());
        for partition_rows in [1, 2, 3, 100] {
            for batch_rows in [1, 3, 64] {
                let (report, sink) = run(
                    &compiled,
                    source(false, batch_rows),
                    settings(partition_rows, OnError::Fail),
                )
                .await;
                assert!(report.is_complete(), "{report:?}");
                assert_eq!(
                    committed(&sink, "summaries", "order_id"),
                    expected,
                    "{strategy}, {partition_rows} rows per partition, {batch_rows} per batch"
                );
            }
        }
    }
}

#[tokio::test]
async fn an_error_in_a_nested_row_rejects_the_whole_order() {
    let flow =
        summaries_flow("auto").replace("qty: qty\n", "qty: qty\n              n: int(sku)\n");
    let compiled = compile_one(&flow, &catalog());
    let (report, sink) = run(
        &compiled,
        source(false, 64),
        settings(100, OnError::DeadLetter),
    )
    .await;
    assert!(report.is_complete(), "{report:?}");
    // Every order with a kept item fails `int(sku)`; the others are written
    // with `[]`.
    let letters = dead_letters(&sink);
    let ids: Vec<&str> = letters.iter().map(|(id, _)| id.as_str()).collect();
    assert_eq!(ids, ["[1]", "[2]", "[4]", "[7]"]);
    assert!(
        letters[0].1.starts_with("summaries.items: n = `int(sku)`:"),
        "{}",
        letters[0].1
    );
    let written: Vec<i64> = committed(&sink, "summaries", "order_id")
        .iter()
        .map(|r| r["order_id"].as_i64().unwrap())
        .collect();
    assert_eq!(written, [3, 5, 6]);
}

// ---------------------------------------------------------------- unnest

const LINES_FLOW: &str = "flows:
  - name: lines
    from:
      - entity: shop.orders
        as: o
      - entity: shop.items
        as: i
        join:
          cardinality: many
          keys: {order_id: o.id}
          strategy: STRATEGY
    sink: warehouse
    to:
      orders:
        key: [id]
        columns:
          id: id
      lines:
        parent: orders
        unnest: i
        key: [line_key]
        columns:
          line_key: $key
          order_id: $parent.id
          pos: $index
          sku: sku
";

#[tokio::test]
async fn unnesting_a_many_alias_writes_one_row_per_joined_row() {
    let mut keys_by_strategy = Vec::new();
    for strategy in ["co_partition", "stage"] {
        let compiled = compile_one(&LINES_FLOW.replace("STRATEGY", strategy), &catalog());
        let mut runs = Vec::new();
        for partition_rows in [1, 3] {
            let (report, sink) = run(
                &compiled,
                source(false, 2),
                settings(partition_rows, OnError::Fail),
            )
            .await;
            assert!(report.is_complete(), "{report:?}");
            assert_eq!(
                committed(&sink, "orders", "id").len(),
                8,
                "every order, items or not"
            );
            runs.push(sorted(
                sink.rows("lines").iter().flat_map(json_rows).collect(),
                &["order_id", "pos"],
            ));
        }
        assert_eq!(
            runs[0], runs[1],
            "{strategy}: keys and positions don't depend on partitions"
        );
        let lines = &runs[0];
        // One per item, in `item_id` order within the order.
        let shown: Vec<(i64, i64, &str)> = lines
            .iter()
            .map(|l| {
                (
                    l["order_id"].as_i64().unwrap(),
                    l["pos"].as_i64().unwrap(),
                    l["sku"].as_str().unwrap(),
                )
            })
            .collect();
        assert_eq!(
            shown,
            [
                (1, 0, "B"),
                (1, 1, "C"),
                (1, 2, "A"),
                (2, 0, "D"),
                (4, 0, "H"),
                (6, 0, "E"),
                (7, 0, "F"),
                (7, 1, "G"),
            ],
            "{strategy}"
        );
        keys_by_strategy.push(
            lines
                .iter()
                .map(|l| l["line_key"].clone())
                .collect::<Vec<_>>(),
        );
    }
    assert_eq!(
        keys_by_strategy[0], keys_by_strategy[1],
        "the strategy doesn't change `$key`"
    );
    let distinct: std::collections::BTreeSet<String> =
        keys_by_strategy[0].iter().map(Value::to_string).collect();
    assert_eq!(distinct.len(), 8);
}

// ---------------------------------------------------------------- keys

#[tokio::test]
async fn composite_keys_of_different_widths_match_by_value() {
    // `prices` is keyed by (sku, line_no) with an int64 `line_no`; items
    // have an int32 one. Not the driver's key, so it is staged.
    let prices = Entity::new(
        "prices",
        vec![
            field("sku", CanonicalType::Utf8, false),
            field("line_no", CanonicalType::Int64, false),
            field("cents", CanonicalType::Int64, false),
        ],
        vec!["sku".into(), "line_no".into()],
    )
    .unwrap();
    let price_rows = rows(
        &prices,
        json!([
            {"sku": "A", "line_no": 1, "cents": 100},
            {"sku": "A", "line_no": 0, "cents": 999},
            {"sku": "B", "line_no": 0, "cents": 200},
        ]),
    );
    let flow = "flows:
  - name: priced
    from:
      - entity: shop.items
        as: it
      - entity: shop.prices
        as: p
        join:
          cardinality: one
          keys: {sku: it.sku, line_no: it.line_no}
    sink: warehouse
    to:
      priced:
        columns:
          item_id: item_id
          cents: p.cents
";
    let compiled = compile_one(flow, &catalog().with("shop", prices.clone()));
    let [join] = compiled.plan.joins().collect::<Vec<_>>()[..] else {
        panic!()
    };
    assert_eq!(join.strategy, Strategy::Stage);
    assert_eq!(
        join.reason,
        "auto: staged, because the join has more than one key pair"
    );
    let source = Arc::new(
        MemSource::new()
            .with_entity(items(), item_rows())
            .unwrap()
            .with_entity(prices, price_rows)
            .unwrap(),
    );
    let (report, sink) = run(&compiled, source, settings(3, OnError::Fail)).await;
    assert!(report.is_complete(), "{report:?}");
    let cents: Vec<(i64, Value)> = committed(&sink, "priced", "item_id")
        .iter()
        .map(|r| (r["item_id"].as_i64().unwrap(), r["cents"].clone()))
        .collect();
    assert_eq!(
        cents,
        [
            (101, json!(200)),
            (102, Value::Null),
            (103, Value::Null),
            (104, Value::Null),
            (105, json!(100)),
            (106, Value::Null),
            (107, Value::Null),
            (108, Value::Null),
        ]
    );
}

// ---------------------------------------------------------------- strategies

fn compile_err(flows: &str, catalog: &Catalog) -> String {
    let dir = project(flows);
    let config = load(dir.path()).unwrap_or_else(|e| panic!("{e}"));
    let checked = check(&config, catalog).unwrap_or_else(|e| panic!("{e}"));
    match compile(&config.flows[0], &checked[0], catalog) {
        Ok(_) => panic!("compiled"),
        Err(e) => e.to_string(),
    }
}

#[test]
fn each_join_gets_a_strategy_and_says_why() {
    let compiled = compile_one(&summaries_flow("auto"), &catalog());
    let joins: Vec<(String, Strategy, String)> = compiled
        .plan
        .joins()
        .map(|j| (j.alias.clone(), j.strategy.clone(), j.reason.clone()))
        .collect();
    assert_eq!(
        joins,
        [
            (
                "c".into(),
                Strategy::Stage,
                "auto: staged, because `o.customer_id` isn't the driver's key `o.id`".into()
            ),
            (
                "r".into(),
                Strategy::Stage,
                "auto: staged, because `c.region_id` isn't the driver's key `o.id`".into()
            ),
            (
                "i".into(),
                Strategy::CoPartition {
                    column: "order_id".into()
                },
                "auto: `i.order_id` is split on the driver's partition boundaries (0002)".into()
            ),
        ]
    );
    assert_eq!(compiled.plan.inputs(), ["orders", "items"]);

    let staged = compile_one(&summaries_flow("stage"), &catalog());
    let i = staged.plan.joins().find(|j| j.alias == "i").unwrap();
    assert_eq!(
        (i.strategy.clone(), i.reason.as_str()),
        (Strategy::Stage, "pinned by `strategy: stage`")
    );
    assert_eq!(staged.plan.inputs(), ["orders"]);
}

#[test]
fn a_join_on_another_connection_is_staged_and_cant_be_pinned_to_co_partition() {
    let discounts = Entity::new(
        "discounts",
        vec![
            field("order_id", CanonicalType::Int64, false),
            field("pct", CanonicalType::Int64, false),
        ],
        vec!["order_id".into()],
    )
    .unwrap();
    let catalog = catalog().with("warehouse", discounts);
    let flow = |strategy: &str| {
        format!(
            "flows:
  - name: discounted
    from:
      - entity: shop.orders
        as: o
      - entity: warehouse.discounts
        as: d
        join:
          cardinality: one
          keys: {{order_id: o.id}}
          strategy: {strategy}
    sink: warehouse
    to:
      discounted:
        columns:
          id: id
          pct: d.pct
"
        )
    };
    let compiled = compile_one(&flow("auto"), &catalog);
    let join = compiled.plan.joins().next().unwrap();
    assert_eq!(join.strategy, Strategy::Stage);
    assert_eq!(
        join.reason,
        "auto: staged, because `warehouse` isn't the driver's connection `shop`"
    );

    let error = compile_err(&flow("co_partition"), &catalog);
    assert!(
        error.contains("join `d` can't be co-partitioned: because `warehouse` isn't the driver's connection `shop`"),
        "{error}"
    );
}

#[test]
fn falling_back_to_staging_keeps_pinned_joins_pinned() {
    let auto = compile_one(&summaries_flow("auto"), &catalog());
    let fallback = auto.plan.without_co_partitioning().unwrap();
    assert!(fallback.joins().all(|j| j.strategy == Strategy::Stage));
    assert_eq!(fallback.inputs(), ["orders"]);
    assert_eq!(
        auto.plan.inputs(),
        ["orders", "items"],
        "the original is unchanged"
    );

    let pinned = compile_one(&summaries_flow("co_partition"), &catalog());
    let error = pinned.plan.without_co_partitioning().err().unwrap();
    assert!(
        error
            .to_string()
            .contains("join `i` pins `strategy: co_partition`"),
        "{error}"
    );
}

#[tokio::test]
async fn a_staged_join_needs_its_source() {
    let compiled = compile_one(&summaries_flow("stage"), &catalog());
    let error = compiled
        .plan
        .stage_with(Staging {
            sources: [("c".to_owned(), source(false, 64) as Arc<dyn Source>)].into(),
            memory: Staging::DEFAULT_MEMORY,
            dir: None,
        })
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("shop.regions (the source of staged join `r`)"),
        "{error}"
    );

    // Never given: the partition fails, saying so, rather than joining
    // nothing.
    let inputs = ferry_core::PartitionInputs::new(vec![(
        "orders".into(),
        futures::stream::iter([Ok(order_rows())]).boxed(),
    )]);
    let outputs: Vec<_> = compiled.plan.execute(inputs).unwrap().collect().await;
    let [Err(error)] = outputs.as_slice() else {
        panic!("{outputs:?}")
    };
    assert!(
        error.to_string().contains("staged join `c` has no source"),
        "{error}"
    );
}
