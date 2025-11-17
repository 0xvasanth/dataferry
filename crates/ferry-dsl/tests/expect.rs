//! `expect:` assertions (B8, 0XT-261) without a database: what fails when
//! the config is validated, what fails at `plan` time against discovered
//! entities, and how the read-time observer accounts for the records a
//! partition read, wrote, filtered and rejected. The end-to-end runs
//! against real Postgres are in `expect_pg.rs`.

mod common;

use std::sync::Arc;

use arrow_array::{ArrayRef, Decimal128Array, Int64Array, RecordBatch, StringArray};
use ferry_core::testkit::{MemSink, MemSource};
use ferry_core::types::{CanonicalField, CanonicalType, DecimalSpec};
use ferry_core::{
    Entity, Epoch, FerryError, IdempotencyStrategy, IdentityPlan, KeyValue, Lease, PartitionId,
    RejectedRecord, SourceRecordId, TargetBatch, TargetSchema, TargetTable, TransformOutput,
    WriteMode,
};
use ferry_dsl::config::{ConfigErrors, ErrorKind, load};
use ferry_dsl::expect::value::Value;
use ferry_dsl::expect::{Expectations, FileSourceLog, MemorySourceLog, SourceLog, SourceRecord};
use ferry_dsl::typecheck::{Catalog, CheckedFlow, check};
use ferry_registry::state::{PartitionRows, RunId};
use ferry_runtime::{Flow, Input};

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
        "connections:\n  - name: shop\n    type: postgres\n    url: x\n  - name: warehouse\n    type: postgres\n    url: y\n",
    );
    write("flows/f.yaml", flows);
    dir
}

fn money() -> CanonicalType {
    CanonicalType::Decimal(DecimalSpec::new(12, 2).unwrap())
}

fn orders_entity() -> Entity {
    Entity::new(
        "orders",
        vec![
            CanonicalField::new("id", CanonicalType::Int64, false),
            CanonicalField::new("total", money(), false),
            CanonicalField::new("qty", CanonicalType::Int64, false),
            CanonicalField::new("note", CanonicalType::Utf8, true),
        ],
        vec!["id".into()],
    )
    .unwrap()
}

fn customers_entity() -> Entity {
    Entity::new(
        "customers",
        vec![
            CanonicalField::new("id", CanonicalType::Int64, false),
            CanonicalField::new("name", CanonicalType::Utf8, false),
        ],
        vec!["id".into()],
    )
    .unwrap()
}

fn catalog() -> Catalog {
    Catalog::new()
        .with("shop", orders_entity())
        .with("shop", customers_entity())
}

/// One flow from `shop.orders` into `orders` and `order_notes`, with
/// `expect:` entries as given (each `name: assertion`).
fn split_flow(expect: &[&str]) -> String {
    let mut flow = String::from(
        "flows:
  - name: orders
    from: shop.orders
    sink: warehouse
    to:
      orders:
        columns:
          id: id
          total_cents: int(round(total * 100))
          note: note
      order_notes:
        parent: orders
        columns:
          order_id: $parent.id
          note: note
",
    );
    if !expect.is_empty() {
        flow.push_str("    expect:\n");
        for e in expect {
            flow.push_str(&format!("      {e}\n"));
        }
    }
    flow
}

fn checked(flows: &str) -> Result<Vec<CheckedFlow>, ConfigErrors> {
    let dir = project(flows);
    let config = load(dir.path()).unwrap_or_else(|e| panic!("{e}"));
    check(&config, &catalog())
}

// ---------------------------------------------------------------- checking

#[test]
fn the_golden_assertions_check_against_the_golden_entity() {
    let config = load(common::repo("examples/golden/ferry")).unwrap_or_else(|e| panic!("{e}"));
    let flows = check(
        &config,
        &Catalog::new().with("shop", common::golden::entity()),
    )
    .unwrap_or_else(|e| panic!("{e}"));
    let expect = &flows[0].expect;
    let names: Vec<&str> = expect.assertions.iter().map(|a| a.name.as_str()).collect();
    assert_eq!(names, ["order_count", "order_total", "item_count"]);
    assert_eq!(
        expect.assertions[1].text,
        "sum(orders.total_cents) == source.sum(int(round(total * 100)))"
    );
    // `source.count()` is the verifier's count; the other two are
    // accumulated at read time.
    let keys: Vec<&str> = expect.sources.iter().map(|s| s.key.as_str()).collect();
    assert_eq!(
        keys,
        [
            "sum(int(round(total * 100)))",
            "sum(coalesce(cardinality(items), 0))"
        ]
    );
}

#[test]
fn a_source_aggregate_used_twice_is_accumulated_once() {
    let flows = checked(&split_flow(&[
        "a: sum(orders.total_cents) == source.sum(total * 100)",
        "b: abs(sum(orders.total_cents) - source.sum(total * 100)) <= 1",
        "c: max(orders.total_cents) <= source.max(total) * 100",
    ]))
    .unwrap_or_else(|e| panic!("{e}"));
    let keys: Vec<&str> = flows[0]
        .expect
        .sources
        .iter()
        .map(|s| s.key.as_str())
        .collect();
    assert_eq!(keys, ["sum(total * 100)", "max(total)"]);
}

/// The one error `assertion` gives when the config is loaded.
fn load_error(assertion: &str) -> ferry_dsl::config::ConfigError {
    let dir = project(&split_flow(&[&format!("a: {assertion}")]));
    match load(dir.path()) {
        Ok(_) => panic!("`{assertion}` loaded"),
        Err(ConfigErrors(errors)) => {
            assert_eq!(errors.len(), 1, "{errors:?}");
            errors.into_iter().next().unwrap()
        }
    }
}

#[test]
fn the_shape_of_an_assertion_is_checked_when_the_config_is_loaded() {
    let cases = [
        ("count(orders)", "compares two values"),
        ("count(orders) == 1 == 1", "exactly one comparison"),
        (
            "avg(orders.total_cents) == 1",
            "`avg` is not an assertion function",
        ),
        ("sums(orders.total_cents) == 1", "did you mean `sum`?"),
        ("total_cents == 1", "is a column, not an aggregate"),
        ("count(orders) == null", "can't compare with null"),
        (
            "source.count_distinct(id) == 1",
            "`source.count_distinct` isn't supported",
        ),
        ("source.sum() == 1", "`source.sum` takes one expression"),
        (
            "sum(orders.total_cents + 1) == 1",
            "takes one target column",
        ),
        ("count(distinct orders.id) == 1", "takes plain arguments"),
        ("round(count(orders), 1.5) == 1", "whole number"),
        (
            "count(orders) == source.count() and 1 > 0",
            "without `and` or `or`",
        ),
        ("count(orders) == 1 1", "unexpected `1` after the assertion"),
    ];
    for (assertion, wanted) in cases {
        let e = load_error(assertion);
        assert_eq!(e.kind, ErrorKind::Expr, "{assertion}: {e}");
        let shown = format!("{} {}", e.message, e.help.as_deref().unwrap_or_default());
        assert!(shown.contains(wanted), "{assertion}: {e}");
        assert_eq!(e.path, "flows[orders].expect.a", "{assertion}");
        // The entry's own line in flows/f.yaml.
        assert_eq!(e.location.as_ref().unwrap().line, 17, "{assertion}: {e}");
    }
}

#[test]
fn a_source_expression_that_does_not_parse_points_into_the_assertion() {
    let e = load_error("sum(orders.total_cents) == source.sum(total * )");
    assert!(e.message.contains("syntax error"), "{e}");
    let location = e.location.unwrap();
    // `      a: ` is 9 columns; the error is inside the parentheses, not at
    // the start of the assertion.
    assert!(
        location.column > 9 + "sum(orders.total_cents) == source.sum(".len() as u32,
        "{location}"
    );
    // B2 rules apply inside: no aggregates in a row expression.
    let e = load_error("count(orders) == source.sum(sum(total))");
    assert!(e.message.contains("unknown function `sum`"), "{e}");
}

/// Every error `check` gives for `assertion`, which must load cleanly: so
/// the error is a `plan` time one.
fn plan_errors(flows: &str) -> Vec<ferry_dsl::config::ConfigError> {
    let dir = project(flows);
    let config = load(dir.path()).unwrap_or_else(|e| panic!("loading must succeed: {e}"));
    match check(&config, &catalog()) {
        Ok(_) => panic!("type-checked"),
        Err(ConfigErrors(errors)) => errors,
    }
}

#[test]
fn an_unknown_column_fails_at_plan_time_with_a_suggestion() {
    let errors = plan_errors(&split_flow(&[
        "ok: count(orders) == source.count()",
        "total: sum(orders.totl_cents) == source.sum(total * 100)",
    ]));
    let [e] = errors.as_slice() else {
        panic!("{errors:?}")
    };
    assert!(
        e.message.contains("`orders` has no column `totl_cents`"),
        "{e}"
    );
    assert_eq!(e.help.as_deref(), Some("did you mean `total_cents`?"));
    assert_eq!(e.path, "flows[orders].expect.total");
    // Pinpointed: the column name, on the entry's line.
    let at = e.location.as_ref().unwrap();
    assert_eq!(at.line, 18);
    assert_eq!(
        at.column as usize,
        "      total: sum(orders.".len() + 1,
        "{e}"
    );
}

#[test]
fn names_and_types_are_checked_at_plan_time() {
    let cases = [
        // An unknown field of the driver, on the source side (B2's error).
        (
            "sum(orders.total_cents) == source.sum(totl * 100)",
            "totl",
            Some("did you mean `total`?"),
        ),
        (
            "sum(total_cents) == source.sum(total * 100)",
            "`total_cents` is ambiguous in a flow with several targets",
            Some("qualify it with its table: `orders.total_cents`"),
        ),
        (
            "count(ordrs.id) == source.count()",
            "`ordrs` is not a target of this flow",
            Some("did you mean `orders`?"),
        ),
        (
            "count(ordrs) == source.count()",
            "`ordrs` is not a target or a column of a target",
            None,
        ),
        (
            "count(*) == source.count()",
            "`count(*)` is ambiguous",
            Some("count one table's rows: `count(orders)`, `count(order_notes)`"),
        ),
        (
            "sum(orders) == source.count()",
            "`orders` is a table",
            Some("`sum(orders.<column>)`"),
        ),
        (
            "sum(orders.note) == 1",
            "`sum` adds numbers, but `orders.note` is",
            None,
        ),
        (
            "source.sum(note) == 1",
            "`sum` adds numbers, but `note` is",
            None,
        ),
        (
            "min(orders.note) == 3",
            "the assertion compares text with a number",
            None,
        ),
        (
            "min(orders.note) - 1 == 3",
            "`-` needs a number, not text",
            None,
        ),
    ];
    for (assertion, wanted, help) in cases {
        let errors = plan_errors(&split_flow(&[&format!("a: {assertion}")]));
        let [e] = errors.as_slice() else {
            panic!("{assertion}: {errors:?}")
        };
        assert!(e.message.contains(wanted), "{assertion}: {e}");
        if let Some(help) = help {
            assert_eq!(e.help.as_deref(), Some(help), "{assertion}: {e}");
        }
        assert_eq!(e.path, "flows[orders].expect.a", "{assertion}");
    }
}

#[test]
fn a_single_target_may_leave_out_the_table_and_text_compares_with_text() {
    let flow = "flows:
  - name: orders
    from: shop.orders
    sink: warehouse
    to:
      orders:
        columns: \"*\"
    expect:
      count: count(orders) == source.count()
      rows: count(*) == count(id)
      total: sum(total) == source.sum(total)
      notes: max(note) == source.max(note)
      nonempty: min(note) >= ''
";
    let flows = checked(flow).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(flows[0].expect.assertions.len(), 5);
}

#[test]
fn source_aggregates_see_the_driver_record_before_any_join() {
    let flow = "flows:
  - name: orders
    from:
      - entity: shop.orders
        as: o
      - entity: shop.customers
        as: c
        join: {keys: {id: o.qty}, cardinality: one}
    sink: warehouse
    to:
      orders:
        columns:
          id: o.id
          customer: c.name
    expect:
      own: count(orders) == source.count(o.note)
      joined: max(orders.customer) == source.max(c.name)
";
    let errors = plan_errors(flow);
    let [e] = errors.as_slice() else {
        panic!("{errors:?}")
    };
    assert_eq!(e.path, "flows[orders].expect.joined");
    assert!(
        e.help.as_deref().unwrap().contains("before any join"),
        "{e}"
    );
}

// ---------------------------------------------------------------- accounting

fn run() -> RunId {
    RunId("run-1".into())
}

fn lease(partition: &str, epoch: u64) -> Lease {
    Lease {
        partition: PartitionId(partition.into()),
        epoch: Epoch(epoch),
    }
}

fn runtime_flow(name: &str) -> Flow {
    let entity = orders_entity();
    Flow {
        name: name.into(),
        inputs: vec![Input {
            source: Arc::new(MemSource::new()),
            entity: entity.clone(),
            co_partition: None,
        }],
        plan: Arc::new(IdentityPlan::new(entity.clone(), "orders")),
        sink: Arc::new(MemSink::new(vec![
            IdempotencyStrategy::DeletePartitionThenInsert,
        ])),
        target: TargetSchema::new(vec![TargetTable {
            name: "orders".into(),
            fields: entity.fields().to_vec(),
            primary_key: vec!["id".into()],
            foreign_keys: Vec::new(),
        }])
        .unwrap(),
        mode: WriteMode::Insert,
        depends_on: Vec::new(),
        finalizer: None,
    }
}

/// Driver rows: `(id, total in cents, note)`, with `qty = 1`.
fn orders(rows: &[(i64, i128, Option<&str>)]) -> RecordBatch {
    let id: ArrayRef = Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0)));
    let total: ArrayRef = Arc::new(
        Decimal128Array::from_iter_values(rows.iter().map(|r| r.1))
            .with_precision_and_scale(12, 2)
            .unwrap(),
    );
    let qty: ArrayRef = Arc::new(Int64Array::from_iter_values(rows.iter().map(|_| 1)));
    let note: ArrayRef = Arc::new(StringArray::from_iter(rows.iter().map(|r| r.2)));
    RecordBatch::try_from_iter([("id", id), ("total", total), ("qty", qty), ("note", note)])
        .unwrap()
}

fn record(id: i64) -> SourceRecordId {
    SourceRecordId(vec![KeyValue::Int(id)])
}

/// An output accounting for `records`, with rows in `orders` for
/// `written` and `rejected` rejected. The rest are filtered.
fn output(records: &[i64], written: &[i64], rejected: &[i64]) -> TransformOutput {
    let index = |id: &i64| records.iter().position(|r| r == id).unwrap() as u32;
    TransformOutput {
        records: records.iter().map(|&id| record(id)).collect(),
        targets: vec![TargetBatch {
            target: "orders".into(),
            batch: orders(&written.iter().map(|&id| (id, 0, None)).collect::<Vec<_>>()),
            source_index: written.iter().map(index).collect(),
        }],
        rejected: rejected
            .iter()
            .map(|id| RejectedRecord {
                index: index(id),
                target: None,
                error: "bad".into(),
            })
            .collect(),
    }
}

fn expectations(expect: &[&str], log: Arc<dyn SourceLog>) -> Expectations {
    let flow = "flows:
  - name: orders
    from: shop.orders
    sink: warehouse
    to:
      orders:
        columns: \"*\"
    expect:
";
    let mut flow = flow.to_owned();
    for e in expect {
        flow.push_str(&format!("      {e}\n"));
    }
    Expectations::new(&checked(&flow).unwrap_or_else(|e| panic!("{e}")), log)
}

const ASSERTIONS: [&str; 5] = [
    "sum: sum(total) == source.sum(total * 100)",
    "notes: count(note) == source.count(note)",
    "lo: min(total) == source.min(total)",
    "hi: max(total) == source.max(total)",
    "parsed: sum(total) == source.sum(int(note))",
];

fn aggregate(record: &SourceRecord, key: &str) -> Result<Value, String> {
    record.aggregates[key].value()
}

fn n(text: &str) -> Value {
    Value::number(text).unwrap()
}

/// `value` is the number `text`, whatever its scale.
#[track_caller]
fn assert_number(value: Result<Value, String>, text: &str) {
    let value = value.unwrap();
    assert_eq!(
        value.compare(&n(text)),
        Some(std::cmp::Ordering::Equal),
        "{value} is not {text}"
    );
}

#[tokio::test]
async fn only_records_the_partition_wrote_are_accumulated() {
    let log = Arc::new(MemorySourceLog::new());
    let ex = expectations(&ASSERTIONS, log.clone());
    let observer = ex.observer();
    observer
        .prepared(&run(), &runtime_flow("orders"))
        .await
        .unwrap();

    let mut attempt = observer.begin(&run(), "orders", &lease("orders/p0", 1));
    let mut reader = attempt
        .reader()
        .expect("a reader for a flow with source terms");
    // Read in two batches: 1..=3, then 4..=6. `int(note)` fails on 3 and 6,
    // which are rejected and filtered, so it doesn't matter.
    reader
        .batch(&orders(&[
            (1, 1050, Some("1")),
            (2, 200, None),
            (3, 999_999, Some("x")),
        ]))
        .unwrap();
    attempt.output(&output(&[1, 2, 3], &[1, 2], &[3])).unwrap();
    reader
        .batch(&orders(&[
            (4, 1, Some("4")),
            (5, 77_700, Some("5")),
            (6, 5, Some("y")),
        ]))
        .unwrap();
    attempt.output(&output(&[4, 5, 6], &[4, 5], &[])).unwrap();
    attempt
        .finish(&PartitionRows {
            read: 6,
            dead_lettered: 0,
        })
        .await
        .unwrap();

    let [r] = log.load(&run()).await.unwrap().try_into().unwrap();
    assert_eq!((r.partition.as_str(), r.epoch), ("orders/p0", 1));
    // Written: 1, 2, 4 and 5. Totals 10.50 + 2.00 + 0.01 + 777.00.
    assert_number(aggregate(&r, "sum(total * 100)"), "78951");
    assert_number(aggregate(&r, "count(note)"), "3");
    assert_number(aggregate(&r, "min(total)"), "0.01");
    assert_number(aggregate(&r, "max(total)"), "777");
    assert_number(aggregate(&r, "sum(int(note))"), "10");
}

#[tokio::test]
async fn an_expression_that_fails_on_a_written_record_makes_its_aggregate_unknown() {
    let log = Arc::new(MemorySourceLog::new());
    let ex = expectations(&ASSERTIONS, log.clone());
    let observer = ex.observer();
    observer
        .prepared(&run(), &runtime_flow("orders"))
        .await
        .unwrap();
    let mut attempt = observer.begin(&run(), "orders", &lease("orders/p0", 1));
    let mut reader = attempt.reader().unwrap();
    reader
        .batch(&orders(&[(1, 100, Some("three")), (2, 100, Some("2"))]))
        .unwrap();
    attempt.output(&output(&[1, 2], &[1, 2], &[])).unwrap();
    attempt
        .finish(&PartitionRows {
            read: 2,
            dead_lettered: 0,
        })
        .await
        .unwrap();
    let [r] = log.load(&run()).await.unwrap().try_into().unwrap();
    let error = aggregate(&r, "sum(int(note))").unwrap_err();
    assert!(error.contains("`sum(int(note))` failed on"), "{error}");
    // The other aggregates are unaffected.
    assert_number(aggregate(&r, "sum(total * 100)"), "200");
}

#[tokio::test]
async fn a_record_output_but_never_read_makes_the_attempt_unknown() {
    let log = Arc::new(MemorySourceLog::new());
    let ex = expectations(&ASSERTIONS[..1], log.clone());
    let observer = ex.observer();
    observer
        .prepared(&run(), &runtime_flow("orders"))
        .await
        .unwrap();
    let mut attempt = observer.begin(&run(), "orders", &lease("orders/p0", 1));
    let mut reader = attempt.reader().unwrap();
    reader.batch(&orders(&[(1, 100, None)])).unwrap();
    attempt.output(&output(&[1, 2], &[1, 2], &[])).unwrap();
    attempt
        .finish(&PartitionRows {
            read: 1,
            dead_lettered: 0,
        })
        .await
        .unwrap();
    let [r] = log.load(&run()).await.unwrap().try_into().unwrap();
    let error = aggregate(&r, "sum(total * 100)").unwrap_err();
    assert!(error.contains("not seen when it was read"), "{error}");
}

#[tokio::test]
async fn a_flow_with_no_source_aggregates_records_nothing() {
    let log = Arc::new(MemorySourceLog::new());
    let ex = expectations(&["count: count(orders) == source.count()"], log.clone());
    let observer = ex.observer();
    observer
        .prepared(&run(), &runtime_flow("orders"))
        .await
        .unwrap();
    let mut attempt = observer.begin(&run(), "orders", &lease("orders/p0", 1));
    assert!(attempt.reader().is_none());
    attempt
        .finish(&PartitionRows {
            read: 0,
            dead_lettered: 0,
        })
        .await
        .unwrap();
    assert!(log.load(&run()).await.unwrap().is_empty());
    assert_eq!(ex.assertions().len(), 1);
}

/// One attempt at `orders/p0` that writes every record of `rows`.
async fn attempt(
    ex: &Expectations,
    epoch: u64,
    rows: &[(i64, i128, Option<&str>)],
) -> ferry_core::Result<()> {
    let observer = ex.observer();
    observer
        .prepared(&run(), &runtime_flow("orders"))
        .await
        .unwrap();
    let mut attempt = observer.begin(&run(), "orders", &lease("orders/p0", epoch));
    attempt.reader().unwrap().batch(&orders(rows)).unwrap();
    let ids: Vec<i64> = rows.iter().map(|r| r.0).collect();
    attempt.output(&output(&ids, &ids, &[])).unwrap();
    attempt
        .finish(&PartitionRows {
            read: rows.len() as u64,
            dead_lettered: 0,
        })
        .await
}

#[tokio::test]
async fn a_later_attempt_replaces_a_partition_and_a_stale_one_is_refused() {
    for durable in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let log: Arc<dyn SourceLog> = if durable {
            Arc::new(FileSourceLog::new(dir.path()))
        } else {
            Arc::new(MemorySourceLog::new())
        };
        let ex = expectations(&ASSERTIONS[..1], log.clone());
        attempt(&ex, 1, &[(1, 100, None), (2, 100, None)])
            .await
            .unwrap();
        attempt(&ex, 2, &[(1, 300, None)]).await.unwrap();
        // Epoch 1 again, as a worker whose lease was superseded.
        let stale = attempt(&ex, 1, &[(9, 9, None)]).await.unwrap_err();
        assert!(
            matches!(
                stale,
                FerryError::StaleLease {
                    epoch: 1,
                    current: 2,
                    ..
                }
            ),
            "{stale:?}"
        );
        let [r] = log.load(&run()).await.unwrap().try_into().unwrap();
        assert_eq!(r.epoch, 2);
        assert_number(aggregate(&r, "sum(total * 100)"), "300");
        if durable {
            // A new process reads the same records.
            let reopened = FileSourceLog::new(dir.path());
            assert_eq!(reopened.load(&run()).await.unwrap(), vec![r]);
            assert!(
                reopened
                    .load(&RunId("other".into()))
                    .await
                    .unwrap()
                    .is_empty()
            );
        }
    }
}
