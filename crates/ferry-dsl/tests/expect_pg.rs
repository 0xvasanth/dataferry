//! `expect:` end to end on real Postgres (B8, 0XT-261): a config's
//! assertions type-checked against the discovered source, the flows run by
//! the runner (A5) with the source-side aggregates accumulated at read
//! time, and the assertions evaluated by the verifier (A8) into its report.

mod pg;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use ferry_core::{Lease, Result, Source, TransformOutput};
use ferry_dsl::expect::{Expectations, FileSourceLog, MemorySourceLog, SourceLog};
use ferry_dsl::typecheck::check;
use ferry_registry::state::{PartitionRows, RunId};
use ferry_runtime::{Flow, PartitionObserver, Runner, WriteObserver};
use ferry_verify::{
    AssertionCheck, ChecksumObserver, FileWriteLog, MemoryWriteLog, Report, Scalar, Status,
    Verification, Verifier, WriteLog,
};
use pg::*;

/// `name` from `shop.orders` into `table`, with `qty = 0` filtered out
/// and `total_cents` mapped by `mapping`, and the two assertions every
/// test checks.
fn orders_flow(name: &str, table: &str, mapping: &str) -> String {
    format!(
        "  - name: {name}
    from: shop.orders
    sink: warehouse
    where: qty > 0
    to:
      {table}:
        columns:
          id: id
          total_cents: {mapping}
    expect:
      order_count: count({table}) == source.count()
      order_total: sum({table}.total_cents) == source.sum(total * 100)
"
    )
}

/// The flows of `checked`, runnable against `db`.
async fn flows(
    db: &Db,
    checked: &[ferry_dsl::typecheck::CheckedFlow],
    sink: &TypedSink,
) -> Vec<Flow> {
    let entity = db
        .source()
        .discover()
        .await
        .unwrap()
        .into_iter()
        .find(|e| e.name() == "orders")
        .unwrap();
    checked
        .iter()
        .map(|c| runtime_flow(db, c, &entity, Arc::new(sink.clone())))
        .collect()
}

fn assertion<'v>(v: &'v Verification, flow: &str, name: &str) -> &'v AssertionCheck {
    v.assertions
        .iter()
        .find(|a| a.flow == flow && a.name == name)
        .unwrap_or_else(|| panic!("no assertion {flow}.{name}: {:#?}", v.assertions))
}

fn number(text: &str) -> Scalar {
    Scalar::parse(text)
}

/// Both sides of a checked assertion, compared by value.
#[track_caller]
fn assert_sides(check: &AssertionCheck, left: &str, right: &str) {
    let outcome = check
        .outcome
        .as_ref()
        .unwrap_or_else(|| panic!("{check:#?}"));
    assert_eq!(
        outcome.left.compare(&number(left)),
        Some(std::cmp::Ordering::Equal),
        "left side of {check:#?}"
    );
    assert_eq!(
        outcome.right.compare(&number(right)),
        Some(std::cmp::Ordering::Equal),
        "right side of {check:#?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_correct_assertion_passes_and_a_wrong_mapping_fails_with_both_values() {
    let db = Db::start(ORDERS).await;
    let (_dir, config) = config(&format!(
        "flows:\n{}{}",
        orders_flow("right", "orders_right", "int(round(total * 100))"),
        // Deliberately wrong: a factor of 10 where cents need 100.
        orders_flow("wrong", "orders_wrong", "int(round(total * 10))"),
    ));
    let checked = plan(&db, &config).await;
    let (store, sink) = db.open();
    let log: Arc<dyn SourceLog> = Arc::new(MemorySourceLog::new());
    let writes: Arc<dyn WriteLog> = Arc::new(MemoryWriteLog::new());
    let expectations = Expectations::new(&checked, log);
    let flows = flows(&db, &checked, &sink).await;
    let runner = Runner::new(store.clone(), flows.clone(), settings())
        .unwrap()
        .with_observer(Arc::new(ChecksumObserver::new(writes.clone(), db.target())))
        .with_observer(expectations.observer());
    let report = runner.start(new_run()).await.unwrap();
    assert!(report.is_complete(), "{report:?}");

    let verifier = expectations.verifier(Verifier::new(store, writes, db.target(), &flows));
    let v = verifier.verify(&report.run).await.unwrap();

    // What the source holds for the 40 rows the flows kept, as Postgres
    // computes it: the two sides must be exactly these.
    let cents = db
        .text("SELECT sum(total * 100)::text FROM src.orders WHERE qty > 0")
        .await
        .unwrap();
    let tenths = db
        .text("SELECT sum(round(total * 10))::text FROM src.orders WHERE qty > 0")
        .await
        .unwrap();
    assert_ne!(cents, tenths);

    let right = assertion(&v, "right", "order_total");
    assert!(right.passed, "{right:#?}");
    assert_sides(right, &cents, &cents);
    assert!(assertion(&v, "right", "order_count").passed);
    // 10 of the 50 were filtered: `source.count()` covers written records.
    assert_sides(assertion(&v, "right", "order_count"), "40", "40");

    // The wrong mapping lands faithfully, so counts and checksums pass;
    // only the assertion catches it, with both values.
    let wrong = assertion(&v, "wrong", "order_total");
    assert!(!wrong.passed, "{wrong:#?}");
    assert!(wrong.error.is_none(), "{wrong:#?}");
    assert_sides(wrong, &tenths, &cents);
    assert!(assertion(&v, "wrong", "order_count").passed);
    assert!(v.tables.iter().all(|t| t.passed), "{:#?}", v.tables);
    assert_eq!(v.status, Status::Failed);
    let [problem] = v.problems.as_slice() else {
        panic!("{:#?}", v.problems)
    };
    let (left_shown, right_shown) = {
        let o = wrong.outcome.as_ref().unwrap();
        (o.left.to_string(), o.right.to_string())
    };
    assert_eq!(
        *problem,
        format!(
            "flow `wrong`: assertion `order_total` failed: {left_shown} == {right_shown} is false"
        )
    );

    // Both values reach both report files.
    let report = Report::new(&v);
    let md = report.to_markdown();
    let row = md
        .lines()
        .find(|l| l.contains("order_total") && l.contains("wrong"))
        .unwrap_or_else(|| panic!("{md}"));
    assert!(
        row.contains(&left_shown) && row.contains(&right_shown),
        "{row}"
    );
    let json: serde_json::Value = serde_json::from_str(&report.to_json()).unwrap();
    let wrong_json = json["assertions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["flow"] == "wrong" && a["name"] == "order_total")
        .unwrap();
    assert_eq!(wrong_json["passed"], false);
    assert_eq!(
        wrong_json["expression"],
        "sum(orders_wrong.total_cents) == source.sum(total * 100)"
    );
    assert!(!wrong_json["outcome"]["left"].is_null(), "{wrong_json}");
    assert!(!wrong_json["outcome"]["right"].is_null(), "{wrong_json}");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_assertion_on_an_unknown_column_fails_at_plan_time() {
    let db = Db::start(ORDERS).await;
    let flows = orders_flow("right", "orders_right", "int(round(total * 100))").replace(
        "sum(orders_right.total_cents)",
        "sum(orders_right.totl_cents)",
    );
    let (_dir, target_side) = config(&format!("flows:\n{flows}"));
    // The config loads: the column is only known to be wrong once the
    // flow's targets are type-checked against the discovered source.
    let errors = check(&target_side, &db.catalog().await).unwrap_err();
    let [e] = errors.0.as_slice() else {
        panic!("{errors}")
    };
    assert!(
        e.message
            .contains("`orders_right` has no column `totl_cents`"),
        "{e}"
    );
    assert_eq!(e.help.as_deref(), Some("did you mean `total_cents`?"));
    assert_eq!(e.path, "flows[right].expect.order_total");
    // So nothing ran: no flow exists to run, and no target was created.
    let tables = db
        .text("SELECT count(*)::text FROM information_schema.tables WHERE table_schema = 'dst'")
        .await;
    assert_eq!(tables.as_deref(), Some("0"));

    // The same on the source side: B2's unknown-field error, at plan time.
    let flows = orders_flow("right", "orders_right", "int(round(total * 100))")
        .replace("source.sum(total * 100)", "source.sum(totl * 100)");
    let (_dir, source_side) = config(&format!("flows:\n{flows}"));
    let errors = check(&source_side, &db.catalog().await).unwrap_err();
    let [e] = errors.0.as_slice() else {
        panic!("{errors}")
    };
    assert!(e.message.contains("totl"), "{e}");
    assert_eq!(e.help.as_deref(), Some("did you mean `total`?"));
}

/// Changes the source once the first partition is read and written, as a
/// live application would mid-run: every total goes up by 1000, and the
/// last five orders are deleted before they are read.
#[derive(Clone)]
struct MutateAfterFirstPartition {
    url: String,
    done: Arc<AtomicBool>,
}

#[async_trait]
impl WriteObserver for MutateAfterFirstPartition {
    async fn prepared(&self, _: &RunId, _: &Flow) -> Result<()> {
        Ok(())
    }

    fn begin(&self, _: &RunId, _: &str, _: &Lease) -> Box<dyn PartitionObserver> {
        Box::new(self.clone())
    }
}

#[async_trait]
impl PartitionObserver for MutateAfterFirstPartition {
    fn output(&mut self, _: &TransformOutput) -> Result<()> {
        Ok(())
    }

    async fn finish(self: Box<Self>, _: &PartitionRows) -> Result<()> {
        if !self.done.swap(true, Ordering::SeqCst) {
            let (client, connection) = tokio_postgres::connect(&self.url, tokio_postgres::NoTls)
                .await
                .unwrap();
            tokio::spawn(connection);
            client
                .batch_execute(
                    "UPDATE src.orders SET total = total + 1000; \
                     DELETE FROM src.orders WHERE id > 45;",
                )
                .await
                .unwrap();
        }
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn source_aggregates_come_from_the_rows_actually_read_while_the_source_changes() {
    let db = Db::start(ORDERS).await;
    let (_dir, config) = config(&format!(
        "flows:\n{}",
        orders_flow("right", "orders_right", "int(round(total * 100))")
    ));
    let checked = plan(&db, &config).await;
    let before = db
        .text("SELECT sum(total * 100)::text FROM src.orders WHERE qty > 0")
        .await
        .unwrap();

    let (store, sink) = db.open();
    let log: Arc<dyn SourceLog> = Arc::new(MemorySourceLog::new());
    let writes: Arc<dyn WriteLog> = Arc::new(MemoryWriteLog::new());
    let expectations = Expectations::new(&checked, log);
    let flows = flows(&db, &checked, &sink).await;
    let mutated = Arc::new(AtomicBool::new(false));
    let runner = Runner::new(store.clone(), flows.clone(), settings())
        .unwrap()
        .with_observer(Arc::new(ChecksumObserver::new(writes.clone(), db.target())))
        .with_observer(expectations.observer())
        .with_observer(Arc::new(MutateAfterFirstPartition {
            url: db.url.clone(),
            done: Arc::clone(&mutated),
        }));
    let report = runner.start(new_run()).await.unwrap();
    assert!(report.is_complete(), "{report:?}");
    assert!(mutated.load(Ordering::SeqCst));
    assert!(
        report.counts.done >= 3,
        "the change landed mid-run: {report:?}"
    );

    let v = expectations
        .verifier(Verifier::new(store, writes, db.target(), &flows))
        .verify(&report.run)
        .await
        .unwrap();
    assert_eq!(v.status, Status::Passed, "{:#?}", v.problems);

    // What landed is a mix of old totals (the first partition) and new
    // ones, without the deleted orders, and the source side agrees with it
    // exactly: it summed the rows the run read.
    let landed = db
        .text("SELECT sum(total_cents)::text FROM dst.orders_right")
        .await
        .unwrap();
    let total = assertion(&v, "right", "order_total");
    assert_sides(total, &landed, &landed);
    // Neither the source before the run nor after it says that, so the
    // source wasn't queried for it.
    let after = db
        .text("SELECT sum(total * 100)::text FROM src.orders WHERE qty > 0")
        .await
        .unwrap();
    for other in [&before, &after] {
        assert_ne!(
            number(&landed).compare(&number(other)),
            Some(std::cmp::Ordering::Equal),
            "{landed} vs {other}"
        );
    }
    let count = assertion(&v, "right", "order_count");
    let rows = db
        .text("SELECT count(*)::text FROM dst.orders_right")
        .await
        .unwrap();
    assert_sides(count, &rows, &rows);
    assert_eq!(
        rows, "36",
        "orders 46..=50 were deleted before they were read"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_retried_partition_is_aggregated_once_and_records_survive_the_process() {
    let db = Db::start(ORDERS).await;
    let (_dir, config) = config(&format!(
        "flows:\n{}",
        orders_flow("right", "orders_right", "int(round(total * 100))")
    ));
    let checked = plan(&db, &config).await;
    let (store, sink) = db.open();
    let dir = tempfile::tempdir().unwrap();
    let source_dir = dir.path().join("source");
    let writes_dir = dir.path().join("writes");
    let log: Arc<dyn SourceLog> = Arc::new(FileSourceLog::new(&source_dir));
    let writes: Arc<dyn WriteLog> = Arc::new(FileWriteLog::new(&writes_dir));
    let expectations = Expectations::new(&checked, log.clone());
    let flows = flows(&db, &checked, &sink).await;
    let runner = Runner::new(store.clone(), flows.clone(), settings())
        .unwrap()
        .with_observer(Arc::new(ChecksumObserver::new(writes.clone(), db.target())))
        .with_observer(expectations.observer());

    // The first attempt at one partition records its source aggregates,
    // then its commit fails: the record is there, the rows aren't.
    // Stored ids are `<flow>/<source id>`; the Postgres source numbers
    // its partitions.
    let failing = "right/orders-1".to_owned();
    sink.fail_next_commit(&failing);
    let report = runner.start(new_run()).await.unwrap();
    let run = report.run.clone();
    assert_eq!(report.counts.failed, 1, "{report:?}");
    assert_eq!(report.failures[0].partition.0, failing, "{report:?}");
    let first = log
        .load(&run)
        .await
        .unwrap()
        .into_iter()
        .find(|r| r.partition == failing)
        .expect("the failed attempt recorded before its commit");

    // The source changes before the retry, so the retry reads other
    // values than the failed attempt did: only the retry's may count.
    db.execute("UPDATE src.orders SET total = total + 1").await;
    let report = runner.retry_failed(&run, HASH, ENGINE).await.unwrap();
    assert!(report.is_complete(), "{report:?}");

    // A new process: every log reopened from its files.
    let (store, _) = db.open();
    let log: Arc<dyn SourceLog> = Arc::new(FileSourceLog::new(&source_dir));
    let writes: Arc<dyn WriteLog> = Arc::new(FileWriteLog::new(&writes_dir));
    let v = Expectations::new(&checked, log.clone())
        .verifier(Verifier::new(store, writes, db.target(), &flows))
        .verify(&run)
        .await
        .unwrap();
    assert_eq!(v.status, Status::Passed, "{:#?}", v.problems);
    // The source side matches what landed, which includes the retried
    // partition's new values: the failed attempt's record was replaced.
    let landed = db
        .text("SELECT sum(total_cents)::text FROM dst.orders_right")
        .await
        .unwrap();
    assert_sides(assertion(&v, "right", "order_total"), &landed, &landed);
    assert_sides(assertion(&v, "right", "order_count"), "40", "40");
    let retried = log
        .load(&run)
        .await
        .unwrap()
        .into_iter()
        .find(|r| r.partition == failing)
        .unwrap();
    assert!(retried.epoch > first.epoch, "{first:?} then {retried:?}");
    let key = "sum(total * 100)";
    let (was, is) = (
        first.aggregates[key].value().unwrap(),
        retried.aggregates[key].value().unwrap(),
    );
    assert_ne!(
        was.compare(&is),
        Some(std::cmp::Ordering::Equal),
        "{was} then {is}"
    );
}
