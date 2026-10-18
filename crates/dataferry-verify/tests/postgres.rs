//! The verifier against a real Postgres (testcontainers): real runs
//! through `dataferry-runtime`, read from the Postgres source (A1), written by
//! a typed test sink, verified by reading the target back through the
//! Postgres source.
//!
//! A8's "done when":
//! - one duplicated target row is a checksum mismatch, and two copies of a
//!   row, which XOR would cancel, are too;
//! - timestamps the target keeps at a lower precision than the source are
//!   normalized, so the checksums match, and a real difference still shows;
//! - a source mutated after the run still verifies, because counts come
//!   from state, not from the source.

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use common::*;
use dataferry_core::{FerryError, Source};
use dataferry_registry::Redactor;
use dataferry_verify::{
    Aggregate, AssertionContext, AssertionOutcome, Checksum, ChecksumSpec, Comparison,
    CompiledAssertion, Constraint, FileWriteLog, MemoryWriteLog, RecordCounts, Report, RunTiming,
    Scalar, Status, Target, Verification, WriteLog,
};
use futures::StreamExt;

/// Row hashes of `table` as it is in the target now.
async fn target_hashes(
    db: &Db,
    table: &dataferry_core::TargetTable,
    spec: &ChecksumSpec,
) -> Vec<u128> {
    let mut stream = db.target().read_back(table).await.unwrap();
    let mut hashes = Vec::new();
    while let Some(batch) = stream.next().await {
        hashes.extend(spec.row_hashes(&batch.unwrap()).unwrap());
    }
    hashes
}

fn xor(hashes: &[u128]) -> u128 {
    hashes.iter().fold(0, |acc, h| acc ^ h)
}

fn table<'a>(v: &'a Verification, name: &str) -> &'a dataferry_verify::TableCheck {
    v.tables.iter().find(|t| t.table == name).unwrap()
}

/// `count(<table>) == source.count()`, as B8 would compile it.
struct CountMatchesSource {
    table: &'static str,
}

#[async_trait]
impl CompiledAssertion for CountMatchesSource {
    fn name(&self) -> &str {
        "order_count"
    }

    fn expression(&self) -> &str {
        "count(orders) == source.count()"
    }

    async fn evaluate(
        &self,
        ctx: &AssertionContext<'_>,
    ) -> dataferry_core::Result<AssertionOutcome> {
        let left = ctx.target(self.table, Aggregate::Count, None).await?;
        let right = Scalar::int(ctx.source_count().into());
        Ok(AssertionOutcome::compare(left, Comparison::Eq, right))
    }
}

/// `sum(orders.amount) == <expected>`.
struct SumIs {
    expected: &'static str,
}

#[async_trait]
impl CompiledAssertion for SumIs {
    fn name(&self) -> &str {
        "amount_total"
    }

    fn expression(&self) -> &str {
        "sum(orders.amount) == <expected>"
    }

    async fn evaluate(
        &self,
        ctx: &AssertionContext<'_>,
    ) -> dataferry_core::Result<AssertionOutcome> {
        let left = ctx.target("orders", Aggregate::Sum, Some("amount")).await?;
        Ok(AssertionOutcome::compare(
            left,
            Comparison::Eq,
            Scalar::parse(self.expected),
        ))
    }
}

/// An assertion that can't be evaluated, whose error echoes a connection
/// string the way a driver error can.
struct Unreachable {
    url: String,
}

#[async_trait]
impl CompiledAssertion for Unreachable {
    fn name(&self) -> &str {
        "unreachable"
    }

    fn expression(&self) -> &str {
        "count(elsewhere) > 0"
    }

    async fn evaluate(&self, _: &AssertionContext<'_>) -> dataferry_core::Result<AssertionOutcome> {
        Err(FerryError::Connector {
            connector: "postgres".into(),
            message: format!("could not connect to {}", self.url),
        })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_split_run_verifies_counts_checksums_constraints_and_assertions() {
    let db = Db::start(ORDERS).await;
    let (store, sink) = db.open();
    let entity = db.entity("orders").await;
    let flows = vec![SplitPlan::flow(
        Arc::new(db.source()),
        &entity,
        Arc::new(sink),
    )];
    let log: Arc<dyn WriteLog> = Arc::new(MemoryWriteLog::new());

    let started = std::time::SystemTime::now();
    let report = runner(&db, store.clone(), flows.clone(), log.clone())
        .start(new_run())
        .await
        .unwrap();
    let finished = std::time::SystemTime::now();
    assert!(report.is_complete(), "{report:?}");
    assert!(report.counts.done >= 5, "{report:?}");

    let v = verifier(&db, store.clone(), &flows, log.clone())
        .with_assertion(
            "orders_split",
            Arc::new(CountMatchesSource { table: "orders" }),
        )
        .with_assertion(
            "orders_split",
            Arc::new(SumIs {
                expected: "1250.00",
            }),
        )
        .verify(&report.run)
        .await
        .unwrap();
    assert_eq!(v.status, Status::Passed, "{:#?}", v.problems);

    // 50 driver rows read: 40 written (to one target or two), 10 filtered
    // by `qty = 0`. The split's extra rows don't count against the reads.
    assert_eq!(v.counts.read, 50);
    let flow = &v.counts.flows[0];
    assert_eq!(
        flow.records,
        RecordCounts {
            written: 40,
            filtered: 10,
            rejected: 0
        }
    );
    assert_eq!(flow.unaccounted, 0);
    // Every third order has a note, minus the noted ones with qty = 0
    // (15, 30, 45).
    let orders = table(&v, "orders");
    let notes = table(&v, "order_notes");
    assert_eq!((orders.written, orders.at_write.rows), (40, 40));
    assert_eq!((notes.written, notes.at_write.rows), (13, 13));
    assert_eq!(orders.read_back, Some(orders.at_write));
    assert_eq!(notes.read_back, Some(notes.at_write));

    // Not-null per non-nullable column, the primary keys, the notes' FK.
    let kinds: Vec<&Constraint> = v.constraints.iter().map(|c| &c.constraint).collect();
    assert!(kinds.contains(&&Constraint::NotNull {
        column: "customer".into()
    }));
    assert!(kinds.contains(&&Constraint::Unique {
        columns: vec!["order_id".into()]
    }));
    assert!(
        kinds
            .iter()
            .any(|k| matches!(k, Constraint::ForeignKey { .. }))
    );
    assert!(
        v.constraints
            .iter()
            .all(|c| c.passed && c.violations == Some(0))
    );

    // Both assertions held, with their values.
    let a = &v.assertions[0];
    assert!(a.passed);
    let outcome = a.outcome.as_ref().unwrap();
    assert_eq!(
        (&outcome.left, &outcome.right),
        (&Scalar::int(40), &Scalar::int(40))
    );
    assert!(v.assertions[1].passed, "{:?}", v.assertions[1]);

    // The report: both files, the config hash and versions, what it proves.
    let dir = tempfile::tempdir().unwrap();
    let redactor = Redactor::new([PASSWORD]);
    let (json_path, md_path) = Report::new(&v)
        .with_run_timing(RunTiming { started, finished })
        .write(dir.path(), &redactor)
        .unwrap();
    let json: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&json_path).unwrap()).unwrap();
    assert_eq!(json["format"], "ferry-report/1");
    assert_eq!(json["status"], "passed");
    assert_eq!(json["run"]["config_hash"], HASH);
    assert_eq!(json["versions"]["engine"], ENGINE);
    assert_eq!(json["run"]["connections"][1]["type"], "postgres");
    assert_eq!(json["counts"]["flows"][0]["records"]["filtered"], 10);
    assert_eq!(json["tables"][0]["at_write"]["rows"], 40);
    // on_error: fail, so nothing was dead-lettered and the table agrees.
    assert_eq!(json["dead_letter"]["dead_lettered"], 0);
    assert_eq!(json["dead_letter"]["rows"], 0);
    assert_eq!(json["dead_letter"]["passed"], true);
    assert!(json["timings"]["run"]["seconds"].as_f64().unwrap() > 0.0);
    assert!(
        json["proves"]
            .as_str()
            .unwrap()
            .contains("what the run computed is what landed")
    );
    assert!(json["does_not_prove"].as_str().unwrap().contains("mapping"));
    let md = std::fs::read_to_string(&md_path).unwrap();
    assert!(md.contains("verification PASSED"), "{md}");
    assert!(md.contains(HASH));
    assert!(
        md.contains("| orders | 40 | 40 | 40 | matches | pass |"),
        "{md}"
    );

    // A failing assertion and one that errors with a connection string in
    // its message: the run fails verification, and the password never
    // reaches the report.
    let v = verifier(&db, store, &flows, log)
        .with_assertion("orders_split", Arc::new(SumIs { expected: "1.00" }))
        .with_assertion(
            "orders_split",
            Arc::new(Unreachable {
                url: db.url.clone(),
            }),
        )
        .verify(&report.run)
        .await
        .unwrap();
    assert_eq!(v.status, Status::Failed);
    assert!(!v.assertions[0].passed && v.assertions[0].error.is_none());
    assert!(v.assertions[1].error.as_deref().unwrap().contains(PASSWORD));
    let (json_path, md_path) = Report::new(&v).write(dir.path(), &redactor).unwrap();
    // Markdown escapes the `***` it was redacted to; it reads the same.
    for (path, redacted) in [(json_path, "***"), (md_path, "\\*\\*\\*")] {
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(text.contains("could not connect to"), "{}", path.display());
        assert!(!text.contains(PASSWORD), "{}: {text}", path.display());
        assert!(text.contains(redacted), "{}: {text}", path.display());
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn one_duplicated_target_row_is_a_checksum_mismatch_and_copies_never_cancel() {
    let db = Db::start(ORDERS).await;
    let (store, sink) = db.open();
    let entity = db.entity("orders").await;
    let flows = vec![identity_flow(
        Arc::new(db.source()),
        &entity,
        "orders",
        Arc::new(sink),
    )];
    let log: Arc<dyn WriteLog> = Arc::new(MemoryWriteLog::new());
    let report = runner(&db, store.clone(), flows.clone(), log.clone())
        .start(new_run())
        .await
        .unwrap();
    assert!(report.is_complete(), "{report:?}");
    let verify = || async {
        verifier(&db, store.clone(), &flows, log.clone())
            .verify(&report.run)
            .await
            .unwrap()
    };
    let clean = verify().await;
    assert_eq!(clean.status, Status::Passed, "{:#?}", clean.problems);
    let written = table(&clean, "orders").at_write;
    assert_eq!(written.rows, 50);

    let target = &flows[0].target.tables()[0];
    let spec = ChecksumSpec::new(target);
    let before = target_hashes(&db, target, &spec).await;

    // One duplicated row.
    db.execute("INSERT INTO dst.orders SELECT * FROM dst.orders WHERE id = 7")
        .await;
    let v = verify().await;
    assert_eq!(v.status, Status::Failed);
    let t = table(&v, "orders");
    let read_back = t.read_back.unwrap();
    assert_eq!(read_back.rows, 51);
    assert_ne!(read_back.sum, written.sum);
    assert!(
        v.problems.iter().any(|p| p.contains("checksum mismatch")),
        "{:#?}",
        v.problems
    );
    // The duplicated key is also a uniqueness violation.
    let unique = v
        .constraints
        .iter()
        .find(|c| matches!(c.constraint, Constraint::Unique { .. }))
        .unwrap();
    assert_eq!(unique.violations, Some(1));

    // Two copies of the row: an XOR of row hashes cancels them and equals
    // the clean XOR again. The sum doesn't.
    db.execute("INSERT INTO dst.orders SELECT * FROM dst.orders WHERE id = 7 LIMIT 1")
        .await;
    let after = target_hashes(&db, target, &spec).await;
    assert_eq!(after.len(), 52);
    assert_eq!(xor(&after), xor(&before), "the scenario XOR can't see");
    let v = verify().await;
    let read_back = table(&v, "orders").read_back.unwrap();
    assert_ne!(read_back.sum, written.sum);
    assert!(v.problems.iter().any(|p| p.contains("checksum mismatch")));

    // Same row count, different rows: drop both copies and row 8, keep the
    // duplicate of 7. Counts agree; only the checksum tells.
    db.execute(
        "DELETE FROM dst.orders WHERE ctid IN (SELECT ctid FROM dst.orders WHERE id = 7 LIMIT 1); \
         DELETE FROM dst.orders WHERE id = 8",
    )
    .await;
    let v = verify().await;
    let t = table(&v, "orders");
    assert_eq!(t.read_back.unwrap().rows, written.rows);
    assert_ne!(t.read_back.unwrap().sum, written.sum);
    assert_eq!(v.status, Status::Failed);
    assert!(
        v.problems
            .iter()
            .any(|p| p.contains("checksum mismatch") && p.contains("`orders`")),
        "{:#?}",
        v.problems
    );
    assert!(
        !v.problems.iter().any(|p| p.contains("the target has")),
        "{:#?}",
        v.problems
    );
}

/// A target that keeps milliseconds and whole seconds, fed microseconds,
/// including halves that round up, and before 1970 (away from zero).
const EVENTS: &str = r#"
CREATE TABLE src.events (id int8 PRIMARY KEY, at timestamptz, wall timestamp, day date);
INSERT INTO src.events VALUES
    (1, '2026-03-01 10:00:00.123456+00', '2026-03-01 10:00:00.499999', '2026-03-01'),
    (2, '2026-03-01 10:00:00.123500+00', '2026-03-01 10:00:00.500000', '2026-03-02'),
    (3, '2026-03-01 10:00:00.999500+00', '2026-03-01 23:59:59.500001', '2026-03-03'),
    (4, '1969-12-31 23:59:59.999500+00', '1969-12-31 23:59:59.500000', '1969-12-31'),
    (5, NULL, NULL, NULL);
CREATE TABLE dst.events (id int8, at timestamptz(3), wall timestamp(0), day date);
"#;

#[tokio::test(flavor = "multi_thread")]
async fn timestamps_the_target_keeps_at_lower_precision_are_normalized() {
    let db = Db::start(EVENTS).await;
    let (store, sink) = db.open();
    let entity = db.entity("events").await;
    let flows = vec![identity_flow(
        Arc::new(db.source()),
        &entity,
        "events",
        Arc::new(sink),
    )];
    let log: Arc<dyn WriteLog> = Arc::new(MemoryWriteLog::new());
    let report = runner(&db, store.clone(), flows.clone(), log.clone())
        .start(new_run())
        .await
        .unwrap();
    assert!(report.is_complete(), "{report:?}");

    // The target really did round what the run wrote.
    assert_eq!(
        db.count(
            "SELECT count(*) FROM dst.events e JOIN src.events s USING (id) \
             WHERE e.at IS DISTINCT FROM s.at OR e.wall IS DISTINCT FROM s.wall"
        )
        .await,
        4
    );

    let v = verifier(&db, store.clone(), &flows, log.clone())
        .verify(&report.run)
        .await
        .unwrap();
    assert_eq!(v.status, Status::Passed, "{:#?}", v.problems);
    let t = table(&v, "events");
    assert_eq!(
        t.timestamp_digits,
        BTreeMap::from([("at".to_string(), 3), ("wall".to_string(), 0)])
    );
    assert_eq!(t.read_back, Some(t.at_write));
    assert_eq!(t.at_write.rows, 5);

    // Without normalization the source's microseconds and the target's
    // rounded values hash differently: it is the rounding that matches.
    let target = &flows[0].target.tables()[0];
    let micros = ChecksumSpec::new(target);
    let mut source_sum = Checksum::default();
    let mut rows = db.source().read(&entity, &whole(&entity)).await.unwrap();
    while let Some(batch) = rows.next().await {
        source_sum.add_batch(&micros, &batch.unwrap()).unwrap();
    }
    let mut target_sum = Checksum::default();
    for h in target_hashes(&db, target, &micros).await {
        target_sum.add(h);
    }
    assert_ne!(source_sum.sum, target_sum.sum);

    // A real difference above the target's precision still shows.
    db.execute("UPDATE dst.events SET at = at + interval '1 millisecond' WHERE id = 2")
        .await;
    let v = verifier(&db, store, &flows, log)
        .verify(&report.run)
        .await
        .unwrap();
    assert_eq!(v.status, Status::Failed);
    assert!(
        v.problems.iter().any(|p| p.contains("checksum mismatch")),
        "{:#?}",
        v.problems
    );
}

fn whole(entity: &dataferry_core::Entity) -> dataferry_core::Partition {
    dataferry_core::Partition {
        id: dataferry_core::PartitionId("whole".into()),
        entity: entity.name().into(),
        scope: dataferry_core::PartitionScope::Whole,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_source_mutated_after_the_run_still_verifies() {
    let db = Db::start(ORDERS).await;
    let (store, sink) = db.open();
    let entity = db.entity("orders").await;
    let flows = vec![SplitPlan::flow(
        Arc::new(db.source()),
        &entity,
        Arc::new(sink),
    )];
    let log: Arc<dyn WriteLog> = Arc::new(MemoryWriteLog::new());
    let report = runner(&db, store.clone(), flows.clone(), log.clone())
        .start(new_run())
        .await
        .unwrap();
    assert!(report.is_complete(), "{report:?}");

    // Rows added, removed and changed in the source after it was read.
    db.execute(
        "INSERT INTO src.orders SELECT i, 'late', now(), 1, 1, NULL, NULL \
             FROM generate_series(51, 80) i; \
         DELETE FROM src.orders WHERE id <= 10; \
         UPDATE src.orders SET amount = amount * 2, qty = 0 WHERE id BETWEEN 11 AND 20",
    )
    .await;
    assert_eq!(db.count("SELECT count(*) FROM src.orders").await, 70);

    let v = verifier(&db, store, &flows, log)
        .with_assertion(
            "orders_split",
            Arc::new(CountMatchesSource { table: "orders" }),
        )
        .verify(&report.run)
        .await
        .unwrap();
    assert_eq!(v.status, Status::Passed, "{:#?}", v.problems);
    // What the run read, not what the source holds now.
    assert_eq!(v.counts.read, 50);
    assert_eq!(v.counts.flows[0].records.written, 40);
    assert_eq!(v.counts.flows[0].records.filtered, 10);
    assert_eq!(table(&v, "orders").read_back.unwrap().rows, 40);
    assert!(v.assertions[0].passed);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_retried_partition_counts_once_and_records_survive_the_process() {
    let db = Db::start(ORDERS).await;
    let (store, sink) = db.open();
    let entity = db.entity("orders").await;
    let flow = |sink: TypedSink| {
        vec![identity_flow(
            Arc::new(db.source()),
            &entity,
            "orders",
            Arc::new(sink),
        )]
    };
    let dir = tempfile::tempdir().unwrap();
    // The first attempt at one partition records its checksum, then its
    // commit fails: the record is there, the rows aren't.
    sink.fail_next_commit("orders/orders-2");
    let log: Arc<dyn WriteLog> = Arc::new(FileWriteLog::new(dir.path()));
    let runner = runner(&db, store.clone(), flow(sink.clone()), log.clone());
    let report = runner.start(new_run()).await.unwrap();
    assert_eq!(report.counts.failed, 1, "{report:?}");
    let recorded = log.load(&report.run).await.unwrap();
    assert!(recorded.iter().any(|r| r.partition == "orders/orders-2"));

    // The failed partition doesn't count: an incomplete run.
    let v = verifier(&db, store.clone(), &flow(sink.clone()), log.clone())
        .verify(&report.run)
        .await
        .unwrap();
    assert_eq!(v.status, Status::Incomplete);
    let t = table(&v, "orders");
    assert_eq!(t.read_back.unwrap(), t.at_write);
    assert!(t.at_write.rows < 50);

    let report = runner
        .retry_failed(&report.run, HASH, ENGINE)
        .await
        .unwrap();
    assert!(report.is_complete(), "{report:?}");

    // A new process: the state store and write log reopened from scratch.
    let (store, sink) = db.open();
    let log: Arc<dyn WriteLog> = Arc::new(FileWriteLog::new(dir.path()));
    let v = verifier(&db, store, &flow(sink), log.clone())
        .verify(&report.run)
        .await
        .unwrap();
    assert_eq!(v.status, Status::Passed, "{:#?}", v.problems);
    let t = table(&v, "orders");
    assert_eq!(t.at_write.rows, 50);
    assert_eq!(t.read_back, Some(t.at_write));
    assert_eq!(v.counts.read, 50);
    // The retry replaced the failed attempt's record with a later epoch.
    let retried = log
        .load(&report.run)
        .await
        .unwrap()
        .into_iter()
        .find(|r| r.partition == "orders/orders-2")
        .unwrap();
    let first = recorded
        .iter()
        .find(|r| r.partition == "orders/orders-2")
        .unwrap();
    assert!(retried.epoch > first.epoch);
}

#[tokio::test(flavor = "multi_thread")]
async fn constraint_checks_find_nulls_and_orphans_after_the_load() {
    let db = Db::start(ORDERS).await;
    let (store, sink) = db.open();
    let entity = db.entity("orders").await;
    let flows = vec![SplitPlan::flow(
        Arc::new(db.source()),
        &entity,
        Arc::new(sink),
    )];
    let log: Arc<dyn WriteLog> = Arc::new(MemoryWriteLog::new());
    let report = runner(&db, store.clone(), flows.clone(), log.clone())
        .start(new_run())
        .await
        .unwrap();
    assert!(report.is_complete(), "{report:?}");

    // Two orders lose their customer, and the orders behind two notes
    // (3 and 6) disappear. A note whose order_id is null references
    // nothing and is no orphan.
    db.execute(
        "UPDATE dst.orders SET customer = NULL WHERE id IN (1, 2); \
         DELETE FROM dst.orders WHERE id IN (3, 6); \
         INSERT INTO dst.order_notes VALUES (NULL, 'loose');",
    )
    .await;
    let v = verifier(&db, store, &flows, log)
        .verify(&report.run)
        .await
        .unwrap();
    assert_eq!(v.status, Status::Failed);
    let violations = |table: &str, kind: &dyn Fn(&Constraint) -> bool| {
        v.constraints
            .iter()
            .find(|c| c.table == table && kind(&c.constraint))
            .unwrap()
            .violations
    };
    assert_eq!(
        violations("orders", &|c| *c
            == Constraint::NotNull {
                column: "customer".into()
            }),
        Some(2)
    );
    assert_eq!(
        violations("order_notes", &|c| matches!(
            c,
            Constraint::ForeignKey { .. }
        )),
        Some(2)
    );
    assert_eq!(
        violations("order_notes", &|c| *c
            == Constraint::NotNull {
                column: "order_id".into()
            }),
        Some(1)
    );
    // Unchanged columns keep passing.
    assert_eq!(
        violations("orders", &|c| *c
            == Constraint::NotNull {
                column: "amount".into()
            }),
        Some(0)
    );
    assert!(
        v.problems
            .iter()
            .any(|p| p.contains("reference no `orders` row in 2 rows"))
    );
}
