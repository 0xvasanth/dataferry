//! Prepare and finalize of the real Postgres sink (A6), run by the runner
//! against a real Postgres: target tables that already exist lose their
//! foreign keys (and, with `drop_indexes`, their secondary indexes) for the
//! load and get them back after it with identical definitions; an orphan
//! is reported by table and key and fails verification; a crash between
//! prepare and finalize still restores everything on resume; and the
//! foreign keys a target schema declares are created at finalize.
//!
//! The sink is built through the registry with the Postgres state backend,
//! the way `dataferry run` builds the state connection.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use common::*;
use dataferry_connector_postgres::PostgresConnector;
use dataferry_core::{
    BatchStream, Entity, Partition, PartitionHint, Result, Sink, Source, SourceCaps,
};
use dataferry_registry::{ConnectorRegistry, Finalizer, RawConnection, Roles, StateStore};
use dataferry_runtime::Flow;
use dataferry_state::PgStateBackend;
use dataferry_verify::{MemoryWriteLog, Report, Status, WriteLog};
use serde_json::json;

/// Orders and their items in `src`, and the same tables in `dst` as a
/// user created them: a foreign key with options that aren't the
/// defaults, indexes of several kinds, and a unique index that enforces
/// something and is never dropped.
const SETUP: &str = r#"
CREATE TABLE src.orders (id int8 PRIMARY KEY, customer text NOT NULL);
INSERT INTO src.orders SELECT i, 'c' || (i % 7) FROM generate_series(1, 40) i;
CREATE TABLE src.order_items (id int8 PRIMARY KEY, order_id int8 NOT NULL, sku text NOT NULL);
INSERT INTO src.order_items SELECT i, (i % 40) + 1, 'sku-' || i FROM generate_series(1, 90) i;

CREATE TABLE dst.orders (id int8 PRIMARY KEY, customer text NOT NULL);
CREATE INDEX orders_customer_idx ON dst.orders (customer) WHERE customer <> '';
CREATE INDEX orders_lower_customer_idx ON dst.orders USING hash (lower(customer));
CREATE UNIQUE INDEX orders_customer_id_key ON dst.orders (customer, id);
CREATE TABLE dst.order_items (id int8 PRIMARY KEY, order_id int8 NOT NULL, sku text NOT NULL);
ALTER TABLE dst.order_items ADD CONSTRAINT order_items_order_fk
    FOREIGN KEY (order_id) REFERENCES dst.orders (id)
    ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED;
CREATE INDEX order_items_order_id_idx ON dst.order_items (order_id DESC NULLS LAST);
"#;

/// Every index and constraint on the two target tables, as Postgres
/// prints them back, with whether each constraint is validated.
async fn definitions(db: &Db) -> Vec<String> {
    let client = db.client().await;
    let rows = client
        .query(
            "SELECT c.relname || ': ' || pg_get_indexdef(i.indexrelid)
             FROM pg_index i
             JOIN pg_class c ON c.oid = i.indexrelid
             WHERE i.indrelid IN ('dst.orders'::regclass, 'dst.order_items'::regclass)
             UNION ALL
             SELECT conrelid::regclass || '.' || conname || ': '
                    || pg_get_constraintdef(oid) || ' validated=' || convalidated
             FROM pg_constraint
             WHERE conrelid IN ('dst.orders'::regclass, 'dst.order_items'::regclass)
             ORDER BY 1",
            &[],
        )
        .await
        .unwrap();
    rows.iter().map(|r| r.get(0)).collect()
}

/// What `_ferry_dropped` holds, as `table.kind.name`.
async fn recorded(db: &Db) -> Vec<String> {
    let client = db.client().await;
    let exists: Option<String> = client
        .query_one("SELECT to_regclass('dst._ferry_dropped')::text", &[])
        .await
        .unwrap()
        .get(0);
    if exists.is_none() {
        return Vec::new();
    }
    client
        .query(
            "SELECT table_name || '.' || kind || '.' || name FROM dst._ferry_dropped ORDER BY 1",
            &[],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect()
}

/// The `dst` sink and the state store, built as a fresh process builds
/// them.
fn open(db: &Db, drop_indexes: bool) -> (Arc<dyn Sink>, Arc<dyn Finalizer>, Arc<dyn StateStore>) {
    let mut registry = ConnectorRegistry::new();
    registry.register_state_capable(PostgresConnector, PgStateBackend);
    let conn: RawConnection = serde_json::from_value(json!({
        "name": "warehouse",
        "type": "postgres",
        "url": db.url,
        "schema": "dst",
        "drop_indexes": drop_indexes,
    }))
    .unwrap();
    let (built, store) = registry.build_with_state(&conn, Roles::SINK).unwrap();
    let endpoints = built.endpoints;
    (
        endpoints.sink.unwrap(),
        endpoints.finalizer.expect("the postgres sink finalizes"),
        store,
    )
}

/// `orders`, then `order_items` once `orders` is done, each copied as is.
async fn flows(
    db: &Db,
    source: Arc<dyn Source>,
    sink: &Arc<dyn Sink>,
    finalizer: &Arc<dyn Finalizer>,
) -> Vec<Flow> {
    let mut orders = identity_flow(
        Arc::clone(&source),
        &db.entity("orders").await,
        "orders",
        Arc::clone(sink),
    );
    let mut items = identity_flow(
        source,
        &db.entity("order_items").await,
        "order_items",
        Arc::clone(sink),
    );
    items.depends_on = vec!["orders".into()];
    for flow in [&mut orders, &mut items] {
        flow.finalizer = Some(Arc::clone(finalizer));
    }
    vec![orders, items]
}

#[tokio::test(flavor = "multi_thread")]
async fn every_dropped_index_and_foreign_key_comes_back_with_an_identical_definition() {
    let db = Db::start(SETUP).await;
    let before = definitions(&db).await;
    assert_eq!(before.len(), 9, "{before:#?}");

    let (sink, finalizer, store) = open(&db, true);
    let flows = flows(&db, Arc::new(db.source()), &sink, &finalizer).await;
    let log: Arc<dyn WriteLog> = Arc::new(MemoryWriteLog::default());
    let report = runner(&db, Arc::clone(&store), flows.clone(), Arc::clone(&log))
        .start(new_run())
        .await
        .unwrap();
    assert!(report.is_complete(), "{report:?}");

    assert_eq!(definitions(&db).await, before);
    assert!(recorded(&db).await.is_empty());
    assert_eq!(report.finalize_problems().count(), 0);
    let mut restored: Vec<&str> = report
        .finalized
        .values()
        .flat_map(|f| f.restored.iter().map(String::as_str))
        .collect();
    restored.sort();
    // The unique index enforces something, so it was never dropped.
    assert_eq!(
        restored,
        vec![
            "order_items.order_items_order_fk",
            "order_items.order_items_order_id_idx",
            "orders.orders_customer_idx",
            "orders.orders_lower_customer_idx",
        ]
    );
    assert_eq!(db.count("SELECT count(*) FROM dst.order_items").await, 90);

    let v = verifier(&db, store, &flows, log)
        .with_finalized(&report)
        .verify(&report.run)
        .await
        .unwrap();
    assert_eq!(v.status, Status::Passed, "{:#?}", v.problems);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_orphan_is_reported_by_table_and_key_and_fails_verification() {
    // Item 999 belongs to an order the source doesn't have.
    let db = Db::start(&format!(
        "{SETUP} INSERT INTO src.order_items VALUES (999, 12345, 'lost');"
    ))
    .await;
    let (sink, finalizer, store) = open(&db, false);
    let flows = flows(&db, Arc::new(db.source()), &sink, &finalizer).await;
    let log: Arc<dyn WriteLog> = Arc::new(MemoryWriteLog::default());
    let runner = runner(&db, Arc::clone(&store), flows.clone(), Arc::clone(&log));
    let report = runner.start(new_run()).await.unwrap();
    assert!(report.is_complete(), "{report:?}");

    // With its key off, the orphan loaded; finalize names it.
    let problems: Vec<_> = report.finalize_problems().collect();
    assert_eq!(problems.len(), 1, "{problems:?}");
    let (flow, problem) = problems[0];
    assert_eq!(flow, "order_items");
    assert_eq!(problem.table, "order_items");
    assert_eq!(problem.object, "order_items_order_fk");
    let orphans = problem.orphans.as_ref().unwrap();
    assert_eq!(
        (orphans.count, orphans.keys.clone()),
        (1, vec!["id=999".into()])
    );

    // The key is back, NOT VALID: never silently off. New orphans fail.
    let defs = definitions(&db).await;
    assert!(
        defs.contains(
            &"dst.order_items.order_items_order_fk: FOREIGN KEY (order_id) REFERENCES \
              dst.orders(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED NOT VALID \
              validated=false"
                .to_owned()
        ),
        "{defs:#?}"
    );
    let client = db.client().await;
    let err = client
        .batch_execute("INSERT INTO dst.order_items VALUES (1000, 54321, 'new')")
        .await
        .unwrap_err();
    let message = err.as_db_error().unwrap().message();
    assert!(message.contains("foreign key constraint"), "{message}");

    // The run fails verification, and the report says where.
    let v = verifier(&db, Arc::clone(&store), &flows, Arc::clone(&log))
        .with_finalized(&report)
        .verify(&report.run)
        .await
        .unwrap();
    assert_eq!(v.status, Status::Failed);
    assert!(
        v.problems
            .iter()
            .any(|p| p.contains("order_items_order_fk") && p.contains("id=999")),
        "{:#?}",
        v.problems
    );
    let md = Report::new(&v).to_markdown();
    assert!(
        md.contains("## Finalize") && md.contains("1: id=999"),
        "{md}"
    );

    // The key stays recorded, so once the data is fixed the next finalize
    // validates it.
    assert_eq!(
        recorded(&db).await,
        vec!["order_items.foreign_key.order_items_order_fk"]
    );
    client
        .batch_execute("DELETE FROM dst.order_items WHERE id = 999")
        .await
        .unwrap();
    let report = runner.resume(&report.run, HASH, ENGINE).await.unwrap();
    assert_eq!(report.finalize_problems().count(), 0);
    assert!(recorded(&db).await.is_empty());
    assert!(
        definitions(&db).await.contains(
            &"dst.order_items.order_items_order_fk: FOREIGN KEY (order_id) REFERENCES \
              dst.orders(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED validated=true"
                .to_owned()
        )
    );
}

/// The Postgres source, whose reads never start once `stuck` is set: a
/// process that dies mid-run.
struct Stuck {
    inner: Arc<dyn Source>,
    stuck: Arc<AtomicBool>,
}

#[async_trait]
impl Source for Stuck {
    async fn discover(&self) -> Result<Vec<Entity>> {
        self.inner.discover().await
    }

    async fn plan_partitions(&self, e: &Entity, hint: &PartitionHint) -> Result<Vec<Partition>> {
        self.inner.plan_partitions(e, hint).await
    }

    async fn read(&self, _: &Entity, _: &Partition) -> Result<BatchStream> {
        self.stuck.store(true, Ordering::SeqCst);
        std::future::pending().await
    }

    async fn capabilities(&self) -> Result<SourceCaps> {
        self.inner.capabilities().await
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_crash_after_prepare_still_restores_every_constraint_on_resume() {
    let db = Db::start(SETUP).await;
    let before = definitions(&db).await;

    // The process dies once prepare dropped everything and the first read
    // started.
    let (sink, finalizer, store) = open(&db, true);
    let stuck = Arc::new(AtomicBool::new(false));
    let source = Arc::new(Stuck {
        inner: Arc::new(db.source()),
        stuck: Arc::clone(&stuck),
    });
    let stuck_flows = flows(&db, source, &sink, &finalizer).await;
    let log: Arc<dyn WriteLog> = Arc::new(MemoryWriteLog::default());
    let runner = runner(&db, Arc::clone(&store), stuck_flows, Arc::clone(&log));
    let run = runner.create(new_run()).await.unwrap();
    let crashed = async {
        while !stuck.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    tokio::select! {
        ended = runner.execute(&run) => panic!("the run should hang in its first read: {ended:?}"),
        () = crashed => {}
    }
    drop(runner);

    // Dropped, and recorded next to the tables.
    let during = definitions(&db).await;
    assert!(
        during.iter().all(|d| !d.contains("order_items_order_fk")
            && !d.contains("orders_customer_idx")
            && !d.contains("order_items_order_id_idx")),
        "{during:#?}"
    );
    assert_eq!(
        recorded(&db).await,
        vec![
            "order_items.foreign_key.order_items_order_fk",
            "order_items.index.order_items_order_id_idx",
            "orders.index.orders_customer_idx",
            "orders.index.orders_lower_customer_idx",
        ]
    );

    // A new process resumes the run: once its leases expire, the load
    // finishes and finalize restores everything as it was.
    let (sink, finalizer, store) = open(&db, true);
    let flows = flows(&db, Arc::new(db.source()), &sink, &finalizer).await;
    let runner = common::runner(&db, store, flows, log);
    let report = runner.resume(&run, HASH, ENGINE).await.unwrap();
    assert!(report.is_complete(), "{report:?}");
    assert_eq!(report.finalize_problems().count(), 0);
    assert_eq!(definitions(&db).await, before);
    assert!(recorded(&db).await.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn foreign_keys_a_target_schema_declares_are_created_at_finalize_once() {
    // No target tables yet: the sink creates them, without keys.
    let db = Db::start(ORDERS).await;
    let (sink, finalizer, store) = open(&db, false);
    let entity = db.entity("orders").await;
    let mut flow = SplitPlan::flow(Arc::new(db.source()), &entity, Arc::clone(&sink));
    flow.finalizer = Some(Arc::clone(&finalizer));
    let declared = "FOREIGN KEY (order_id) REFERENCES dst.orders(id)";
    let keys = || async {
        db.client()
            .await
            .query(
                "SELECT conname || ': ' || pg_get_constraintdef(oid) || ' validated=' \
                 || convalidated FROM pg_constraint \
                 WHERE conrelid = 'dst.order_notes'::regclass AND contype = 'f'",
                &[],
            )
            .await
            .unwrap()
            .iter()
            .map(|r| r.get::<_, String>(0))
            .collect::<Vec<_>>()
    };

    for attempt in 0..2 {
        // The second run loads into the tables the first created, keys and
        // all: the key is dropped, restored, and not declared twice.
        let log: Arc<dyn WriteLog> = Arc::new(MemoryWriteLog::default());
        let runner = runner(
            &db,
            Arc::clone(&store),
            vec![flow.clone()],
            Arc::clone(&log),
        );
        let report = runner.start(new_run()).await.unwrap();
        assert!(report.is_complete(), "attempt {attempt}: {report:?}");
        assert_eq!(report.finalize_problems().count(), 0, "attempt {attempt}");
        assert_eq!(
            keys().await,
            vec![format!(
                "order_notes_order_id_fkey: {declared} validated=true"
            )],
            "attempt {attempt}"
        );
        assert!(recorded(&db).await.is_empty());
        let v = verifier(&db, Arc::clone(&store), std::slice::from_ref(&flow), log)
            .with_finalized(&report)
            .verify(&report.run)
            .await
            .unwrap();
        assert_eq!(v.status, Status::Passed, "{:#?}", v.problems);
    }
}
