//! `prepare` and `finalize` racing on one target schema, on a real
//! Postgres: two workers starting the same flow, or a resume starting while
//! a killed run's prepare is still in the server. Each call checks the
//! catalog and then creates or drops what it found missing or present, so
//! without the schema's advisory lock two of them can both find a table
//! missing and both create it (`duplicate key value violates unique
//! constraint "pg_type_typname_nsp_index"`), or both drop the same foreign
//! key.
//!
//! Every sink here has a pool of its own, as separate processes do, and
//! the calls are spawned together so they overlap in the server.

mod common;

use std::sync::Arc;

use common::{Db, db, lease, order_schema, orders, partition};
use dataferry_connector_postgres::{ForeignKeys, LoadOptions, PostgresSink};
use dataferry_core::{IdempotencyStrategy, Sink, WriteMode};
use dataferry_registry::{Finalized, Finalizer};

/// Calls racing in each round.
const RACERS: usize = 8;

/// `url` with every transaction serializable unless it says otherwise.
fn serializable(db: &Db) -> String {
    db.url_with("options=-c%20default_transaction_isolation%3Dserializable")
}

fn sinks(url: &str, schema: &str, load: LoadOptions) -> Vec<Arc<PostgresSink>> {
    (0..RACERS)
        .map(|_| Arc::new(PostgresSink::new(url, schema).unwrap().with_load(load)))
        .collect()
}

/// Runs `call` on every sink at once and returns each result, by sink.
async fn race<T, F, Fut>(sinks: &[Arc<PostgresSink>], call: F) -> Vec<dataferry_core::Result<T>>
where
    T: Send + 'static,
    F: Fn(Arc<PostgresSink>) -> Fut,
    Fut: Future<Output = dataferry_core::Result<T>> + Send + 'static,
{
    let tasks: Vec<_> = sinks
        .iter()
        .map(|s| tokio::spawn(call(Arc::clone(s))))
        .collect();
    let mut results = Vec::new();
    for task in tasks {
        results.push(task.await.unwrap());
    }
    results
}

async fn race_prepare(sinks: &[Arc<PostgresSink>]) {
    let results = race(sinks, |sink| async move {
        sink.prepare(&order_schema(), WriteMode::Insert).await
    })
    .await;
    for (i, r) in results.iter().enumerate() {
        if let Err(e) = r {
            panic!("prepare {i} of {RACERS} failed: {e}");
        }
    }
}

async fn race_finalize(sinks: &[Arc<PostgresSink>]) {
    let results: Vec<dataferry_core::Result<Finalized>> =
        race(
            sinks,
            |sink| async move { sink.finalize(&order_schema()).await },
        )
        .await;
    for (i, r) in results.iter().enumerate() {
        match r {
            Ok(f) => assert!(f.is_clean(), "finalize {i} of {RACERS}: {f:?}"),
            Err(e) => panic!("finalize {i} of {RACERS} failed: {e}"),
        }
    }
}

/// The schema's tables, foreign keys (with `NOT VALID` if they aren't) and
/// non-constraint indexes, one line each.
async fn objects(db: &Db, schema: &str) -> Vec<String> {
    db.strings(&format!(
        "SELECT 'table ' || c.relname FROM pg_class c \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = '{schema}' AND c.relkind = 'r' AND c.relname <> '_ferry_dropped' \
         UNION ALL \
         SELECT 'fk ' || k.conname || CASE WHEN k.convalidated THEN '' ELSE ' NOT VALID' END \
         FROM pg_constraint k JOIN pg_namespace n ON n.oid = k.connamespace \
         WHERE n.nspname = '{schema}' AND k.contype = 'f' \
         UNION ALL \
         SELECT 'index ' || indexname FROM pg_indexes \
         WHERE schemaname = '{schema}' AND indexname NOT LIKE '%_pkey' \
         ORDER BY 1"
    ))
    .await
}

/// What `_ferry_dropped` records, one line per object.
async fn recorded(db: &Db, schema: &str) -> Vec<String> {
    db.strings(&format!(
        "SELECT table_name || ' ' || kind || ' ' || name FROM \"{schema}\"._ferry_dropped \
         ORDER BY 1"
    ))
    .await
}

const CREATED: [&str; 3] = ["table item_options", "table order_items", "table orders"];

/// The ticket's case: concurrent prepares on a fresh schema all succeed,
/// and each leaves its sink ready to write. Then the same sinks finalize at
/// once, and each declared foreign key is created once and validated.
/// Several rounds, each on a fresh schema, so the race gets more than one
/// chance to show. In odd rounds the schema exists but has no tables (as
/// `public` doesn't), so the prepares race on `CREATE TABLE` rather than on
/// `CREATE SCHEMA`. From round 4 the server's default isolation is
/// serializable, whose snapshot, taken before the lock was granted, would
/// miss the tables the lock's holder created.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_prepares_and_finalizes_on_a_fresh_schema_all_succeed() {
    let db = db().await;
    for round in 0..8 {
        let schema = format!("fresh_{round}");
        let url = if round < 4 {
            db.url.clone()
        } else {
            serializable(&db)
        };
        if round % 2 == 1 {
            db.client
                .batch_execute(&format!("CREATE SCHEMA \"{schema}\""))
                .await
                .unwrap();
        }
        let sinks = sinks(&url, &schema, LoadOptions::default());
        race_prepare(&sinks).await;
        assert_eq!(objects(&db, &schema).await, CREATED, "{schema}");

        // Every sink prepared its targets, not just the one that created
        // them: each writes and commits a partition of its own.
        for (i, sink) in sinks.iter().enumerate() {
            let id = format!("o{i}");
            let mut txn = sink
                .begin(
                    &partition(&id),
                    &lease(&id, 1),
                    IdempotencyStrategy::DeletePartitionThenInsert,
                )
                .await
                .unwrap();
            txn.write("orders", orders(&[(&id, "new")])).await.unwrap();
            txn.commit().await.unwrap();
        }
        assert_eq!(db.count(&schema, "orders").await, RACERS as i64);

        race_finalize(&sinks).await;
        let mut want: Vec<&str> = CREATED.to_vec();
        want.extend([
            "fk item_options_order_id_line_no_fkey",
            "fk order_items_order_id_fkey",
        ]);
        want.sort();
        assert_eq!(objects(&db, &schema).await, want, "{schema}");
        assert_eq!(recorded(&db, &schema).await, Vec::<String>::new());
    }
}

/// A6 on tables that already exist: concurrent prepares drop each foreign
/// key and loose index once and record it once, and concurrent finalizes
/// put each back once, valid, and forget its record. From round 3 the
/// server's default isolation is serializable, as in the fresh-schema test.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_prepares_and_finalizes_on_existing_tables_drop_and_restore_each_object_once() {
    let db = db().await;
    let load = LoadOptions {
        drop_indexes: true,
        foreign_keys: ForeignKeys::Defer,
    };
    for round in 0..6 {
        let schema = format!("existing_{round}");
        let url = if round < 3 {
            db.url.clone()
        } else {
            serializable(&db)
        };
        // A loaded schema: tables, their declared foreign keys, and an
        // index of the user's own.
        let setup = PostgresSink::new(&db.url, &schema).unwrap();
        setup
            .prepare(&order_schema(), WriteMode::Insert)
            .await
            .unwrap();
        assert!(setup.finalize(&order_schema()).await.unwrap().is_clean());
        db.client
            .batch_execute(&format!(
                "CREATE INDEX order_items_by_sku ON \"{schema}\".order_items (sku)"
            ))
            .await
            .unwrap();
        let loaded = objects(&db, &schema).await;
        assert_eq!(
            loaded,
            [
                "fk item_options_order_id_line_no_fkey",
                "fk order_items_order_id_fkey",
                "index order_items_by_sku",
                "table item_options",
                "table order_items",
                "table orders",
            ]
        );

        let sinks = sinks(&url, &schema, load);
        race_prepare(&sinks).await;
        assert_eq!(objects(&db, &schema).await, CREATED, "{schema}");
        assert_eq!(
            recorded(&db, &schema).await,
            [
                "item_options foreign_key item_options_order_id_line_no_fkey",
                "order_items foreign_key order_items_order_id_fkey",
                "order_items index order_items_by_sku",
            ]
        );

        race_finalize(&sinks).await;
        assert_eq!(objects(&db, &schema).await, loaded, "{schema}");
        assert_eq!(recorded(&db, &schema).await, Vec::<String>::new());
    }
}
