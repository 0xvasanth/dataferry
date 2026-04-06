//! A7's golden dead-letter scenario, rerun with the real compiled plan:
//! `examples/golden/ferry` planned (B7) against the golden documents on a
//! real Mongo, run by the runner (A5) into the real Postgres sink with the
//! state store on the same connection (`TxnWithState`), and verified (A8)
//! with the flow's `expect:` (B8).
//!
//! ferry-verify's `golden_0006_lands_nowhere_but_once_in_the_dead_letter_queue_however_often_it_runs`
//! stood in a hand-written `GoldenPlan` for the mapping, which rejects
//! like `int(qty)` would. Here the rejection is the compiled `int(qty)`'s.

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use common::plan::{expected, sorted};
use common::repo;
use ferry_connector_mongo::MongoConnector;
use ferry_connector_postgres::{PostgresConnector, PostgresOptions};
use ferry_core::IdempotencyStrategy;
use ferry_dsl::config::load;
use ferry_dsl::expect::{Expectations, MemorySourceLog, SourceLog};
use ferry_dsl::plan::plan;
use ferry_dsl::typecheck::Catalog;
use ferry_registry::state::NewRun;
use ferry_registry::{BuildContext, ConnectorRegistry, RawConnection, Roles, StateCapable};
use ferry_runtime::{Runner, Settings};
use ferry_state::PgStateBackend;
use ferry_verify::{ChecksumObserver, MemoryWriteLog, PgTarget, Report, Status, WriteLog};
use mongodb::bson::{Bson, Document};
use serde_json::{Value, json};
use testcontainers_modules::mongo::Mongo;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tokio_postgres::NoTls;

const DEAD_LETTERED: &str = "650000000000000000000006";

fn connection(value: Value) -> RawConnection {
    serde_json::from_value(value).unwrap()
}

/// Rows of a query, as JSON objects.
async fn json_rows(client: &tokio_postgres::Client, sql: &str) -> Vec<Value> {
    client
        .query(&format!("SELECT row_to_json(t)::text FROM ({sql}) t"), &[])
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .iter()
        .map(|r| serde_json::from_str(r.get::<_, &str>(0)).unwrap())
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn golden_0006_lands_nowhere_but_once_in_the_dead_letter_queue_with_the_compiled_plan() {
    let (mongo, pg) = tokio::join!(Mongo::default().start(), Postgres::default().start());
    let (mongo, pg) = (mongo.expect("start mongo"), pg.expect("start postgres"));
    let mongo_url = format!(
        "mongodb://127.0.0.1:{}",
        mongo.get_host_port_ipv4(27017).await.unwrap()
    );
    let pg_url = format!(
        "postgres://postgres:postgres@127.0.0.1:{}/postgres",
        pg.get_host_port_ipv4(5432).await.unwrap()
    );
    let documents: Vec<Document> = serde_json::from_str::<Value>(
        &std::fs::read_to_string(repo("fixtures/golden/source/orders.json")).unwrap(),
    )
    .unwrap()
    .as_array()
    .unwrap()
    .iter()
    .map(|d| match Bson::try_from(d.clone()).unwrap() {
        Bson::Document(doc) => doc,
        other => panic!("not a document: {other:?}"),
    })
    .collect();
    mongodb::Client::with_uri_str(&mongo_url)
        .await
        .unwrap()
        .database("shop")
        .collection::<Document>("orders")
        .insert_many(documents)
        .await
        .unwrap();

    // The golden connections, built through the registry as `ferry run`
    // builds them: the warehouse with the state store on its pool.
    let mut registry = ConnectorRegistry::new();
    registry
        .register_state_capable(PostgresConnector, PgStateBackend)
        .register(MongoConnector);
    let shop = registry
        .build(
            &connection(
                json!({"name": "shop", "type": "mongo", "url": mongo_url, "database": "shop"}),
            ),
            Roles::SOURCE,
        )
        .unwrap()
        .endpoints;
    let warehouse_conn =
        connection(json!({"name": "warehouse", "type": "postgres", "url": pg_url}));
    let (warehouse, store) = registry
        .build_with_state(&warehouse_conn, Roles::SINK)
        .unwrap();
    let source = shop.source.unwrap();
    let sink = warehouse.endpoints.sink.unwrap();

    let config = load(repo("examples/golden/ferry")).unwrap_or_else(|e| panic!("{e}"));
    let mut catalog = Catalog::new();
    for entity in source.discover().await.unwrap() {
        catalog.insert("shop", entity);
    }
    let caps = sink.capabilities().await.unwrap();
    let plan = plan(
        &config,
        &catalog,
        &BTreeMap::from([("warehouse".into(), caps)]),
    )
    .unwrap_or_else(|e| panic!("{e}"));
    let planned = &plan.flows[0];
    assert_eq!(planned.strategy, IdempotencyStrategy::TxnWithState);
    let flow = planned
        .runtime_flow(
            &|_| Some(Arc::clone(&source)),
            Arc::clone(&sink),
            warehouse.endpoints.finalizer.clone(),
            None,
        )
        .unwrap();

    let options: PostgresOptions =
        serde_json::from_value(Value::Object(warehouse_conn.fields.clone())).unwrap();
    let pool = PostgresConnector
        .pool(&BuildContext::new("warehouse"), &options)
        .unwrap();
    let target = Arc::new(PgTarget::new(&pg_url, "public", pool).unwrap());
    let (client, conn) = tokio_postgres::connect(&pg_url, NoTls).await.unwrap();
    tokio::spawn(conn);
    client.batch_execute("SET TIME ZONE 'UTC'").await.unwrap();

    let mut runs = Vec::new();
    for attempt in 0..2 {
        // The second run reads every partition again, …0006's included.
        let log: Arc<dyn WriteLog> = Arc::new(MemoryWriteLog::default());
        let sources: Arc<dyn SourceLog> = Arc::new(MemorySourceLog::new());
        let expectations = Expectations::new(&plan.checked(), Arc::clone(&sources));
        let settings = Settings {
            lease_ttl: Duration::from_secs(10),
            heartbeat: Duration::from_secs(2),
            poll: Duration::from_millis(50),
            ..ferry_dsl::plan::settings(&config)
        };
        let report = Runner::new(Arc::clone(&store), vec![flow.clone()], settings)
            .unwrap()
            .with_observer(Arc::new(ChecksumObserver::new(
                Arc::clone(&log),
                target.clone(),
            )))
            .with_observer(expectations.observer())
            .start(NewRun {
                config_hash: plan.hash.as_str().into(),
                engine_version: "0.0.1-test".into(),
                connections: vec![
                    ("shop".into(), "mongo".into()),
                    ("warehouse".into(), "postgres".into()),
                ],
            })
            .await
            .unwrap();
        assert!(report.is_complete(), "attempt {attempt}: {report:?}");
        assert_eq!(report.counts.done, 3, "partitions of four documents");
        assert_eq!(report.totals.read, 10);
        assert_eq!(report.totals.dead_lettered, 1);

        // Row for row the fixtures: no row of …0006 in either table.
        // Timestamps as the fixtures write them: UTC, to the microsecond.
        let orders = json_rows(
            &client,
            "SELECT id, customer_id, status, \
                    to_char(placed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"') \
                      AS placed_at, \
                    total_cents FROM public.orders",
        )
        .await;
        assert_eq!(sorted(orders, &["id"]), sorted(expected("orders"), &["id"]));
        let items = json_rows(&client, "SELECT * FROM public.order_items").await;
        assert_eq!(
            sorted(items, &["order_id", "line_no"]),
            sorted(expected("order_items"), &["order_id", "line_no"])
        );

        // Exactly one dead letter, the whole document, as this run's.
        let letters = json_rows(
            &client,
            "SELECT flow, source_id, run, target, error, record->>'_id' AS id, \
                    record->'items'->1->'qty' AS qty FROM public._ferry_dlq",
        )
        .await;
        assert_eq!(
            letters,
            [json!({
                "flow": "orders",
                "source_id": format!("[\"{DEAD_LETTERED}\"]"),
                "run": report.run.0,
                "target": "order_items",
                "error": "order_items.qty = `int(qty)`: int(): `\"three\"` is not an integer \
                          (at items[1])",
                "id": DEAD_LETTERED,
                // A variant (`json`) field of the record keeps its JSON text.
                "qty": "\"three\"",
            })],
            "attempt {attempt}"
        );

        let verifier = ferry_verify::Verifier::new(
            Arc::clone(&store),
            Arc::clone(&log),
            target.clone(),
            std::slice::from_ref(&flow),
        )
        .with_finalized(&report);
        let v = expectations
            .verifier(verifier)
            .verify(&report.run)
            .await
            .unwrap();
        assert_eq!(v.status, Status::Passed, "{:#?}", v.problems);
        assert_eq!((v.dead_letters.rows, v.dead_letters.passed), (1, true));
        assert_eq!(
            v.dead_letters.reasons[0].example,
            format!("[\"{DEAD_LETTERED}\"]")
        );
        let passed: Vec<(&str, bool)> = v
            .assertions
            .iter()
            .map(|a| (a.name.as_str(), a.passed))
            .collect();
        assert_eq!(
            passed,
            [
                ("order_count", true),
                ("order_total", true),
                ("item_count", true)
            ]
        );
        let md = Report::new(&v).to_markdown();
        // The error's backticks are escaped: they read as written.
        assert!(md.contains("\\`\"three\"\\` is not an integer"), "{md}");
        runs.push(report.run);
    }
    assert_ne!(runs[0], runs[1]);
}
