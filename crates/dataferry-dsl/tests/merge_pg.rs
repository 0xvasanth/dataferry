//! `examples/pg-merge` end to end on real Postgres (B6, 0XT-264): the
//! config loaded and compiled as written, both flows run by the runner (A5)
//! from the Postgres source (A1) into the Postgres sink (A3), with the state
//! store on the sink's database (`TxnWithState`). The customer lookup is
//! staged and the items are co-partitioned on `order_id`, so both
//! strategies run against the real source.
//!
//! Then the same merge over customers that repeat an id, under `on_error:
//! dead_letter`: the orders of that customer land in `_ferry_dlq` (A7), and
//! the rest of the run is written.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::plan::runtime_flow;
use dataferry_connector_postgres::{PostgresConnector, PostgresSource};
use dataferry_core::{Sink, Source};
use dataferry_dsl::compile::{CompiledFlow, Staging, Strategy, compile};
use dataferry_dsl::config::load;
use dataferry_dsl::typecheck::{Catalog, check};
use dataferry_registry::state::NewRun;
use dataferry_registry::{ConnectorRegistry, RawConnection, Roles, StateStore};
use dataferry_runtime::{OnError, RunReport, Runner, Settings};
use dataferry_state::PgStateBackend;
use serde_json::{Value, json};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tokio_postgres::{Client, NoTls};

struct Db {
    _container: ContainerAsync<Postgres>,
    /// The `app` database: `examples/pg-merge/source.sql`.
    app: String,
    /// The `reporting` database the flows write to.
    reporting: String,
}

async fn connect(url: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(url, NoTls).await.unwrap();
    tokio::spawn(connection);
    client
}

impl Db {
    /// The example's source, then `changes` to it.
    async fn start(changes: &str) -> Self {
        let container = Postgres::default().start().await.expect("start postgres");
        let port = container.get_host_port_ipv4(5432).await.unwrap();
        let url = |db: &str| format!("postgres://postgres:postgres@127.0.0.1:{port}/{db}");
        let admin = connect(&url("postgres")).await;
        admin.batch_execute("CREATE DATABASE app").await.unwrap();
        admin
            .batch_execute("CREATE DATABASE reporting")
            .await
            .unwrap();
        let source = std::fs::read_to_string(common::repo("examples/pg-merge/source.sql")).unwrap();
        let app = connect(&url("app")).await;
        app.batch_execute(&source).await.unwrap();
        app.batch_execute(changes).await.unwrap();
        Self {
            _container: container,
            app: url("app"),
            reporting: url("reporting"),
        }
    }

    fn source(&self) -> Arc<dyn Source> {
        Arc::new(
            PostgresSource::new(&self.app, "public")
                .unwrap()
                .with_batch_rows(2),
        )
    }

    /// `app`'s tables, as the config names them: `app.public.<table>`.
    async fn catalog(&self) -> Catalog {
        let mut catalog = Catalog::new();
        for entity in self.source().discover().await.unwrap() {
            catalog.insert("app.public", entity);
        }
        catalog
    }

    /// The Postgres sink on `reporting`, and the state store beside it.
    fn sink(&self) -> (Arc<dyn Sink>, Arc<dyn StateStore>) {
        let mut registry = ConnectorRegistry::new();
        registry.register_state_capable(PostgresConnector, PgStateBackend);
        let conn: RawConnection = serde_json::from_value(json!({
            "name": "reporting", "type": "postgres", "url": self.reporting,
        }))
        .unwrap();
        let (built, store) = registry.build_with_state(&conn, Roles::SINK).unwrap();
        (built.endpoints.sink.unwrap(), store)
    }

    async fn rows(&self, sql: &str) -> Vec<Value> {
        connect(&self.reporting)
            .await
            .query(&format!("SELECT to_jsonb(r)::text FROM ({sql}) r"), &[])
            .await
            .unwrap()
            .iter()
            .map(|row| serde_json::from_str(row.get::<_, &str>(0)).unwrap())
            .collect()
    }
}

/// `examples/pg-merge/ferry`, compiled against what `app` holds.
async fn compiled(db: &Db) -> Vec<CompiledFlow> {
    let config = load(common::repo("examples/pg-merge/ferry")).unwrap_or_else(|e| panic!("{e}"));
    let catalog = db.catalog().await;
    let checked = check(&config, &catalog).unwrap_or_else(|e| panic!("{e}"));
    config
        .flows
        .iter()
        .zip(&checked)
        .map(|(flow, checked)| compile(flow, checked, &catalog).unwrap_or_else(|e| panic!("{e}")))
        .collect()
}

async fn run(db: &Db, flows: &[(&CompiledFlow, Vec<String>)], on_error: OnError) -> RunReport {
    let (sink, store) = db.sink();
    let flows = flows
        .iter()
        .map(|(compiled, depends_on)| {
            let mut flow = runtime_flow(
                compiled,
                |_| db.source(),
                "app",
                Arc::clone(&sink),
                Staging::DEFAULT_MEMORY,
            );
            flow.depends_on = depends_on.clone();
            flow
        })
        .collect();
    let settings = Settings {
        workers: 2,
        // Several partitions of four orders, each with its own items.
        partition_rows: 2,
        on_error,
        lease_ttl: Duration::from_secs(30),
        heartbeat: Duration::from_secs(5),
        poll: Duration::from_millis(50),
        ..Settings::default()
    };
    Runner::new(store, flows, settings)
        .unwrap()
        .start(NewRun {
            config_hash: "sha256:pg-merge".into(),
            engine_version: "0.0.1-test".into(),
            connections: vec![
                ("app".into(), "postgres".into()),
                ("reporting".into(), "postgres".into()),
            ],
        })
        .await
        .unwrap()
}

const SUMMARIES: &str = "SELECT order_id, customer_id, customer_name, status, \
     to_char(placed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI') AS placed_at, total_cents, items \
     FROM order_summaries ORDER BY order_id";

fn summary(
    order_id: i64,
    customer: Option<(i64, &str)>,
    status: &str,
    day: u32,
    total_cents: i64,
    items: Value,
) -> Value {
    json!({
        "order_id": order_id,
        "customer_id": customer.map(|c| c.0),
        "customer_name": customer.map(|c| c.1),
        "status": status,
        "placed_at": format!("2026-03-{day:02} 10:00"),
        "total_cents": total_cents,
        "items": items,
    })
}

#[tokio::test]
async fn the_pg_merge_example_writes_one_summary_per_order_with_nested_items() {
    let db = Db::start("").await;
    let flows = compiled(&db).await;
    let flow = |name: &str| flows.iter().find(|f| f.name == name).unwrap();
    let (customers, summaries) = (flow("customers"), flow("order_summaries"));
    let strategies: Vec<(&str, &Strategy)> = summaries
        .plan
        .joins()
        .map(|j| (j.alias.as_str(), &j.strategy))
        .collect();
    assert_eq!(
        strategies,
        [
            ("c", &Strategy::Stage),
            (
                "i",
                &Strategy::CoPartition {
                    column: "order_id".into()
                }
            ),
        ]
    );

    let report = run(
        &db,
        &[
            (customers, Vec::new()),
            (summaries, vec!["customers".into()]),
        ],
        OnError::Fail,
    )
    .await;
    assert!(report.is_complete(), "{report:?}");

    let item = |sku: &str, qty: i64, unit_cents: i64| json!({"sku": sku, "qty": qty, "unit_cents": unit_cents});
    assert_eq!(
        db.rows(SUMMARIES).await,
        [
            // Items sorted by line_no, though inserted out of order.
            summary(
                10,
                Some((1, "Ana")),
                "paid",
                1,
                2550,
                json!([item("SKU-1", 2, 1000), item("SKU-2", 1, 550)])
            ),
            // 11 is cancelled. 12 has no items: [], not null.
            summary(12, Some((2, "Bo")), "paid", 3, 0, json!([])),
            // 13 has no customer, and its `qty: 0` item is dropped.
            summary(13, None, "pending", 4, 300, json!([item("SKU-6", 3, 100)])),
        ]
    );
    assert_eq!(
        db.rows("SELECT id, name, email FROM customers ORDER BY id")
            .await,
        [
            json!({"id": 1, "name": "Ana", "email": "ana@example.dev"}),
            json!({"id": 2, "name": "Bo", "email": null}),
        ]
    );
    assert_eq!(report.totals.written.get("order_summaries"), Some(&3));
}

/// Customers keyed by a surrogate `row_id`, with customer 2 twice, and a
/// second order for that customer.
const DUPLICATE_BO: &str = "
ALTER TABLE orders DROP CONSTRAINT orders_customer_id_fkey;
ALTER TABLE customers DROP CONSTRAINT customers_pkey;
ALTER TABLE customers ADD COLUMN row_id bigserial PRIMARY KEY;
INSERT INTO customers (id, name, email) VALUES (2, 'Bo again', 'bo@example.dev');
INSERT INTO orders (id, customer_id, status, placed_at, total_cents)
    VALUES (14, 2, 'paid', '2026-03-05 10:00:00+00', 100);
";

#[tokio::test]
async fn a_duplicate_customer_dead_letters_their_orders_and_the_run_carries_on() {
    let db = Db::start(DUPLICATE_BO).await;
    let flows = compiled(&db).await;
    let summaries = flows.iter().find(|f| f.name == "order_summaries").unwrap();
    // Only the merge: `customers` would upsert two rows with one id.
    let report = run(&db, &[(summaries, Vec::new())], OnError::DeadLetter).await;
    assert!(report.is_complete(), "{report:?}");

    let error = "join `c` is `cardinality: one`, but 2 rows of `app.public.customers` match id = 2";
    assert_eq!(
        db.rows("SELECT flow, source_id, target, error FROM _ferry_dlq ORDER BY source_id")
            .await,
        [
            json!({"flow": "order_summaries", "source_id": "[12]", "target": null, "error": error}),
            json!({"flow": "order_summaries", "source_id": "[14]", "target": null, "error": error}),
        ]
    );
    let item = |sku: &str, qty: i64, unit_cents: i64| json!({"sku": sku, "qty": qty, "unit_cents": unit_cents});
    assert_eq!(
        db.rows(SUMMARIES).await,
        [
            summary(
                10,
                Some((1, "Ana")),
                "paid",
                1,
                2550,
                json!([item("SKU-1", 2, 1000), item("SKU-2", 1, 550)])
            ),
            summary(13, None, "pending", 4, 300, json!([item("SKU-6", 3, 100)])),
        ]
    );
    assert_eq!(report.totals.dead_lettered, 2);
}
