//! Shared harness for the sink tests: a real Postgres in a container, the
//! orders → order_items → item_options fixture, a probe, and the state
//! store a run keeps in the same database: dataferry-state's `PgStateBackend`
//! (A4), whose ledger writes the done-record on the sink's open
//! transaction.

#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use async_trait::async_trait;
use dataferry_connector_postgres::PostgresConnector;
use dataferry_core::testkit::{SinkFixture, SinkProbe};
use dataferry_core::types::{CanonicalField, CanonicalType};
use dataferry_core::{
    Epoch, FerryError, ForeignKey, Lease, Partition, PartitionId, PartitionScope, Result, Sink,
    TargetSchema, TargetTable, WriteMode,
};
use dataferry_registry::state::{NewRun, RunId};
use dataferry_registry::{ConnectorRegistry, RawConnection, Roles, StateStore};
use dataferry_state::PgStateBackend;
use serde_json::json;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tokio_postgres::{Client, NoTls};

pub struct Db {
    _container: ContainerAsync<Postgres>,
    pub port: u16,
    /// Superuser URL.
    pub url: String,
    pub client: Client,
}

/// A fresh Postgres.
pub async fn db() -> Db {
    let container = Postgres::default().start().await.expect("start postgres");
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    let client = connect(&url).await;
    Db {
        _container: container,
        port,
        url,
        client,
    }
}

pub async fn connect(url: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(url, NoTls).await.unwrap();
    tokio::spawn(connection);
    client
}

impl Db {
    /// The superuser URL with extra query parameters.
    pub fn url_with(&self, params: &str) -> String {
        format!("{}?{params}", self.url)
    }

    pub async fn count(&self, schema: &str, table: &str) -> i64 {
        self.client
            .query_one(
                &format!("SELECT count(*) FROM \"{schema}\".\"{table}\""),
                &[],
            )
            .await
            .unwrap()
            .get(0)
    }

    /// Rows of a query as strings, for compact assertions.
    pub async fn rows(&self, sql: &str) -> Vec<String> {
        self.client
            .query(&format!("SELECT q.v::text FROM ({sql}) AS q(v)"), &[])
            .await
            .map(|rows| rows.iter().map(|r| r.get(0)).collect())
            .unwrap_or_else(|e| panic!("{sql}: {e}"))
    }

    pub async fn strings(&self, sql: &str) -> Vec<String> {
        self.client
            .query(sql, &[])
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"))
            .iter()
            .map(|r| r.get(0))
            .collect()
    }

    /// The `postgres` connection entry for `schema`.
    pub fn connection(&self, schema: &str) -> RawConnection {
        serde_json::from_value(json!({
            "name": "warehouse",
            "type": "postgres",
            "url": self.url,
            "schema": schema,
        }))
        .unwrap()
    }

    /// A sink built through the registry with the real state backend, as
    /// the state connection of a run is built.
    pub fn sink_with_state(&self, schema: &str) -> (Arc<dyn Sink>, Arc<dyn StateStore>) {
        sink_with_state(self.connection(schema))
    }

    /// A sink built through the registry without the state store.
    pub fn plain_sink(&self, schema: &str) -> Arc<dyn Sink> {
        let mut registry = ConnectorRegistry::new();
        registry.register(PostgresConnector);
        let built = registry
            .build(&self.connection(schema), Roles::SINK)
            .unwrap();
        built.endpoints.sink.unwrap()
    }

    pub fn probe(&self, schema: &str) -> Probe {
        Probe {
            url: self.url.clone(),
            schema: schema.into(),
        }
    }

    /// The epoch `partition` of `run` was recorded done under, if it is
    /// done. Read from dataferry-state's table rather than through `is_done`, so
    /// a test can tell which lease's commit wrote the record.
    pub async fn done_epoch(&self, run: &RunId, partition: &str) -> Option<i64> {
        self.client
            .query_opt(
                "SELECT epoch FROM _ferry_partitions \
                 WHERE run_id = $1 AND partition_id = $2 AND status = 'done'",
                &[&run.0, &partition],
            )
            .await
            .unwrap()
            .map(|r| r.get(0))
    }
}

/// A sink and its state store built from `conn` through the registry, with
/// `PostgresConnector` registered as state-capable with `PgStateBackend`.
pub fn sink_with_state(conn: RawConnection) -> (Arc<dyn Sink>, Arc<dyn StateStore>) {
    let mut registry = ConnectorRegistry::new();
    registry.register_state_capable(PostgresConnector, PgStateBackend);
    let (built, store) = registry.build_with_state(&conn, Roles::SINK).unwrap();
    (built.endpoints.sink.unwrap(), store)
}

/// Counts committed rows by querying the database on its own connection.
pub struct Probe {
    url: String,
    schema: String,
}

#[async_trait]
impl SinkProbe for Probe {
    async fn row_count(&self, target: &str) -> Result<u64> {
        let client = connect(&self.url).await;
        let n: i64 = client
            .query_one(
                &format!("SELECT count(*) FROM \"{}\".\"{target}\"", self.schema),
                &[],
            )
            .await
            .map_err(|e| FerryError::Connector {
                connector: "probe".into(),
                message: e.to_string(),
            })?
            .get(0);
        Ok(n as u64)
    }
}

// ---- fixtures ----

pub fn utf8(name: &str, nullable: bool) -> CanonicalField {
    CanonicalField::new(name, CanonicalType::Utf8, nullable)
}

pub fn int64(name: &str, nullable: bool) -> CanonicalField {
    CanonicalField::new(name, CanonicalType::Int64, nullable)
}

fn fk(columns: &[&str], table: &str, references: &[&str]) -> ForeignKey {
    ForeignKey {
        columns: columns.iter().map(|c| c.to_string()).collect(),
        references_table: table.into(),
        references_columns: references.iter().map(|c| c.to_string()).collect(),
    }
}

/// orders ← order_items ← item_options, linked by foreign keys the way the
/// planner declares `parent:` targets.
pub fn order_schema() -> TargetSchema {
    TargetSchema::new(vec![
        TargetTable {
            name: "orders".into(),
            fields: vec![utf8("id", false), utf8("status", true)],
            primary_key: vec!["id".into()],
            foreign_keys: Vec::new(),
        },
        TargetTable {
            name: "order_items".into(),
            fields: vec![
                utf8("order_id", false),
                int64("line_no", false),
                utf8("sku", true),
                int64("qty", true),
            ],
            primary_key: vec!["order_id".into(), "line_no".into()],
            foreign_keys: vec![fk(&["order_id"], "orders", &["id"])],
        },
        TargetTable {
            name: "item_options".into(),
            fields: vec![
                utf8("order_id", false),
                int64("line_no", false),
                utf8("name", false),
            ],
            primary_key: vec!["order_id".into(), "line_no".into(), "name".into()],
            foreign_keys: vec![fk(
                &["order_id", "line_no"],
                "order_items",
                &["order_id", "line_no"],
            )],
        },
    ])
    .unwrap()
}

pub fn orders(rows: &[(&str, &str)]) -> RecordBatch {
    let id: ArrayRef = Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.0)));
    let status: ArrayRef = Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.1)));
    RecordBatch::try_from_iter_with_nullable([("id", id, false), ("status", status, true)]).unwrap()
}

/// `(order_id, line_no, sku, qty)`.
pub fn items(rows: &[(&str, i64, &str, i64)]) -> RecordBatch {
    let order_id: ArrayRef = Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.0)));
    let line_no: ArrayRef = Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.1)));
    let sku: ArrayRef = Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.2)));
    let qty: ArrayRef = Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.3)));
    RecordBatch::try_from_iter_with_nullable([
        ("order_id", order_id, false),
        ("line_no", line_no, false),
        ("sku", sku, true),
        ("qty", qty, true),
    ])
    .unwrap()
}

/// `(order_id, line_no, name)`.
pub fn options(rows: &[(&str, i64, &str)]) -> RecordBatch {
    let order_id: ArrayRef = Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.0)));
    let line_no: ArrayRef = Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.1)));
    let name: ArrayRef = Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.2)));
    RecordBatch::try_from_iter_with_nullable([
        ("order_id", order_id, false),
        ("line_no", line_no, false),
        ("name", name, false),
    ])
    .unwrap()
}

pub fn partition(id: &str) -> Partition {
    Partition {
        id: PartitionId(id.into()),
        entity: "orders".into(),
        scope: PartitionScope::Whole,
    }
}

pub fn lease(partition: &str, epoch: u64) -> Lease {
    Lease {
        partition: PartitionId(partition.into()),
        epoch: Epoch(epoch),
    }
}

/// One partition of orders with items, the second order's items written in
/// a second batch, as a transform emits them across batches.
pub fn sink_fixture(mode: WriteMode) -> SinkFixture {
    SinkFixture {
        schema: order_schema(),
        mode,
        partition: partition("orders-0"),
        batches: vec![
            ("orders".into(), orders(&[("o1", "new"), ("o2", "paid")])),
            (
                "order_items".into(),
                items(&[("o1", 0, "A", 1), ("o1", 1, "B", 2)]),
            ),
            ("order_items".into(), items(&[("o2", 0, "C", 3)])),
            ("item_options".into(), options(&[("o1", 0, "gift")])),
        ],
    }
}

// ---- runs in the state store ----

/// A run in the real state store whose partitions each sit in a flow of
/// their own name, so a test can lease the one it wants.
pub struct Run {
    pub store: Arc<dyn StateStore>,
    pub id: RunId,
}

impl Run {
    /// Creates a run and plans `partitions` (of `orders`) in it.
    pub async fn plan(store: Arc<dyn StateStore>, partitions: &[&str]) -> Self {
        let id = store
            .create_run(NewRun {
                config_hash: "sha256:sink-tests".into(),
                engine_version: "sink-tests".into(),
                connections: vec![("warehouse".into(), "postgres".into())],
            })
            .await
            .unwrap();
        for p in partitions {
            store
                .store_partitions(&id, p, &[partition(p)])
                .await
                .unwrap();
        }
        Self { store, id }
    }

    /// Leases `partition`, the way a worker does. The lease expires at once,
    /// so the next call takes it over with a higher epoch, as a second
    /// worker does after the first one died.
    pub async fn lease(&self, partition: &str) -> Lease {
        self.store
            .lease_next(&self.id, partition, Duration::ZERO)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("`{partition}` is not pending"))
    }

    pub async fn is_done(&self, partition: &str) -> bool {
        self.store
            .is_done(&self.id, &PartitionId(partition.into()))
            .await
            .unwrap()
    }
}
