//! Shared harness for the sink tests: a real Postgres in a container, the
//! orders → order_items → item_options fixture, a probe, and a minimal
//! state backend whose ledger writes its done-record on the sink's open
//! transaction.
//!
//! The backend stands in for ferry-state's `PgStateBackend` (A4), which is
//! built in parallel: it keeps leases and done-records in tables of the same
//! database, and its ledger refuses a lease older than the partition's
//! newest one. The real backend is tested against this sink at integration.

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use async_trait::async_trait;
use deadpool_postgres::Pool;
use ferry_connector_postgres::PostgresConnector;
use ferry_core::testkit::{SinkFixture, SinkProbe};
use ferry_core::types::{CanonicalField, CanonicalType};
use ferry_core::{
    Epoch, FerryError, ForeignKey, Lease, Partition, PartitionId, PartitionScope, Result, Sink,
    TargetSchema, TargetTable, WriteMode, WriteSummary,
};
use ferry_registry::state::{NewRun, PartitionCounts, RunId, RunRecord};
use ferry_registry::{ConnectorRegistry, Ledger, RawConnection, Roles, StateBackend, StateStore};
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

    /// A sink built through the registry, with the test state backend, as
    /// the state connection of a run is built.
    pub async fn sink_with_state(&self, schema: &str) -> (Arc<dyn Sink>, Arc<dyn StateStore>) {
        let mut registry = ConnectorRegistry::new();
        registry.register_state_capable(PostgresConnector, TestBackend);
        let (built, store) = registry
            .build_with_state(&self.connection(schema), Roles::SINK)
            .unwrap();
        (built.endpoints.sink.unwrap(), store)
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

    /// The done-record epoch of `partition`, if any.
    pub async fn done_epoch(&self, partition: &str) -> Option<i64> {
        self.client
            .query_opt(
                "SELECT epoch FROM ferry_test_state.done WHERE partition = $1",
                &[&partition],
            )
            .await
            .unwrap()
            .map(|r| r.get(0))
    }

    /// Records a lease at `epoch` for `partition`, as the state store does
    /// when another worker takes it over.
    pub async fn lease(&self, partition: &str, epoch: i64) {
        self.client
            .execute(
                "INSERT INTO ferry_test_state.leases (partition, epoch, expires) \
                 VALUES ($1, $2, now() + interval '1 hour') \
                 ON CONFLICT (partition) DO UPDATE SET epoch = EXCLUDED.epoch",
                &[&partition, &epoch],
            )
            .await
            .unwrap();
    }
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

// ---- the test state backend ----

const STATE_SCHEMA: &str = r#"
CREATE SCHEMA IF NOT EXISTS ferry_test_state;
CREATE TABLE IF NOT EXISTS ferry_test_state.runs (id serial PRIMARY KEY);
CREATE TABLE IF NOT EXISTS ferry_test_state.leases (
    partition text PRIMARY KEY, epoch bigint NOT NULL, expires timestamptz NOT NULL);
CREATE TABLE IF NOT EXISTS ferry_test_state.done (
    partition text PRIMARY KEY, epoch bigint NOT NULL, rows bigint NOT NULL);
"#;

/// Creates the test backend's tables. The real backend creates its own on
/// first use; this one is kept minimal.
pub async fn create_state_tables(client: &Client) {
    client.batch_execute(STATE_SCHEMA).await.unwrap();
}

pub struct TestBackend;

impl StateBackend for TestBackend {
    type Pool = Pool;
    type Conn = Client;

    fn open(&self, pool: Pool) -> (Arc<dyn StateStore>, Arc<dyn Ledger<Client>>) {
        (
            Arc::new(TestStore {
                pool,
                plans: Mutex::default(),
            }),
            Arc::new(TestLedger),
        )
    }
}

fn state_error(e: impl std::fmt::Display) -> FerryError {
    FerryError::Connector {
        connector: "test-state".into(),
        message: e.to_string(),
    }
}

/// Writes the done-record on the sink's connection, refusing a lease older
/// than the newest lease or done-record of its partition.
pub struct TestLedger;

#[async_trait]
impl Ledger<Client> for TestLedger {
    async fn record_done(
        &self,
        conn: &mut Client,
        lease: &Lease,
        summary: &WriteSummary,
    ) -> Result<()> {
        let partition = &lease.partition.0;
        let current: i64 = conn
            .query_one(
                "SELECT GREATEST(\
                   (SELECT max(epoch) FROM ferry_test_state.leases WHERE partition = $1), \
                   (SELECT max(epoch) FROM ferry_test_state.done WHERE partition = $1), 0)",
                &[partition],
            )
            .await
            .map_err(state_error)?
            .get(0);
        if current as u64 > lease.epoch.0 {
            return Err(FerryError::StaleLease {
                partition: partition.clone(),
                epoch: lease.epoch.0,
                current: current as u64,
            });
        }
        let rows: u64 = summary.rows_written.values().sum();
        conn.execute(
            "INSERT INTO ferry_test_state.done (partition, epoch, rows) VALUES ($1, $2, $3) \
             ON CONFLICT (partition) DO UPDATE SET epoch = EXCLUDED.epoch, rows = EXCLUDED.rows",
            &[partition, &(lease.epoch.0 as i64), &(rows as i64)],
        )
        .await
        .map_err(state_error)?;
        Ok(())
    }
}

pub struct TestStore {
    pool: Pool,
    plans: Mutex<HashMap<(RunId, String), Vec<Partition>>>,
}

impl TestStore {
    async fn client(&self) -> Result<deadpool_postgres::Object> {
        self.pool.get().await.map_err(state_error)
    }
}

#[async_trait]
impl StateStore for TestStore {
    async fn create_run(&self, _: NewRun) -> Result<RunId> {
        let client = self.client().await?;
        client
            .batch_execute(STATE_SCHEMA)
            .await
            .map_err(state_error)?;
        let id: i32 = client
            .query_one(
                "INSERT INTO ferry_test_state.runs DEFAULT VALUES RETURNING id",
                &[],
            )
            .await
            .map_err(state_error)?
            .get(0);
        Ok(RunId(id.to_string()))
    }

    async fn load_run(&self, run: &RunId) -> Result<Option<RunRecord>> {
        Ok(Some(RunRecord {
            id: run.clone(),
            config_hash: String::new(),
            engine_version: String::new(),
            connections: Vec::new(),
        }))
    }

    async fn store_partitions(
        &self,
        run: &RunId,
        flow: &str,
        partitions: &[Partition],
    ) -> Result<()> {
        self.plans
            .lock()
            .unwrap()
            .insert((run.clone(), flow.into()), partitions.to_vec());
        Ok(())
    }

    async fn load_partitions(&self, run: &RunId, flow: &str) -> Result<Vec<Partition>> {
        Ok(self
            .plans
            .lock()
            .unwrap()
            .get(&(run.clone(), flow.into()))
            .cloned()
            .unwrap_or_default())
    }

    async fn lease_next(&self, run: &RunId, flow: &str, ttl: Duration) -> Result<Option<Lease>> {
        let client = self.client().await?;
        for p in self.load_partitions(run, flow).await? {
            let row = client
                .query_opt(
                    "INSERT INTO ferry_test_state.leases AS l (partition, epoch, expires) \
                     SELECT $1, 1, now() + make_interval(secs => $2) \
                     WHERE NOT EXISTS (SELECT 1 FROM ferry_test_state.done WHERE partition = $1) \
                     ON CONFLICT (partition) DO UPDATE \
                       SET epoch = l.epoch + 1, expires = EXCLUDED.expires \
                       WHERE l.expires <= now() \
                     RETURNING epoch",
                    &[&p.id.0, &ttl.as_secs_f64()],
                )
                .await
                .map_err(state_error)?;
            if let Some(row) = row {
                return Ok(Some(Lease {
                    partition: p.id,
                    epoch: Epoch(row.get::<_, i64>(0) as u64),
                }));
            }
        }
        Ok(None)
    }

    async fn renew(&self, _: &RunId, _: &Lease, _: Duration) -> Result<()> {
        unimplemented!("not used by the sink tests")
    }

    async fn mark_done(&self, _: &RunId, _: &Lease, _: &WriteSummary) -> Result<()> {
        unimplemented!("under TxnWithState the sink's commit records it")
    }

    async fn mark_failed(&self, _: &RunId, _: &Lease, _: &str) -> Result<()> {
        unimplemented!("not used by the sink tests")
    }

    async fn is_done(&self, _: &RunId, partition: &PartitionId) -> Result<bool> {
        let client = self.client().await?;
        Ok(client
            .query_opt(
                "SELECT 1 FROM ferry_test_state.done WHERE partition = $1",
                &[&partition.0],
            )
            .await
            .map_err(state_error)?
            .is_some())
    }

    async fn counts(&self, _: &RunId) -> Result<PartitionCounts> {
        unimplemented!("not used by the sink tests")
    }
}
