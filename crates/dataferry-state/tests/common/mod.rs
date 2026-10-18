//! Helpers for the Postgres tests: a database in a container, and
//! `testpg`, a minimal state-capable Postgres connector standing in for A3's.
//!
//! Its sink works the way 0004 §8 fixes the shared handles: it takes a
//! client from the connection's `deadpool_postgres::Pool`, opens the
//! partition's transaction with an explicit BEGIN, and under `TxnWithState`
//! hands that same `tokio_postgres::Client` to the ledger before COMMIT.

#![allow(dead_code)]

use std::sync::{Arc, Mutex};

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use async_trait::async_trait;
use dataferry_core::testkit::{SinkFixture, SinkProbe};
use dataferry_core::types::{CanonicalField, CanonicalType};
use dataferry_core::{
    FerryError, IdempotencyStrategy, Lease, Partition, PartitionId, PartitionScope, Result, Sink,
    SinkCaps, SinkTxn, TargetSchema, TargetTable, WriteMode, WriteSummary,
};
use dataferry_registry::{
    BuildContext, BuildError, Connector, Endpoints, Ledger, RawConnection, Roles, Secret,
    StateCapable,
};
use deadpool_postgres::{Manager, Object, Pool};
use schemars::JsonSchema;
use serde::Deserialize;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tokio_postgres::{Client, NoTls};

pub struct Server {
    _container: ContainerAsync<Postgres>,
    port: u16,
}

impl Server {
    pub async fn start() -> Self {
        let container = Postgres::default().start().await.expect("start postgres");
        let port = container.get_host_port_ipv4(5432).await.unwrap();
        Self {
            _container: container,
            port,
        }
    }

    pub fn url(&self, database: &str) -> String {
        format!(
            "postgres://postgres:postgres@127.0.0.1:{}/{database}",
            self.port
        )
    }

    /// A fresh, empty database.
    pub async fn database(&self, name: &str) -> String {
        self.client("postgres")
            .await
            .batch_execute(&format!("CREATE DATABASE {name}"))
            .await
            .unwrap();
        self.url(name)
    }

    pub async fn client(&self, database: &str) -> Client {
        let (client, connection) = tokio_postgres::connect(&self.url(database), NoTls)
            .await
            .unwrap();
        tokio::spawn(connection);
        client
    }
}

/// What A3's `StateCapable::pool` builds: lazy, no I/O.
pub fn pool(url: &str) -> Pool {
    let config: tokio_postgres::Config = url.parse().unwrap();
    Pool::builder(Manager::new(config, NoTls))
        .max_size(16)
        .build()
        .unwrap()
}

/// Ways the test sink's commit can go wrong.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Fault {
    #[default]
    None,
    /// The process dies after the rows and `record_done`, before COMMIT:
    /// the connection is dropped with the transaction open.
    CrashBeforeCommit,
}

#[derive(Clone, Default)]
pub struct TestPg {
    pub fault: Arc<Mutex<Fault>>,
}

impl TestPg {
    pub fn set_fault(&self, fault: Fault) {
        *self.fault.lock().unwrap() = fault;
    }

    fn endpoints(
        &self,
        wants: Roles,
        pool: Pool,
        ledger: Option<Arc<dyn Ledger<Client>>>,
    ) -> Endpoints {
        Endpoints {
            source: None,
            sink: wants.sink.then(|| {
                Arc::new(TestSink {
                    pool,
                    ledger,
                    fault: Arc::clone(&self.fault),
                }) as _
            }),
            finalizer: None,
            ddl: None,
        }
    }
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TestPgOptions {
    url: Secret,
}

impl Connector for TestPg {
    const TYPE: &'static str = "testpg";
    const ROLES: Roles = Roles::SINK;
    type Options = TestPgOptions;

    fn build(
        &self,
        ctx: &BuildContext<'_>,
        options: TestPgOptions,
        wants: Roles,
    ) -> Result<Endpoints, BuildError> {
        let pool = self.pool(ctx, &options)?;
        Ok(self.endpoints(wants, pool, None))
    }

    fn describe(&self, _: &TestPgOptions) -> String {
        "testpg".into()
    }
}

impl StateCapable for TestPg {
    type Pool = Pool;
    type Conn = Client;

    fn pool(&self, _: &BuildContext<'_>, options: &TestPgOptions) -> Result<Pool, BuildError> {
        let config: tokio_postgres::Config = options
            .url
            .expose()
            .parse()
            .map_err(|_| BuildError::option("url", "not a Postgres URL"))?;
        Ok(Pool::builder(Manager::new(config, NoTls))
            .max_size(16)
            .build()
            .expect("a pool without timeouts builds"))
    }

    fn build_with_ledger(
        &self,
        _: &BuildContext<'_>,
        _: TestPgOptions,
        wants: Roles,
        pool: Pool,
        ledger: Arc<dyn Ledger<Client>>,
    ) -> Result<Endpoints, BuildError> {
        Ok(self.endpoints(wants, pool, Some(ledger)))
    }
}

pub fn connection(name: &str, url: &str) -> RawConnection {
    serde_json::from_value(serde_json::json!({
        "name": name,
        "type": "testpg",
        "url": url,
    }))
    .unwrap()
}

fn pg(e: tokio_postgres::Error) -> FerryError {
    FerryError::Connector {
        connector: "testpg".into(),
        message: e.as_db_error().map_or(e.to_string(), |db| db.to_string()),
    }
}

/// Writes `(partition_id, id)` rows. No key on the tables, so a partition
/// written twice shows up as duplicates.
struct TestSink {
    pool: Pool,
    ledger: Option<Arc<dyn Ledger<Client>>>,
    fault: Arc<Mutex<Fault>>,
}

#[async_trait]
impl Sink for TestSink {
    async fn prepare(&self, target: &TargetSchema, _: WriteMode) -> Result<()> {
        let client = self.pool.get().await.map_err(|e| FerryError::Connector {
            connector: "testpg".into(),
            message: e.to_string(),
        })?;
        for table in target.tables() {
            client
                .batch_execute(&format!(
                    "CREATE TABLE IF NOT EXISTS {} (partition_id text NOT NULL, id bigint NOT NULL)",
                    table.name
                ))
                .await
                .map_err(pg)?;
        }
        Ok(())
    }

    async fn begin(
        &self,
        partition: &Partition,
        lease: &Lease,
        strategy: IdempotencyStrategy,
    ) -> Result<Box<dyn SinkTxn>> {
        let caps = self.capabilities().await?;
        if !caps.strategies.contains(&strategy) {
            return Err(FerryError::Connector {
                connector: "testpg".into(),
                message: format!("strategy {strategy:?} is not supported"),
            });
        }
        let client = self.pool.get().await.map_err(|e| FerryError::Connector {
            connector: "testpg".into(),
            message: e.to_string(),
        })?;
        client.batch_execute("BEGIN").await.map_err(pg)?;
        Ok(Box::new(TestTxn {
            client: Some(client),
            partition: partition.id.clone(),
            lease: lease.clone(),
            ledger: match strategy {
                IdempotencyStrategy::TxnWithState => self.ledger.clone(),
                _ => None,
            },
            fault: *self.fault.lock().unwrap(),
            cleared: Vec::new(),
            summary: WriteSummary::default(),
        }))
    }

    async fn capabilities(&self) -> Result<SinkCaps> {
        let mut strategies = Vec::new();
        if self.ledger.is_some() {
            strategies.push(IdempotencyStrategy::TxnWithState);
        }
        strategies.push(IdempotencyStrategy::DeletePartitionThenInsert);
        Ok(SinkCaps {
            strategies,
            bulk_copy: false,
        })
    }
}

struct TestTxn {
    /// `None` once the transaction ended.
    client: Option<Object>,
    partition: PartitionId,
    lease: Lease,
    ledger: Option<Arc<dyn Ledger<Client>>>,
    fault: Fault,
    /// Targets whose earlier rows for this partition were deleted.
    cleared: Vec<String>,
    summary: WriteSummary,
}

impl TestTxn {
    fn client(&mut self) -> &mut Client {
        self.client.as_mut().expect("the transaction is open")
    }

    async fn rollback(&mut self) {
        if let Some(client) = self.client.take() {
            let _ = client.batch_execute("ROLLBACK").await;
        }
    }
}

/// Dropped mid-transaction (a worker that panicked): close the connection
/// rather than return it to the pool inside an open transaction.
impl Drop for TestTxn {
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            drop(Object::take(client));
        }
    }
}

#[async_trait]
impl SinkTxn for TestTxn {
    async fn write(&mut self, target: &str, batch: RecordBatch) -> Result<()> {
        // Every strategy here deletes the partition's earlier rows first,
        // in the same transaction.
        if !self.cleared.iter().any(|t| t == target) {
            let partition = self.partition.0.clone();
            self.client()
                .execute(
                    &format!("DELETE FROM {target} WHERE partition_id = $1"),
                    &[&partition],
                )
                .await
                .map_err(pg)?;
            self.cleared.push(target.to_owned());
        }
        let ids: Vec<i64> = batch
            .column_by_name("id")
            .expect("an id column")
            .as_primitive::<Int64Type>()
            .values()
            .to_vec();
        let partition = self.partition.0.clone();
        self.client()
            .execute(
                &format!("INSERT INTO {target} (partition_id, id) SELECT $1, unnest($2::bigint[])"),
                &[&partition, &ids],
            )
            .await
            .map_err(pg)?;
        *self
            .summary
            .rows_written
            .entry(target.to_owned())
            .or_default() += ids.len() as u64;
        Ok(())
    }

    async fn commit(mut self: Box<Self>) -> Result<WriteSummary> {
        if let Some(ledger) = self.ledger.clone() {
            let lease = self.lease.clone();
            let summary = self.summary.clone();
            if let Err(e) = ledger.record_done(self.client(), &lease, &summary).await {
                self.rollback().await;
                return Err(e);
            }
        }
        if self.fault == Fault::CrashBeforeCommit {
            // Taken out of the pool and dropped: the connection closes with
            // the transaction open, and the server rolls it back.
            drop(Object::take(self.client.take().unwrap()));
            return Err(FerryError::Connector {
                connector: "testpg".into(),
                message: "simulated crash before COMMIT".into(),
            });
        }
        let client = self.client.take().unwrap();
        client.batch_execute("COMMIT").await.map_err(pg)?;
        Ok(std::mem::take(&mut self.summary))
    }

    async fn abort(mut self: Box<Self>) -> Result<()> {
        self.rollback().await;
        Ok(())
    }
}

/// Row counts in a target database.
pub struct Probe {
    pub pool: Pool,
}

#[async_trait]
impl SinkProbe for Probe {
    async fn row_count(&self, target: &str) -> Result<u64> {
        let client = self.pool.get().await.unwrap();
        let row = client
            .query_one(&format!("SELECT count(*) FROM {target}"), &[])
            .await
            .map_err(pg)?;
        Ok(row.get::<_, i64>(0) as u64)
    }
}

/// `(rows, distinct rows)` in `target`: equal when no row was written twice.
pub async fn row_counts(pool: &Pool, target: &str) -> (i64, i64) {
    let client = pool.get().await.unwrap();
    let row = client
        .query_one(
            &format!("SELECT count(*), count(DISTINCT (partition_id, id)) FROM {target}"),
            &[],
        )
        .await
        .unwrap();
    (row.get(0), row.get(1))
}

pub fn rows(ids: std::ops::Range<i64>) -> RecordBatch {
    let id: ArrayRef = Arc::new(Int64Array::from(ids.collect::<Vec<_>>()));
    RecordBatch::try_from_iter([("id", id)]).unwrap()
}

pub fn partition(i: usize) -> Partition {
    Partition {
        id: PartitionId(format!("orders-{i}")),
        entity: "orders".into(),
        scope: PartitionScope::Whole,
    }
}

pub fn schema() -> TargetSchema {
    let table = |name: &str| TargetTable {
        name: name.into(),
        fields: vec![CanonicalField::new("id", CanonicalType::Int64, false)],
        primary_key: vec!["id".into()],
        foreign_keys: Vec::new(),
    };
    TargetSchema::new(vec![table("orders"), table("order_items")]).unwrap()
}

/// Two targets, so the registry's suite sees them commit or roll back
/// together.
pub fn sink_fixture() -> SinkFixture {
    SinkFixture {
        schema: schema(),
        mode: WriteMode::Insert,
        partition: partition(0),
        batches: vec![
            ("orders".into(), rows(0..3)),
            ("order_items".into(), rows(0..5)),
        ],
    }
}
