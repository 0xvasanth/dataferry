//! A real Postgres (testcontainers) with a source schema `src` read
//! through the Postgres source (A1), a typed test sink writing to `dst`,
//! and the state store in the same database, so the sink commits rows and
//! done-records together (`TxnWithState`).
//!
//! The test sink stands in for the real Postgres sink (A3), which S3
//! reruns these scenarios with. Like a load under A6 it creates target
//! tables without constraints, so a test can inject the duplicates, nulls
//! and orphans the verifier must find.

#![allow(dead_code)]

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Decimal128Type, Float64Type, Int16Type, Int32Type, Int64Type,
    TimestampMicrosecondType,
};
use arrow_array::{Array, BooleanArray, RecordBatch};
use async_trait::async_trait;
use deadpool_postgres::{Manager, Object, Pool};
use ferry_connector_postgres::PostgresSource;
use ferry_core::types::CanonicalType;
use ferry_core::{
    Entity, FerryError, ForeignKey, IdempotencyStrategy, IdentityPlan, Lease, Partition,
    PartitionId, PartitionInputs, Result, Sink, SinkCaps, SinkTxn, Source, TargetBatch,
    TargetSchema, TargetTable, TransformOutput, TransformPlan, TransformStream, WriteMode,
    WriteSummary,
};
use ferry_registry::state::NewRun;
use ferry_registry::{Ledger, StateBackend, StateStore};
use ferry_runtime::{Flow, Input, Runner, Settings};
use ferry_state::PgStateBackend;
use ferry_verify::{ChecksumObserver, PgTarget, Verifier, WriteLog};
use futures::StreamExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tokio_postgres::{Client, NoTls};

pub const HASH: &str = "sha256:verify-tests";
pub const ENGINE: &str = "0.0.1-verify";
/// The application role's password: it must never reach a report.
pub const PASSWORD: &str = "s3cr3t-pw";

pub fn new_run() -> NewRun {
    NewRun {
        config_hash: HASH.into(),
        engine_version: ENGINE.into(),
        connections: vec![
            ("shop".into(), "postgres".into()),
            ("warehouse".into(), "postgres".into()),
        ],
    }
}

pub fn settings() -> Settings {
    Settings {
        workers: 4,
        partition_rows: 10,
        lease_ttl: Duration::from_secs(5),
        heartbeat: Duration::from_secs(1),
        poll: Duration::from_millis(50),
        ..Settings::default()
    }
}

// ---- the database ----

pub struct Db {
    _container: ContainerAsync<Postgres>,
    /// As the application role, whose password is `PASSWORD`.
    pub url: String,
    pub pool: Pool,
}

impl Db {
    /// A fresh server with schemas `src` and `dst`, and `setup` run in it.
    pub async fn start(setup: &str) -> Self {
        let container = Postgres::default().start().await.expect("start postgres");
        let port = container.get_host_port_ipv4(5432).await.unwrap();
        let admin = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
        let client = connect(&admin).await;
        client
            .batch_execute(&format!(
                "CREATE ROLE ferry_app LOGIN SUPERUSER PASSWORD '{PASSWORD}'; \
                 CREATE SCHEMA src; CREATE SCHEMA dst;"
            ))
            .await
            .unwrap();
        client.batch_execute(setup).await.unwrap();
        let url = format!("postgres://ferry_app:{PASSWORD}@127.0.0.1:{port}/postgres");
        let config: tokio_postgres::Config = url.parse().unwrap();
        let pool = Pool::builder(Manager::new(config, NoTls))
            .max_size(16)
            .build()
            .unwrap();
        Self {
            _container: container,
            url,
            pool,
        }
    }

    pub async fn client(&self) -> Client {
        connect(&self.url).await
    }

    pub async fn execute(&self, sql: &str) {
        self.client().await.batch_execute(sql).await.unwrap();
    }

    pub async fn count(&self, sql: &str) -> i64 {
        self.client()
            .await
            .query_one(sql, &[])
            .await
            .unwrap()
            .get(0)
    }

    pub fn source(&self) -> PostgresSource {
        PostgresSource::new(&self.url, "src")
            .unwrap()
            .with_batch_rows(7)
    }

    pub fn target(&self) -> Arc<PgTarget> {
        Arc::new(PgTarget::new(&self.url, "dst", self.pool.clone()).unwrap())
    }

    /// The state store in this database, and a sink bound to its ledger.
    pub fn open(&self) -> (Arc<dyn StateStore>, TypedSink) {
        let (store, ledger) = PgStateBackend.open(self.pool.clone());
        (store, TypedSink::new(self.pool.clone(), ledger))
    }

    pub async fn entity(&self, name: &str) -> Entity {
        self.source()
            .discover()
            .await
            .unwrap()
            .into_iter()
            .find(|e| e.name() == name)
            .unwrap()
    }
}

async fn connect(url: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(url, NoTls).await.unwrap();
    tokio::spawn(connection);
    client
}

/// 50 orders. `qty` is 0 for every fifth (the split flow filters those),
/// `note` is set on every third, `placed_at` has microseconds and is null
/// on every eleventh.
pub const ORDERS: &str = r#"
CREATE TABLE src.orders (
    id int8 PRIMARY KEY,
    customer text NOT NULL,
    placed_at timestamptz,
    amount numeric(12, 2) NOT NULL,
    qty int4 NOT NULL,
    note text,
    meta jsonb
);
INSERT INTO src.orders
SELECT i,
       'c' || (i % 7),
       CASE WHEN i % 11 <> 0
            THEN timestamptz '2026-01-01 00:00:00+00' + i * interval '1 hour 0.000123 sec' END,
       (i * 1.25)::numeric(12, 2),
       i % 5,
       CASE WHEN i % 3 = 0 THEN 'note ' || i END,
       jsonb_build_object('b', i, 'a', 'x')
FROM generate_series(1, 50) i;
"#;

// ---- flows ----

pub fn table_of(entity: &Entity, name: &str) -> TargetTable {
    TargetTable {
        name: name.into(),
        fields: entity.fields().to_vec(),
        primary_key: entity.primary_key().to_vec(),
        foreign_keys: Vec::new(),
    }
}

/// `entity` copied unchanged to `dst.<target>`.
pub fn identity_flow(
    source: Arc<dyn Source>,
    entity: &Entity,
    target: &str,
    sink: Arc<dyn Sink>,
) -> Flow {
    Flow {
        name: target.into(),
        inputs: vec![Input {
            source,
            entity: entity.clone(),
            co_partition: None,
        }],
        plan: Arc::new(IdentityPlan::new(entity.clone(), target)),
        sink,
        target: TargetSchema::new(vec![table_of(entity, target)]).unwrap(),
        mode: WriteMode::Insert,
        depends_on: Vec::new(),
    }
}

/// `orders` split into `orders` (every column) and `order_notes`
/// (`order_id`, `note`, only where a note is set), with `qty = 0` orders
/// filtered out of both: a filtered record, and a record with rows in one
/// target or two.
pub struct SplitPlan {
    entity: Entity,
    inputs: Vec<String>,
    order: Vec<String>,
}

impl SplitPlan {
    pub fn flow(source: Arc<dyn Source>, entity: &Entity, sink: Arc<dyn Sink>) -> Flow {
        let notes = TargetTable {
            name: "order_notes".into(),
            fields: vec![
                entity.field("id").unwrap().clone().renamed("order_id"),
                entity.field("note").unwrap().clone(),
            ],
            primary_key: vec!["order_id".into()],
            foreign_keys: vec![ForeignKey {
                columns: vec!["order_id".into()],
                references_table: "orders".into(),
                references_columns: vec!["id".into()],
            }],
        };
        Flow {
            name: "orders_split".into(),
            inputs: vec![Input {
                source,
                entity: entity.clone(),
                co_partition: None,
            }],
            plan: Arc::new(SplitPlan {
                entity: entity.clone(),
                inputs: vec![entity.name().into()],
                order: vec!["orders".into(), "order_notes".into()],
            }),
            sink,
            target: TargetSchema::new(vec![table_of(entity, "orders"), notes]).unwrap(),
            mode: WriteMode::Insert,
            depends_on: Vec::new(),
        }
    }
}

trait Renamed {
    fn renamed(self, name: &str) -> Self;
}

impl Renamed for ferry_core::types::CanonicalField {
    fn renamed(mut self, name: &str) -> Self {
        self.name = name.into();
        self
    }
}

impl TransformPlan for SplitPlan {
    fn inputs(&self) -> &[String] {
        &self.inputs
    }

    fn write_order(&self) -> &[String] {
        &self.order
    }

    fn execute(&self, mut inputs: PartitionInputs) -> Result<TransformStream> {
        let input = inputs.take(self.entity.name())?;
        let entity = self.entity.clone();
        Ok(input
            .map(move |batch| {
                let batch = batch?;
                let records = entity.record_ids(&batch)?;
                let qty = batch
                    .column_by_name("qty")
                    .unwrap()
                    .as_primitive::<Int32Type>();
                let note = batch.column_by_name("note").unwrap();
                let kept: BooleanArray = qty.iter().map(|q| Some(q != Some(0))).collect();
                let noted: BooleanArray = (0..batch.num_rows())
                    .map(|i| Some(qty.value(i) != 0 && note.is_valid(i)))
                    .collect();
                let rows = |mask: &BooleanArray| -> Vec<u32> {
                    (0..mask.len() as u32)
                        .filter(|&i| mask.value(i as usize))
                        .collect()
                };
                let filter = |b: &RecordBatch, m: &BooleanArray| {
                    arrow_select::filter::filter_record_batch(b, m).unwrap()
                };
                let notes = RecordBatch::try_from_iter([
                    ("order_id", batch.column_by_name("id").unwrap().clone()),
                    ("note", note.clone()),
                ])
                .unwrap();
                Ok(TransformOutput {
                    targets: vec![
                        TargetBatch {
                            target: "orders".into(),
                            source_index: rows(&kept),
                            batch: filter(&batch, &kept),
                        },
                        TargetBatch {
                            target: "order_notes".into(),
                            source_index: rows(&noted),
                            batch: filter(&notes, &noted),
                        },
                    ],
                    records,
                    rejected: Vec::new(),
                })
            })
            .boxed())
    }
}

// ---- running and verifying ----

/// A runner for `flows` on `store`, checksumming into `log`.
pub fn runner(
    db: &Db,
    store: Arc<dyn StateStore>,
    flows: Vec<Flow>,
    log: Arc<dyn WriteLog>,
) -> Runner {
    Runner::new(store, flows, settings())
        .unwrap()
        .with_observer(Arc::new(ChecksumObserver::new(log, db.target())))
}

pub fn verifier(
    db: &Db,
    store: Arc<dyn StateStore>,
    flows: &[Flow],
    log: Arc<dyn WriteLog>,
) -> Verifier {
    Verifier::new(store, log, db.target(), flows)
}

// ---- the typed test sink ----

fn pg(e: tokio_postgres::Error) -> FerryError {
    FerryError::Connector {
        connector: "typedpg".into(),
        message: e.as_db_error().map_or(e.to_string(), |db| db.to_string()),
    }
}

/// Writes rows as typed columns of `dst.<target>`, through `unnest` of one
/// array per column. Offers only `TxnWithState`: its commit records the
/// partition done on the transaction's connection before COMMIT.
#[derive(Clone)]
pub struct TypedSink {
    pool: Pool,
    ledger: Arc<dyn Ledger<Client>>,
    /// Partitions whose next commit fails, once each.
    fail_commit: Arc<Mutex<BTreeSet<String>>>,
}

impl TypedSink {
    fn new(pool: Pool, ledger: Arc<dyn Ledger<Client>>) -> Self {
        Self {
            pool,
            ledger,
            fail_commit: Arc::default(),
        }
    }

    /// The next commit of `partition` (a stored id) fails.
    pub fn fail_next_commit(&self, partition: &str) {
        self.fail_commit.lock().unwrap().insert(partition.into());
    }
}

fn sql_type(ty: &CanonicalType) -> String {
    match ty {
        CanonicalType::Int16 => "int2".into(),
        CanonicalType::Int32 => "int4".into(),
        CanonicalType::Int64 => "int8".into(),
        CanonicalType::Float64 => "float8".into(),
        CanonicalType::Bool => "boolean".into(),
        CanonicalType::Utf8 => "text".into(),
        CanonicalType::Json => "jsonb".into(),
        CanonicalType::Date => "date".into(),
        CanonicalType::Decimal(d) => format!("numeric({}, {})", d.precision(), d.scale()),
        CanonicalType::Timestamp { tz: Some(_) } => "timestamptz".into(),
        CanonicalType::Timestamp { tz: None } => "timestamp".into(),
        other => panic!("the test sink doesn't write {other:?}"),
    }
}

#[async_trait]
impl Sink for TypedSink {
    async fn prepare(&self, target: &TargetSchema, _: WriteMode) -> Result<()> {
        let client = self.pool.get().await.map_err(|e| FerryError::Connector {
            connector: "typedpg".into(),
            message: e.to_string(),
        })?;
        for t in target.tables() {
            let columns: Vec<String> = t
                .fields
                .iter()
                .map(|f| format!("\"{}\" {}", f.name, sql_type(&f.ty)))
                .collect();
            client
                .batch_execute(&format!(
                    "CREATE TABLE IF NOT EXISTS dst.\"{}\" ({})",
                    t.name,
                    columns.join(", ")
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
        assert_eq!(strategy, IdempotencyStrategy::TxnWithState);
        let client = self.pool.get().await.map_err(|e| FerryError::Connector {
            connector: "typedpg".into(),
            message: e.to_string(),
        })?;
        client.batch_execute("BEGIN").await.map_err(pg)?;
        Ok(Box::new(TypedTxn {
            client: Some(client),
            partition: partition.id.clone(),
            lease: lease.clone(),
            sink: self.clone(),
            summary: WriteSummary::default(),
        }))
    }

    async fn capabilities(&self) -> Result<SinkCaps> {
        Ok(SinkCaps {
            strategies: vec![IdempotencyStrategy::TxnWithState],
            bulk_copy: false,
        })
    }
}

struct TypedTxn {
    client: Option<Object>,
    partition: PartitionId,
    lease: Lease,
    sink: TypedSink,
    summary: WriteSummary,
}

impl Drop for TypedTxn {
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            drop(Object::take(client));
        }
    }
}

type Param = Box<dyn tokio_postgres::types::ToSql + Sync + Send>;

/// One column as an array parameter, and how to turn an element of it
/// into the column's type.
fn column_param(array: &dyn Array, ty: &CanonicalType, n: usize) -> (Param, String, String) {
    let at = |i: usize| array.is_valid(i);
    let c = format!("c{n}");
    match ty {
        CanonicalType::Int16 => {
            let a = array.as_primitive::<Int16Type>();
            let v: Vec<Option<i64>> = (0..a.len())
                .map(|i| at(i).then(|| a.value(i).into()))
                .collect();
            (Box::new(v), "int8[]".into(), c)
        }
        CanonicalType::Int32 => {
            let a = array.as_primitive::<Int32Type>();
            let v: Vec<Option<i64>> = (0..a.len())
                .map(|i| at(i).then(|| a.value(i).into()))
                .collect();
            (Box::new(v), "int8[]".into(), c)
        }
        CanonicalType::Int64 => {
            let a = array.as_primitive::<Int64Type>();
            let v: Vec<Option<i64>> = (0..a.len()).map(|i| at(i).then(|| a.value(i))).collect();
            (Box::new(v), "int8[]".into(), c)
        }
        CanonicalType::Float64 => {
            let a = array.as_primitive::<Float64Type>();
            let v: Vec<Option<f64>> = (0..a.len()).map(|i| at(i).then(|| a.value(i))).collect();
            (Box::new(v), "float8[]".into(), c)
        }
        CanonicalType::Bool => {
            let a = array.as_boolean();
            let v: Vec<Option<bool>> = (0..a.len()).map(|i| at(i).then(|| a.value(i))).collect();
            (Box::new(v), "boolean[]".into(), c)
        }
        CanonicalType::Utf8 | CanonicalType::Json => {
            let a = array.as_string::<i32>();
            let v: Vec<Option<String>> = (0..a.len())
                .map(|i| at(i).then(|| a.value(i).to_owned()))
                .collect();
            let cast = if *ty == CanonicalType::Json {
                format!("{c}::jsonb")
            } else {
                c
            };
            (Box::new(v), "text[]".into(), cast)
        }
        CanonicalType::Decimal(d) => {
            let a = array.as_primitive::<Decimal128Type>();
            let v: Vec<Option<String>> = (0..a.len())
                .map(|i| at(i).then(|| a.value_as_string(i)))
                .collect();
            let cast = format!("{c}::numeric({}, {})", d.precision(), d.scale());
            (Box::new(v), "text[]".into(), cast)
        }
        CanonicalType::Date => {
            let a = array.as_primitive::<Date32Type>();
            let v: Vec<Option<i32>> = (0..a.len()).map(|i| at(i).then(|| a.value(i))).collect();
            (
                Box::new(v),
                "int4[]".into(),
                format!("date '1970-01-01' + {c}"),
            )
        }
        CanonicalType::Timestamp { tz } => {
            let a = array.as_primitive::<TimestampMicrosecondType>();
            let v: Vec<Option<i64>> = (0..a.len()).map(|i| at(i).then(|| a.value(i))).collect();
            let epoch = if tz.is_some() {
                "timestamptz '1970-01-01 00:00:00+00'"
            } else {
                "timestamp '1970-01-01 00:00:00'"
            };
            (
                Box::new(v),
                "int8[]".into(),
                format!("{epoch} + {c} * interval '1 microsecond'"),
            )
        }
        other => panic!("the test sink doesn't write {other:?}"),
    }
}

impl TypedTxn {
    fn client(&mut self) -> &mut Client {
        self.client.as_mut().expect("the transaction is open")
    }
}

#[async_trait]
impl SinkTxn for TypedTxn {
    async fn write(&mut self, target: &str, batch: RecordBatch) -> Result<()> {
        let schema = batch.schema();
        let mut params: Vec<Param> = Vec::new();
        let (mut unnest, mut names, mut select) = (Vec::new(), Vec::new(), Vec::new());
        for (n, field) in schema.fields().iter().enumerate() {
            let ty = ferry_core::types::CanonicalField::from_arrow(field)
                .unwrap()
                .ty;
            let (param, array_type, expr) = column_param(batch.column(n).as_ref(), &ty, n);
            params.push(param);
            unnest.push(format!("${}::{array_type}", n + 1));
            names.push(format!("\"{}\"", field.name()));
            select.push(expr);
        }
        let columns: Vec<String> = (0..names.len()).map(|n| format!("c{n}")).collect();
        let sql = format!(
            "INSERT INTO dst.\"{target}\" ({}) SELECT {} FROM unnest({}) AS u({})",
            names.join(", "),
            select.join(", "),
            unnest.join(", "),
            columns.join(", ")
        );
        let refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
            params.iter().map(|p| p.as_ref() as _).collect();
        self.client().execute(&sql, &refs).await.map_err(pg)?;
        *self
            .summary
            .rows_written
            .entry(target.to_owned())
            .or_default() += batch.num_rows() as u64;
        Ok(())
    }

    async fn commit(mut self: Box<Self>) -> Result<WriteSummary> {
        if self
            .sink
            .fail_commit
            .lock()
            .unwrap()
            .remove(&self.partition.0)
        {
            let client = self.client.take().unwrap();
            client.batch_execute("ROLLBACK").await.map_err(pg)?;
            return Err(FerryError::Connector {
                connector: "typedpg".into(),
                message: "injected commit failure".into(),
            });
        }
        let (lease, summary, ledger) = (
            self.lease.clone(),
            self.summary.clone(),
            Arc::clone(&self.sink.ledger),
        );
        ledger.record_done(self.client(), &lease, &summary).await?;
        let client = self.client.take().unwrap();
        client.batch_execute("COMMIT").await.map_err(pg)?;
        Ok(std::mem::take(&mut self.summary))
    }

    async fn abort(mut self: Box<Self>) -> Result<()> {
        if let Some(client) = self.client.take() {
            client.batch_execute("ROLLBACK").await.map_err(pg)?;
        }
        Ok(())
    }
}
