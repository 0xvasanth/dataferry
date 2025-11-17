//! A real Postgres (testcontainers) for `expect:` end to end: a source
//! schema `src` read by the Postgres source (A1), a typed test sink writing
//! `dst` (as ferry-verify's tests do, standing in for A3) with the state
//! store in the same database (`TxnWithState`), and a transform plan
//! compiled from the type-checked config: the flow `where:` and each
//! column's B2 expression, evaluated on every driver batch. So a mapping
//! like `total_cents: int(round(total * 10))` is what lands.

#![allow(dead_code)]

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow_array::cast::AsArray;
use arrow_array::types::{Decimal128Type, Int32Type, Int64Type};
use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch};
use arrow_schema::Schema;
use async_trait::async_trait;
use deadpool_postgres::{Manager, Object, Pool};
use ferry_connector_postgres::PostgresSource;
use ferry_core::types::{CanonicalField, CanonicalType};
use ferry_core::{
    Entity, FerryError, IdempotencyStrategy, Lease, Partition, PartitionId, PartitionInputs,
    Result, Sink, SinkCaps, SinkTxn, Source, TargetBatch, TargetSchema, TargetTable,
    TransformOutput, TransformPlan, TransformStream, WriteMode, WriteSummary,
};
use ferry_dsl::config::{Config, load};
use ferry_dsl::expr::Compiled;
use ferry_dsl::typecheck::{Catalog, CheckedFlow, ColumnValue, check};
use ferry_registry::state::NewRun;
use ferry_registry::{Ledger, StateBackend, StateStore};
use ferry_runtime::{Flow, Input, Settings};
use ferry_state::PgStateBackend;
use ferry_verify::PgTarget;
use futures::StreamExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tokio_postgres::{Client, NoTls};

pub const HASH: &str = "sha256:expect-tests";
pub const ENGINE: &str = "0.0.1-expect";

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

/// One worker, so partitions run one after the other and a test can act
/// between them.
pub fn settings() -> Settings {
    Settings {
        workers: 1,
        partition_rows: 10,
        lease_ttl: Duration::from_secs(5),
        heartbeat: Duration::from_secs(1),
        poll: Duration::from_millis(50),
        ..Settings::default()
    }
}

/// 50 orders. `total` is `i * 1.25`; `qty` is 0 for every fifth, which
/// the flows filter out.
pub const ORDERS: &str = r#"
CREATE TABLE src.orders (
    id int8 PRIMARY KEY,
    total numeric(12, 2) NOT NULL,
    qty int4 NOT NULL,
    note text
);
INSERT INTO src.orders
SELECT i, (i * 1.25)::numeric(12, 2), i % 5, CASE WHEN i % 3 = 0 THEN 'note ' || i END
FROM generate_series(1, 50) i;
"#;

// ---- the database ----

pub struct Db {
    _container: ContainerAsync<Postgres>,
    pub url: String,
    pub pool: Pool,
}

impl Db {
    pub async fn start(setup: &str) -> Self {
        let container = Postgres::default().start().await.expect("start postgres");
        let port = container.get_host_port_ipv4(5432).await.unwrap();
        let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
        let client = connect(&url).await;
        client
            .batch_execute("CREATE SCHEMA src; CREATE SCHEMA dst;")
            .await
            .unwrap();
        client.batch_execute(setup).await.unwrap();
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

    /// One value, as Postgres prints it.
    pub async fn text(&self, sql: &str) -> Option<String> {
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

    /// Every entity of `src`, discovered, as `shop.<entity>`.
    pub async fn catalog(&self) -> Catalog {
        let mut catalog = Catalog::new();
        for entity in self.source().discover().await.unwrap() {
            catalog.insert("shop", entity);
        }
        catalog
    }
}

async fn connect(url: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(url, NoTls).await.unwrap();
    tokio::spawn(connection);
    client
}

// ---- the config ----

/// A `ferry/` directory with `flows` as its one flow file, loaded.
pub fn config(flows: &str) -> (tempfile::TempDir, Config) {
    let dir = tempfile::tempdir().unwrap();
    let write = |path: &str, text: &str| {
        let path = dir.path().join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, text).unwrap();
    };
    write("ferry.yaml", "version: 1\nname: expect\n");
    write(
        "connections.yaml",
        "connections:\n  - name: shop\n    type: postgres\n    url: x\n  - name: warehouse\n    type: postgres\n    url: y\n",
    );
    write("flows/f.yaml", flows);
    let config = load(dir.path()).unwrap_or_else(|e| panic!("{e}"));
    (dir, config)
}

/// The config's flows, type-checked against the discovered source.
pub async fn plan(db: &Db, config: &Config) -> Vec<CheckedFlow> {
    check(config, &db.catalog().await).unwrap_or_else(|e| panic!("{e}"))
}

/// A checked flow, ready to run: one driver, one target, every column an
/// expression (all these tests need).
pub fn runtime_flow(db: &Db, checked: &CheckedFlow, entity: &Entity, sink: Arc<dyn Sink>) -> Flow {
    let [target] = checked.targets.as_slice() else {
        panic!("one target per flow in these tests")
    };
    let table = TargetTable {
        name: target.table.clone(),
        fields: target.columns.iter().map(|c| c.field.clone()).collect(),
        primary_key: vec!["id".into()],
        foreign_keys: Vec::new(),
    };
    Flow {
        name: checked.name.clone(),
        inputs: vec![Input {
            source: Arc::new(db.source()),
            entity: entity.clone(),
            co_partition: None,
        }],
        plan: Arc::new(ExprPlan {
            entity: entity.clone(),
            inputs: vec![entity.name().to_owned()],
            order: vec![target.table.clone()],
            flow: checked.clone(),
        }),
        sink,
        target: TargetSchema::new(vec![table]).unwrap(),
        mode: WriteMode::Insert,
        depends_on: Vec::new(),
        finalizer: None,
    }
}

/// The flow `where:` and each column's expression, on every driver batch.
struct ExprPlan {
    entity: Entity,
    inputs: Vec<String>,
    order: Vec<String>,
    flow: CheckedFlow,
}

fn plan_error(message: impl std::fmt::Display) -> FerryError {
    FerryError::InvalidTransformOutput {
        reason: message.to_string(),
    }
}

/// `batch` laid out as `expr`'s scope reads it.
fn scoped(batch: &RecordBatch, expr: &Compiled) -> Result<RecordBatch> {
    let indices = expr
        .input_schema()
        .fields()
        .iter()
        .map(|f| batch.schema().index_of(f.name()).map_err(plan_error))
        .collect::<Result<Vec<_>>>()?;
    batch.project(&indices).map_err(plan_error)
}

fn evaluate(batch: &RecordBatch, expr: &Compiled) -> Result<ArrayRef> {
    expr.evaluate(&scoped(batch, expr)?).map_err(plan_error)
}

impl TransformPlan for ExprPlan {
    fn inputs(&self) -> &[String] {
        &self.inputs
    }

    fn write_order(&self) -> &[String] {
        &self.order
    }

    fn execute(&self, mut inputs: PartitionInputs) -> Result<TransformStream> {
        let input = inputs.take(self.entity.name())?;
        let entity = self.entity.clone();
        let flow = self.flow.clone();
        Ok(input
            .map(move |batch| {
                let batch = batch?;
                let records = entity.record_ids(&batch)?;
                // A null predicate counts as false (0003 §5.6).
                let mut keep = vec![true; batch.num_rows()];
                for predicate in &flow.filter {
                    let result = evaluate(&batch, &predicate.expr)?;
                    let result = result.as_boolean();
                    for (i, k) in keep.iter_mut().enumerate() {
                        *k &= result.is_valid(i) && result.value(i);
                    }
                }
                let target = &flow.targets[0];
                let mut columns = Vec::new();
                for column in &target.columns {
                    let ColumnValue::Expr(expr) = &column.value else {
                        panic!("only expression columns in these tests")
                    };
                    columns.push(evaluate(&batch, expr)?);
                }
                let schema = Schema::new(
                    target
                        .columns
                        .iter()
                        .map(|c| c.field.to_arrow())
                        .collect::<Vec<_>>(),
                );
                let mapped = RecordBatch::try_new(Arc::new(schema), columns).map_err(plan_error)?;
                let mask = BooleanArray::from(keep.clone());
                let kept = arrow_select::filter::filter_record_batch(&mapped, &mask)
                    .map_err(plan_error)?;
                Ok(TransformOutput {
                    targets: vec![TargetBatch {
                        target: target.table.clone(),
                        source_index: (0..keep.len() as u32)
                            .filter(|&i| keep[i as usize])
                            .collect(),
                        batch: kept,
                    }],
                    records,
                    rejected: Vec::new(),
                })
            })
            .boxed())
    }
}

// ---- the typed test sink ----

fn pg(e: tokio_postgres::Error) -> FerryError {
    FerryError::Connector {
        connector: "typedpg".into(),
        message: e.as_db_error().map_or(e.to_string(), |db| db.to_string()),
    }
}

fn pool_error(e: impl std::fmt::Display) -> FerryError {
    FerryError::Connector {
        connector: "typedpg".into(),
        message: e.to_string(),
    }
}

/// Writes rows as typed columns of `dst.<target>`. Offers only
/// `TxnWithState`: its commit records the partition done on the
/// transaction's connection before COMMIT.
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
        CanonicalType::Int32 => "int4".into(),
        CanonicalType::Int64 => "int8".into(),
        CanonicalType::Utf8 => "text".into(),
        CanonicalType::Decimal(d) => format!("numeric({}, {})", d.precision(), d.scale()),
        other => panic!("the test sink doesn't write {other:?}"),
    }
}

#[async_trait]
impl Sink for TypedSink {
    async fn prepare(&self, target: &TargetSchema, _: WriteMode) -> Result<()> {
        let client = self.pool.get().await.map_err(pool_error)?;
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
        let client = self.pool.get().await.map_err(pool_error)?;
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
        // An abandoned transaction's connection must not go back to the
        // pool mid-transaction.
        if let Some(client) = self.client.take() {
            drop(Object::take(client));
        }
    }
}

type Param = Box<dyn tokio_postgres::types::ToSql + Sync + Send>;

/// One column as an array parameter: the parameter, its array type, and
/// the select expression that turns an element into the column's type.
fn column_param(array: &dyn Array, ty: &CanonicalType, n: usize) -> (Param, String, String) {
    let at = |i: usize| array.is_valid(i);
    let c = format!("c{n}");
    match ty {
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
        CanonicalType::Utf8 => {
            let a = array.as_string::<i32>();
            let v: Vec<Option<String>> = (0..a.len())
                .map(|i| at(i).then(|| a.value(i).to_owned()))
                .collect();
            (Box::new(v), "text[]".into(), c)
        }
        CanonicalType::Decimal(d) => {
            let a = array.as_primitive::<Decimal128Type>();
            let v: Vec<Option<String>> = (0..a.len())
                .map(|i| at(i).then(|| a.value_as_string(i)))
                .collect();
            let cast = format!("{c}::numeric({}, {})", d.precision(), d.scale());
            (Box::new(v), "text[]".into(), cast)
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
            let ty = CanonicalField::from_arrow(field).unwrap().ty;
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
