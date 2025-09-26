//! The runner against a real Postgres (testcontainers): the golden dataset
//! written through a durable test sink, under both state backends.
//!
//! - The chaos test (A5's "done when"): 50 runs, each killed with SIGKILL
//!   at random points in a child process and resumed until it finishes;
//!   the target must then hold exactly the expected rows. Once with the
//!   file state store (the sink deletes the partition's rows, then
//!   inserts), once with the state store in the target database, where the
//!   sink's commit writes the done-record (`TxnWithState`).
//! - The two crash windows 0001 names, deterministically: after the commit
//!   but before `mark_done`, and with `TxnWithState` the done-record only
//!   ever written by the sink.
//!
//! The test sink stores each row as its JSON text next to its partition
//! id, with no key: a partition written twice shows up as duplicates. The
//! real Postgres sink (A3) replaces it in S2's run of the same harness.

mod common;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{BooleanArray, RecordBatch};
use async_trait::async_trait;
use common::{Event, Log, SpyStore};
use deadpool_postgres::{Manager, Object, Pool};
use ferry_core::types::{CanonicalField, CanonicalType};
use ferry_core::{
    BatchStream, Entity, FerryError, IdempotencyStrategy, IdentityPlan, KeyRange, KeyValue, Lease,
    Partition, PartitionHint, PartitionId, PartitionScope, Result, Sink, SinkCaps, SinkTxn, Source,
    SourceCaps, TargetSchema, TargetTable, WriteMode, WriteSummary,
};
use ferry_registry::state::NewRun;
use ferry_registry::{Ledger, StateBackend, StateStore};
use ferry_runtime::testkit::chaos::{self, Progress, Scenario};
use ferry_runtime::{Flow, Input, Runner, Settings};
use ferry_state::{FileStateStore, PgStateBackend};
use futures::{FutureExt, StreamExt, stream};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tokio_postgres::{Client, NoTls};

/// Copies of the golden rows in the source, so a run has enough partitions
/// for kills to land between them.
const COPIES: usize = 40;
const ENGINE: &str = "0.0.1-chaos";
const HASH: &str = "sha256:chaos";

// ---- the golden dataset ----

fn orders_entity() -> Entity {
    let utc = CanonicalType::Timestamp {
        tz: Some("+00:00".into()),
    };
    Entity::new(
        "orders",
        vec![
            CanonicalField::new("id", CanonicalType::Utf8, false),
            CanonicalField::new("customer_id", CanonicalType::Utf8, true),
            CanonicalField::new("status", CanonicalType::Utf8, false),
            CanonicalField::new("placed_at", utc, true),
            CanonicalField::new("total_cents", CanonicalType::Int64, false),
        ],
        vec!["id".into()],
    )
    .unwrap()
}

fn items_entity() -> Entity {
    Entity::new(
        "order_items",
        vec![
            // The generated UUIDv5 child key (B5), as text: each copy gets
            // its own, and arrow-json reads UUIDs only as text.
            CanonicalField::new("id", CanonicalType::Utf8, false),
            CanonicalField::new("order_id", CanonicalType::Utf8, false),
            CanonicalField::new("line_no", CanonicalType::Int64, false),
            CanonicalField::new("sku", CanonicalType::Utf8, false),
            CanonicalField::new("qty", CanonicalType::Int64, false),
            CanonicalField::new("unit_cents", CanonicalType::Int64, false),
        ],
        vec!["order_id".into(), "line_no".into()],
    )
    .unwrap()
}

/// `fixtures/golden/expected/<name>.json`, `COPIES` times, with `-<copy>`
/// appended to each of `keys` (the order id, and an item's own id). Every
/// column of the file must be a field of `entity`, so a column added to the
/// expected files can't go unchecked here.
fn golden(name: &str, entity: &Entity, keys: &[&str]) -> RecordBatch {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/golden/expected")
        .join(format!("{name}.json"));
    let rows: Vec<serde_json::Value> =
        serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    let mut lines = Vec::new();
    for copy in 0..COPIES {
        for row in &rows {
            let mut row = row.clone();
            for key in keys {
                let id = row[*key].as_str().unwrap().to_owned();
                row[*key] = format!("{id}-{copy:03}").into();
            }
            lines.push(row.to_string());
        }
    }
    let text = lines.join("\n");
    let mut reader = arrow_json::ReaderBuilder::new(entity.arrow_schema())
        .with_batch_size(lines.len())
        .with_strict_mode(true)
        .build(text.as_bytes())
        .unwrap();
    reader.next().unwrap().unwrap()
}

fn golden_orders() -> RecordBatch {
    golden("orders", &orders_entity(), &["id"])
}

fn golden_items() -> RecordBatch {
    golden("order_items", &items_entity(), &["order_id", "id"])
}

/// Rows as the JSON lines the sink stores, sorted.
fn json_rows(batch: &RecordBatch) -> Vec<String> {
    let mut out = Vec::new();
    let mut writer = arrow_json::LineDelimitedWriter::new(&mut out);
    writer.write(batch).unwrap();
    writer.finish().unwrap();
    let mut rows: Vec<String> = String::from_utf8(out)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect();
    rows.sort();
    rows
}

/// One entity's rows in memory, split into ranges of the first key column
/// (strings here, unlike `MemSource`).
struct Golden {
    entities: Vec<(Entity, RecordBatch)>,
    batch_rows: usize,
}

impl Golden {
    fn new() -> Self {
        Self {
            entities: vec![
                (orders_entity(), golden_orders()),
                (items_entity(), golden_items()),
            ],
            batch_rows: 4,
        }
    }

    fn rows(&self, entity: &str) -> &RecordBatch {
        &self
            .entities
            .iter()
            .find(|(e, _)| e.name() == entity)
            .unwrap()
            .1
    }
}

fn keys(batch: &RecordBatch, column: &str) -> Vec<KeyValue> {
    let column = batch.column_by_name(column).unwrap();
    (0..batch.num_rows())
        .map(|i| KeyValue::from_array(column.as_ref(), i).unwrap())
        .collect()
}

#[async_trait]
impl Source for Golden {
    async fn discover(&self) -> Result<Vec<Entity>> {
        Ok(self.entities.iter().map(|(e, _)| e.clone()).collect())
    }

    async fn plan_partitions(&self, e: &Entity, hint: &PartitionHint) -> Result<Vec<Partition>> {
        let column = &e.primary_key()[0];
        let mut keys = keys(self.rows(e.name()), column);
        keys.sort();
        keys.dedup();
        let mut bounds = vec![None];
        bounds.extend(
            keys.into_iter()
                .step_by(hint.target_rows as usize)
                .skip(1)
                .map(Some),
        );
        bounds.push(None);
        bounds
            .windows(2)
            .enumerate()
            .map(|(i, w)| {
                Ok(Partition {
                    id: PartitionId(format!("{}-{i}", e.name())),
                    entity: e.name().into(),
                    scope: PartitionScope::KeyRange(KeyRange::new(
                        column.clone(),
                        w[0].clone(),
                        w[1].clone(),
                    )?),
                })
            })
            .collect()
    }

    async fn read(&self, e: &Entity, p: &Partition) -> Result<BatchStream> {
        let rows = self.rows(e.name());
        let PartitionScope::KeyRange(range) = &p.scope else {
            unreachable!("planned as ranges")
        };
        let mask: BooleanArray = keys(rows, range.column())
            .iter()
            .map(|k| Some(range.contains(k)))
            .collect();
        let selected = arrow_select::filter::filter_record_batch(rows, &mask).unwrap();
        let batches: Vec<Result<RecordBatch>> = (0..selected.num_rows())
            .step_by(self.batch_rows)
            .map(|o| Ok(selected.slice(o, self.batch_rows.min(selected.num_rows() - o))))
            .collect();
        Ok(stream::iter(batches).boxed())
    }

    async fn capabilities(&self) -> Result<SourceCaps> {
        Ok(SourceCaps::default())
    }
}

// ---- the durable test sink ----

fn pg(e: tokio_postgres::Error) -> FerryError {
    FerryError::Connector {
        connector: "jsonpg".into(),
        message: e.as_db_error().map_or(e.to_string(), |db| db.to_string()),
    }
}

/// Writes `(partition_id, row)` with the row as JSON text, deleting the
/// partition's earlier rows first, in the partition's transaction. Built
/// with a ledger, it also offers `TxnWithState`: its commit records the
/// partition done on the same connection before COMMIT (0004 §8).
#[derive(Clone)]
struct JsonSink {
    pool: Pool,
    ledger: Option<Arc<dyn Ledger<Client>>>,
}

#[async_trait]
impl Sink for JsonSink {
    async fn prepare(&self, target: &TargetSchema, _: WriteMode) -> Result<()> {
        let client = self.pool.get().await.map_err(|e| FerryError::Connector {
            connector: "jsonpg".into(),
            message: e.to_string(),
        })?;
        for t in target.tables() {
            client
                .batch_execute(&format!(
                    "CREATE TABLE IF NOT EXISTS {} (partition_id text NOT NULL, row text NOT NULL)",
                    t.name
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
        let client = self.pool.get().await.map_err(|e| FerryError::Connector {
            connector: "jsonpg".into(),
            message: e.to_string(),
        })?;
        client.batch_execute("BEGIN").await.map_err(pg)?;
        Ok(Box::new(JsonTxn {
            client: Some(client),
            partition: partition.id.clone(),
            lease: lease.clone(),
            ledger: match strategy {
                IdempotencyStrategy::TxnWithState => self.ledger.clone(),
                _ => None,
            },
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

struct JsonTxn {
    client: Option<Object>,
    partition: PartitionId,
    lease: Lease,
    ledger: Option<Arc<dyn Ledger<Client>>>,
    cleared: Vec<String>,
    summary: WriteSummary,
}

impl JsonTxn {
    fn client(&mut self) -> &mut Client {
        self.client.as_mut().expect("the transaction is open")
    }
}

/// Dropped mid-transaction: close the connection rather than pool it with
/// the transaction open.
impl Drop for JsonTxn {
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            drop(Object::take(client));
        }
    }
}

#[async_trait]
impl SinkTxn for JsonTxn {
    async fn write(&mut self, target: &str, batch: RecordBatch) -> Result<()> {
        let partition = self.partition.0.clone();
        if !self.cleared.iter().any(|t| t == target) {
            self.client()
                .execute(
                    &format!("DELETE FROM {target} WHERE partition_id = $1"),
                    &[&partition],
                )
                .await
                .map_err(pg)?;
            self.cleared.push(target.to_owned());
        }
        let rows = json_rows(&batch);
        self.client()
            .execute(
                &format!("INSERT INTO {target} (partition_id, row) SELECT $1, unnest($2::text[])"),
                &[&partition, &rows],
            )
            .await
            .map_err(pg)?;
        *self
            .summary
            .rows_written
            .entry(target.to_owned())
            .or_default() += rows.len() as u64;
        Ok(())
    }

    async fn commit(mut self: Box<Self>) -> Result<WriteSummary> {
        if let Some(ledger) = self.ledger.clone() {
            let (lease, summary) = (self.lease.clone(), self.summary.clone());
            ledger.record_done(self.client(), &lease, &summary).await?;
        }
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

// ---- wiring ----

fn pool(url: &str) -> Pool {
    let config: tokio_postgres::Config = url.parse().unwrap();
    Pool::builder(Manager::new(config, NoTls))
        .max_size(16)
        .build()
        .unwrap()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Backend {
    /// `backend: file`, so the sink falls back to deleting then inserting.
    File,
    /// `backend: postgres` on the sink's own database: `TxnWithState`.
    Postgres,
}

impl Backend {
    fn name(self) -> &'static str {
        match self {
            Backend::File => "file",
            Backend::Postgres => "postgres",
        }
    }

    fn parse(name: &str) -> Self {
        match name {
            "file" => Backend::File,
            "postgres" => Backend::Postgres,
            other => panic!("unknown backend {other}"),
        }
    }

    /// The state store and the sink for a run whose files are in `dir`.
    fn open(self, url: &str, dir: &Path) -> (Arc<dyn StateStore>, JsonSink) {
        let pool = pool(url);
        match self {
            Backend::File => (
                Arc::new(FileStateStore::new(dir.join("state"))),
                JsonSink { pool, ledger: None },
            ),
            Backend::Postgres => {
                let (store, ledger) = PgStateBackend.open(pool.clone());
                (
                    store,
                    JsonSink {
                        pool,
                        ledger: Some(ledger),
                    },
                )
            }
        }
    }
}

fn table(entity: &Entity) -> TargetTable {
    TargetTable {
        name: entity.name().into(),
        fields: entity.fields().to_vec(),
        primary_key: entity.primary_key().to_vec(),
        foreign_keys: Vec::new(),
    }
}

/// `orders` and, once it is done, `order_items`, each copied unchanged.
fn flows(sink: Arc<dyn Sink>) -> Vec<Flow> {
    let source: Arc<dyn Source> = Arc::new(Golden::new());
    let flow = |entity: Entity, depends_on: Vec<String>| Flow {
        name: entity.name().into(),
        inputs: vec![Input {
            source: Arc::clone(&source),
            entity: entity.clone(),
            co_partition: None,
        }],
        plan: Arc::new(IdentityPlan::new(entity.clone(), entity.name())),
        sink: Arc::clone(&sink),
        target: TargetSchema::new(vec![table(&entity)]).unwrap(),
        mode: WriteMode::Insert,
        depends_on,
    };
    vec![
        flow(orders_entity(), Vec::new()),
        flow(items_entity(), vec!["orders".into()]),
    ]
}

fn settings() -> Settings {
    Settings {
        workers: 4,
        partition_rows: 10,
        lease_ttl: Duration::from_millis(500),
        heartbeat: Duration::from_millis(150),
        poll: Duration::from_millis(50),
        ..Settings::default()
    }
}

fn new_run() -> NewRun {
    NewRun {
        config_hash: HASH.into(),
        engine_version: ENGINE.into(),
        connections: vec![
            ("shop".into(), "golden".into()),
            ("warehouse".into(), "jsonpg".into()),
        ],
    }
}

/// Every row in the target, as `(table, sorted rows)`, against the golden
/// rows: `Err` lists what is missing and what is there too often.
async fn verify(url: &str) -> std::result::Result<(), String> {
    let (client, connection) = tokio_postgres::connect(url, NoTls).await.unwrap();
    tokio::spawn(connection);
    let mut problems = Vec::new();
    for (table, expected) in [("orders", golden_orders()), ("order_items", golden_items())] {
        let mut actual: Vec<String> = client
            .query(&format!("SELECT row FROM {table}"), &[])
            .await
            .map_err(|e| e.to_string())?
            .iter()
            .map(|r| r.get(0))
            .collect();
        actual.sort();
        let expected = json_rows(&expected);
        if actual != expected {
            let mut counts: BTreeMap<&str, i64> = BTreeMap::new();
            for r in &expected {
                *counts.entry(r).or_default() += 1;
            }
            for r in &actual {
                *counts.entry(r).or_default() -= 1;
            }
            let missing = counts.values().filter(|n| **n > 0).count();
            let extra = counts.values().filter(|n| **n < 0).count();
            problems.push(format!(
                "{table}: {} rows, expected {}: {missing} missing, {extra} duplicated or unexpected",
                actual.len(),
                expected.len()
            ));
        }
    }
    if problems.is_empty() {
        Ok(())
    } else {
        Err(problems.join("; "))
    }
}

struct Server {
    _container: ContainerAsync<Postgres>,
    port: u16,
}

impl Server {
    async fn start() -> Self {
        let container = Postgres::default().start().await.expect("start postgres");
        let port = container.get_host_port_ipv4(5432).await.unwrap();
        Self {
            _container: container,
            port,
        }
    }

    async fn database(&self, name: &str) -> String {
        let url = |db: &str| format!("postgres://postgres:postgres@127.0.0.1:{}/{db}", self.port);
        let (client, connection) = tokio_postgres::connect(&url("postgres"), NoTls)
            .await
            .unwrap();
        tokio::spawn(connection);
        client
            .batch_execute(&format!("CREATE DATABASE {name}"))
            .await
            .unwrap();
        url(name)
    }
}

// ---- the chaos test ----

const DIR: &str = "FERRY_CHAOS_DIR";
const URL: &str = "FERRY_CHAOS_URL";
const BACKEND: &str = "FERRY_CHAOS_BACKEND";

/// The child process: one attempt at the run kept in `$FERRY_CHAOS_DIR`.
/// Only the chaos tests run it, through `--ignored`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "the chaos tests' child process"]
async fn chaos_child() {
    let dir = PathBuf::from(std::env::var(DIR).expect("run by the chaos tests"));
    let url = std::env::var(URL).unwrap();
    let backend = Backend::parse(&std::env::var(BACKEND).unwrap());
    let (store, sink) = backend.open(&url, &dir);
    let runner = Runner::new(store, flows(Arc::new(sink)), settings()).unwrap();
    let report = chaos::child_run(&runner, new_run(), &dir).await.unwrap();
    assert!(report.is_complete(), "{report:?}");
}

struct GoldenChaos {
    url: String,
    backend: Backend,
}

#[async_trait]
impl Scenario for GoldenChaos {
    async fn reset(&self) -> std::result::Result<(), String> {
        let (client, connection) = tokio_postgres::connect(&self.url, NoTls)
            .await
            .map_err(|e| e.to_string())?;
        tokio::spawn(connection);
        client
            .batch_execute(
                "DROP TABLE IF EXISTS orders, order_items; \
                 DROP TABLE IF EXISTS _ferry_leases, _ferry_partitions, _ferry_runs",
            )
            .await
            .map_err(|e| e.to_string())
    }

    fn child(&self, dir: &Path) -> tokio::process::Command {
        let mut cmd = tokio::process::Command::new(std::env::current_exe().unwrap());
        cmd.args([
            "chaos_child",
            "--exact",
            "--ignored",
            "--test-threads=1",
            "-q",
        ])
        .env(DIR, dir)
        .env(URL, &self.url)
        .env(BACKEND, self.backend.name())
        .stdout(std::process::Stdio::null());
        cmd
    }

    async fn progress(&self, dir: &Path) -> std::result::Result<Progress, String> {
        let Some(run) = chaos::recorded_run(dir) else {
            return Ok(Progress::default());
        };
        let (store, _) = self.backend.open(&self.url, dir);
        let counts = store.counts(&run).await.map_err(|e| e.to_string())?;
        Ok(Progress {
            started: true,
            done: counts.done,
            total: counts.pending + counts.leased + counts.done + counts.failed,
        })
    }

    async fn verify(&self) -> std::result::Result<(), String> {
        verify(&self.url).await
    }
}

async fn chaos(backend: Backend, seed: u64) {
    let server = Server::start().await;
    let scenario = GoldenChaos {
        url: server.database(&format!("chaos_{}", backend.name())).await,
        backend,
    };
    let work = tempfile::tempdir().unwrap();
    let config = chaos::Config {
        runs: 50,
        seed,
        max_kills: 3,
        kill_window: None,
        timeout: Duration::from_secs(60),
    };
    let report = chaos::run(&config, &scenario, work.path()).await.unwrap();
    eprintln!("{backend:?}: {report:?}");
    assert_eq!(report.runs, 50);
    // The kills have to land inside runs, between partitions, for the test
    // to mean anything.
    assert!(report.kills_mid_run >= 50, "{report:?}");
    assert!(report.kills_between_partitions >= 25, "{report:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn chaos_kill_9_with_the_file_state_store() {
    chaos(Backend::File, 0x0f_e2_27).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn chaos_kill_9_with_the_state_store_in_the_target_database() {
    chaos(Backend::Postgres, 0x5a_fe).await;
}

// ---- the crash windows, deterministically ----

/// The run's flows on `backend`, with a spy on the store.
fn spied(backend: Backend, url: &str, dir: &Path) -> (Runner, SpyStore) {
    let (store, sink) = backend.open(url, dir);
    let spy = SpyStore::new(store, Log::default());
    let runner = Runner::new(Arc::new(spy.clone()), flows(Arc::new(sink)), settings()).unwrap();
    (runner, spy)
}

#[tokio::test(flavor = "multi_thread")]
async fn under_txn_with_state_only_the_sink_writes_done_records() {
    let server = Server::start().await;
    let url = server.database("txn_with_state").await;
    let dir = tempfile::tempdir().unwrap();
    let (runner, spy) = spied(Backend::Postgres, &url, dir.path());

    let report = runner.start(new_run()).await.unwrap();

    assert!(report.is_complete(), "{report:?}");
    verify(&url).await.unwrap();
    // Every partition is done, and the runner never called mark_done.
    assert_eq!(spy.log.count(|e| matches!(e, Event::MarkDone(_))), 0);
    let commits = spy.log.count(|e| matches!(e, Event::RecordRows(..)));
    assert_eq!(report.counts.done as usize, commits);
    assert_eq!(report.totals.read as usize, 8 * COPIES + 7 * COPIES);
    assert_eq!(
        report.totals.written.get("orders"),
        Some(&(8 * COPIES as u64))
    );

    // Without the ledger (file store), the runner records every partition.
    let url = server.database("delete_then_insert").await;
    let (runner, spy) = spied(Backend::File, &url, dir.path());
    let report = runner.start(new_run()).await.unwrap();
    assert!(report.is_complete());
    assert_eq!(
        spy.log.count(|e| matches!(e, Event::MarkDone(_))) as u64,
        report.counts.done
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_crash_between_the_commit_and_mark_done_re_runs_the_partition_without_dupes() {
    let server = Server::start().await;
    let url = server.database("commit_then_crash").await;
    let dir = tempfile::tempdir().unwrap();
    let (runner, spy) = spied(Backend::File, &url, dir.path());
    // The 5th mark_done never happens: its rows are committed, its lease is
    // left to expire.
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let seen = Arc::clone(&calls);
    spy.on_mark_done(move |_| {
        let n = seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
        async move {
            if n >= 5 {
                std::future::pending::<()>().await;
            }
        }
        .boxed()
    });
    let run = runner.create(new_run()).await.unwrap();
    // The process dies once the 5th is stuck and the four before it are
    // recorded, however long the run takes to get there.
    let crashed = async {
        while calls.load(std::sync::atomic::Ordering::SeqCst) < 5
            || spy.log.count(|e| matches!(e, Event::MarkDone(_))) < 4
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    tokio::select! {
        ended = runner.execute(&run) => panic!("the run should hang on its 5th mark_done: {ended:?}"),
        () = crashed => {}
    }
    drop(runner);
    // Rows of partitions that committed but were never recorded are in
    // the target already.
    let committed: i64 = {
        let (client, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
        tokio::spawn(connection);
        client
            .query_one("SELECT count(*) FROM orders", &[])
            .await
            .unwrap()
            .get(0)
    };
    let store = FileStateStore::new(dir.path().join("state"));
    let done = store.counts(&run).await.unwrap().done;
    assert_eq!(done, 4);
    assert!(
        committed > 4 * 10,
        "{committed} rows for {done} done partitions"
    );

    let (runner, _) = spied(Backend::File, &url, dir.path());
    let report = runner.resume(&run, HASH, ENGINE).await.unwrap();
    assert!(report.is_complete(), "{report:?}");
    verify(&url).await.unwrap();
}
