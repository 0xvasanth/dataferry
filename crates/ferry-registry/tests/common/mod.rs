//! `fakesql`: a state-capable connector and state backend over an
//! in-memory transactional database, standing in for Postgres. A connection
//! stages rows and done-records, and only COMMIT makes both visible at once.
//!
//! Defined outside the crate and registered through the public API only,
//! like a third-party connector. `Faults` break one guarantee each, so tests
//! can check the contract suite catches them.

#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use async_trait::async_trait;
use ferry_core::testkit::{MemSource, SinkFixture, SinkProbe};
use ferry_core::types::{CanonicalField, CanonicalType};
use ferry_core::{
    Epoch, FerryError, IdempotencyStrategy, Lease, Partition, PartitionId, PartitionScope, Result,
    Sink, SinkCaps, SinkTxn, TargetSchema, TargetTable, WriteMode, WriteSummary,
};
use ferry_registry::state::{NewRun, PartitionCounts, PartitionRows, RunId, RunRecord, RunTotals};
use ferry_registry::{
    BuildContext, BuildError, Connector, Endpoints, Ledger, Roles, Secret, StateBackend,
    StateCapable, StateStore,
};
use schemars::JsonSchema;
use serde::Deserialize;

pub const URL: &str = "fakesql://app:s3cr3t-pw@db.internal/shop";

// ---- the fake database server ----

struct LeaseState {
    epoch: Epoch,
    expires: Instant,
}

#[derive(Default)]
pub struct Db {
    /// Committed row counts by partition, then target.
    rows: BTreeMap<PartitionId, BTreeMap<String, u64>>,
    done: BTreeMap<PartitionId, Epoch>,
    failed: BTreeMap<PartitionId, String>,
    reported: BTreeMap<PartitionId, PartitionRows>,
    leases: BTreeMap<PartitionId, LeaseState>,
    runs: BTreeMap<RunId, RunRecord>,
    plans: BTreeMap<(RunId, String), Vec<Partition>>,
}

/// Ways to break one guarantee.
#[derive(Debug, Clone, Copy, Default)]
pub struct Faults {
    /// The sink claims `TxnWithState` even when built without a ledger.
    pub txn_with_state_without_ledger: bool,
    /// Writes autocommit instead of joining the partition's transaction.
    pub rows_outside_txn: bool,
    /// The ledger writes the done-record on its own connection, outside the
    /// sink's transaction.
    pub done_outside_txn: bool,
}

/// Every database the fake connector has opened, by URL, as a server would
/// hold them.
#[derive(Default)]
pub struct Server {
    dbs: Mutex<HashMap<String, Arc<Mutex<Db>>>>,
    /// How many pools have been opened.
    pub opened: AtomicUsize,
    /// Makes every sink commit fail after its last write and its
    /// `record_done`, right before COMMIT.
    pub crash_before_commit: AtomicBool,
    pub faults: Mutex<Faults>,
}

impl Server {
    pub fn db(&self, url: &str) -> Arc<Mutex<Db>> {
        Arc::clone(self.dbs.lock().unwrap().entry(url.to_owned()).or_default())
    }

    pub fn committed_rows(&self, url: &str) -> u64 {
        let db = self.db(url);
        let db = db.lock().unwrap();
        db.rows.values().flat_map(BTreeMap::values).sum()
    }

    pub fn done_records(&self, url: &str) -> usize {
        self.db(url).lock().unwrap().done.len()
    }

    fn faults(&self) -> Faults {
        *self.faults.lock().unwrap()
    }
}

/// The driver's pool: a handle to one database.
#[derive(Clone)]
pub struct FakePool {
    db: Arc<Mutex<Db>>,
    server: Arc<Server>,
}

/// The driver's connection with an open transaction. Nothing it stages is
/// visible until `commit`; dropping it rolls back.
pub struct FakeConn {
    pool: FakePool,
    rows: Vec<(PartitionId, String, u64)>,
    done: Vec<(PartitionId, Epoch)>,
}

impl FakeConn {
    fn begin(pool: &FakePool) -> Self {
        Self {
            pool: pool.clone(),
            rows: Vec::new(),
            done: Vec::new(),
        }
    }

    fn commit(self) {
        apply(&self.pool.db, self.rows, self.done);
    }
}

fn apply(db: &Mutex<Db>, rows: Vec<(PartitionId, String, u64)>, done: Vec<(PartitionId, Epoch)>) {
    let mut db = db.lock().unwrap();
    for (partition, target, _) in &rows {
        // Delete-then-insert: a re-run replaces the partition's rows.
        if let Some(targets) = db.rows.get_mut(partition) {
            targets.remove(target);
        }
    }
    for (partition, target, n) in rows {
        *db.rows
            .entry(partition)
            .or_default()
            .entry(target)
            .or_default() += n;
    }
    db.done.extend(done);
}

fn stale(lease: &Lease, current: Epoch) -> FerryError {
    FerryError::StaleLease {
        partition: lease.partition.0.clone(),
        epoch: lease.epoch.0,
        current: current.0,
    }
}

fn check_current(db: &Db, lease: &Lease) -> Result<()> {
    match db.leases.get(&lease.partition) {
        Some(state) if state.epoch > lease.epoch => Err(stale(lease, state.epoch)),
        _ => Ok(()),
    }
}

// ---- the state backend (A4's side) ----

pub struct FakeBackend;

impl StateBackend for FakeBackend {
    type Pool = FakePool;
    type Conn = FakeConn;

    fn open(&self, pool: FakePool) -> (Arc<dyn StateStore>, Arc<dyn Ledger<FakeConn>>) {
        (
            Arc::new(FakeStore { pool: pool.clone() }),
            Arc::new(FakeLedger { pool }),
        )
    }
}

struct FakeLedger {
    pool: FakePool,
}

#[async_trait]
impl Ledger<FakeConn> for FakeLedger {
    async fn record_done(
        &self,
        conn: &mut FakeConn,
        lease: &Lease,
        _: &WriteSummary,
    ) -> Result<()> {
        assert!(
            Arc::ptr_eq(&conn.pool.db, &self.pool.db),
            "the ledger must run on a connection to its own database"
        );
        check_current(&conn.pool.db.lock().unwrap(), lease)?;
        let record = (lease.partition.clone(), lease.epoch);
        if self.pool.server.faults().done_outside_txn {
            apply(&self.pool.db, Vec::new(), vec![record]);
        } else {
            conn.done.push(record);
        }
        Ok(())
    }
}

pub struct FakeStore {
    pool: FakePool,
}

impl FakeStore {
    /// Counts the partitions of `run`, or of one of its flows.
    fn counted(&self, run: &RunId, flow: Option<&str>) -> PartitionCounts {
        let db = self.pool.db.lock().unwrap();
        let now = Instant::now();
        let mut counts = PartitionCounts::default();
        let plans = db
            .plans
            .iter()
            .filter(|((r, f), _)| r == run && flow.is_none_or(|flow| f == flow));
        for p in plans.flat_map(|(_, plan)| plan) {
            if db.done.contains_key(&p.id) {
                counts.done += 1;
            } else if db.failed.contains_key(&p.id) {
                counts.failed += 1;
            } else if db.leases.get(&p.id).is_some_and(|l| l.expires > now) {
                counts.leased += 1;
            } else {
                counts.pending += 1;
            }
        }
        counts
    }

    fn checked(&self, lease: &Lease, f: impl FnOnce(&mut Db)) -> Result<()> {
        let mut db = self.pool.db.lock().unwrap();
        check_current(&db, lease)?;
        f(&mut db);
        Ok(())
    }
}

#[async_trait]
impl StateStore for FakeStore {
    async fn create_run(&self, run: NewRun) -> Result<RunId> {
        let mut db = self.pool.db.lock().unwrap();
        let id = RunId(format!("run-{}", db.runs.len() + 1));
        db.runs.insert(
            id.clone(),
            RunRecord {
                id: id.clone(),
                config_hash: run.config_hash,
                engine_version: run.engine_version,
                connections: run.connections,
            },
        );
        Ok(id)
    }

    async fn load_run(&self, run: &RunId) -> Result<Option<RunRecord>> {
        Ok(self.pool.db.lock().unwrap().runs.get(run).cloned())
    }

    async fn store_partitions(
        &self,
        run: &RunId,
        flow: &str,
        partitions: &[Partition],
    ) -> Result<()> {
        self.pool
            .db
            .lock()
            .unwrap()
            .plans
            .insert((run.clone(), flow.to_owned()), partitions.to_vec());
        Ok(())
    }

    async fn load_partitions(&self, run: &RunId, flow: &str) -> Result<Vec<Partition>> {
        let db = self.pool.db.lock().unwrap();
        Ok(db
            .plans
            .get(&(run.clone(), flow.to_owned()))
            .cloned()
            .unwrap_or_default())
    }

    async fn lease_next(&self, run: &RunId, flow: &str, ttl: Duration) -> Result<Option<Lease>> {
        let mut db = self.pool.db.lock().unwrap();
        let now = Instant::now();
        let plan = db
            .plans
            .get(&(run.clone(), flow.to_owned()))
            .cloned()
            .unwrap_or_default();
        for p in plan {
            let busy = db.leases.get(&p.id).is_some_and(|l| l.expires > now);
            if busy || db.done.contains_key(&p.id) || db.failed.contains_key(&p.id) {
                continue;
            }
            let epoch = Epoch(db.leases.get(&p.id).map_or(1, |l| l.epoch.0 + 1));
            db.leases.insert(
                p.id.clone(),
                LeaseState {
                    epoch,
                    expires: now + ttl,
                },
            );
            return Ok(Some(Lease {
                partition: p.id,
                epoch,
            }));
        }
        Ok(None)
    }

    async fn renew(&self, _: &RunId, lease: &Lease, ttl: Duration) -> Result<()> {
        self.checked(lease, |db| {
            db.leases.get_mut(&lease.partition).unwrap().expires = Instant::now() + ttl;
        })
    }

    async fn mark_done(&self, _: &RunId, lease: &Lease, _: &WriteSummary) -> Result<()> {
        self.checked(lease, |db| {
            db.done.insert(lease.partition.clone(), lease.epoch);
        })
    }

    async fn mark_failed(&self, _: &RunId, lease: &Lease, error: &str) -> Result<()> {
        self.checked(lease, |db| {
            db.failed.insert(lease.partition.clone(), error.to_owned());
        })
    }

    async fn record_rows(&self, _: &RunId, lease: &Lease, rows: &PartitionRows) -> Result<()> {
        self.checked(lease, |db| {
            db.reported.insert(lease.partition.clone(), *rows);
        })
    }

    async fn is_done(&self, _: &RunId, partition: &PartitionId) -> Result<bool> {
        Ok(self.pool.db.lock().unwrap().done.contains_key(partition))
    }

    async fn counts(&self, run: &RunId) -> Result<PartitionCounts> {
        Ok(self.counted(run, None))
    }

    async fn flow_counts(&self, run: &RunId, flow: &str) -> Result<PartitionCounts> {
        Ok(self.counted(run, Some(flow)))
    }

    async fn reset_failed(&self, run: &RunId) -> Result<u64> {
        let mut db = self.pool.db.lock().unwrap();
        if !db.runs.contains_key(run) {
            return Err(FerryError::Connector {
                connector: "state:fake".into(),
                message: format!("no run `{}`", run.0),
            });
        }
        let ids: Vec<PartitionId> = db
            .plans
            .iter()
            .filter(|((r, _), _)| r == run)
            .flat_map(|(_, plan)| plan.iter().map(|p| p.id.clone()))
            .collect();
        let before = db.failed.len();
        db.failed.retain(|id, _| !ids.contains(id));
        Ok((before - db.failed.len()) as u64)
    }

    async fn totals(&self, _: &RunId) -> Result<RunTotals> {
        let db = self.pool.db.lock().unwrap();
        let mut totals = RunTotals::default();
        for partition in db.done.keys() {
            let rows = db.reported.get(partition).copied().unwrap_or_default();
            totals.read += rows.read;
            totals.dead_lettered += rows.dead_lettered;
            for (target, n) in db.rows.get(partition).into_iter().flatten() {
                *totals.written.entry(target.clone()).or_default() += n;
            }
        }
        Ok(totals)
    }
}

// ---- the connector (A3's side) ----

#[derive(Clone, Default)]
pub struct FakeSql {
    pub server: Arc<Server>,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FakeSqlOptions {
    url: Secret,
}

impl FakeSql {
    fn open_pool(&self, options: &FakeSqlOptions) -> Result<FakePool, BuildError> {
        if !options.url.expose().starts_with("fakesql://") {
            return Err(BuildError::option("url", "expected a fakesql:// URL"));
        }
        self.server.opened.fetch_add(1, Ordering::SeqCst);
        Ok(FakePool {
            db: self.server.db(options.url.expose()),
            server: Arc::clone(&self.server),
        })
    }

    fn endpoints(
        &self,
        wants: Roles,
        pool: FakePool,
        ledger: Option<Arc<dyn Ledger<FakeConn>>>,
    ) -> Endpoints {
        Endpoints {
            source: wants.source.then(|| Arc::new(MemSource::new()) as _),
            sink: wants.sink.then(|| Arc::new(FakeSink { pool, ledger }) as _),
            finalizer: None,
            ddl: None,
        }
    }
}

impl Connector for FakeSql {
    const TYPE: &'static str = "fakesql";
    const ROLES: Roles = Roles::BOTH;
    type Options = FakeSqlOptions;

    fn build(
        &self,
        _: &BuildContext<'_>,
        o: FakeSqlOptions,
        wants: Roles,
    ) -> Result<Endpoints, BuildError> {
        Ok(self.endpoints(wants, self.open_pool(&o)?, None))
    }

    fn describe(&self, _: &FakeSqlOptions) -> String {
        "fakesql".into()
    }
}

impl StateCapable for FakeSql {
    type Pool = FakePool;
    type Conn = FakeConn;

    fn pool(&self, _: &BuildContext<'_>, options: &FakeSqlOptions) -> Result<FakePool, BuildError> {
        self.open_pool(options)
    }

    fn build_with_ledger(
        &self,
        _: &BuildContext<'_>,
        _: FakeSqlOptions,
        wants: Roles,
        pool: FakePool,
        ledger: Arc<dyn Ledger<FakeConn>>,
    ) -> Result<Endpoints, BuildError> {
        Ok(self.endpoints(wants, pool, Some(ledger)))
    }
}

struct FakeSink {
    pool: FakePool,
    ledger: Option<Arc<dyn Ledger<FakeConn>>>,
}

#[async_trait]
impl Sink for FakeSink {
    async fn prepare(&self, _: &TargetSchema, _: WriteMode) -> Result<()> {
        Ok(())
    }

    async fn begin(
        &self,
        partition: &Partition,
        lease: &Lease,
        strategy: IdempotencyStrategy,
    ) -> Result<Box<dyn SinkTxn>> {
        if !self.capabilities().await?.strategies.contains(&strategy) {
            return Err(FerryError::Connector {
                connector: "fakesql".into(),
                message: format!("strategy {strategy:?} is not supported by this sink"),
            });
        }
        let ledger = match strategy {
            IdempotencyStrategy::TxnWithState => self.ledger.clone(),
            _ => None,
        };
        Ok(Box::new(FakeTxn {
            conn: FakeConn::begin(&self.pool),
            partition: partition.id.clone(),
            lease: lease.clone(),
            ledger,
            summary: WriteSummary::default(),
        }))
    }

    /// `TxnWithState` only when bound to a ledger.
    async fn capabilities(&self) -> Result<SinkCaps> {
        let mut strategies = Vec::new();
        if self.ledger.is_some() || self.pool.server.faults().txn_with_state_without_ledger {
            strategies.push(IdempotencyStrategy::TxnWithState);
        }
        strategies.push(IdempotencyStrategy::DeletePartitionThenInsert);
        Ok(SinkCaps {
            strategies,
            bulk_copy: false,
        })
    }
}

struct FakeTxn {
    conn: FakeConn,
    partition: PartitionId,
    lease: Lease,
    ledger: Option<Arc<dyn Ledger<FakeConn>>>,
    summary: WriteSummary,
}

#[async_trait]
impl SinkTxn for FakeTxn {
    async fn write(&mut self, target: &str, batch: RecordBatch) -> Result<()> {
        let n = batch.num_rows() as u64;
        let row = (self.partition.clone(), target.to_owned(), n);
        if self.conn.pool.server.faults().rows_outside_txn {
            apply(&self.conn.pool.db, vec![row], Vec::new());
        } else {
            self.conn.rows.push(row);
        }
        *self
            .summary
            .rows_written
            .entry(target.to_owned())
            .or_default() += n;
        Ok(())
    }

    async fn commit(self: Box<Self>) -> Result<WriteSummary> {
        let mut this = *self;
        if let Some(ledger) = &this.ledger {
            ledger
                .record_done(&mut this.conn, &this.lease, &this.summary)
                .await?;
        }
        if this
            .conn
            .pool
            .server
            .crash_before_commit
            .load(Ordering::SeqCst)
        {
            // The process dies here: `this.conn` is dropped, and with it the
            // open transaction.
            return Err(FerryError::Connector {
                connector: "fakesql".into(),
                message: "simulated crash before COMMIT".into(),
            });
        }
        this.conn.commit();
        Ok(this.summary)
    }

    async fn abort(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}

/// What the contract suite looks at: the database behind [`URL`].
pub struct FakeProbe {
    pub server: Arc<Server>,
}

#[async_trait]
impl SinkProbe for FakeProbe {
    async fn row_count(&self, target: &str) -> Result<u64> {
        let db = self.server.db(URL);
        let db = db.lock().unwrap();
        Ok(db.rows.values().filter_map(|t| t.get(target)).sum())
    }
}

// ---- fixtures ----

pub fn rows(n: i64) -> RecordBatch {
    let id: ArrayRef = Arc::new(Int64Array::from((1..=n).collect::<Vec<_>>()));
    RecordBatch::try_from_iter([("id", id)]).unwrap()
}

pub fn partition() -> Partition {
    Partition {
        id: PartitionId("orders-0".into()),
        entity: "orders".into(),
        scope: PartitionScope::Whole,
    }
}

/// Two targets, so the suite sees them commit or roll back together.
pub fn sink_fixture() -> SinkFixture {
    let table = |name: &str| TargetTable {
        name: name.into(),
        fields: vec![CanonicalField::new("id", CanonicalType::Int64, false)],
        primary_key: vec!["id".into()],
        foreign_keys: Vec::new(),
    };
    SinkFixture {
        schema: TargetSchema::new(vec![table("orders"), table("order_items")]).unwrap(),
        mode: WriteMode::Insert,
        partition: partition(),
        batches: vec![("orders".into(), rows(3)), ("order_items".into(), rows(5))],
    }
}
