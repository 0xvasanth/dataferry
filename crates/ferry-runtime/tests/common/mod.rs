//! Shared fixtures for the runner tests: an `orders` entity over
//! `MemSource`, spies on the sink and the state store that log what the
//! runner did in one order, and small plans with more than one target or
//! input.

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use async_trait::async_trait;
use ferry_core::testkit::{MemSink, MemSource};
use ferry_core::types::{CanonicalField, CanonicalType};
use ferry_core::{
    Entity, Epoch, IdempotencyStrategy, IdentityPlan, Lease, Partition, PartitionHint, PartitionId,
    PartitionInputs, Result, Sink, SinkCaps, SinkTxn, Source, SourceCaps, TargetBatch,
    TargetSchema, TargetTable, TransformOutput, TransformPlan, TransformStream, WriteMode,
    WriteSummary,
};
use ferry_registry::StateStore;
use ferry_registry::state::{NewRun, PartitionCounts, PartitionRows, RunId, RunRecord, RunTotals};
use ferry_runtime::{Flow, Input, Settings};
use ferry_state::FileStateStore;
use futures::future::BoxFuture;
use futures::{StreamExt, stream};
use tokio::time::Instant;

pub const HASH: &str = "sha256:runner-tests";
pub const ENGINE: &str = "0.0.1-test";

pub fn new_run() -> NewRun {
    NewRun {
        config_hash: HASH.into(),
        engine_version: ENGINE.into(),
        connections: vec![
            ("shop".into(), "mem".into()),
            ("warehouse".into(), "mem".into()),
        ],
    }
}

/// Short leases and fast polling, so tests that wait out a lease are quick.
pub fn settings() -> Settings {
    Settings {
        workers: 4,
        partition_rows: 100,
        lease_ttl: Duration::from_millis(600),
        heartbeat: Duration::from_millis(100),
        poll: Duration::from_millis(20),
        ..Settings::default()
    }
}

pub fn order_fields() -> Vec<CanonicalField> {
    vec![
        CanonicalField::new("id", CanonicalType::Int64, false),
        CanonicalField::new("name", CanonicalType::Utf8, false),
    ]
}

pub fn orders_entity() -> Entity {
    Entity::new("orders", order_fields(), vec!["id".into()]).unwrap()
}

pub fn orders(ids: std::ops::Range<i64>) -> RecordBatch {
    let names: Vec<String> = ids.clone().map(|i| format!("order-{i}")).collect();
    let id: ArrayRef = Arc::new(Int64Array::from_iter_values(ids));
    let name: ArrayRef = Arc::new(StringArray::from(names));
    RecordBatch::try_from_iter([("id", id), ("name", name)]).unwrap()
}

pub fn orders_source(n: i64, batch_rows: usize) -> Arc<MemSource> {
    Arc::new(
        MemSource::new()
            .with_entity(orders_entity(), orders(0..n))
            .unwrap()
            .with_batch_rows(batch_rows),
    )
}

pub fn table(name: &str, fields: Vec<CanonicalField>, key: &str) -> TargetTable {
    TargetTable {
        name: name.into(),
        fields,
        primary_key: vec![key.into()],
        foreign_keys: Vec::new(),
    }
}

/// `orders` from `source` copied unchanged to `target`.
pub fn identity_flow(
    name: &str,
    source: Arc<dyn Source>,
    target: &str,
    sink: Arc<dyn Sink>,
) -> Flow {
    Flow {
        name: name.into(),
        inputs: vec![Input {
            source,
            entity: orders_entity(),
            co_partition: None,
        }],
        plan: Arc::new(IdentityPlan::new(orders_entity(), target)),
        sink,
        target: TargetSchema::new(vec![table(target, order_fields(), "id")]).unwrap(),
        mode: WriteMode::Insert,
        finalizer: None,
        depends_on: Vec::new(),
    }
}

pub fn mem_sink() -> MemSink {
    MemSink::new(vec![IdempotencyStrategy::DeletePartitionThenInsert])
}

/// The ids committed to `target`, sorted. Duplicates stay.
pub fn ids(sink: &MemSink, target: &str) -> Vec<i64> {
    let mut ids: Vec<i64> = sink
        .rows(target)
        .iter()
        .flat_map(|b| {
            b.column_by_name("id")
                .unwrap()
                .as_primitive::<Int64Type>()
                .values()
                .to_vec()
        })
        .collect();
    ids.sort_unstable();
    ids
}

// ---- spies ----

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    Leased(Lease),
    Renewed(Lease),
    RecordRows(Lease, PartitionRows),
    Write {
        lease: Lease,
        target: String,
        rows: usize,
        bytes: usize,
    },
    Commit(Lease),
    Abort(Lease),
    MarkDone(Lease),
    MarkFailed(Lease),
    /// A `WriteObserver` was told the flow's sink is prepared.
    Prepared(String),
    /// A `PartitionObserver` saw an output with this many rows.
    Observed(Lease, usize),
    /// A `PartitionObserver` finished with these rows.
    Finished(Lease, PartitionRows),
}

/// Events in the order they happened, each with the (tokio) instant it was
/// logged at.
#[derive(Clone, Default)]
pub struct Log(Arc<Mutex<Vec<(Instant, Event)>>>);

impl Log {
    pub fn push(&self, event: Event) {
        self.0.lock().unwrap().push((Instant::now(), event));
    }

    pub fn events(&self) -> Vec<Event> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .map(|(_, e)| e.clone())
            .collect()
    }

    pub fn timeline(&self) -> Vec<(Instant, Event)> {
        self.0.lock().unwrap().clone()
    }

    pub fn count(&self, f: impl Fn(&Event) -> bool) -> usize {
        self.0.lock().unwrap().iter().filter(|(_, e)| f(e)).count()
    }
}

type Hook = Arc<dyn Fn(&Lease) -> BoxFuture<'static, ()> + Send + Sync>;

/// Wraps a sink, logging every write, commit and abort, and running an
/// optional hook (a delay, a wait) before each write or commit.
#[derive(Clone)]
pub struct SpySink {
    inner: Arc<dyn Sink>,
    pub log: Log,
    on_write: Arc<Mutex<Option<Hook>>>,
    on_commit: Arc<Mutex<Option<Hook>>>,
}

impl SpySink {
    pub fn new(inner: Arc<dyn Sink>, log: Log) -> Self {
        Self {
            inner,
            log,
            on_write: Arc::default(),
            on_commit: Arc::default(),
        }
    }

    pub fn on_write(
        &self,
        hook: impl Fn(&Lease) -> BoxFuture<'static, ()> + Send + Sync + 'static,
    ) {
        *self.on_write.lock().unwrap() = Some(Arc::new(hook));
    }

    pub fn on_commit(
        &self,
        hook: impl Fn(&Lease) -> BoxFuture<'static, ()> + Send + Sync + 'static,
    ) {
        *self.on_commit.lock().unwrap() = Some(Arc::new(hook));
    }
}

#[async_trait]
impl Sink for SpySink {
    async fn prepare(&self, target: &TargetSchema, mode: WriteMode) -> Result<()> {
        self.inner.prepare(target, mode).await
    }

    async fn begin(
        &self,
        partition: &Partition,
        lease: &Lease,
        strategy: IdempotencyStrategy,
    ) -> Result<Box<dyn SinkTxn>> {
        Ok(Box::new(SpyTxn {
            inner: self.inner.begin(partition, lease, strategy).await?,
            lease: lease.clone(),
            sink: self.clone(),
        }))
    }

    async fn capabilities(&self) -> Result<SinkCaps> {
        self.inner.capabilities().await
    }
}

struct SpyTxn {
    inner: Box<dyn SinkTxn>,
    lease: Lease,
    sink: SpySink,
}

#[async_trait]
impl SinkTxn for SpyTxn {
    async fn write(&mut self, target: &str, batch: RecordBatch) -> Result<()> {
        let hook = self.sink.on_write.lock().unwrap().clone();
        if let Some(hook) = hook {
            hook(&self.lease).await;
        }
        self.sink.log.push(Event::Write {
            lease: self.lease.clone(),
            target: target.into(),
            rows: batch.num_rows(),
            bytes: batch
                .columns()
                .iter()
                .map(|c| c.to_data().get_slice_memory_size().unwrap())
                .sum(),
        });
        self.inner.write(target, batch).await
    }

    async fn commit(self: Box<Self>) -> Result<WriteSummary> {
        let hook = self.sink.on_commit.lock().unwrap().clone();
        if let Some(hook) = hook {
            hook(&self.lease).await;
        }
        let summary = self.inner.commit().await?;
        self.sink.log.push(Event::Commit(self.lease.clone()));
        Ok(summary)
    }

    async fn abort(self: Box<Self>) -> Result<()> {
        self.sink.log.push(Event::Abort(self.lease.clone()));
        self.inner.abort().await
    }
}

type RenewHook = Arc<dyn Fn(&Lease) -> Option<Result<()>> + Send + Sync>;
type LatencyHook = Arc<dyn Fn(&Event) -> Duration + Send + Sync>;

/// Wraps a state store, logging the calls the runner makes per partition.
/// `on_renew` can answer a renewal instead of the store. `reply_after`
/// makes leases and renewals slow to come back: they are logged when the
/// store took them, and returned as much later as it says for that event.
/// `crash` makes it a dead process's store.
#[derive(Clone)]
pub struct SpyStore {
    inner: Arc<dyn StateStore>,
    pub log: Log,
    on_renew: Arc<Mutex<Option<RenewHook>>>,
    on_mark_done: Arc<Mutex<Option<Hook>>>,
    latency: Arc<Mutex<Option<LatencyHook>>>,
    dead: Arc<AtomicBool>,
    /// Calls inside the wrapped store right now.
    in_store: Arc<AtomicUsize>,
    /// The wrapped store, if it is a file store: its calls outlive their
    /// futures, so `crash` waits for them there too.
    file: Option<Arc<FileStateStore>>,
}

/// Counts a call as inside the store until it is dropped.
struct InStore(Arc<AtomicUsize>);

impl Drop for InStore {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl SpyStore {
    pub fn new(inner: Arc<dyn StateStore>, log: Log) -> Self {
        Self {
            inner,
            log,
            on_renew: Arc::default(),
            on_mark_done: Arc::default(),
            latency: Arc::default(),
            dead: Arc::default(),
            in_store: Arc::default(),
            file: None,
        }
    }

    pub fn file(store: Arc<FileStateStore>, log: Log) -> Self {
        Self {
            file: Some(Arc::clone(&store)),
            ..Self::new(store, log)
        }
    }

    /// From now on no call reaches the store, as if the process that holds
    /// it was killed; returns once the calls already inside it are done.
    ///
    /// Dropping a runner aborts its tasks, but a task in the middle of a
    /// store call (a heartbeat's renewal, say) finishes it first, and could
    /// still be writing when the test resumes the run with a new store. A
    /// file store's call even finishes after its future is dropped.
    pub async fn crash(&self) {
        self.dead.store(true, Ordering::SeqCst);
        while self.in_store.load(Ordering::SeqCst) > 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        if let Some(file) = &self.file {
            file.settled().await;
        }
    }

    /// Runs `call` on the store, unless it crashed: then never.
    async fn call<T>(&self, call: impl std::future::Future<Output = T>) -> T {
        self.in_store.fetch_add(1, Ordering::SeqCst);
        let guard = InStore(Arc::clone(&self.in_store));
        if self.dead.load(Ordering::SeqCst) {
            drop(guard);
            return std::future::pending().await;
        }
        let out = call.await;
        drop(guard);
        out
    }

    pub fn reply_after(&self, latency: impl Fn(&Event) -> Duration + Send + Sync + 'static) {
        *self.latency.lock().unwrap() = Some(Arc::new(latency));
    }

    async fn reply(&self, logged: Event) {
        let hook = self.latency.lock().unwrap().clone();
        let latency = hook.map_or(Duration::ZERO, |hook| hook(&logged));
        if !latency.is_zero() {
            tokio::time::sleep(latency).await;
        }
    }

    /// Runs before every `mark_done` reaches the store.
    pub fn on_mark_done(
        &self,
        hook: impl Fn(&Lease) -> BoxFuture<'static, ()> + Send + Sync + 'static,
    ) {
        *self.on_mark_done.lock().unwrap() = Some(Arc::new(hook));
    }

    pub fn on_renew(&self, hook: impl Fn(&Lease) -> Option<Result<()>> + Send + Sync + 'static) {
        *self.on_renew.lock().unwrap() = Some(Arc::new(hook));
    }
}

#[async_trait]
impl StateStore for SpyStore {
    async fn create_run(&self, run: NewRun) -> Result<RunId> {
        self.call(self.inner.create_run(run)).await
    }
    async fn load_run(&self, run: &RunId) -> Result<Option<RunRecord>> {
        self.call(self.inner.load_run(run)).await
    }
    async fn store_partitions(&self, run: &RunId, flow: &str, p: &[Partition]) -> Result<()> {
        self.call(self.inner.store_partitions(run, flow, p)).await
    }
    async fn load_partitions(&self, run: &RunId, flow: &str) -> Result<Vec<Partition>> {
        self.call(self.inner.load_partitions(run, flow)).await
    }
    async fn lease_next(&self, run: &RunId, flow: &str, ttl: Duration) -> Result<Option<Lease>> {
        let lease = self.call(self.inner.lease_next(run, flow, ttl)).await?;
        if let Some(lease) = &lease {
            self.log.push(Event::Leased(lease.clone()));
            self.reply(Event::Leased(lease.clone())).await;
        }
        Ok(lease)
    }
    async fn renew(&self, run: &RunId, lease: &Lease, ttl: Duration) -> Result<()> {
        let hook = self.on_renew.lock().unwrap().clone();
        if let Some(answer) = hook.and_then(|hook| hook(lease)) {
            return answer;
        }
        self.call(self.inner.renew(run, lease, ttl)).await?;
        self.log.push(Event::Renewed(lease.clone()));
        self.reply(Event::Renewed(lease.clone())).await;
        Ok(())
    }
    async fn mark_done(&self, run: &RunId, lease: &Lease, summary: &WriteSummary) -> Result<()> {
        let hook = self.on_mark_done.lock().unwrap().clone();
        if let Some(hook) = hook {
            hook(lease).await;
        }
        self.call(self.inner.mark_done(run, lease, summary)).await?;
        self.log.push(Event::MarkDone(lease.clone()));
        Ok(())
    }
    async fn mark_failed(&self, run: &RunId, lease: &Lease, error: &str) -> Result<()> {
        self.call(self.inner.mark_failed(run, lease, error)).await?;
        self.log.push(Event::MarkFailed(lease.clone()));
        Ok(())
    }
    async fn record_rows(&self, run: &RunId, lease: &Lease, rows: &PartitionRows) -> Result<()> {
        self.call(self.inner.record_rows(run, lease, rows)).await?;
        self.log.push(Event::RecordRows(lease.clone(), *rows));
        Ok(())
    }
    async fn is_done(&self, run: &RunId, partition: &PartitionId) -> Result<bool> {
        self.call(self.inner.is_done(run, partition)).await
    }
    async fn counts(&self, run: &RunId) -> Result<PartitionCounts> {
        self.call(self.inner.counts(run)).await
    }
    async fn flow_counts(&self, run: &RunId, flow: &str) -> Result<PartitionCounts> {
        self.call(self.inner.flow_counts(run, flow)).await
    }
    async fn reset_failed(&self, run: &RunId) -> Result<u64> {
        self.call(self.inner.reset_failed(run)).await
    }
    async fn totals(&self, run: &RunId) -> Result<RunTotals> {
        self.call(self.inner.totals(run)).await
    }
}

/// Counts `plan_partitions` calls, to prove resume doesn't re-plan.
pub struct CountingSource {
    pub inner: Arc<dyn Source>,
    pub plans: Mutex<usize>,
}

impl CountingSource {
    pub fn new(inner: Arc<dyn Source>) -> Self {
        Self {
            inner,
            plans: Mutex::new(0),
        }
    }
}

#[async_trait]
impl Source for CountingSource {
    async fn discover(&self) -> Result<Vec<Entity>> {
        self.inner.discover().await
    }
    async fn plan_partitions(&self, e: &Entity, hint: &PartitionHint) -> Result<Vec<Partition>> {
        *self.plans.lock().unwrap() += 1;
        self.inner.plan_partitions(e, hint).await
    }
    async fn read(&self, e: &Entity, p: &Partition) -> Result<ferry_core::BatchStream> {
        self.inner.read(e, p).await
    }
    async fn capabilities(&self) -> Result<SourceCaps> {
        self.inner.capabilities().await
    }
}

// ---- plans with more than one target or input ----

/// `orders` → `orders` (parent) and `order_items` (two per order, child):
/// the shape of a split. Emits one output per input batch.
pub struct SplitPlan {
    inputs: Vec<String>,
    order: Vec<String>,
}

impl Default for SplitPlan {
    fn default() -> Self {
        Self {
            inputs: vec!["orders".into()],
            order: vec!["orders".into(), "order_items".into()],
        }
    }
}

pub fn item_fields() -> Vec<CanonicalField> {
    vec![
        CanonicalField::new("id", CanonicalType::Int64, false),
        CanonicalField::new("order_id", CanonicalType::Int64, false),
    ]
}

pub fn split_target() -> TargetSchema {
    TargetSchema::new(vec![
        table("orders", order_fields(), "id"),
        table("order_items", item_fields(), "id"),
    ])
    .unwrap()
}

impl TransformPlan for SplitPlan {
    fn inputs(&self) -> &[String] {
        &self.inputs
    }

    fn write_order(&self) -> &[String] {
        &self.order
    }

    fn execute(&self, mut inputs: PartitionInputs) -> Result<TransformStream> {
        let entity = orders_entity();
        Ok(inputs
            .take("orders")?
            .map(move |batch| {
                let batch = batch?;
                let records = entity.record_ids(&batch)?;
                let order_ids = batch
                    .column(0)
                    .as_primitive::<Int64Type>()
                    .values()
                    .to_vec();
                let item_ids: Vec<i64> =
                    order_ids.iter().flat_map(|o| [o * 2, o * 2 + 1]).collect();
                let parents: Vec<i64> = order_ids.iter().flat_map(|o| [*o, *o]).collect();
                let lineage: Vec<u32> = (0..order_ids.len() as u32).flat_map(|i| [i, i]).collect();
                let id: ArrayRef = Arc::new(Int64Array::from(item_ids));
                let order_id: ArrayRef = Arc::new(Int64Array::from(parents));
                let items =
                    RecordBatch::try_from_iter([("id", id), ("order_id", order_id)]).unwrap();
                Ok(TransformOutput {
                    targets: vec![
                        TargetBatch {
                            target: "orders".into(),
                            source_index: (0..records.len() as u32).collect(),
                            batch,
                        },
                        TargetBatch {
                            target: "order_items".into(),
                            source_index: lineage,
                            batch: items,
                        },
                    ],
                    records,
                    rejected: Vec::new(),
                })
            })
            .boxed())
    }
}

/// Rejects every record whose id is in `bad`; copies the rest.
pub struct RejectingPlan {
    pub inner: IdentityPlan,
    pub bad: Vec<i64>,
}

impl TransformPlan for RejectingPlan {
    fn inputs(&self) -> &[String] {
        self.inner.inputs()
    }

    fn write_order(&self) -> &[String] {
        self.inner.write_order()
    }

    fn execute(&self, inputs: PartitionInputs) -> Result<TransformStream> {
        let bad = self.bad.clone();
        Ok(self
            .inner
            .execute(inputs)?
            .map(move |out| {
                let mut out = out?;
                let ids = out.targets[0]
                    .batch
                    .column(0)
                    .as_primitive::<Int64Type>()
                    .clone();
                let keep: Vec<bool> = ids.values().iter().map(|i| !bad.contains(i)).collect();
                for (i, keep) in keep.iter().enumerate() {
                    if !keep {
                        out.rejected.push(ferry_core::RejectedRecord {
                            index: i as u32,
                            target: None,
                            error: "qty is not a number".into(),
                        });
                    }
                }
                let mask = arrow_array::BooleanArray::from(keep.clone());
                let t = &mut out.targets[0];
                t.batch = arrow_select::filter::filter_record_batch(&t.batch, &mask).unwrap();
                t.source_index = (0..keep.len() as u32)
                    .filter(|i| keep[*i as usize])
                    .collect();
                Ok(out)
            })
            .boxed())
    }
}

/// `orders` merged with its `items` (co-partitioned on `order_id`): writes
/// each order, and each item with lineage to its order. Checks that every
/// item it was given belongs to an order of the same partition, which only
/// holds if the runner fed matching ranges.
pub struct MergePlan {
    inputs: Vec<String>,
    order: Vec<String>,
    pub stray_items: Arc<Mutex<Vec<i64>>>,
}

impl Default for MergePlan {
    fn default() -> Self {
        Self {
            inputs: vec!["orders".into(), "items".into()],
            order: vec!["orders".into(), "order_items".into()],
            stray_items: Arc::default(),
        }
    }
}

pub fn items_entity() -> Entity {
    Entity::new("items", item_fields(), vec!["id".into()]).unwrap()
}

/// Two items per order: ids `2o` and `2o + 1`.
pub fn items(orders: std::ops::Range<i64>) -> RecordBatch {
    let ids: Vec<i64> = orders.clone().flat_map(|o| [o * 2, o * 2 + 1]).collect();
    let parents: Vec<i64> = orders.flat_map(|o| [o, o]).collect();
    let id: ArrayRef = Arc::new(Int64Array::from(ids));
    let order_id: ArrayRef = Arc::new(Int64Array::from(parents));
    RecordBatch::try_from_iter([("id", id), ("order_id", order_id)]).unwrap()
}

impl TransformPlan for MergePlan {
    fn inputs(&self) -> &[String] {
        &self.inputs
    }

    fn write_order(&self) -> &[String] {
        &self.order
    }

    fn execute(&self, mut inputs: PartitionInputs) -> Result<TransformStream> {
        let orders = inputs.take("orders")?;
        let items = inputs.take("items")?;
        let stray = Arc::clone(&self.stray_items);
        let entity = orders_entity();
        let merged = async move {
            let orders: Vec<RecordBatch> = orders
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .collect::<Result<_>>()?;
            let items: Vec<RecordBatch> = items
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .collect::<Result<_>>()?;
            let orders =
                arrow_select::concat::concat_batches(&entity.arrow_schema(), &orders).unwrap();
            let items =
                arrow_select::concat::concat_batches(&items_entity().arrow_schema(), &items)
                    .unwrap();
            let records = entity.record_ids(&orders)?;
            let order_ids = orders
                .column(0)
                .as_primitive::<Int64Type>()
                .values()
                .to_vec();
            let index: HashMap<i64, u32> = order_ids
                .iter()
                .enumerate()
                .map(|(i, o)| (*o, i as u32))
                .collect();
            let mut lineage = Vec::new();
            let mut keep = Vec::new();
            for (item, parent) in items
                .column(0)
                .as_primitive::<Int64Type>()
                .values()
                .iter()
                .zip(items.column(1).as_primitive::<Int64Type>().values())
            {
                match index.get(parent) {
                    Some(i) => {
                        lineage.push(*i);
                        keep.push(true);
                    }
                    None => {
                        stray.lock().unwrap().push(*item);
                        keep.push(false);
                    }
                }
            }
            let mask = arrow_array::BooleanArray::from(keep);
            let items = arrow_select::filter::filter_record_batch(&items, &mask).unwrap();
            Ok(TransformOutput {
                targets: vec![
                    TargetBatch {
                        target: "orders".into(),
                        source_index: (0..records.len() as u32).collect(),
                        batch: orders,
                    },
                    TargetBatch {
                        target: "order_items".into(),
                        source_index: lineage,
                        batch: items,
                    },
                ],
                records,
                rejected: Vec::new(),
            })
        };
        Ok(stream::once(merged).boxed())
    }
}

pub fn lease(partition: &str, epoch: u64) -> Lease {
    Lease {
        partition: PartitionId(partition.into()),
        epoch: Epoch(epoch),
    }
}
