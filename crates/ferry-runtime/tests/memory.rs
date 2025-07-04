//! `memory_budget` on a 10M-row synthetic source: the process's peak heap,
//! measured by a counting global allocator, stays under the budget while
//! a slow sink keeps every read-ahead channel full.
//!
//! Alone in its own test binary, so no other test's allocations count.

use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use arrow_array::builder::{Float64Builder, Int64Builder, StringBuilder};
use arrow_array::{ArrayRef, RecordBatch};
use async_trait::async_trait;
use ferry_core::types::{CanonicalField, CanonicalType};
use ferry_core::{
    BatchStream, Entity, IdempotencyStrategy, IdentityPlan, KeyRange, KeyValue, Lease, Partition,
    PartitionHint, PartitionId, PartitionScope, Result, Sink, SinkCaps, SinkTxn, Source,
    SourceCaps, TargetSchema, TargetTable, WriteMode, WriteSummary,
};
use ferry_registry::state::NewRun;
use ferry_runtime::{Flow, Input, Runner, Settings};
use ferry_state::FileStateStore;
use futures::{StreamExt, stream};
use peak_alloc::PeakAlloc;

#[global_allocator]
static HEAP: PeakAlloc = PeakAlloc;

const ROWS: i64 = 10_000_000;
const BATCH_ROWS: i64 = 8192;
const BUDGET: u64 = 64 << 20;

fn fields() -> Vec<CanonicalField> {
    vec![
        CanonicalField::new("id", CanonicalType::Int64, false),
        CanonicalField::new("customer", CanonicalType::Utf8, false),
        CanonicalField::new("amount", CanonicalType::Float64, false),
    ]
}

fn entity() -> Entity {
    Entity::new("events", fields(), vec!["id".into()]).unwrap()
}

/// `rows` rows, generated batch by batch as they are read: the source
/// holds nothing.
struct Synthetic {
    rows: i64,
}

fn generate(from: i64, to: i64) -> RecordBatch {
    let n = (to - from) as usize;
    let mut id = Int64Builder::with_capacity(n);
    let mut customer = StringBuilder::with_capacity(n, n * 20);
    let mut amount = Float64Builder::with_capacity(n);
    let mut name = String::new();
    for i in from..to {
        id.append_value(i);
        name.clear();
        write!(name, "customer-{:010}", i % 1_000_003).unwrap();
        customer.append_value(&name);
        amount.append_value(i as f64 / 100.0);
    }
    let columns: Vec<ArrayRef> = vec![
        Arc::new(id.finish()),
        Arc::new(customer.finish()),
        Arc::new(amount.finish()),
    ];
    RecordBatch::try_new(entity().arrow_schema(), columns).unwrap()
}

#[async_trait]
impl Source for Synthetic {
    async fn discover(&self) -> Result<Vec<Entity>> {
        Ok(vec![entity()])
    }

    async fn plan_partitions(&self, e: &Entity, hint: &PartitionHint) -> Result<Vec<Partition>> {
        let step = hint.target_rows as i64;
        let rows = self.rows;
        let starts: Vec<i64> = (0..rows).step_by(step as usize).collect();
        starts
            .iter()
            .enumerate()
            .map(|(i, &lo)| {
                let lower = (i > 0).then_some(KeyValue::Int(lo));
                let upper = (lo + step < rows).then_some(KeyValue::Int(lo + step));
                Ok(Partition {
                    id: PartitionId(format!("{}-{i}", e.name())),
                    entity: e.name().into(),
                    scope: PartitionScope::KeyRange(KeyRange::new("id", lower, upper)?),
                })
            })
            .collect()
    }

    async fn read(&self, _: &Entity, p: &Partition) -> Result<BatchStream> {
        let PartitionScope::KeyRange(range) = &p.scope else {
            unreachable!("planned as ranges")
        };
        let bound = |k: Option<&KeyValue>, default| match k {
            Some(KeyValue::Int(v)) => *v,
            _ => default,
        };
        let (lo, hi) = (bound(range.lower(), 0), bound(range.upper(), self.rows));
        Ok(stream::iter((lo..hi).step_by(BATCH_ROWS as usize))
            .map(move |from| Ok(generate(from, (from + BATCH_ROWS).min(hi))))
            .boxed())
    }

    async fn capabilities(&self) -> Result<SourceCaps> {
        Ok(SourceCaps::default())
    }
}

/// Counts rows and drops them, taking a few microseconds per row so the
/// sink, not the source, sets the pace and read-ahead fills up.
#[derive(Clone, Default)]
struct Slow {
    rows: Arc<AtomicU64>,
}

#[async_trait]
impl Sink for Slow {
    async fn prepare(&self, _: &TargetSchema, _: WriteMode) -> Result<()> {
        Ok(())
    }

    async fn begin(
        &self,
        _: &Partition,
        _: &Lease,
        _: IdempotencyStrategy,
    ) -> Result<Box<dyn SinkTxn>> {
        Ok(Box::new(SlowTxn {
            rows: Arc::clone(&self.rows),
            written: 0,
        }))
    }

    async fn capabilities(&self) -> Result<SinkCaps> {
        Ok(SinkCaps {
            strategies: vec![IdempotencyStrategy::DeletePartitionThenInsert],
            bulk_copy: false,
        })
    }
}

struct SlowTxn {
    rows: Arc<AtomicU64>,
    written: u64,
}

#[async_trait]
impl SinkTxn for SlowTxn {
    async fn write(&mut self, _: &str, batch: RecordBatch) -> Result<()> {
        tokio::time::sleep(Duration::from_micros(2) * batch.num_rows() as u32).await;
        self.written += batch.num_rows() as u64;
        Ok(())
    }

    async fn commit(self: Box<Self>) -> Result<WriteSummary> {
        self.rows.fetch_add(self.written, Ordering::Relaxed);
        Ok(WriteSummary {
            rows_written: [("events".to_owned(), self.written)].into(),
        })
    }

    async fn abort(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}

/// Runs `rows` rows through four workers, one partition each, and
/// returns the peak heap while it ran.
async fn peak_heap(rows: i64, budget: u64) -> u64 {
    let dir = tempfile::tempdir().unwrap();
    let sink = Slow::default();
    let flow = Flow {
        name: "events".into(),
        inputs: vec![Input {
            source: Arc::new(Synthetic { rows }),
            entity: entity(),
            co_partition: None,
        }],
        plan: Arc::new(IdentityPlan::new(entity(), "events")),
        sink: Arc::new(sink.clone()),
        target: TargetSchema::new(vec![TargetTable {
            name: "events".into(),
            fields: fields(),
            primary_key: vec!["id".into()],
            foreign_keys: Vec::new(),
        }])
        .unwrap(),
        mode: WriteMode::Insert,
        depends_on: Vec::new(),
    };
    let settings = Settings {
        workers: 4,
        memory_budget: budget,
        partition_rows: (rows / 4) as u64,
        ..Settings::default()
    };
    let store = Arc::new(FileStateStore::new(dir.path()));
    let runner = Runner::new(store, vec![flow], settings).unwrap();
    HEAP.reset_peak_usage();
    let report = runner
        .start(NewRun {
            config_hash: "sha256:memory".into(),
            engine_version: "0.0.1-test".into(),
            connections: Vec::new(),
        })
        .await
        .unwrap();
    let peak = HEAP.peak_usage() as u64;
    assert!(report.is_complete(), "{report:?}");
    assert_eq!(sink.rows.load(Ordering::Relaxed), rows as u64);
    assert_eq!(report.totals.read, rows as u64);
    let data = row_bytes(&generate(0, BATCH_ROWS)) * (rows / BATCH_ROWS) as u64;
    eprintln!(
        "{rows} rows: peak heap {:.1} MiB for {:.0} MiB of rows, budget {} MiB",
        peak as f64 / (1 << 20) as f64,
        data as f64 / (1 << 20) as f64,
        budget >> 20
    );
    // Each worker's partition alone is bigger than the whole budget: a
    // runner that held a partition, or even a large part of one, fails.
    assert!(data / 4 > budget, "the data should dwarf the budget");
    peak
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ten_million_rows_stay_under_the_memory_budget() {
    let peak = peak_heap(ROWS, BUDGET).await;
    assert!(
        peak < BUDGET,
        "peak heap {peak} bytes is over the {BUDGET}-byte budget"
    );

    // A quarter of the budget, a quarter of the peak: the budget, not a
    // fixed pipeline depth, is what bounds it.
    let small = BUDGET / 4;
    let small_peak = peak_heap(ROWS / 4, small).await;
    assert!(
        small_peak < small,
        "peak heap {small_peak} bytes is over the {small}-byte budget"
    );
}

/// Row bytes of a batch, the way the runner counts them.
fn row_bytes(batch: &RecordBatch) -> u64 {
    batch
        .columns()
        .iter()
        .map(|c| c.to_data().get_slice_memory_size().unwrap() as u64)
        .sum()
}
