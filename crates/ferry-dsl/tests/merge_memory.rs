//! A staged merge of two 5M-row inputs stays under the memory budget (B6,
//! 0XT-264): 5M orders, each nesting its items from 5M order items that
//! are staged (`strategy: stage`), run by the runner (A5) with four
//! workers. The process's peak heap, measured by a counting global
//! allocator as ferry-runtime's `tests/memory.rs` does, stays under the
//! budget, while the staged side alone is several times the budget.
//!
//! The budget is split the way a run would split `memory_budget`: three
//! quarters to the runner (read-ahead and write batches), a quarter to the
//! flow's stages ([`Staging::memory`]).
//!
//! Alone in its own test binary, so no other test's allocations count.

mod common;

use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow_array::builder::{Int64Builder, StringBuilder};
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, ArrayRef, RecordBatch};
use async_trait::async_trait;
use common::plan::{compile_one, runtime_flow};
use ferry_core::types::{CanonicalField, CanonicalType};
use ferry_core::{
    BatchStream, Entity, IdempotencyStrategy, KeyRange, KeyValue, Lease, Partition, PartitionHint,
    PartitionId, PartitionScope, Result, Sink, SinkCaps, SinkTxn, Source, SourceCaps, TargetSchema,
    WriteMode, WriteSummary,
};
use ferry_dsl::compile::Strategy;
use ferry_dsl::typecheck::Catalog;
use ferry_registry::state::NewRun;
use ferry_runtime::{Runner, Settings};
use ferry_state::FileStateStore;
use futures::{StreamExt, stream};
use peak_alloc::PeakAlloc;

#[global_allocator]
static HEAP: PeakAlloc = PeakAlloc;

const ORDERS: i64 = 5_000_000;
const ITEMS: i64 = 5_000_000;
const BATCH_ROWS: i64 = 8192;
const BUDGET: u64 = 64 << 20;
/// Item `j` belongs to order `2 * (j * STRIDE mod ITEMS/2)`: every even
/// order has two items, read far apart, and every odd one none.
const STRIDE: i64 = 7919;

fn orders() -> Entity {
    Entity::new(
        "orders",
        vec![
            CanonicalField::new("id", CanonicalType::Int64, false),
            CanonicalField::new("status", CanonicalType::Utf8, false),
        ],
        vec!["id".into()],
    )
    .unwrap()
}

fn items() -> Entity {
    Entity::new(
        "items",
        vec![
            CanonicalField::new("item_id", CanonicalType::Int64, false),
            CanonicalField::new("order_id", CanonicalType::Int64, false),
            CanonicalField::new("qty", CanonicalType::Int64, false),
            CanonicalField::new("sku", CanonicalType::Utf8, false),
        ],
        vec!["item_id".into()],
    )
    .unwrap()
}

fn order_of(item: i64) -> i64 {
    2 * ((item * STRIDE) % (ITEMS / 2))
}

fn generate_orders(from: i64, to: i64) -> RecordBatch {
    let n = (to - from) as usize;
    let mut id = Int64Builder::with_capacity(n);
    let mut status = StringBuilder::with_capacity(n, n * 4);
    for i in from..to {
        id.append_value(i);
        status.append_value("paid");
    }
    let columns: Vec<ArrayRef> = vec![Arc::new(id.finish()), Arc::new(status.finish())];
    RecordBatch::try_new(orders().arrow_schema(), columns).unwrap()
}

fn generate_items(from: i64, to: i64) -> RecordBatch {
    let n = (to - from) as usize;
    let mut item = Int64Builder::with_capacity(n);
    let mut order = Int64Builder::with_capacity(n);
    let mut qty = Int64Builder::with_capacity(n);
    let mut sku = StringBuilder::with_capacity(n, n * 12);
    let mut text = String::new();
    for j in from..to {
        item.append_value(j);
        order.append_value(order_of(j));
        qty.append_value(j % 7);
        text.clear();
        write!(text, "sku-{j:08}").unwrap();
        sku.append_value(&text);
    }
    let columns: Vec<ArrayRef> = vec![
        Arc::new(item.finish()),
        Arc::new(order.finish()),
        Arc::new(qty.finish()),
        Arc::new(sku.finish()),
    ];
    RecordBatch::try_new(items().arrow_schema(), columns).unwrap()
}

/// Rows generated batch by batch as they are read, split into ranges of
/// the primary key: the source holds nothing.
struct Synthetic {
    entity: Entity,
    rows: i64,
    generate: fn(i64, i64) -> RecordBatch,
}

#[async_trait]
impl Source for Synthetic {
    async fn discover(&self) -> Result<Vec<Entity>> {
        Ok(vec![self.entity.clone()])
    }

    async fn plan_partitions(&self, e: &Entity, hint: &PartitionHint) -> Result<Vec<Partition>> {
        assert!(hint.co_partition.is_none(), "the items are staged");
        let step = hint.target_rows as i64;
        let key = &e.primary_key()[0];
        (0..self.rows)
            .step_by(step as usize)
            .enumerate()
            .map(|(i, lo)| {
                let lower = (i > 0).then_some(KeyValue::Int(lo));
                let upper = (lo + step < self.rows).then_some(KeyValue::Int(lo + step));
                Ok(Partition {
                    id: PartitionId(format!("{}-{i}", e.name())),
                    entity: e.name().into(),
                    scope: PartitionScope::KeyRange(KeyRange::new(key.clone(), lower, upper)?),
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
        let generate = self.generate;
        Ok(stream::iter((lo..hi).step_by(BATCH_ROWS as usize))
            .map(move |from| Ok(generate(from, (from + BATCH_ROWS).min(hi))))
            .boxed())
    }

    async fn capabilities(&self) -> Result<SourceCaps> {
        Ok(SourceCaps::default())
    }
}

/// The inverse of `STRIDE` modulo `ITEMS / 2`.
fn inverse() -> i64 {
    let (m, mut t, mut new_t, mut r, mut new_r) = (ITEMS / 2, 0i64, 1i64, ITEMS / 2, STRIDE);
    while new_r != 0 {
        let q = r / new_r;
        (t, new_t) = (new_t, t - q * new_t);
        (r, new_r) = (new_r, r - q * new_r);
    }
    assert_eq!(r, 1, "STRIDE must be coprime with ITEMS / 2");
    t.rem_euclid(m)
}

/// Checks every summary as it is written, then drops it: order `k` nests
/// exactly its two items in `item_id` order, or `[]`.
#[derive(Clone, Default)]
struct Checking {
    rows: Arc<AtomicU64>,
    nested: Arc<AtomicU64>,
}

#[async_trait]
impl Sink for Checking {
    async fn prepare(&self, _: &TargetSchema, _: WriteMode) -> Result<()> {
        Ok(())
    }

    async fn begin(
        &self,
        _: &Partition,
        _: &Lease,
        _: IdempotencyStrategy,
    ) -> Result<Box<dyn SinkTxn>> {
        Ok(Box::new(CheckingTxn {
            sink: self.clone(),
            inverse: inverse(),
            rows: 0,
            nested: 0,
        }))
    }

    async fn capabilities(&self) -> Result<SinkCaps> {
        Ok(SinkCaps {
            strategies: vec![IdempotencyStrategy::DeletePartitionThenInsert],
            bulk_copy: false,
        })
    }
}

struct CheckingTxn {
    sink: Checking,
    inverse: i64,
    rows: u64,
    nested: u64,
}

#[async_trait]
impl SinkTxn for CheckingTxn {
    async fn write(&mut self, _: &str, batch: RecordBatch) -> Result<()> {
        let ids = batch.column(0).as_primitive::<Int64Type>();
        let lists = batch.column(1).as_list::<i32>();
        for row in 0..batch.num_rows() {
            let id = ids.value(row);
            let items = lists.value(row);
            let items = items.as_struct();
            let item_ids = items.column(0).as_primitive::<Int64Type>();
            let got: Vec<i64> = item_ids.values().to_vec();
            let expected = if id % 2 == 0 {
                let j = (id / 2 * self.inverse) % (ITEMS / 2);
                vec![j, j + ITEMS / 2]
            } else {
                Vec::new()
            };
            assert_eq!(got, expected, "order {id}");
            assert!(!lists.is_null(row), "order {id}: never a null array");
            self.nested += item_ids.len() as u64;
        }
        self.rows += batch.num_rows() as u64;
        Ok(())
    }

    async fn commit(self: Box<Self>) -> Result<WriteSummary> {
        self.sink.rows.fetch_add(self.rows, Ordering::Relaxed);
        self.sink.nested.fetch_add(self.nested, Ordering::Relaxed);
        Ok(WriteSummary {
            rows_written: [("summaries".to_owned(), self.rows)].into(),
        })
    }

    async fn abort(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}

const FLOW: &str = "flows:
  - name: summaries
    from:
      - entity: shop.orders
        as: o
      - entity: shop.items
        as: i
        join:
          cardinality: many
          keys: {order_id: o.id}
          strategy: stage
    sink: warehouse
    to:
      summaries:
        columns:
          id: id
          items:
            nest: i
            order_by: [item_id]
            columns:
              item_id: item_id
              qty: qty
              sku: sku
";

/// Row bytes of a batch, the way the runner counts them.
fn row_bytes(batch: &RecordBatch) -> u64 {
    batch
        .columns()
        .iter()
        .map(|c| c.to_data().get_slice_memory_size().unwrap() as u64)
        .sum()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_staged_merge_of_two_5m_row_inputs_stays_under_the_memory_budget() {
    let compiled = compile_one(
        FLOW,
        &Catalog::new().with("shop", orders()).with("shop", items()),
    );
    assert_eq!(
        compiled.plan.joins().next().unwrap().strategy,
        Strategy::Stage
    );
    let sink = Checking::default();
    let order_source: Arc<dyn Source> = Arc::new(Synthetic {
        entity: orders(),
        rows: ORDERS,
        generate: generate_orders,
    });
    let item_source: Arc<dyn Source> = Arc::new(Synthetic {
        entity: items(),
        rows: ITEMS,
        generate: generate_items,
    });
    let flow = runtime_flow(
        &compiled,
        |_| Arc::clone(&item_source),
        "shop",
        Arc::new(sink.clone()),
        BUDGET / 4,
    );
    let flow = ferry_runtime::Flow {
        inputs: vec![ferry_runtime::Input {
            source: order_source,
            ..flow.inputs[0].clone()
        }],
        ..flow
    };
    let dir = tempfile::tempdir().unwrap();
    let settings = Settings {
        workers: 4,
        memory_budget: BUDGET - BUDGET / 4,
        partition_rows: (ORDERS / 8) as u64,
        ..Settings::default()
    };
    let runner = Runner::new(
        Arc::new(FileStateStore::new(dir.path())),
        vec![flow],
        settings,
    )
    .unwrap();
    HEAP.reset_peak_usage();
    let report = runner
        .start(NewRun {
            config_hash: "sha256:merge-memory".into(),
            engine_version: "0.0.1-test".into(),
            connections: Vec::new(),
        })
        .await
        .unwrap();
    let peak = HEAP.peak_usage() as u64;
    assert!(report.is_complete(), "{report:?}");
    assert_eq!(sink.rows.load(Ordering::Relaxed), ORDERS as u64);
    assert_eq!(sink.nested.load(Ordering::Relaxed), ITEMS as u64);

    let staged = row_bytes(&generate_items(0, BATCH_ROWS)) * (ITEMS / BATCH_ROWS) as u64;
    eprintln!(
        "{ORDERS} orders x {ITEMS} items, staged: peak heap {:.1} MiB for {:.0} MiB of staged rows, budget {} MiB",
        peak as f64 / (1 << 20) as f64,
        staged as f64 / (1 << 20) as f64,
        BUDGET >> 20
    );
    // A stage held in memory, or even the runner's share of it, fails.
    assert!(
        staged > 2 * BUDGET,
        "the staged side should dwarf the budget"
    );
    assert!(
        peak < BUDGET,
        "peak heap {peak} bytes is over the {BUDGET}-byte budget"
    );
}
