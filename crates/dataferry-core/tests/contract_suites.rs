//! The contract suites are only useful if they catch broken connectors. Each
//! test here runs a suite against the in-memory fakes (which must pass) or
//! against a wrapper that breaks exactly one guarantee (which must be caught,
//! for the right reason).

use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use async_trait::async_trait;
use dataferry_core::testkit::{MemSink, MemSource, SinkFixture, check_sink, check_source};
use dataferry_core::types::{CanonicalField, CanonicalType};
use dataferry_core::{
    BatchStream, Entity, Epoch, ForeignKey, IdempotencyStrategy, Lease, Partition, PartitionHint,
    PartitionId, PartitionScope, Result, Sink, SinkCaps, SinkTxn, Source, SourceCaps, TargetSchema,
    TargetTable, WriteMode, WriteSummary,
};
use futures::StreamExt;

fn orders_entity() -> Entity {
    Entity::new(
        "orders",
        vec![
            CanonicalField::new("id", CanonicalType::Int64, false),
            CanonicalField::new("status", CanonicalType::Utf8, true),
        ],
        vec!["id".into()],
    )
    .unwrap()
}

fn orders_rows(ids: &[i64]) -> RecordBatch {
    let id: ArrayRef = Arc::new(Int64Array::from(ids.to_vec()));
    let status: ArrayRef = Arc::new(StringArray::from(vec!["paid"; ids.len()]));
    RecordBatch::try_new(orders_entity().arrow_schema(), vec![id, status]).unwrap()
}

/// Ten orders with gaps in the key space, read in batches of two.
fn mem_source() -> MemSource {
    MemSource::new()
        .with_entity(
            orders_entity(),
            orders_rows(&[3, 1, 9, 4, 20, 21, 7, 100, 55, 8]),
        )
        .unwrap()
        .with_batch_rows(2)
}

const HINT: PartitionHint = PartitionHint {
    target_rows: 3,
    co_partition: None,
};

#[tokio::test]
async fn mem_source_passes_the_source_contract() {
    let source = mem_source();
    let partitions = source
        .plan_partitions(&orders_entity(), &HINT)
        .await
        .unwrap();
    assert_eq!(partitions.len(), 4, "10 rows at ~3 per partition");

    assert_eq!(check_source(&source, &HINT).await, Vec::<String>::new());
}

/// Wraps a source and lets each test break one thing about it.
struct Broken<F> {
    inner: MemSource,
    tweak: F,
}

enum Tweak {
    AddWholePartition,
    DropLastPartition,
    DropColumnOnRead,
}

#[async_trait]
impl Source for Broken<Tweak> {
    async fn discover(&self) -> Result<Vec<Entity>> {
        self.inner.discover().await
    }

    async fn plan_partitions(&self, e: &Entity, hint: &PartitionHint) -> Result<Vec<Partition>> {
        let mut p = self.inner.plan_partitions(e, hint).await?;
        match self.tweak {
            Tweak::AddWholePartition => p.push(Partition {
                id: PartitionId("orders-extra".into()),
                entity: "orders".into(),
                scope: PartitionScope::Whole,
            }),
            Tweak::DropLastPartition => {
                p.pop();
            }
            Tweak::DropColumnOnRead => {}
        }
        Ok(p)
    }

    async fn read(&self, e: &Entity, p: &Partition) -> Result<BatchStream> {
        let stream = self.inner.read(e, p).await?;
        Ok(match self.tweak {
            Tweak::DropColumnOnRead => stream.map(|b| Ok(b?.project(&[0]).unwrap())).boxed(),
            _ => stream,
        })
    }

    async fn capabilities(&self) -> Result<SourceCaps> {
        self.inner.capabilities().await
    }
}

async fn source_violations(tweak: Tweak) -> Vec<String> {
    let source = Broken {
        inner: mem_source(),
        tweak,
    };
    let v = check_source(&source, &HINT).await;
    assert!(!v.is_empty(), "the broken source passed the contract");
    v
}

#[tokio::test]
async fn source_contract_catches_overlapping_partitions() {
    let v = source_violations(Tweak::AddWholePartition).await;
    assert!(
        v.iter().all(|m| m.contains("more than one partition")),
        "{v:#?}"
    );
    assert_eq!(v.len(), 10, "every row is reported once: {v:#?}");
}

#[tokio::test]
async fn source_contract_catches_rows_missing_from_every_partition() {
    let v = source_violations(Tweak::DropLastPartition).await;
    // Sorted keys 1 3 4 | 7 8 9 | 20 21 55 | 100: the last range is [100, ∞).
    assert_eq!(
        v,
        ["orders: row SourceRecordId([Int(100)]) is in no partition"]
    );
}

#[tokio::test]
async fn source_contract_catches_batches_that_do_not_match_the_entity() {
    let v = source_violations(Tweak::DropColumnOnRead).await;
    assert!(v.iter().any(|m| m.contains("schema differs")), "{v:#?}");
}

#[tokio::test]
async fn mem_source_read_fault_surfaces_as_a_stream_error() {
    let source = mem_source().failing_after_batches(1);
    let whole = Partition {
        id: PartitionId("orders-0".into()),
        entity: "orders".into(),
        scope: PartitionScope::Whole,
    };
    let items: Vec<_> = source
        .read(&orders_entity(), &whole)
        .await
        .unwrap()
        .collect()
        .await;
    assert_eq!(items.len(), 2);
    assert!(items[0].is_ok());
    assert!(items[1].is_err());
}

// ---------------------------------------------------------------- sinks

fn field(name: &str, ty: CanonicalType, nullable: bool) -> CanonicalField {
    CanonicalField::new(name, ty, nullable)
}

fn sink_fixture() -> SinkFixture {
    let orders = TargetTable {
        name: "orders".into(),
        fields: vec![
            field("id", CanonicalType::Int64, false),
            field("status", CanonicalType::Utf8, true),
        ],
        primary_key: vec!["id".into()],
        foreign_keys: vec![],
    };
    let items = TargetTable {
        name: "order_items".into(),
        fields: vec![
            field("id", CanonicalType::Int64, false),
            field("order_id", CanonicalType::Int64, false),
        ],
        primary_key: vec!["id".into()],
        foreign_keys: vec![ForeignKey {
            columns: vec!["order_id".into()],
            references_table: "orders".into(),
            references_columns: vec!["id".into()],
        }],
    };
    let item_rows = {
        let schema = Arc::new(arrow_schema::Schema::new(
            items
                .fields
                .iter()
                .map(|f| f.to_arrow())
                .collect::<Vec<_>>(),
        ));
        let id: ArrayRef = Arc::new(Int64Array::from(vec![10, 11, 12]));
        let order_id: ArrayRef = Arc::new(Int64Array::from(vec![1, 1, 2]));
        RecordBatch::try_new(schema, vec![id, order_id]).unwrap()
    };
    SinkFixture {
        schema: TargetSchema::new(vec![orders, items]).unwrap(),
        mode: WriteMode::Insert,
        partition: Partition {
            id: PartitionId("orders-0".into()),
            entity: "orders".into(),
            scope: PartitionScope::Whole,
        },
        batches: vec![
            ("orders".into(), orders_rows(&[1, 2])),
            ("order_items".into(), item_rows),
        ],
    }
}

#[tokio::test]
async fn mem_sink_passes_the_sink_contract_with_each_strategy() {
    for strategies in [
        vec![IdempotencyStrategy::TxnWithState],
        vec![IdempotencyStrategy::DeletePartitionThenInsert],
    ] {
        let sink = MemSink::new(strategies.clone());
        let v = check_sink(&sink, &sink, &sink_fixture()).await;
        assert_eq!(v, Vec::<String>::new(), "{strategies:?}");
    }
}

#[tokio::test]
async fn sink_contract_rejects_a_sink_with_no_safe_strategy_for_the_mode() {
    let sink = MemSink::new(vec![]);
    let v = check_sink(&sink, &sink, &sink_fixture()).await;
    assert_eq!(v.len(), 1);
    assert!(v[0].contains("no idempotency strategy"), "{v:?}");
}

/// Wraps [`MemSink`] and breaks one guarantee.
struct BrokenSink {
    inner: MemSink,
    flaw: Flaw,
    begun: std::sync::atomic::AtomicU64,
}

#[derive(Clone, Copy)]
enum Flaw {
    /// Ignores the lease and always commits as the newest epoch.
    NoFencing,
    /// Treats every transaction as a new partition, so re-runs append.
    AppendOnRerun,
    /// Commits on abort.
    AbortCommits,
}

#[async_trait]
impl Sink for BrokenSink {
    async fn prepare(&self, t: &TargetSchema, m: WriteMode) -> Result<()> {
        self.inner.prepare(t, m).await
    }

    async fn begin(
        &self,
        p: &Partition,
        lease: &Lease,
        s: IdempotencyStrategy,
    ) -> Result<Box<dyn SinkTxn>> {
        let n = self.begun.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let mut p = p.clone();
        let mut lease = lease.clone();
        match self.flaw {
            Flaw::NoFencing => lease.epoch = Epoch(u64::MAX),
            Flaw::AppendOnRerun => p.id = PartitionId(format!("{}-{n}", p.id.0)),
            Flaw::AbortCommits => {}
        }
        let inner = self.inner.begin(&p, &lease, s).await?;
        Ok(Box::new(BrokenTxn {
            inner,
            flaw: self.flaw,
        }))
    }

    async fn capabilities(&self) -> Result<SinkCaps> {
        self.inner.capabilities().await
    }
}

struct BrokenTxn {
    inner: Box<dyn SinkTxn>,
    flaw: Flaw,
}

#[async_trait]
impl SinkTxn for BrokenTxn {
    async fn write(&mut self, target: &str, batch: RecordBatch) -> Result<()> {
        self.inner.write(target, batch).await
    }

    async fn commit(self: Box<Self>) -> Result<WriteSummary> {
        self.inner.commit().await
    }

    async fn abort(self: Box<Self>) -> Result<()> {
        match self.flaw {
            Flaw::AbortCommits => self.inner.commit().await.map(|_| ()),
            _ => self.inner.abort().await,
        }
    }
}

async fn sink_violations(flaw: Flaw) -> Vec<String> {
    let inner = MemSink::new(vec![IdempotencyStrategy::TxnWithState]);
    let sink = BrokenSink {
        inner: inner.clone(),
        flaw,
        begun: Default::default(),
    };
    let v = check_sink(&sink, &inner, &sink_fixture()).await;
    assert!(!v.is_empty(), "the broken sink passed the contract");
    v
}

#[tokio::test]
async fn sink_contract_catches_a_sink_that_accepts_stale_leases() {
    let v = sink_violations(Flaw::NoFencing).await;
    assert_eq!(v, ["commit under a superseded lease was accepted"]);
}

#[tokio::test]
async fn sink_contract_catches_duplicates_on_rerun() {
    let v = sink_violations(Flaw::AppendOnRerun).await;
    assert!(
        v.iter().any(|m| m.contains("after re-running the partition, `orders` has 4 rows instead of 2")),
        "{v:#?}"
    );
}

#[tokio::test]
async fn sink_contract_catches_writes_that_survive_abort() {
    let v = sink_violations(Flaw::AbortCommits).await;
    assert!(
        v.iter()
            .any(|m| m.contains("after abort, `orders` has 2 rows instead of 0")),
        "{v:#?}"
    );
    assert!(
        v.iter()
            .any(|m| m.contains("after abort, `order_items` has 3 rows")),
        "{v:#?}"
    );
}

// ------------------------------------------------- MemSink's own behavior

async fn prepared_sink() -> (MemSink, SinkFixture) {
    let sink = MemSink::new(vec![IdempotencyStrategy::TxnWithState]);
    let f = sink_fixture();
    sink.prepare(&f.schema, f.mode).await.unwrap();
    (sink, f)
}

fn lease(epoch: u64) -> Lease {
    Lease {
        partition: PartitionId("orders-0".into()),
        epoch: Epoch(epoch),
    }
}

#[tokio::test]
async fn injected_write_failure_leaves_nothing_committed() {
    use dataferry_core::testkit::{MemSinkFaults, SinkProbe};
    let (sink, f) = prepared_sink().await;
    sink.inject(MemSinkFaults {
        fail_write_number: Some(2),
        ..Default::default()
    });
    let mut txn = sink
        .begin(&f.partition, &lease(1), IdempotencyStrategy::TxnWithState)
        .await
        .unwrap();
    txn.write("orders", f.batches[0].1.clone()).await.unwrap();
    assert!(
        txn.write("order_items", f.batches[1].1.clone())
            .await
            .is_err()
    );
    drop(txn);
    assert_eq!(
        sink.row_count("orders").await.unwrap(),
        0,
        "a dropped txn commits nothing"
    );
}

#[tokio::test]
async fn injected_commit_failure_applies_nothing_and_a_retry_succeeds() {
    use dataferry_core::testkit::{MemSinkFaults, SinkProbe};
    let (sink, f) = prepared_sink().await;
    sink.inject(MemSinkFaults {
        fail_next_commits: 1,
        ..Default::default()
    });
    for (epoch, should_fail) in [(1, true), (2, false)] {
        let mut txn = sink
            .begin(
                &f.partition,
                &lease(epoch),
                IdempotencyStrategy::TxnWithState,
            )
            .await
            .unwrap();
        txn.write("orders", f.batches[0].1.clone()).await.unwrap();
        assert_eq!(txn.commit().await.is_err(), should_fail, "epoch {epoch}");
    }
    assert_eq!(sink.row_count("orders").await.unwrap(), 2);
}

#[tokio::test]
async fn mem_sink_rejects_batches_that_do_not_match_the_target() {
    let (sink, f) = prepared_sink().await;
    let mut txn = sink
        .begin(&f.partition, &lease(1), IdempotencyStrategy::TxnWithState)
        .await
        .unwrap();

    let err = txn
        .write("customers", f.batches[0].1.clone())
        .await
        .unwrap_err();
    assert!(err.to_string().contains("not prepared"), "{err}");

    // order_items rows sent to orders: wrong columns.
    let err = txn
        .write("orders", f.batches[1].1.clone())
        .await
        .unwrap_err();
    assert!(err.to_string().contains("expects columns"), "{err}");

    let null_id: ArrayRef = Arc::new(Int64Array::from(vec![None, Some(2)]));
    let status: ArrayRef = Arc::new(StringArray::from(vec!["paid", "paid"]));
    let schema = Arc::new(arrow_schema::Schema::new(vec![
        arrow_schema::Field::new("id", arrow_schema::DataType::Int64, true),
        arrow_schema::Field::new("status", arrow_schema::DataType::Utf8, true),
    ]));
    let with_null = RecordBatch::try_new(schema, vec![null_id, status]).unwrap();
    let err = txn.write("orders", with_null).await.unwrap_err();
    assert!(err.to_string().contains("not nullable"), "{err}");
}

#[tokio::test]
async fn mem_sink_refuses_a_strategy_it_does_not_support() {
    let (sink, f) = prepared_sink().await;
    let result = sink
        .begin(&f.partition, &lease(1), IdempotencyStrategy::UpsertByKey)
        .await;
    assert!(result.is_err());
}
