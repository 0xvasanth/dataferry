//! The contract suites are only useful if they catch broken connectors. Each
//! test here runs a suite against the in-memory fakes (which must pass) or
//! against a wrapper that breaks exactly one guarantee (which must be caught,
//! for the right reason).

use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use async_trait::async_trait;
use ferry_core::testkit::{MemSource, check_source};
use ferry_core::types::{CanonicalField, CanonicalType};
use ferry_core::{
    BatchStream, Entity, Partition, PartitionHint, PartitionId, PartitionScope, Result, Source,
    SourceCaps,
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

const HINT: PartitionHint = PartitionHint { target_rows: 3 };

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
