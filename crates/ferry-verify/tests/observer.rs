//! The checksum observer's record of one partition: driver records by
//! what became of them, rows and checksums per target, and the target's
//! timestamp precision.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, TimestampMicrosecondArray};
use async_trait::async_trait;
use ferry_core::testkit::{MemSink, MemSource};
use ferry_core::types::{CanonicalField, CanonicalType};
use ferry_core::{
    BatchStream, Entity, Epoch, ForeignKey, IdentityPlan, KeyValue, Lease, PartitionId,
    RejectedRecord, Result, SourceRecordId, TargetBatch, TargetSchema, TargetTable,
    TransformOutput, WriteMode,
};
use ferry_registry::state::{PartitionRows, RunId};
use ferry_runtime::{Flow, Input, WriteObserver};
use ferry_verify::{
    Aggregate, Checksum, ChecksumObserver, ChecksumSpec, MemoryWriteLog, RecordCounts, Scalar,
    Target, WriteLog,
};

/// A target whose `orders.placed_at` keeps milliseconds. Nothing else is
/// asked of it here.
struct Millis;

#[async_trait]
impl Target for Millis {
    fn describe(&self) -> String {
        "test target".into()
    }
    async fn timestamp_digits(&self, table: &TargetTable) -> Result<BTreeMap<String, u8>> {
        Ok(match table.name.as_str() {
            "orders" => BTreeMap::from([("placed_at".to_string(), 3)]),
            _ => BTreeMap::new(),
        })
    }
    async fn read_back(&self, _: &TargetTable) -> Result<BatchStream> {
        unreachable!()
    }
    async fn null_count(&self, _: &str, _: &str) -> Result<u64> {
        unreachable!()
    }
    async fn duplicate_keys(&self, _: &str, _: &[String]) -> Result<u64> {
        unreachable!()
    }
    async fn orphans(&self, _: &str, _: &ForeignKey) -> Result<u64> {
        unreachable!()
    }
    async fn aggregate(&self, _: &str, _: Aggregate, _: Option<&str>) -> Result<Scalar> {
        unreachable!()
    }
}

fn orders_table() -> TargetTable {
    TargetTable {
        name: "orders".into(),
        fields: vec![
            CanonicalField::new("id", CanonicalType::Int64, false),
            CanonicalField::new(
                "placed_at",
                CanonicalType::Timestamp {
                    tz: Some("UTC".into()),
                },
                true,
            ),
        ],
        primary_key: vec!["id".into()],
        foreign_keys: Vec::new(),
    }
}

fn items_table() -> TargetTable {
    TargetTable {
        name: "order_items".into(),
        fields: vec![CanonicalField::new("order_id", CanonicalType::Int64, false)],
        primary_key: Vec::new(),
        foreign_keys: Vec::new(),
    }
}

/// A flow with two targets. Only its name and targets matter here.
fn flow() -> Flow {
    let table = orders_table();
    let entity = Entity::new("orders", table.fields.clone(), vec!["id".into()]).unwrap();
    Flow {
        name: "orders".into(),
        inputs: vec![Input {
            source: Arc::new(MemSource::new()),
            entity: entity.clone(),
            co_partition: None,
        }],
        plan: Arc::new(IdentityPlan::new(entity, "orders")),
        sink: Arc::new(MemSink::new(Vec::new())),
        target: TargetSchema::new(vec![table, items_table()]).unwrap(),
        mode: WriteMode::Insert,
        depends_on: Vec::new(),
    }
}

fn orders(ids: Vec<i64>, micros: Vec<i64>) -> RecordBatch {
    let id: ArrayRef = Arc::new(Int64Array::from(ids));
    let at: ArrayRef = Arc::new(TimestampMicrosecondArray::from(micros).with_timezone("UTC"));
    RecordBatch::try_from_iter([("id", id), ("placed_at", at)]).unwrap()
}

fn items(ids: Vec<i64>) -> RecordBatch {
    let id: ArrayRef = Arc::new(Int64Array::from(ids));
    RecordBatch::try_from_iter([("order_id", id)]).unwrap()
}

fn lease(epoch: u64) -> Lease {
    Lease {
        partition: PartitionId("orders/orders-0".into()),
        epoch: Epoch(epoch),
    }
}

#[tokio::test]
async fn records_what_became_of_every_driver_record_and_checksums_per_target() {
    let log = Arc::new(MemoryWriteLog::new());
    let observer = ChecksumObserver::new(log.clone(), Arc::new(Millis));
    let run = RunId("run".into());
    observer.prepared(&run, &flow()).await.unwrap();

    let mut attempt = observer.begin(&run, "orders", &lease(4));
    // Record 1 fans out to two items, 2 has an order and no item, 3 is
    // rejected, 4 is filtered (no rows anywhere).
    let record = |id| SourceRecordId(vec![KeyValue::Int(id)]);
    attempt
        .output(&TransformOutput {
            records: vec![record(1), record(2), record(3), record(4)],
            targets: vec![
                TargetBatch {
                    target: "orders".into(),
                    batch: orders(vec![1, 2], vec![1_000_123_456, 2_000_000_000]),
                    source_index: vec![0, 1],
                },
                TargetBatch {
                    target: "order_items".into(),
                    batch: items(vec![1, 1]),
                    source_index: vec![0, 0],
                },
            ],
            rejected: vec![RejectedRecord {
                index: 2,
                target: Some("order_items".into()),
                error: "qty is not a number".into(),
            }],
        })
        .unwrap();
    // A second output: one more written record.
    attempt
        .output(&TransformOutput {
            records: vec![record(5)],
            targets: vec![TargetBatch {
                target: "orders".into(),
                batch: orders(vec![5], vec![5_000_000_000]),
                source_index: vec![0],
            }],
            rejected: Vec::new(),
        })
        .unwrap();
    attempt
        .finish(&PartitionRows {
            read: 5,
            dead_lettered: 1,
        })
        .await
        .unwrap();

    let records = log.load(&run).await.unwrap();
    assert_eq!(records.len(), 1);
    let r = &records[0];
    assert_eq!(
        (r.flow.as_str(), r.partition.as_str(), r.epoch),
        ("orders", "orders/orders-0", 4)
    );
    assert_eq!(r.read, 5);
    assert_eq!(
        r.records,
        RecordCounts {
            written: 3,
            filtered: 1,
            rejected: 1
        }
    );
    // Rows per target, hashed with the target's precision.
    let spec = ChecksumSpec::new(&orders_table())
        .with_timestamp_digits("placed_at", 3)
        .unwrap();
    let mut expected = Checksum::default();
    expected
        .add_batch(
            &spec,
            &orders(
                vec![5, 2, 1],
                vec![5_000_000_000, 2_000_000_000, 1_000_123_456],
            ),
        )
        .unwrap();
    let orders_write = &r.targets["orders"];
    assert_eq!(orders_write.checksum, expected);
    assert_eq!(
        orders_write.timestamp_digits,
        BTreeMap::from([("placed_at".to_string(), 3)])
    );
    assert_eq!(r.targets["order_items"].checksum.rows, 2);
    assert!(r.targets["order_items"].timestamp_digits.is_empty());
}

#[tokio::test]
async fn every_target_gets_a_record_even_with_no_rows() {
    let log = Arc::new(MemoryWriteLog::new());
    let observer = ChecksumObserver::new(log.clone(), Arc::new(Millis));
    let run = RunId("run".into());
    observer.prepared(&run, &flow()).await.unwrap();
    let attempt = observer.begin(&run, "orders", &lease(1));
    attempt.finish(&PartitionRows::default()).await.unwrap();
    let r = &log.load(&run).await.unwrap()[0];
    assert_eq!(r.targets.len(), 2);
    assert!(
        r.targets
            .values()
            .all(|t| t.checksum == Checksum::default())
    );
}

/// An output that skipped `validate`: a record rejected for one target
/// with rows in another, and the same rejection listed twice. Each record
/// is still counted once (regression: counts were set sizes, so such a
/// record counted as written and rejected, and the filtered count, a
/// subtraction, could underflow).
#[tokio::test]
async fn an_unvalidated_output_counts_each_record_once() {
    let log = Arc::new(MemoryWriteLog::new());
    let observer = ChecksumObserver::new(log.clone(), Arc::new(Millis));
    let run = RunId("run".into());
    observer.prepared(&run, &flow()).await.unwrap();
    let mut attempt = observer.begin(&run, "orders", &lease(1));
    let rejected = |target: &str| RejectedRecord {
        index: 0,
        target: Some(target.into()),
        error: "bad".into(),
    };
    attempt
        .output(&TransformOutput {
            records: vec![
                SourceRecordId(vec![KeyValue::Int(1)]),
                SourceRecordId(vec![KeyValue::Int(2)]),
            ],
            targets: vec![TargetBatch {
                target: "order_items".into(),
                batch: items(vec![1]),
                source_index: vec![0],
            }],
            rejected: vec![rejected("orders"), rejected("order_items")],
        })
        .unwrap();
    attempt.finish(&PartitionRows::default()).await.unwrap();
    assert_eq!(
        log.load(&run).await.unwrap()[0].records,
        RecordCounts {
            written: 0,
            filtered: 1,
            rejected: 1
        }
    );
}

#[tokio::test]
async fn refuses_unprepared_flows_unknown_targets_and_stale_attempts() {
    let log = Arc::new(MemoryWriteLog::new());
    let observer = ChecksumObserver::new(log.clone(), Arc::new(Millis));
    let run = RunId("run".into());
    let output = |target: &str| TransformOutput {
        records: vec![SourceRecordId(vec![KeyValue::Int(1)])],
        targets: vec![TargetBatch {
            target: target.into(),
            batch: items(vec![1]),
            source_index: vec![0],
        }],
        rejected: Vec::new(),
    };

    let mut attempt = observer.begin(&run, "orders", &lease(1));
    let err = attempt.output(&output("order_items")).unwrap_err();
    assert!(err.to_string().contains("wasn't prepared"), "{err}");

    observer.prepared(&run, &flow()).await.unwrap();
    let mut attempt = observer.begin(&run, "orders", &lease(1));
    let err = attempt.output(&output("refunds")).unwrap_err();
    assert!(
        err.to_string().contains("isn't one of its targets"),
        "{err}"
    );

    // A later attempt recorded first; the earlier one is refused.
    observer
        .begin(&run, "orders", &lease(3))
        .finish(&PartitionRows::default())
        .await
        .unwrap();
    let err = observer
        .begin(&run, "orders", &lease(2))
        .finish(&PartitionRows::default())
        .await
        .unwrap_err();
    assert!(
        matches!(err, ferry_core::FerryError::StaleLease { .. }),
        "{err}"
    );
}
