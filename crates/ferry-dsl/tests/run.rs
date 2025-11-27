//! The golden flow, compiled from `examples/golden` (B4, B5), run by the
//! runner (A5) from an in-memory source into `MemSink`, partition by
//! partition, against `fixtures/golden/expected`.

mod common;

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{BooleanArray, RecordBatch};
use async_trait::async_trait;
use common::golden;
use common::plan::{expected, golden_flow, json_rows, sorted};
use ferry_core::testkit::MemSink;
use ferry_core::{
    BatchStream, Entity, IdempotencyStrategy, KeyRange, KeyValue, Partition, PartitionHint,
    PartitionId, PartitionScope, Result, Source, SourceCaps,
};
use ferry_dsl::compile::CompiledFlow;
use ferry_registry::state::NewRun;
use ferry_runtime::{Flow, Input, RunReport, Runner, Settings};
use ferry_state::FileStateStore;
use futures::StreamExt;
use serde_json::Value;

/// The golden documents, split into ranges of `_id` (text keys, which
/// `MemSource` keeps whole), so the flow's `partition_rows: 4` gives
/// several partitions.
struct Golden {
    entity: Entity,
    rows: RecordBatch,
}

fn key_of(batch: &RecordBatch, row: usize) -> KeyValue {
    KeyValue::from_array(batch.column_by_name("_id").unwrap().as_ref(), row).unwrap()
}

#[async_trait]
impl Source for Golden {
    async fn discover(&self) -> Result<Vec<Entity>> {
        Ok(vec![self.entity.clone()])
    }

    async fn plan_partitions(&self, e: &Entity, hint: &PartitionHint) -> Result<Vec<Partition>> {
        let mut keys: Vec<KeyValue> = (0..self.rows.num_rows())
            .map(|r| key_of(&self.rows, r))
            .collect();
        keys.sort();
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
                        "_id",
                        w[0].clone(),
                        w[1].clone(),
                    )?),
                })
            })
            .collect()
    }

    async fn read(&self, _: &Entity, partition: &Partition) -> Result<BatchStream> {
        let batch = match &partition.scope {
            PartitionScope::Whole => self.rows.clone(),
            PartitionScope::KeyRange(range) => {
                let mask: BooleanArray = (0..self.rows.num_rows())
                    .map(|r| Some(range.contains(&key_of(&self.rows, r))))
                    .collect();
                arrow_select::filter::filter_record_batch(&self.rows, &mask).unwrap()
            }
        };
        Ok(futures::stream::iter([Ok(batch)]).boxed())
    }

    async fn capabilities(&self) -> Result<SourceCaps> {
        Ok(SourceCaps {
            range_scan: true,
            predicate_pushdown: false,
            consistent_snapshot: true,
        })
    }
}

fn runtime_flow(compiled: &CompiledFlow, rows: RecordBatch, sink: &MemSink) -> Flow {
    let entity = compiled.plan.entity().clone();
    Flow {
        name: compiled.name.clone(),
        inputs: vec![Input {
            source: Arc::new(Golden {
                entity: entity.clone(),
                rows,
            }),
            entity,
            co_partition: None,
        }],
        plan: Arc::new(compiled.plan.clone()),
        sink: Arc::new(sink.clone()),
        target: compiled.target.clone(),
        mode: compiled.mode,
        depends_on: Vec::new(),
        finalizer: None,
    }
}

/// Runs the golden flow over `rows` to completion (or failure).
async fn run(rows: RecordBatch) -> (RunReport, MemSink) {
    let compiled = golden_flow();
    let sink = MemSink::new(vec![IdempotencyStrategy::DeletePartitionThenInsert]);
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(FileStateStore::new(dir.path()));
    let settings = Settings {
        // One per partition: all three are leased before any can fail, so
        // the first failure (which stops leasing) can't leave one pending.
        workers: 3,
        // The golden flow's `runtime: partition_rows` (B7 will pass it).
        partition_rows: 4,
        lease_ttl: Duration::from_secs(30),
        heartbeat: Duration::from_secs(5),
        poll: Duration::from_millis(20),
        ..Settings::default()
    };
    let runner = Runner::new(store, vec![runtime_flow(&compiled, rows, &sink)], settings).unwrap();
    let report = runner
        .start(NewRun {
            config_hash: "sha256:test".into(),
            engine_version: "0.0.1-test".into(),
            connections: vec![
                ("shop".into(), "mongo".into()),
                ("warehouse".into(), "postgres".into()),
            ],
        })
        .await
        .unwrap();
    (report, sink)
}

fn committed(sink: &MemSink, target: &str) -> Vec<Value> {
    sink.rows(target).iter().flat_map(json_rows).collect()
}

/// The golden documents without `…0006`.
fn without_the_dead_letter() -> RecordBatch {
    let batch = golden::batch();
    let docs = golden::documents();
    let keep: BooleanArray = docs
        .iter()
        .map(|d| Some(golden::id(d) != golden::DEAD_LETTERED))
        .collect();
    arrow_select::filter::filter_record_batch(&batch, &keep).unwrap()
}

#[tokio::test]
async fn the_runner_writes_exactly_the_expected_golden_rows() {
    let (report, sink) = run(without_the_dead_letter()).await;
    assert!(report.is_complete(), "{report:?}");
    assert_eq!(
        report.counts.done, 3,
        "nine documents in partitions of four"
    );
    assert_eq!(report.totals.read, 9);
    assert_eq!(report.totals.written.get("orders"), Some(&8));
    assert_eq!(report.totals.written.get("order_items"), Some(&7));
    assert_eq!(
        sorted(committed(&sink, "orders"), &["id"]),
        sorted(expected("orders"), &["id"])
    );
    assert_eq!(
        sorted(committed(&sink, "order_items"), &["order_id", "line_no"]),
        sorted(expected("order_items"), &["order_id", "line_no"])
    );
}

#[tokio::test]
async fn a_rejected_document_fails_only_its_partition_under_on_error_fail() {
    // The runner has no dead-letter sink yet (A7), so a rejected record
    // fails its partition (`on_error: fail`). The other partitions commit,
    // and nothing of the failed one is written: no orphan rows.
    let (report, sink) = run(golden::batch()).await;
    assert!(!report.is_complete());
    assert_eq!((report.counts.done, report.counts.failed), (2, 1));
    let [failure] = report.failures.as_slice() else {
        panic!("{report:?}")
    };
    assert!(
        failure.error.contains(golden::DEAD_LETTERED),
        "{}",
        failure.error
    );
    assert!(failure.error.contains("three"), "{}", failure.error);

    // `_id`s …05 to …08 share the failed partition with …06.
    let failed = [
        "650000000000000000000005",
        "650000000000000000000007",
        "650000000000000000000008",
    ];
    let keep = |rows: Vec<Value>, column: &str| -> Vec<Value> {
        rows.into_iter()
            .filter(|r| !failed.contains(&r[column].as_str().unwrap()))
            .collect()
    };
    assert_eq!(
        sorted(committed(&sink, "orders"), &["id"]),
        sorted(keep(expected("orders"), "id"), &["id"])
    );
    assert_eq!(
        sorted(committed(&sink, "order_items"), &["order_id", "line_no"]),
        sorted(
            keep(expected("order_items"), "order_id"),
            &["order_id", "line_no"]
        )
    );
}
