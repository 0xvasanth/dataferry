//! `on_error: dead_letter` and `max_errors` (A7) against `MemSink` and the
//! file state store: rejected records land in `_ferry_dlq` in their
//! partition's transaction and nowhere else, a re-run partition leaves one
//! dead letter per record and counts it once, `max_errors` stops the run
//! at the record that passes it (counted across executions), and records
//! the source couldn't read go to the queue without reaching the plan.
//!
//! The plans here reject records by hand; the Postgres sink and the golden
//! document …0006 are in ferry-verify's tests.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, Int64Array};
use async_trait::async_trait;
use common::*;
use ferry_core::testkit::{MemSink, MemSource};
use ferry_core::{
    BatchStream, Entity, IdentityPlan, Partition, PartitionHint, Result, Source, SourceCaps,
};
use ferry_registry::rejected::rejected_batch;
use ferry_runtime::{DLQ_TABLE, OnError, Runner, Settings};
use ferry_state::FileStateStore;
use futures::{FutureExt, StreamExt, stream};

/// `(source_id, target, error, record)` of every dead letter, sorted.
fn dead_letters(sink: &MemSink) -> Vec<(String, Option<String>, String, Option<String>)> {
    let mut rows = Vec::new();
    for b in sink.rows(DLQ_TABLE) {
        let text = |name: &str| b.column_by_name(name).unwrap().as_string::<i32>().clone();
        let (flow, id, target, error, record) = (
            text("flow"),
            text("source_id"),
            text("target"),
            text("error"),
            text("record"),
        );
        for i in 0..b.num_rows() {
            assert_eq!(flow.value(i), "orders");
            let opt = |a: &arrow_array::StringArray| a.is_valid(i).then(|| a.value(i).to_owned());
            rows.push((
                id.value(i).to_owned(),
                opt(&target),
                error.value(i).to_owned(),
                opt(&record),
            ));
        }
    }
    rows.sort();
    rows
}

fn dead_letter_settings(max_errors: Option<u64>) -> Settings {
    Settings {
        workers: 1,
        on_error: OnError::DeadLetter,
        max_errors,
        ..settings()
    }
}

fn rejecting_flow(sink: &MemSink, n: i64, bad: Vec<i64>) -> ferry_runtime::Flow {
    let mut flow = identity_flow(
        "orders",
        orders_source(n, 25),
        "orders",
        Arc::new(sink.clone()),
    );
    flow.plan = Arc::new(RejectingPlan {
        inner: IdentityPlan::new(orders_entity(), "orders"),
        bad,
    });
    flow
}

#[tokio::test]
async fn rejected_records_go_to_the_queue_with_their_raw_record_and_nothing_else_lands() {
    let dir = tempfile::tempdir().unwrap();
    let sink = mem_sink();
    let flow = rejecting_flow(&sink, 200, vec![7, 150]);
    let store = Arc::new(FileStateStore::new(dir.path()));
    let runner = Runner::new(store, vec![flow], dead_letter_settings(None)).unwrap();
    let report = runner.start(new_run()).await.unwrap();

    assert!(report.is_complete(), "{report:?}");
    let error = "qty is not a number".to_owned();
    assert_eq!(
        dead_letters(&sink),
        vec![
            (
                "[150]".into(),
                None,
                error.clone(),
                Some(r#"{"id":150,"name":"order-150"}"#.into())
            ),
            (
                "[7]".into(),
                None,
                error,
                Some(r#"{"id":7,"name":"order-7"}"#.into())
            ),
        ]
    );
    let expected: Vec<i64> = (0..200).filter(|i| ![7, 150].contains(i)).collect();
    assert_eq!(ids(&sink, "orders"), expected);
    assert_eq!(report.totals.read, 200);
    assert_eq!(report.totals.dead_lettered, 2);
    assert_eq!(report.totals.written[DLQ_TABLE], 2);
}

#[tokio::test]
async fn under_on_error_fail_no_dead_letter_table_is_prepared() {
    let dir = tempfile::tempdir().unwrap();
    let sink = mem_sink();
    let flow = rejecting_flow(&sink, 200, vec![150]);
    let store = Arc::new(FileStateStore::new(dir.path()));
    let runner = Runner::new(store, vec![flow], settings()).unwrap();
    let report = runner.start(new_run()).await.unwrap();
    assert_eq!(report.counts.failed, 1);
    assert!(sink.rows(DLQ_TABLE).is_empty());
    assert_eq!(report.totals.dead_lettered, 0);
}

#[tokio::test]
async fn a_partition_re_run_after_its_commit_keeps_one_dead_letter_and_counts_it_once() {
    let dir = tempfile::tempdir().unwrap();
    let log = Log::default();
    let sink = mem_sink();
    let flow = || rejecting_flow(&sink, 200, vec![150]);
    // The partition with the bad record commits, then the process dies
    // before it is recorded done: it runs again on resume. One dead letter
    // fits max_errors only if the re-run doesn't count it twice.
    let settings = dead_letter_settings(Some(1));
    let spy = SpyStore::file(Arc::new(FileStateStore::new(dir.path())), log.clone());
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = Arc::clone(&calls);
    spy.on_mark_done(move |lease| {
        let stuck = lease.partition.0 == "orders/orders-1";
        seen.fetch_add(stuck as usize, Ordering::SeqCst);
        async move {
            if stuck {
                std::future::pending::<()>().await;
            }
        }
        .boxed()
    });
    let runner = Runner::new(Arc::new(spy.clone()), vec![flow()], settings.clone()).unwrap();
    let run = runner.create(new_run()).await.unwrap();
    let crashed = async {
        while calls.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    };
    tokio::select! {
        ended = runner.execute(&run) => panic!("the run should hang in mark_done: {ended:?}"),
        () = crashed => {}
    }
    drop(runner);
    spy.crash().await;
    assert_eq!(dead_letters(&sink).len(), 1, "committed before the crash");

    // The dead partition's lease expires and the partition runs again.
    tokio::time::sleep(settings.lease_ttl).await;
    let store = Arc::new(FileStateStore::new(dir.path()));
    let runner = Runner::new(store, vec![flow()], settings).unwrap();
    let report = runner.resume(&run, HASH, ENGINE).await.unwrap();
    assert!(report.is_complete(), "{report:?}");
    assert_eq!(dead_letters(&sink).len(), 1);
    assert_eq!(dead_letters(&sink)[0].0, "[150]");
    assert_eq!(report.totals.dead_lettered, 1);
}

#[tokio::test]
async fn max_errors_stops_the_run_at_the_record_that_passes_it_counting_earlier_executions() {
    let dir = tempfile::tempdir().unwrap();
    let sink = mem_sink();
    // Four bad records, one in each of the first four partitions.
    let flow = || rejecting_flow(&sink, 1000, vec![10, 110, 210, 310]);
    let runner = |max| {
        Runner::new(
            Arc::new(FileStateStore::new(dir.path())),
            vec![flow()],
            dead_letter_settings(Some(max)),
        )
        .unwrap()
    };
    let report = runner(3).start(new_run()).await.unwrap();
    assert_eq!(report.failures.len(), 1, "{report:?}");
    let failure = &report.failures[0];
    assert_eq!(failure.partition.0, "orders/orders-3");
    assert!(
        failure.error.contains("max_errors exceeded")
            && failure
                .error
                .contains("4 dead-lettered records, more than max_errors (3)"),
        "{}",
        failure.error
    );
    // The failed partition rolled back: three dead letters, none of 310.
    assert_eq!(
        dead_letters(&sink)
            .into_iter()
            .map(|d| d.0)
            .collect::<Vec<_>>(),
        vec!["[10]", "[110]", "[210]"]
    );
    assert_eq!(report.totals.dead_lettered, 3);
    assert_eq!(report.counts.done, 3);

    // A retry under the same budget counts the three already done, from
    // the state store, and stops again.
    let report = runner(3)
        .retry_failed(&report.run, HASH, ENGINE)
        .await
        .unwrap();
    assert_eq!(report.counts.failed, 1, "{report:?}");
    assert!(report.failures[0].error.contains("max_errors (3)"));

    // With room for the fourth, the run finishes.
    let report = runner(4)
        .retry_failed(&report.run, HASH, ENGINE)
        .await
        .unwrap();
    assert!(report.is_complete(), "{report:?}");
    assert_eq!(report.totals.dead_lettered, 4);
    assert_eq!(dead_letters(&sink).len(), 4);
}

#[tokio::test]
async fn max_errors_stops_a_partition_as_soon_as_it_passes_the_budget() {
    let dir = tempfile::tempdir().unwrap();
    let sink = mem_sink();
    // All four in one partition, of a batch each.
    let mut flow = rejecting_flow(&sink, 100, vec![1, 2, 3, 4]);
    flow.inputs[0].source = orders_source(100, 1);
    let store = Arc::new(FileStateStore::new(dir.path()));
    let runner = Runner::new(store, vec![flow], dead_letter_settings(Some(3))).unwrap();
    let report = runner.start(new_run()).await.unwrap();
    assert_eq!(report.counts.failed, 1);
    assert!(
        report.failures[0]
            .error
            .contains("4 dead-lettered records, more than max_errors (3)"),
        "{}",
        report.failures[0].error
    );
    assert!(dead_letters(&sink).is_empty());
    assert!(ids(&sink, "orders").is_empty());
}

/// `MemSource` orders, plus a rejected batch in the first partition: the
/// records the source couldn't read as `orders`.
struct Unreadable {
    inner: Arc<MemSource>,
    ids: Vec<i64>,
}

#[async_trait]
impl Source for Unreadable {
    async fn discover(&self) -> Result<Vec<Entity>> {
        self.inner.discover().await
    }

    async fn plan_partitions(&self, e: &Entity, hint: &PartitionHint) -> Result<Vec<Partition>> {
        self.inner.plan_partitions(e, hint).await
    }

    async fn read(&self, e: &Entity, p: &Partition) -> Result<BatchStream> {
        let rows = self.inner.read(e, p).await?;
        if p.id.0 != "orders-0" {
            return Ok(rows);
        }
        let id: ArrayRef = Arc::new(Int64Array::from(self.ids.clone()));
        let rejected = rejected_batch(
            e,
            vec![id],
            self.ids
                .iter()
                .map(|_| "`name` holds Int32".into())
                .collect(),
            self.ids
                .iter()
                .map(|id| Some(format!(r#"{{"id":{id},"name":5}}"#)))
                .collect(),
        );
        Ok(rows.chain(stream::iter([rejected])).boxed())
    }

    async fn capabilities(&self) -> Result<SourceCaps> {
        self.inner.capabilities().await
    }
}

#[tokio::test]
async fn a_record_the_source_could_not_read_is_dead_lettered_without_reaching_the_plan() {
    let dir = tempfile::tempdir().unwrap();
    let sink = mem_sink();
    let source = Arc::new(Unreadable {
        inner: orders_source(200, 25),
        ids: vec![5000],
    });
    let flow = identity_flow("orders", source.clone(), "orders", Arc::new(sink.clone()));
    let store = Arc::new(FileStateStore::new(dir.path()));
    let runner = Runner::new(store, vec![flow], dead_letter_settings(Some(1))).unwrap();
    let report = runner.start(new_run()).await.unwrap();

    assert!(report.is_complete(), "{report:?}");
    assert_eq!(
        dead_letters(&sink),
        vec![(
            "[5000]".into(),
            None,
            "`name` holds Int32".into(),
            Some(r#"{"id":5000,"name":5}"#.into())
        )]
    );
    assert_eq!(ids(&sink, "orders"), (0..200).collect::<Vec<_>>());
    // It was read, and it is accounted for as dead-lettered.
    assert_eq!(report.totals.read, 201);
    assert_eq!(report.totals.dead_lettered, 1);

    // Under on_error: fail it fails its partition, naming the record.
    let dir = tempfile::tempdir().unwrap();
    let sink = mem_sink();
    let flow = identity_flow("orders", source, "orders", Arc::new(sink.clone()));
    let store = Arc::new(FileStateStore::new(dir.path()));
    let runner = Runner::new(store, vec![flow], settings()).unwrap();
    let report = runner.start(new_run()).await.unwrap();
    assert_eq!(report.failures.len(), 1);
    assert!(
        report.failures[0].error.contains("[Int(5000)]")
            && report.failures[0].error.contains("`name` holds Int32"),
        "{}",
        report.failures[0].error
    );
}

#[tokio::test]
async fn the_dead_letter_table_name_is_reserved() {
    let sink = mem_sink();
    let flow = identity_flow(
        "orders",
        orders_source(10, 5),
        DLQ_TABLE,
        Arc::new(sink.clone()),
    );
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(FileStateStore::new(dir.path()));
    let err = Runner::new(store, vec![flow], settings())
        .err()
        .expect("refused");
    assert!(err.to_string().contains("dead-letter table"), "{err}");
}

#[tokio::test]
async fn an_empty_rejected_batch_rejects_nothing() {
    let source = Arc::new(Unreadable {
        inner: orders_source(200, 25),
        ids: Vec::new(),
    });
    for on_error in [OnError::Fail, OnError::DeadLetter] {
        let dir = tempfile::tempdir().unwrap();
        let sink = mem_sink();
        let flow = identity_flow("orders", source.clone(), "orders", Arc::new(sink.clone()));
        let store = Arc::new(FileStateStore::new(dir.path()));
        let settings = Settings {
            on_error,
            ..settings()
        };
        let report = Runner::new(store, vec![flow], settings)
            .unwrap()
            .start(new_run())
            .await
            .unwrap();
        assert!(report.is_complete(), "{on_error:?}: {report:?}");
        assert_eq!(report.totals.dead_lettered, 0);
        assert_eq!(ids(&sink, "orders"), (0..200).collect::<Vec<_>>());
    }
}
