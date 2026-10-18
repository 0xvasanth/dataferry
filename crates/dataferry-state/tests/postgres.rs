//! `PgStateBackend` against a real Postgres (testcontainers): the shared
//! suite, and the transaction it shares with a sink on the same pool
//! (0004 §8).

mod common;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use common::{
    Fault, Probe, Server, TestPg, connection, partition, pool, row_counts, rows, schema,
    sink_fixture,
};
use dataferry_core::{
    FerryError, IdempotencyStrategy, Lease, Result, Sink, WriteMode, WriteSummary,
};
use dataferry_registry::state::{NewRun, PartitionCounts, RunId};
use dataferry_registry::testkit::{ConnectorFixture, check_state_capable};
use dataferry_registry::{ConnectorRegistry, Roles, StateStore};
use dataferry_state::testkit::check_state_store;
use dataferry_state::{PgStateBackend, PgStateStore, resume};

const LONG: Duration = Duration::from_secs(600);
const HASH: &str = "sha256:postgres-tests";
const ENGINE: &str = "0.0.1";

async fn new_run(store: &dyn StateStore, n: usize) -> RunId {
    let run = store
        .create_run(NewRun {
            config_hash: HASH.into(),
            engine_version: ENGINE.into(),
            connections: vec![("warehouse".into(), "testpg".into())],
        })
        .await
        .unwrap();
    let parts: Vec<_> = (0..n).map(partition).collect();
    store
        .store_partitions(&run, "orders", &parts)
        .await
        .unwrap();
    run
}

/// Writes the rows of partition `i` (ids `i*10 .. i*10+n`) under `lease`
/// and commits.
async fn write(
    sink: &dyn Sink,
    i: usize,
    lease: &Lease,
    strategy: IdempotencyStrategy,
    n: i64,
) -> Result<WriteSummary> {
    let base = i as i64 * 10;
    let mut txn = sink.begin(&partition(i), lease, strategy).await?;
    txn.write("orders", rows(base..base + n)).await?;
    txn.commit().await
}

/// The state connection, built through the registry the way the CLI does:
/// one pool for the sink and the store.
struct Warehouse {
    connector: TestPg,
    registry: ConnectorRegistry,
    sink: Arc<dyn Sink>,
    store: Arc<dyn StateStore>,
}

impl Warehouse {
    async fn new(url: &str) -> Self {
        let connector = TestPg::default();
        let mut registry = ConnectorRegistry::new();
        registry.register_state_capable(connector.clone(), PgStateBackend);
        let (built, store) = registry
            .build_with_state(&connection("warehouse", url), Roles::SINK)
            .unwrap();
        let sink = built.endpoints.sink.unwrap();
        sink.prepare(&schema(), WriteMode::Insert).await.unwrap();
        Self {
            connector,
            registry,
            sink,
            store,
        }
    }
}

#[tokio::test]
async fn the_postgres_store_passes_the_state_store_suite() {
    let server = Server::start().await;
    let pool = pool(&server.database("suite").await);
    // Each handle is a fresh store on the shared pool, as a second worker
    // process would have.
    let violations = check_state_store(&|| Arc::new(PgStateStore::new(pool.clone()))).await;
    assert!(violations.is_empty(), "{violations:#?}");
}

#[tokio::test]
async fn the_backend_passes_the_registry_state_capable_suite() {
    let server = Server::start().await;
    let url = server.database("contract").await;
    let fixture = ConnectorFixture {
        options: serde_json::json!({ "url": url }),
        secret: "postgres:postgres".into(),
    };
    let probe = Probe { pool: pool(&url) };
    let violations = check_state_capable(
        TestPg::default(),
        PgStateBackend,
        &fixture,
        &sink_fixture(),
        &probe,
    )
    .await;
    assert!(violations.is_empty(), "{violations:#?}");
}

#[tokio::test]
async fn the_rows_and_the_done_record_commit_in_the_sinks_transaction() {
    let server = Server::start().await;
    let url = server.database("warehouse").await;
    let w = Warehouse::new(&url).await;
    let run = new_run(w.store.as_ref(), 1).await;
    let caps = w.sink.capabilities().await.unwrap();
    assert_eq!(
        caps.strategy_for(WriteMode::Insert),
        Some(IdempotencyStrategy::TxnWithState)
    );

    let lease = w
        .store
        .lease_next(&run, "orders", LONG)
        .await
        .unwrap()
        .unwrap();
    let mut txn = w
        .sink
        .begin(&partition(0), &lease, IdempotencyStrategy::TxnWithState)
        .await
        .unwrap();
    txn.write("orders", rows(0..4)).await.unwrap();

    // While the sink's transaction is open, its rows aren't visible, and
    // the partition can't be leased to anyone else.
    let probe = pool(&url);
    assert_eq!(row_counts(&probe, "orders").await, (0, 0));
    assert!(!w.store.is_done(&run, &lease.partition).await.unwrap());

    let summary = txn.commit().await.unwrap();
    assert_eq!(summary.rows_written["orders"], 4);
    assert_eq!(row_counts(&probe, "orders").await, (4, 4));
    assert!(w.store.is_done(&run, &lease.partition).await.unwrap());
    assert_eq!(w.store.totals(&run).await.unwrap().written["orders"], 4);
}

#[tokio::test]
async fn a_crash_after_record_done_before_commit_reruns_on_resume_with_each_row_once() {
    let server = Server::start().await;
    let url = server.database("warehouse").await;
    let w = Warehouse::new(&url).await;
    let run = new_run(w.store.as_ref(), 2).await;
    let probe = pool(&url);

    // The worker's lease would expire while it is dead; ZERO stands in.
    let lease = w
        .store
        .lease_next(&run, "orders", Duration::ZERO)
        .await
        .unwrap()
        .unwrap();
    w.connector.set_fault(Fault::CrashBeforeCommit);
    let err = write(
        w.sink.as_ref(),
        0,
        &lease,
        IdempotencyStrategy::TxnWithState,
        5,
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("simulated crash"), "{err}");

    // Neither the rows nor the done-record survived.
    assert_eq!(row_counts(&probe, "orders").await, (0, 0));
    let client = server.client("warehouse").await;
    let status: String = client
        .query_one(
            "SELECT status FROM _ferry_partitions WHERE run_id = $1 AND partition_id = $2",
            &[&run.0, &lease.partition.0],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(status, "leased");

    // Resume, the way a restarted process does: a new connection to the
    // state store, the run checked against its pin, the plan read back.
    w.connector.set_fault(Fault::None);
    let restarted = Warehouse::new(&url).await;
    resume(restarted.store.as_ref(), &run, HASH, ENGINE)
        .await
        .unwrap();
    let plan = restarted
        .store
        .load_partitions(&run, "orders")
        .await
        .unwrap();
    assert_eq!(plan, vec![partition(0), partition(1)]);

    let mut seen = Vec::new();
    while let Some(next) = restarted
        .store
        .lease_next(&run, "orders", LONG)
        .await
        .unwrap()
    {
        let i = plan.iter().position(|p| p.id == next.partition).unwrap();
        if i == 0 {
            assert!(next.epoch > lease.epoch);
        }
        write(
            restarted.sink.as_ref(),
            i,
            &next,
            IdempotencyStrategy::TxnWithState,
            5,
        )
        .await
        .unwrap();
        seen.push(i);
    }
    assert_eq!(seen, [0, 1], "the crashed partition re-ran, then the other");
    assert_eq!(row_counts(&probe, "orders").await, (10, 10));
    assert_eq!(
        restarted.store.counts(&run).await.unwrap(),
        PartitionCounts {
            done: 2,
            ..Default::default()
        }
    );
}

#[tokio::test]
async fn a_commit_under_a_superseded_lease_fails_with_stale_lease_and_leaves_no_rows() {
    let server = Server::start().await;
    let url = server.database("warehouse").await;
    let w = Warehouse::new(&url).await;
    let run = new_run(w.store.as_ref(), 1).await;
    let probe = pool(&url);

    // Worker A stalls past its lease; worker B, its own process with its
    // own pool, takes the partition over.
    let a = w
        .store
        .lease_next(&run, "orders", Duration::ZERO)
        .await
        .unwrap()
        .unwrap();
    let b_side = Warehouse::new(&url).await;
    let b = b_side
        .store
        .lease_next(&run, "orders", LONG)
        .await
        .unwrap()
        .unwrap();
    assert!(b.epoch > a.epoch);

    let err = write(w.sink.as_ref(), 0, &a, IdempotencyStrategy::TxnWithState, 3)
        .await
        .unwrap_err();
    assert_eq!(
        err,
        FerryError::StaleLease {
            partition: a.partition.0.clone(),
            epoch: a.epoch.0,
            current: b.epoch.0,
        }
    );
    assert_eq!(row_counts(&probe, "orders").await, (0, 0));
    assert!(!w.store.is_done(&run, &a.partition).await.unwrap());

    write(
        b_side.sink.as_ref(),
        0,
        &b,
        IdempotencyStrategy::TxnWithState,
        3,
    )
    .await
    .unwrap();
    assert_eq!(row_counts(&probe, "orders").await, (3, 3));
    assert!(w.store.is_done(&run, &a.partition).await.unwrap());
}

#[tokio::test]
async fn a_lease_can_not_be_taken_over_while_its_commit_is_in_flight() {
    let server = Server::start().await;
    let url = server.database("warehouse").await;
    let w = Warehouse::new(&url).await;
    let run = new_run(w.store.as_ref(), 1).await;

    // A's lease has expired, but A got to record_done first: the row lock
    // record_done holds inside A's transaction makes B's lease_next skip
    // the partition instead of re-leasing it under A's commit.
    let a = w
        .store
        .lease_next(&run, "orders", Duration::ZERO)
        .await
        .unwrap()
        .unwrap();
    let (_, ledger) = dataferry_registry::StateBackend::open(&PgStateBackend, pool(&url));
    let summary = WriteSummary::default();
    // The sink's open transaction, paused between record_done and COMMIT.
    let mut sink_conn = server.client("warehouse").await;
    sink_conn.batch_execute("BEGIN").await.unwrap();
    ledger
        .record_done(&mut sink_conn, &a, &summary)
        .await
        .unwrap();

    let b = w.store.lease_next(&run, "orders", LONG).await.unwrap();
    assert_eq!(
        b, None,
        "the partition was re-leased under an in-flight commit"
    );

    sink_conn.batch_execute("COMMIT").await.unwrap();
    assert!(w.store.is_done(&run, &a.partition).await.unwrap());
    assert_eq!(
        w.store.lease_next(&run, "orders", LONG).await.unwrap(),
        None
    );
}

#[tokio::test]
async fn concurrent_workers_lease_each_partition_exactly_once() {
    let server = Server::start().await;
    let url = server.database("warehouse").await;
    let store: Arc<dyn StateStore> = Arc::new(PgStateStore::new(pool(&url)));
    let run = new_run(store.as_ref(), 40).await;

    let workers: Vec<_> = (0..8)
        .map(|_| {
            // A store per worker, all on the same database.
            let store = PgStateStore::new(pool(&url));
            let run = run.clone();
            tokio::spawn(async move {
                let mut got = Vec::new();
                while let Some(lease) = store.lease_next(&run, "orders", LONG).await.unwrap() {
                    got.push(lease);
                }
                got
            })
        })
        .collect();
    let mut leases = Vec::new();
    for w in workers {
        leases.extend(w.await.unwrap());
    }
    let ids: BTreeSet<_> = leases.iter().map(|l| l.partition.clone()).collect();
    let epochs: BTreeSet<_> = leases.iter().map(|l| l.epoch).collect();
    assert_eq!(leases.len(), 40, "every partition leased exactly once");
    assert_eq!(ids.len(), 40);
    assert_eq!(epochs.len(), 40, "every lease has its own epoch");
    assert_eq!(store.counts(&run).await.unwrap().leased, 40);
}

#[tokio::test]
async fn concurrent_first_use_creates_the_tables_once() {
    let server = Server::start().await;
    let url = server.database("fresh").await;
    let tasks: Vec<_> = (0..6)
        .map(|_| {
            let store = PgStateStore::new(pool(&url));
            tokio::spawn(async move {
                store
                    .create_run(NewRun {
                        config_hash: HASH.into(),
                        engine_version: ENGINE.into(),
                        connections: Vec::new(),
                    })
                    .await
            })
        })
        .collect();
    for t in tasks {
        t.await.unwrap().unwrap();
    }
    let client = server.client("fresh").await;
    let n: i64 = client
        .query_one("SELECT count(*) FROM _ferry_runs", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(n, 6);
}

#[tokio::test]
async fn a_sink_on_another_connection_falls_back_and_reruns_without_duplicates() {
    let server = Server::start().await;
    let warehouse = server.database("warehouse").await;
    let reporting = server.database("reporting").await;
    let w = Warehouse::new(&warehouse).await;

    // The flow sinks to `reporting`, a second connection of the same type.
    // It is built with plain `build`, so it is not bound to the ledger.
    let built = w
        .registry
        .build(&connection("reporting", &reporting), Roles::SINK)
        .unwrap();
    let sink = built.endpoints.sink.unwrap();
    sink.prepare(&schema(), WriteMode::Insert).await.unwrap();
    let caps = sink.capabilities().await.unwrap();
    assert!(!caps.strategies.contains(&IdempotencyStrategy::TxnWithState));
    let strategy = caps.strategy_for(WriteMode::Insert).unwrap();
    assert_eq!(strategy, IdempotencyStrategy::DeletePartitionThenInsert);

    let run = new_run(w.store.as_ref(), 1).await;
    let probe = pool(&reporting);
    // The sink commits, then the worker dies before the runner records the
    // partition done.
    let first = w
        .store
        .lease_next(&run, "orders", Duration::ZERO)
        .await
        .unwrap()
        .unwrap();
    write(sink.as_ref(), 0, &first, strategy, 6).await.unwrap();
    assert_eq!(row_counts(&probe, "orders").await, (6, 6));
    assert!(!w.store.is_done(&run, &first.partition).await.unwrap());

    // The partition re-runs; its strategy replaces the first attempt's rows.
    let second = w
        .store
        .lease_next(&run, "orders", LONG)
        .await
        .unwrap()
        .expect("the partition is pending again");
    assert_eq!(second.partition, first.partition);
    let summary = write(sink.as_ref(), 0, &second, strategy, 6).await.unwrap();
    w.store.mark_done(&run, &second, &summary).await.unwrap();
    assert_eq!(row_counts(&probe, "orders").await, (6, 6));
    assert!(w.store.is_done(&run, &first.partition).await.unwrap());

    // The state lives on `warehouse` only.
    let client = server.client("reporting").await;
    let table: Option<String> = client
        .query_one("SELECT to_regclass('_ferry_runs')::text", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(table, None);
}
