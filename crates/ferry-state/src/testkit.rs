//! The state store contract suite: what every [`StateStore`] backend
//! guarantees, whether it ships here or in a third-party crate.
//!
//! Like ferry-core's suites, it returns every violation it finds instead of
//! panicking on the first, so a test can assert that a deliberately broken
//! store is caught for the right reason.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use ferry_core::{
    Epoch, FerryError, KeyRange, KeyValue, Lease, Partition, PartitionId, PartitionScope, Result,
    WriteSummary,
};
use ferry_registry::StateStore;
use ferry_registry::state::{NewRun, PartitionCounts, PartitionRows, RunId};

const LONG: Duration = Duration::from_secs(600);

/// Checks, against stores from `open` (each call is a fresh handle on the
/// same state, as after a restart):
///
/// - a run keeps its config hash (as an opaque string), engine version and
///   connections, and a second handle loads it back;
/// - partitions are planned once: storing them again is a no-op, storing
///   different ones fails and changes nothing, a partition id can't be in
///   two flows of a run, and they load back in planned order;
/// - `lease_next` hands out each pending partition once while its lease is
///   live, in planned order, and an expired lease is leased again with a
///   strictly higher epoch;
/// - epoch fencing, with two workers: once a partition is leased again, the
///   first worker's `renew`, `mark_done`, `mark_failed` and `record_rows`
///   fail with [`FerryError::StaleLease`] naming both epochs, and the second
///   worker's commit stands;
/// - `renew` keeps a lease from being taken over, done and failed
///   partitions are never leased again, and an epoch the store never issued
///   is refused;
/// - counts and row totals follow the partitions, counting a re-run
///   partition once.
pub async fn check_state_store(open: &dyn Fn() -> Arc<dyn StateStore>) -> Vec<String> {
    let mut v = Vec::new();
    for (name, check) in CHECKS {
        let store = open();
        let reopened = open();
        if let Err(e) = check(store, reopened, &mut v).await {
            v.push(format!("{name}: operation failed: {e}"));
        }
    }
    v
}

type Check =
    for<'a> fn(Arc<dyn StateStore>, Arc<dyn StateStore>, &'a mut Vec<String>) -> BoxFuture<'a>;

type BoxFuture<'a> = Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;

const CHECKS: [(&str, Check); 7] = [
    ("runs", |s, r, v| Box::pin(runs(s, r, v))),
    ("planning", |s, r, v| Box::pin(planning(s, r, v))),
    ("leasing", |s, r, v| Box::pin(leasing(s, r, v))),
    ("fencing", |s, r, v| Box::pin(fencing(s, r, v))),
    ("heartbeats", |s, r, v| Box::pin(heartbeats(s, r, v))),
    ("terminal states", |s, r, v| Box::pin(terminal(s, r, v))),
    ("totals", |s, r, v| Box::pin(totals(s, r, v))),
];

/// Not a real hash: the store must not parse or normalize it.
const HASH: &str = "sha256:Opaque Hash/with ünïcode & 'quotes' 🚢";

async fn new_run(store: &dyn StateStore) -> Result<RunId> {
    store
        .create_run(NewRun {
            config_hash: HASH.into(),
            engine_version: "0.0.1-suite".into(),
            connections: vec![
                ("warehouse".into(), "postgres".into()),
                ("shop".into(), "mongo".into()),
            ],
        })
        .await
}

fn partitions(entity: &str, n: i64) -> Vec<Partition> {
    (0..n)
        .map(|i| Partition {
            id: PartitionId(format!("{entity}-{i}")),
            entity: entity.into(),
            scope: PartitionScope::KeyRange(
                KeyRange::new(
                    "id",
                    (i > 0).then_some(KeyValue::Int(i * 100)),
                    (i < n - 1).then_some(KeyValue::Int((i + 1) * 100)),
                )
                .expect("a valid range"),
            ),
        })
        .collect()
}

/// A run with `n` partitions planned under flow "orders".
async fn planned(store: &dyn StateStore, n: i64) -> Result<(RunId, Vec<Partition>)> {
    let run = new_run(store).await?;
    let parts = partitions("orders", n);
    store.store_partitions(&run, "orders", &parts).await?;
    Ok((run, parts))
}

fn summary(rows: &[(&str, u64)]) -> WriteSummary {
    WriteSummary {
        rows_written: rows.iter().map(|(t, n)| (t.to_string(), *n)).collect(),
    }
}

/// Expects `result` to be a `StaleLease` for `lease`, superseded by
/// `current`.
fn expect_stale(
    v: &mut Vec<String>,
    what: &str,
    result: Result<()>,
    lease: &Lease,
    current: Epoch,
) {
    let want = FerryError::StaleLease {
        partition: lease.partition.0.clone(),
        epoch: lease.epoch.0,
        current: current.0,
    };
    match result {
        Err(e) if e == want => {}
        Err(e) => v.push(format!(
            "{what} under a superseded lease failed with {e:?}, not {want:?}"
        )),
        Ok(()) => v.push(format!("{what} under a superseded lease succeeded")),
    }
}

async fn runs(
    store: Arc<dyn StateStore>,
    reopened: Arc<dyn StateStore>,
    v: &mut Vec<String>,
) -> Result<()> {
    let a = new_run(store.as_ref()).await?;
    let b = new_run(store.as_ref()).await?;
    if a == b {
        v.push(format!("two runs got the same id {a:?}"));
    }
    match reopened.load_run(&a).await? {
        None => v.push("a second handle doesn't find the run".into()),
        Some(record) => {
            if record.id != a {
                v.push(format!("load_run({a:?}) returned run {:?}", record.id));
            }
            if record.config_hash != HASH {
                v.push(format!(
                    "the config hash came back as {:?}, not {HASH:?}",
                    record.config_hash
                ));
            }
            if record.engine_version != "0.0.1-suite" {
                v.push(format!(
                    "the engine version came back as {:?}",
                    record.engine_version
                ));
            }
            let want = vec![
                ("warehouse".to_owned(), "postgres".to_owned()),
                ("shop".to_owned(), "mongo".to_owned()),
            ];
            if record.connections != want {
                v.push(format!(
                    "the connections came back as {:?}",
                    record.connections
                ));
            }
        }
    }
    for missing in ["no-such-run", "../escape", ""] {
        if store.load_run(&RunId(missing.into())).await?.is_some() {
            v.push(format!("load_run({missing:?}) found a run"));
        }
    }
    Ok(())
}

async fn planning(
    store: Arc<dyn StateStore>,
    reopened: Arc<dyn StateStore>,
    v: &mut Vec<String>,
) -> Result<()> {
    let (run, parts) = planned(store.as_ref(), 3).await?;
    let loaded = reopened.load_partitions(&run, "orders").await?;
    if loaded != parts {
        v.push(format!(
            "partitions loaded back as {loaded:?}, not as planned"
        ));
    }

    if let Err(e) = store.store_partitions(&run, "orders", &parts).await {
        v.push(format!("storing the same plan again failed: {e}"));
    }
    let replanned = partitions("orders", 4);
    if store
        .store_partitions(&run, "orders", &replanned)
        .await
        .is_ok()
    {
        v.push("a flow was planned twice with different partitions".into());
    }
    if store.load_partitions(&run, "orders").await? != parts {
        v.push("re-planning a flow changed its stored partitions".into());
    }

    let items = partitions("items", 2);
    store.store_partitions(&run, "items", &items).await?;
    if store.load_partitions(&run, "items").await? != items {
        v.push("a second flow's partitions didn't load back".into());
    }
    let clash = partitions("orders", 1);
    if store
        .store_partitions(&run, "refunds", &clash)
        .await
        .is_ok()
    {
        v.push("a partition id was planned in two flows of one run".into());
    }
    let twice = [items[0].clone(), items[0].clone()];
    if store.store_partitions(&run, "twice", &twice).await.is_ok() {
        v.push("a flow planned the same partition id twice".into());
    }
    for flow in ["refunds", "twice", "never-planned"] {
        let left = store.load_partitions(&run, flow).await?;
        if !left.is_empty() {
            v.push(format!(
                "flow `{flow}` has partitions {left:?} after a refused plan"
            ));
        }
    }

    // The same ids in another run are another run's partitions.
    let other = new_run(store.as_ref()).await?;
    if let Err(e) = store.store_partitions(&other, "orders", &parts).await {
        v.push(format!(
            "another run couldn't plan the same partition ids: {e}"
        ));
    }
    Ok(())
}

async fn leasing(
    store: Arc<dyn StateStore>,
    reopened: Arc<dyn StateStore>,
    v: &mut Vec<String>,
) -> Result<()> {
    let (run, parts) = planned(store.as_ref(), 3).await?;
    if store
        .lease_next(&run, "no-such-flow", LONG)
        .await?
        .is_some()
    {
        v.push("a flow with no partitions handed out a lease".into());
    }
    let mut leased = Vec::new();
    while let Some(lease) = store.lease_next(&run, "orders", LONG).await? {
        if leased.len() == parts.len() {
            v.push(format!(
                "{lease:?} handed out after every partition was leased"
            ));
            break;
        }
        leased.push(lease);
    }
    let ids: Vec<&PartitionId> = leased.iter().map(|l| &l.partition).collect();
    let want: Vec<&PartitionId> = parts.iter().map(|p| &p.id).collect();
    if ids != want {
        v.push(format!(
            "live leases went to {ids:?}, not once each in order {want:?}"
        ));
    }
    let counts = reopened.counts(&run).await?;
    let want = PartitionCounts {
        leased: 3,
        ..Default::default()
    };
    if counts != want {
        v.push(format!(
            "with every partition leased, counts are {counts:?}"
        ));
    }

    // Expired leases go back to pending, and are re-leased with a strictly
    // higher epoch every time.
    let (run, _) = planned(store.as_ref(), 1).await?;
    let mut last = Epoch(0);
    for attempt in 0..3 {
        let Some(lease) = store.lease_next(&run, "orders", Duration::ZERO).await? else {
            v.push(format!(
                "an expired lease wasn't leased again (attempt {attempt})"
            ));
            break;
        };
        if lease.epoch <= last {
            v.push(format!(
                "re-leasing gave epoch {} after epoch {}",
                lease.epoch.0, last.0
            ));
        }
        last = lease.epoch;
        let counts = store.counts(&run).await?;
        if counts.pending != 1 || counts.leased != 0 {
            v.push(format!(
                "an expired lease isn't counted pending: {counts:?}"
            ));
        }
    }
    Ok(())
}

async fn fencing(
    store: Arc<dyn StateStore>,
    reopened: Arc<dyn StateStore>,
    v: &mut Vec<String>,
) -> Result<()> {
    let (run, parts) = planned(store.as_ref(), 1).await?;
    let partition = &parts[0].id;
    // Worker A's lease expires at once (it stalled), and worker B, on its
    // own handle, takes the partition over.
    let a = store
        .lease_next(&run, "orders", Duration::ZERO)
        .await?
        .expect("a pending partition");
    let Some(b) = reopened.lease_next(&run, "orders", LONG).await? else {
        v.push("worker B couldn't lease the partition after A's lease expired".into());
        return Ok(());
    };
    if b.partition != a.partition || b.epoch <= a.epoch {
        v.push(format!("worker B got {b:?} after worker A's {a:?}"));
    }

    let rows = PartitionRows {
        read: 1,
        dead_lettered: 0,
    };
    expect_stale(v, "renew", store.renew(&run, &a, LONG).await, &a, b.epoch);
    expect_stale(
        v,
        "record_rows",
        store.record_rows(&run, &a, &rows).await,
        &a,
        b.epoch,
    );
    expect_stale(
        v,
        "mark_failed",
        store.mark_failed(&run, &a, "worker A gave up").await,
        &a,
        b.epoch,
    );
    expect_stale(
        v,
        "mark_done",
        store.mark_done(&run, &a, &summary(&[("orders", 1)])).await,
        &a,
        b.epoch,
    );
    if store.is_done(&run, partition).await? {
        v.push("worker A's superseded commit marked the partition done".into());
    }

    reopened
        .mark_done(&run, &b, &summary(&[("orders", 2)]))
        .await?;
    if !store.is_done(&run, partition).await? {
        v.push("worker B's commit didn't mark the partition done".into());
    }
    // A late retry by A still can't overwrite B's done-record.
    expect_stale(
        v,
        "mark_done after the partition was done",
        store.mark_done(&run, &a, &summary(&[("orders", 9)])).await,
        &a,
        b.epoch,
    );
    let totals = store.totals(&run).await?;
    if totals.written.get("orders") != Some(&2) {
        v.push(format!(
            "after a superseded commit, rows written are {:?}, not worker B's",
            totals.written
        ));
    }

    // An epoch nobody was given is refused, not taken as current.
    let (run, _) = planned(store.as_ref(), 1).await?;
    let lease = store
        .lease_next(&run, "orders", LONG)
        .await?
        .expect("a pending partition");
    let forged = Lease {
        partition: lease.partition.clone(),
        epoch: Epoch(lease.epoch.0 + 1000),
    };
    if store.mark_done(&run, &forged, &summary(&[])).await.is_ok() {
        v.push("mark_done accepted an epoch the store never issued".into());
    }
    let elsewhere = Lease {
        partition: PartitionId("not-planned".into()),
        epoch: lease.epoch,
    };
    if store
        .mark_done(&run, &elsewhere, &summary(&[]))
        .await
        .is_ok()
    {
        v.push("mark_done accepted a lease on a partition the run doesn't have".into());
    }
    Ok(())
}

async fn heartbeats(
    store: Arc<dyn StateStore>,
    reopened: Arc<dyn StateStore>,
    v: &mut Vec<String>,
) -> Result<()> {
    let (run, _) = planned(store.as_ref(), 1).await?;
    let lease = store
        .lease_next(&run, "orders", Duration::ZERO)
        .await?
        .expect("a pending partition");
    // Expired but not taken over: the worker's heartbeat wins it back.
    store.renew(&run, &lease, LONG).await?;
    if let Some(other) = reopened.lease_next(&run, "orders", LONG).await? {
        v.push(format!("a renewed lease was taken over by {other:?}"));
    }
    let counts = store.counts(&run).await?;
    if counts.leased != 1 {
        v.push(format!("a renewed lease isn't counted leased: {counts:?}"));
    }
    Ok(())
}

async fn terminal(
    store: Arc<dyn StateStore>,
    reopened: Arc<dyn StateStore>,
    v: &mut Vec<String>,
) -> Result<()> {
    let (run, parts) = planned(store.as_ref(), 3).await?;
    let first = store
        .lease_next(&run, "orders", LONG)
        .await?
        .expect("pending");
    let second = store
        .lease_next(&run, "orders", LONG)
        .await?
        .expect("pending");
    store
        .mark_done(&run, &first, &summary(&[("orders", 4)]))
        .await?;
    store.mark_failed(&run, &second, "bad row").await?;

    if reopened.renew(&run, &first, LONG).await.is_ok() {
        v.push("a done partition's lease was renewed".into());
    }
    if reopened.mark_failed(&run, &first, "late").await.is_ok() {
        v.push("a done partition was marked failed".into());
    }
    if !reopened.is_done(&run, &parts[0].id).await? {
        v.push("a second handle doesn't see the done-record".into());
    }
    if reopened.is_done(&run, &parts[1].id).await? {
        v.push("a failed partition is reported done".into());
    }
    // Done and failed partitions are finished: only the untouched one is
    // left to lease.
    let next = reopened.lease_next(&run, "orders", LONG).await?;
    if next.as_ref().map(|l| &l.partition) != Some(&parts[2].id) {
        v.push(format!(
            "with one done and one failed, lease_next gave {next:?}, not `{}`",
            parts[2].id.0
        ));
    }
    if let Some(again) = reopened.lease_next(&run, "orders", LONG).await? {
        v.push(format!(
            "a done or failed partition was leased again: {again:?}"
        ));
    }
    let counts = reopened.counts(&run).await?;
    let want = PartitionCounts {
        pending: 0,
        leased: 1,
        done: 1,
        failed: 1,
    };
    if counts != want {
        v.push(format!("counts are {counts:?}, not {want:?}"));
    }
    Ok(())
}

async fn totals(
    store: Arc<dyn StateStore>,
    reopened: Arc<dyn StateStore>,
    v: &mut Vec<String>,
) -> Result<()> {
    let (run, _) = planned(store.as_ref(), 3).await?;
    // Partition 0 runs twice: the first attempt's lease expires after it
    // reported rows, and only the second attempt's numbers count.
    let attempt = store
        .lease_next(&run, "orders", Duration::ZERO)
        .await?
        .expect("pending");
    let rows = |read, dead_lettered| PartitionRows {
        read,
        dead_lettered,
    };
    store.record_rows(&run, &attempt, &rows(100, 100)).await?;
    let p0 = store
        .lease_next(&run, "orders", LONG)
        .await?
        .expect("expired");
    if p0.partition != attempt.partition {
        v.push(format!("the expired partition wasn't leased first: {p0:?}"));
    }
    store.record_rows(&run, &p0, &rows(10, 1)).await?;
    store
        .mark_done(&run, &p0, &summary(&[("orders", 9), ("order_items", 20)]))
        .await?;

    let p1 = store
        .lease_next(&run, "orders", LONG)
        .await?
        .expect("pending");
    store.record_rows(&run, &p1, &rows(5, 0)).await?;
    store
        .mark_done(&run, &p1, &summary(&[("orders", 5)]))
        .await?;

    // Failed partitions count nothing.
    let p2 = store
        .lease_next(&run, "orders", LONG)
        .await?
        .expect("pending");
    store.record_rows(&run, &p2, &rows(7, 7)).await?;
    store.mark_failed(&run, &p2, "too many errors").await?;

    let totals = reopened.totals(&run).await?;
    let want_written: BTreeMap<String, u64> =
        [("orders".to_owned(), 14), ("order_items".to_owned(), 20)].into();
    if totals.read != 15 || totals.dead_lettered != 1 || totals.written != want_written {
        v.push(format!(
            "totals are {totals:?}, not read 15, dead-lettered 1, written {want_written:?}"
        ));
    }
    Ok(())
}
