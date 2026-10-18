//! The state-capable extension (0004 §8), with the in-memory transactional
//! `fakesql` connector and backend from `common`, registered through
//! `register_state_capable` only.

mod common;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use common::{FakeBackend, FakeSql, URL, partition, rows};
use dataferry_core::{
    Epoch, FerryError, IdempotencyStrategy, Lease, Result, Sink, WriteMode, WriteSummary,
};
use dataferry_registry::state::{NewRun, PartitionCounts, RunId};
use dataferry_registry::{
    BuildError, ConnectorRegistry, RawConnection, RegisterError, Roles, StateStore,
};
use serde_json::json;

fn warehouse() -> RawConnection {
    serde_json::from_value(json!({
        "name": "warehouse",
        "type": "fakesql",
        "url": URL,
    }))
    .unwrap()
}

struct Harness {
    connector: FakeSql,
    sink: Arc<dyn Sink>,
    store: Arc<dyn StateStore>,
    run: RunId,
}

impl Harness {
    async fn new() -> Self {
        let connector = FakeSql::default();
        let mut registry = ConnectorRegistry::new();
        registry.register_state_capable(connector.clone(), FakeBackend);
        let (built, store) = registry
            .build_with_state(&warehouse(), Roles::SINK)
            .unwrap();
        let run = store
            .create_run(NewRun {
                config_hash: "sha256:abc".into(),
                engine_version: "0.0.1".into(),
                connections: vec![("warehouse".into(), "fakesql".into())],
            })
            .await
            .unwrap();
        store
            .store_partitions(&run, "orders", &[partition()])
            .await
            .unwrap();
        Self {
            connector,
            sink: built.endpoints.sink.unwrap(),
            store,
            run,
        }
    }

    /// The one database, after checking a single pool was opened for both
    /// the sink and the state store.
    fn server(&self) -> &common::Server {
        assert_eq!(
            self.connector.server.opened.load(Ordering::SeqCst),
            1,
            "one pool for the sink and the state store"
        );
        &self.connector.server
    }

    async fn lease(&self, ttl: Duration) -> Lease {
        self.store
            .lease_next(&self.run, "orders", ttl)
            .await
            .unwrap()
            .expect("a pending partition")
    }

    /// Writes `n` rows for the partition under `lease` and commits.
    async fn write_partition(&self, lease: &Lease, n: i64) -> Result<WriteSummary> {
        let mut txn = self
            .sink
            .begin(&partition(), lease, IdempotencyStrategy::TxnWithState)
            .await?;
        txn.write("orders", rows(n)).await?;
        txn.commit().await
    }

    async fn is_done(&self) -> bool {
        self.store
            .is_done(&self.run, &partition().id)
            .await
            .unwrap()
    }
}

#[tokio::test]
async fn the_sink_and_the_store_share_one_pool_and_commit_together() {
    let h = Harness::new().await;
    let caps = h.sink.capabilities().await.unwrap();
    assert_eq!(
        caps.strategy_for(WriteMode::Insert),
        Some(IdempotencyStrategy::TxnWithState)
    );

    let lease = h.lease(Duration::from_secs(60)).await;
    assert!(!h.is_done().await);
    let summary = h.write_partition(&lease, 3).await.unwrap();
    assert_eq!(summary.rows_written["orders"], 3);

    // The ledger wrote the done-record on the sink's connection; the store,
    // opened on the same pool, sees it.
    let db = h.server();
    assert_eq!(db.committed_rows(URL), 3);
    assert!(h.is_done().await);
    assert_eq!(
        h.store.counts(&h.run).await.unwrap(),
        PartitionCounts {
            done: 1,
            ..Default::default()
        }
    );
}

#[tokio::test]
async fn a_crash_before_commit_leaves_neither_rows_nor_done_record() {
    let h = Harness::new().await;
    let db = h.server();

    // The lease expires at once, as if the worker died holding it.
    let first = h.lease(Duration::ZERO).await;
    db.crash_before_commit.store(true, Ordering::SeqCst);
    let err = h.write_partition(&first, 3).await.unwrap_err();
    assert!(
        err.to_string().contains("simulated crash before COMMIT"),
        "{err}"
    );
    assert_eq!(db.committed_rows(URL), 0, "no rows without the done-record");
    assert_eq!(db.done_records(URL), 0, "no done-record without the rows");
    assert!(!h.is_done().await);
    assert_eq!(h.store.counts(&h.run).await.unwrap().pending, 1);

    // The partition is leased again and re-runs from the start; each row
    // lands once.
    db.crash_before_commit.store(false, Ordering::SeqCst);
    let second = h.lease(Duration::from_secs(60)).await;
    assert!(second.epoch > first.epoch);
    h.write_partition(&second, 3).await.unwrap();
    assert_eq!(db.committed_rows(URL), 3);
    assert!(h.is_done().await);
    assert_eq!(
        h.store
            .lease_next(&h.run, "orders", Duration::ZERO)
            .await
            .unwrap(),
        None
    );
}

#[tokio::test]
async fn a_superseded_lease_cannot_commit() {
    let h = Harness::new().await;
    let db = h.server();
    let old = h.lease(Duration::ZERO).await;
    let new = h.lease(Duration::from_secs(60)).await;

    let err = h.write_partition(&old, 3).await.unwrap_err();
    assert_eq!(
        err,
        FerryError::StaleLease {
            partition: "orders-0".into(),
            epoch: old.epoch.0,
            current: new.epoch.0,
        }
    );
    assert_eq!(db.committed_rows(URL), 0);
    assert!(!h.is_done().await);

    h.write_partition(&new, 2).await.unwrap();
    assert_eq!(db.committed_rows(URL), 2);
}

#[tokio::test]
async fn a_sink_built_without_a_ledger_never_offers_txn_with_state() {
    let connector = FakeSql::default();
    let mut registry = ConnectorRegistry::new();
    registry.register_state_capable(connector.clone(), FakeBackend);

    let built = registry.build(&warehouse(), Roles::SINK).unwrap();
    let sink = built.endpoints.sink.unwrap();
    let caps = sink.capabilities().await.unwrap();
    assert!(!caps.strategies.contains(&IdempotencyStrategy::TxnWithState));
    assert_eq!(
        caps.strategy_for(WriteMode::Insert),
        Some(IdempotencyStrategy::DeletePartitionThenInsert)
    );
    let lease = Lease {
        partition: partition().id,
        epoch: Epoch(1),
    };
    assert!(
        sink.begin(&partition(), &lease, IdempotencyStrategy::TxnWithState)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn an_unused_state_connection_still_gets_a_store() {
    let mut registry = ConnectorRegistry::new();
    registry.register_state_capable(FakeSql::default(), FakeBackend);
    let (built, store) = registry
        .build_with_state(&warehouse(), Roles::NONE)
        .unwrap();
    assert!(built.endpoints.source.is_none() && built.endpoints.sink.is_none());
    let run = store
        .create_run(NewRun {
            config_hash: "sha256:abc".into(),
            engine_version: "0.0.1".into(),
            connections: vec![],
        })
        .await
        .unwrap();
    assert_eq!(
        store.load_run(&run).await.unwrap().unwrap().config_hash,
        "sha256:abc"
    );
}

#[test]
fn both_halves_and_the_store_come_from_one_build() {
    let connector = FakeSql::default();
    let mut registry = ConnectorRegistry::new();
    registry.register_state_capable(connector.clone(), FakeBackend);
    let (built, _) = registry
        .build_with_state(&warehouse(), Roles::BOTH)
        .unwrap();
    assert!(built.endpoints.source.is_some() && built.endpoints.sink.is_some());
    assert_eq!(connector.server.opened.load(Ordering::SeqCst), 1);
    // `url` is a `Secret` field, so its value is in the redaction set.
    assert!(
        built
            .secrets
            .contains(&"fakesql://app:s3cr3t-pw@db.internal/shop".to_owned())
    );
}

#[test]
fn a_connector_registered_without_a_backend_cannot_hold_state() {
    let mut registry = ConnectorRegistry::new();
    registry.register(FakeSql::default());
    assert!(!registry.connector("fakesql").unwrap().state_capable());
    let err = registry
        .build_with_state(&warehouse(), Roles::SINK)
        .err()
        .expect("refused");
    assert!(
        err.to_string()
            .contains("the `fakesql` connector in this build can't hold the state store"),
        "{err}"
    );

    let mut registry = ConnectorRegistry::new();
    registry.register_state_capable(FakeSql::default(), FakeBackend);
    assert!(registry.connector("fakesql").unwrap().state_capable());
}

#[test]
fn bad_options_fail_before_a_pool_is_opened() {
    let connector = FakeSql::default();
    let mut registry = ConnectorRegistry::new();
    registry.register_state_capable(connector.clone(), FakeBackend);
    let mut raw = warehouse();
    raw.fields
        .insert("url".into(), json!("postgres://elsewhere"));
    let err = registry
        .build_with_state(&raw, Roles::SINK)
        .err()
        .expect("refused");
    assert_eq!(err, BuildError::option("url", "expected a fakesql:// URL"));
    assert_eq!(connector.server.opened.load(Ordering::SeqCst), 0);

    raw.fields.insert("pool_size".into(), json!(4));
    assert!(matches!(
        registry.build_with_state(&raw, Roles::SINK),
        Err(BuildError::Options(_))
    ));
}

#[test]
fn a_state_capable_type_collides_with_a_plain_one() {
    let mut registry = ConnectorRegistry::new();
    registry.register(FakeSql::default());
    assert_eq!(
        registry
            .try_register_state_capable(FakeSql::default(), FakeBackend)
            .err(),
        Some(RegisterError::DuplicateType {
            type_name: "fakesql".into()
        })
    );
}

#[test]
fn replacing_a_state_capable_type_keeps_it_state_capable() {
    let mut registry = ConnectorRegistry::new();
    registry.register_state_capable(FakeSql::default(), FakeBackend);

    // A plain `replace` would silently cost every flow its TxnWithState.
    assert_eq!(
        registry.try_replace(FakeSql::default()).err(),
        Some(RegisterError::WouldDropStateCapability {
            type_name: "fakesql".into()
        })
    );
    assert!(registry.connector("fakesql").unwrap().state_capable());

    let patched = FakeSql::default();
    registry.replace_state_capable(patched.clone(), FakeBackend);
    assert!(registry.connector("fakesql").unwrap().state_capable());
    if let Err(e) = registry.build_with_state(&warehouse(), Roles::SINK) {
        panic!("{e}");
    }
    assert_eq!(
        patched.server.opened.load(Ordering::SeqCst),
        1,
        "the replacement is the one that built"
    );
}

#[test]
fn a_plain_type_can_be_replaced_by_a_state_capable_one() {
    let mut registry = ConnectorRegistry::new();
    registry.register(FakeSql::default());
    registry.replace_state_capable(FakeSql::default(), FakeBackend);
    assert!(registry.connector("fakesql").unwrap().state_capable());
}
