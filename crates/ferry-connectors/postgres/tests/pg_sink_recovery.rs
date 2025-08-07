//! What A3 promises when things go wrong, on a real Postgres: a crash in
//! the middle of a partition, a crash between the sink's commit and the
//! runner's done-record, a source that dropped an item between runs, and a
//! commit under a superseded lease.

mod common;

use std::sync::Arc;

use common::{Db, create_state_tables, db, items, lease, options, order_schema, orders, partition};
use ferry_connector_postgres::PostgresSink;
use ferry_core::{FerryError, IdempotencyStrategy, Sink, WriteMode};

const APP: &str = "ferry_crash_test";

/// Kills the server process behind every open transaction the sink holds,
/// as if the machine running ferry lost power: the client sees a broken
/// connection and the server rolls the transaction back.
async fn kill_sink_backends(db: &Db) -> i64 {
    db.client
        .query_one(
            "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity \
             WHERE application_name = $1 AND state LIKE 'idle in transaction%'",
            &[&APP],
        )
        .await
        .unwrap()
        .get(0)
}

/// The rows of the full partition, in write order.
async fn write_full_partition(
    sink: &dyn Sink,
    epoch: u64,
    strategy: IdempotencyStrategy,
) -> ferry_core::Result<ferry_core::WriteSummary> {
    let mut txn = sink
        .begin(&partition("orders-0"), &lease("orders-0", epoch), strategy)
        .await?;
    txn.write("orders", orders(&[("o1", "new"), ("o2", "paid")]))
        .await?;
    txn.write(
        "order_items",
        items(&[("o1", 0, "A", 1), ("o1", 1, "B", 2)]),
    )
    .await?;
    txn.write("order_items", items(&[("o2", 0, "C", 3)]))
        .await?;
    txn.write("item_options", options(&[("o1", 0, "gift")]))
        .await?;
    txn.commit().await
}

async fn assert_partition_written_once(db: &Db, schema: &str) {
    assert_eq!(db.count(schema, "orders").await, 2);
    assert_eq!(db.count(schema, "order_items").await, 3);
    assert_eq!(db.count(schema, "item_options").await, 1);
    let skus = db
        .strings(&format!(
            "SELECT order_id || '/' || line_no || '/' || sku FROM \"{schema}\".order_items \
             ORDER BY 1"
        ))
        .await;
    assert_eq!(skus, ["o1/0/A", "o1/1/B", "o2/0/C"]);
}

/// Crash mid-partition, then re-run: under each strategy the first attempt
/// leaves nothing, and the re-run writes each row exactly once.
#[tokio::test]
async fn a_crash_mid_partition_leaves_nothing_and_the_rerun_writes_each_row_once() {
    let db = db().await;
    create_state_tables(&db.client).await;
    let url = db.url_with(&format!("application_name={APP}"));
    let cases = [
        (
            "delete_then_insert",
            IdempotencyStrategy::DeletePartitionThenInsert,
        ),
        ("txn_with_state", IdempotencyStrategy::TxnWithState),
    ];
    for (schema, strategy) in cases {
        let sink: Arc<dyn Sink> = match strategy {
            IdempotencyStrategy::TxnWithState => {
                let mut registry = ferry_registry::ConnectorRegistry::new();
                registry.register_state_capable(
                    ferry_connector_postgres::PostgresConnector,
                    common::TestBackend,
                );
                let conn = serde_json::from_value(serde_json::json!({
                    "name": "warehouse", "type": "postgres", "url": url, "schema": schema,
                }))
                .unwrap();
                let (built, _) = registry
                    .build_with_state(&conn, ferry_registry::Roles::SINK)
                    .unwrap();
                built.endpoints.sink.unwrap()
            }
            _ => Arc::new(PostgresSink::new(&url, schema).unwrap()),
        };
        sink.prepare(&order_schema(), WriteMode::Insert)
            .await
            .unwrap();

        let mut txn = sink
            .begin(&partition("orders-0"), &lease("orders-0", 1), strategy)
            .await
            .unwrap();
        txn.write("orders", orders(&[("o1", "new"), ("o2", "paid")]))
            .await
            .unwrap();
        txn.write(
            "order_items",
            items(&[("o1", 0, "A", 1), ("o1", 1, "B", 2)]),
        )
        .await
        .unwrap();
        assert_eq!(kill_sink_backends(&db).await, 1, "{schema}");
        let err = txn
            .write("order_items", items(&[("o2", 0, "C", 3)]))
            .await
            .expect_err("the connection is gone");
        assert!(matches!(err, FerryError::Connector { .. }), "{err}");
        drop(txn);
        assert_eq!(db.count(schema, "orders").await, 0, "{schema}");
        assert_eq!(db.count(schema, "order_items").await, 0, "{schema}");
        assert_eq!(db.done_epoch("orders-0").await, None, "{schema}");

        let summary = write_full_partition(sink.as_ref(), 2, strategy)
            .await
            .unwrap();
        assert_eq!(summary.rows_written["order_items"], 3);
        assert_partition_written_once(&db, schema).await;
        if strategy == IdempotencyStrategy::TxnWithState {
            assert_eq!(db.done_epoch("orders-0").await, Some(2));
            // Reset for the next case's partition of the same name.
            db.client
                .batch_execute("DELETE FROM ferry_test_state.done")
                .await
                .unwrap();
        }
    }
}

/// A worker dropped without commit or abort (a panic, a cancelled task)
/// must not hand an open transaction back to the pool.
#[tokio::test]
async fn a_dropped_transaction_rolls_back_and_the_pool_stays_usable() {
    let db = db().await;
    let sink = PostgresSink::new(&db.url, "public").unwrap();
    sink.prepare(&order_schema(), WriteMode::Insert)
        .await
        .unwrap();
    for _ in 0..3 {
        let mut txn = sink
            .begin(
                &partition("orders-0"),
                &lease("orders-0", 1),
                IdempotencyStrategy::DeletePartitionThenInsert,
            )
            .await
            .unwrap();
        // A row the re-run below doesn't replace: if the open transaction
        // went back to the pool, the next partition's COMMIT would keep it.
        txn.write("orders", orders(&[("ghost", "new")]))
            .await
            .unwrap();
        drop(txn);
    }
    assert_eq!(db.count("public", "orders").await, 0);
    write_full_partition(&sink, 2, IdempotencyStrategy::DeletePartitionThenInsert)
        .await
        .unwrap();
    assert_partition_written_once(&db, "public").await;
    let open = db
        .strings("SELECT state FROM pg_stat_activity WHERE state LIKE 'idle in transaction%'")
        .await;
    assert!(open.is_empty(), "{open:?}");
}

/// Without the state store on the sink, the runner records a partition done
/// after the sink commits. A crash in between re-runs a partition that is
/// already in the target, and DeletePartitionThenInsert replaces it.
#[tokio::test]
async fn a_crash_after_commit_but_before_the_done_record_reruns_without_duplicates() {
    let db = db().await;
    for mode in [WriteMode::Insert, WriteMode::Upsert] {
        let schema = format!("{mode:?}").to_lowercase();
        let sink = PostgresSink::new(&db.url, &schema).unwrap();
        sink.prepare(&order_schema(), mode).await.unwrap();
        let strategy = sink
            .capabilities()
            .await
            .unwrap()
            .strategy_for(mode)
            .unwrap();
        assert_eq!(strategy, IdempotencyStrategy::DeletePartitionThenInsert);
        write_full_partition(&sink, 1, strategy).await.unwrap();
        // ...the process dies here, before the state store hears of it.
        write_full_partition(&sink, 2, strategy).await.unwrap();
        write_full_partition(&sink, 3, strategy).await.unwrap();
        assert_partition_written_once(&db, &schema).await;
    }
}

async fn item_rows(db: &Db, schema: &str) -> Vec<String> {
    db.strings(&format!(
        "SELECT order_id || '/' || line_no || '/' || sku || 'x' || qty \
         FROM \"{schema}\".order_items ORDER BY 1"
    ))
    .await
}

async fn option_rows(db: &Db, schema: &str) -> Vec<String> {
    db.strings(&format!(
        "SELECT order_id || '/' || line_no || '/' || name FROM \"{schema}\".item_options \
         ORDER BY 1"
    ))
    .await
}

/// Run 1 writes o1 with three items (one with an option), o2 with two, and
/// o3 with one. Between runs the source drops o1's third item, changes the
/// quantity of its first, and empties o3; run 2's partition holds o1 and o3
/// only.
async fn lost_item_scenario(db: &Db, sink: &dyn Sink, schema: &str, strategy: IdempotencyStrategy) {
    let mut txn = sink
        .begin(&partition("orders-0"), &lease("orders-0", 1), strategy)
        .await
        .unwrap();
    txn.write(
        "orders",
        orders(&[("o1", "new"), ("o2", "new"), ("o3", "new")]),
    )
    .await
    .unwrap();
    txn.write(
        "order_items",
        items(&[
            ("o1", 0, "A", 1),
            ("o1", 1, "B", 1),
            ("o1", 2, "C", 1),
            ("o2", 0, "D", 1),
            ("o2", 1, "E", 1),
            ("o3", 0, "F", 1),
        ]),
    )
    .await
    .unwrap();
    txn.write(
        "item_options",
        options(&[("o1", 2, "gift"), ("o2", 0, "wrap")]),
    )
    .await
    .unwrap();
    txn.commit().await.unwrap();

    let mut txn = sink
        .begin(&partition("orders-1"), &lease("orders-1", 1), strategy)
        .await
        .unwrap();
    txn.write("orders", orders(&[("o1", "paid"), ("o3", "paid")]))
        .await
        .unwrap();
    txn.write(
        "order_items",
        items(&[("o1", 0, "A", 5), ("o1", 1, "B", 1)]),
    )
    .await
    .unwrap();
    txn.write("item_options", options(&[])).await.unwrap();
    txn.commit().await.unwrap();

    assert_eq!(
        item_rows(db, schema).await,
        // o1's third item is gone and its first is updated; o2, not in the
        // batch, keeps its items; o3's only item is gone with nothing new.
        ["o1/0/Ax5", "o1/1/Bx1", "o2/0/Dx1", "o2/1/Ex1"],
        "{schema}"
    );
    // The dropped item's option went with it; o2's survives.
    assert_eq!(option_rows(db, schema).await, ["o2/0/wrap"], "{schema}");
    assert_eq!(
        db.strings(&format!(
            "SELECT id || ':' || status FROM \"{schema}\".orders ORDER BY 1"
        ))
        .await,
        ["o1:paid", "o2:new", "o3:paid"],
        "{schema}"
    );
}

#[tokio::test]
async fn an_item_lost_between_runs_is_removed_on_upsert() {
    let db = db().await;
    create_state_tables(&db.client).await;
    for strategy in [
        IdempotencyStrategy::UpsertByKey,
        IdempotencyStrategy::DeletePartitionThenInsert,
    ] {
        let schema = format!("{strategy:?}").to_lowercase();
        let sink = db.plain_sink(&schema);
        sink.prepare(&order_schema(), WriteMode::Upsert)
            .await
            .unwrap();
        lost_item_scenario(&db, sink.as_ref(), &schema, strategy).await;
    }
    let (sink, _) = db.sink_with_state("txn_with_state").await;
    sink.prepare(&order_schema(), WriteMode::Upsert)
        .await
        .unwrap();
    lost_item_scenario(
        &db,
        sink.as_ref(),
        "txn_with_state",
        IdempotencyStrategy::TxnWithState,
    )
    .await;
}

/// Insert mode re-running the same partition after the source lost an item
/// (a crash after commit, then a re-read that sees newer data).
#[tokio::test]
async fn an_item_lost_before_an_insert_rerun_is_removed() {
    let db = db().await;
    let sink = db.plain_sink("public");
    sink.prepare(&order_schema(), WriteMode::Insert)
        .await
        .unwrap();
    write_full_partition(
        sink.as_ref(),
        1,
        IdempotencyStrategy::DeletePartitionThenInsert,
    )
    .await
    .unwrap();

    let mut txn = sink
        .begin(
            &partition("orders-0"),
            &lease("orders-0", 2),
            IdempotencyStrategy::DeletePartitionThenInsert,
        )
        .await
        .unwrap();
    txn.write("orders", orders(&[("o1", "new"), ("o2", "paid")]))
        .await
        .unwrap();
    txn.write(
        "order_items",
        items(&[("o1", 1, "B", 2), ("o2", 0, "C", 3)]),
    )
    .await
    .unwrap();
    txn.commit().await.unwrap();
    assert_eq!(item_rows(&db, "public").await, ["o1/1/Bx2", "o2/0/Cx3"]);
    assert!(option_rows(&db, "public").await.is_empty());
}

/// The epoch fence: the ledger runs inside the sink's transaction, so a
/// commit it refuses leaves neither rows nor a done-record.
#[tokio::test]
async fn a_commit_under_a_stale_epoch_is_rejected_and_writes_nothing() {
    let db = db().await;
    create_state_tables(&db.client).await;
    let (sink, _) = db.sink_with_state("public").await;
    sink.prepare(&order_schema(), WriteMode::Insert)
        .await
        .unwrap();

    // Another worker has since leased the partition at epoch 2.
    db.lease("orders-0", 2).await;
    let err = write_full_partition(sink.as_ref(), 1, IdempotencyStrategy::TxnWithState)
        .await
        .unwrap_err();
    assert_eq!(
        err,
        FerryError::StaleLease {
            partition: "orders-0".into(),
            epoch: 1,
            current: 2
        }
    );
    assert_eq!(db.count("public", "orders").await, 0);
    assert_eq!(db.count("public", "order_items").await, 0);
    assert_eq!(db.done_epoch("orders-0").await, None);

    // The current owner commits; the superseded one still can't afterwards.
    write_full_partition(sink.as_ref(), 2, IdempotencyStrategy::TxnWithState)
        .await
        .unwrap();
    let err = write_full_partition(sink.as_ref(), 1, IdempotencyStrategy::TxnWithState)
        .await
        .unwrap_err();
    assert!(
        matches!(err, FerryError::StaleLease { current: 2, .. }),
        "{err}"
    );
    assert_partition_written_once(&db, "public").await;
    assert_eq!(db.done_epoch("orders-0").await, Some(2));
}
