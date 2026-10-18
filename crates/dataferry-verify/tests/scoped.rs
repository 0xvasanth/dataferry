//! One run into two sinks, each verified against its own target (S3): the
//! state store's run-wide totals are scoped to the flows a verifier
//! checks, so neither sink is reported unverified or failed for the other
//! one's rows. Both sinks are the real Postgres sink, on two schemas of
//! one database, writing a table of the same name.

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;

use common::{Db, new_run, settings};
use dataferry_connector_postgres::PostgresConnector;
use dataferry_core::{IdentityPlan, Sink, TargetSchema, WriteMode};
use dataferry_registry::{ConnectorRegistry, RawConnection, Roles, StateStore};
use dataferry_runtime::{Flow, Input, Runner};
use dataferry_state::PgStateBackend;
use dataferry_verify::{ChecksumObserver, MemoryWriteLog, PgTarget, Status, Target, WriteLog};
use serde_json::json;

const ORDERS: &str = "CREATE TABLE src.orders (id bigint PRIMARY KEY, amount numeric(10,2), \
                      note text); \
                      INSERT INTO src.orders SELECT i, i * 1.25, 'n' || i \
                      FROM generate_series(1, 50) i;";

fn connection(db: &Db, name: &str, schema: &str) -> RawConnection {
    serde_json::from_value(json!({
        "name": name,
        "type": "postgres",
        "url": db.url,
        "schema": schema,
    }))
    .unwrap()
}

/// `warehouse` (schema `dst`, the state connection) and `reporting`
/// (schema `rep`), as `dataferry run` builds them.
fn sinks(db: &Db) -> (Arc<dyn Sink>, Arc<dyn Sink>, Arc<dyn StateStore>) {
    let mut registry = ConnectorRegistry::new();
    registry.register_state_capable(PostgresConnector, PgStateBackend);
    let (warehouse, store) = registry
        .build_with_state(&connection(db, "warehouse", "dst"), Roles::SINK)
        .unwrap();
    let reporting = registry
        .build(&connection(db, "reporting", "rep"), Roles::SINK)
        .unwrap();
    (
        warehouse.endpoints.sink.unwrap(),
        reporting.endpoints.sink.unwrap(),
        store,
    )
}

fn target(db: &Db, schema: &str) -> Arc<dyn Target> {
    Arc::new(PgTarget::new(&db.url, schema, db.pool.clone()).unwrap())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_run_into_two_sinks_verifies_each_sink_against_its_own_share_of_the_totals() {
    let db = Db::start(ORDERS).await;
    let (warehouse, reporting, store) = sinks(&db);
    let entity = db.entity("orders").await;
    let flow = |name: &str, sink: Arc<dyn Sink>| Flow {
        name: name.into(),
        inputs: vec![Input {
            source: Arc::new(db.source()),
            entity: entity.clone(),
            co_partition: None,
        }],
        plan: Arc::new(IdentityPlan::new(entity.clone(), "orders")),
        sink,
        target: TargetSchema::new(vec![common::table_of(&entity, "orders")]).unwrap(),
        mode: WriteMode::Insert,
        finalizer: None,
        depends_on: Vec::new(),
    };
    let flows = vec![
        flow("to_warehouse", warehouse),
        flow("to_reporting", reporting),
    ];
    let log: Arc<dyn WriteLog> = Arc::new(MemoryWriteLog::new());
    // Only the warehouse can be read back here: the reporting flow is
    // still recorded, so its share can be taken out of the totals.
    let observer = ChecksumObserver::per_flow(
        Arc::clone(&log),
        BTreeMap::from([("to_warehouse".to_owned(), target(&db, "dst"))]),
    );
    let report = Runner::new(Arc::clone(&store), flows.clone(), settings())
        .unwrap()
        .with_observer(Arc::new(observer))
        .start(new_run())
        .await
        .unwrap();
    assert!(report.is_complete(), "{report:?}");
    // The state store's totals are the run's: both sinks' rows together.
    assert_eq!(report.totals.read, 100);
    assert_eq!(report.totals.written["orders"], 100);

    for (flow, schema) in [(&flows[0], "dst"), (&flows[1], "rep")] {
        let v = dataferry_verify::Verifier::new(
            Arc::clone(&store),
            Arc::clone(&log),
            target(&db, schema),
            std::slice::from_ref(flow),
        )
        .in_run(&flows)
        .verify(&report.run)
        .await
        .unwrap();
        assert_eq!(v.status, Status::Passed, "{}: {:#?}", flow.name, v.problems);
        assert_eq!(v.counts.read, 50, "{}", flow.name);
        assert_eq!(v.counts.dead_lettered, 0);
        assert_eq!(
            v.counts.written,
            BTreeMap::from([("orders".to_owned(), 50)])
        );
        let [table] = v.tables.as_slice() else {
            panic!("one table")
        };
        assert_eq!((table.written, table.at_write.rows), (50, 50));
        assert_eq!(table.read_back, Some(table.at_write));
        // Only this sink's partitions.
        assert_eq!(v.partitions.done, report.counts.done / 2);
    }

    // Unscoped, the warehouse's verifier compares its 50 rows with the
    // run's 100, which is what made S2 report such runs unverified.
    let v = dataferry_verify::Verifier::new(
        Arc::clone(&store),
        Arc::clone(&log),
        target(&db, "dst"),
        &flows[..1],
    )
    .verify(&report.run)
    .await
    .unwrap();
    assert_eq!(v.status, Status::Failed);
    assert!(
        v.problems
            .contains(&"the state store recorded 100 rows read, the write log 50".to_owned()),
        "{:#?}",
        v.problems
    );
    assert!(
        v.problems.contains(
            &"`orders`: the sink reported 100 rows committed, but 50 were handed to it".to_owned()
        ),
        "{:#?}",
        v.problems
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_flow_of_the_other_sink_with_no_write_record_fails_the_scoping_loudly() {
    let db = Db::start(ORDERS).await;
    let (warehouse, reporting, store) = sinks(&db);
    let entity = db.entity("orders").await;
    let flow = |name: &str, sink: Arc<dyn Sink>| Flow {
        name: name.into(),
        inputs: vec![Input {
            source: Arc::new(db.source()),
            entity: entity.clone(),
            co_partition: None,
        }],
        plan: Arc::new(IdentityPlan::new(entity.clone(), "orders")),
        sink,
        target: TargetSchema::new(vec![common::table_of(&entity, "orders")]).unwrap(),
        mode: WriteMode::Insert,
        finalizer: None,
        depends_on: Vec::new(),
    };
    let flows = vec![
        flow("to_warehouse", warehouse),
        flow("to_reporting", reporting),
    ];
    let log: Arc<dyn WriteLog> = Arc::new(MemoryWriteLog::new());
    let report = Runner::new(Arc::clone(&store), flows.clone(), settings())
        .unwrap()
        .with_observer(Arc::new(ChecksumObserver::new(
            Arc::clone(&log),
            target(&db, "dst"),
        )))
        .start(new_run())
        .await
        .unwrap();
    assert!(report.is_complete(), "{report:?}");
    // The warehouse's verifier is told about a flow whose partitions it
    // can't account for: one the log never saw.
    let empty: Arc<dyn WriteLog> = Arc::new(MemoryWriteLog::new());
    for record in log.load(&report.run).await.unwrap() {
        if record.flow == "to_warehouse" {
            empty.record(&report.run, &record).await.unwrap();
        }
    }
    let v =
        dataferry_verify::Verifier::new(Arc::clone(&store), empty, target(&db, "dst"), &flows[..1])
            .in_run(&flows)
            .verify(&report.run)
            .await
            .unwrap();
    assert_eq!(v.status, Status::Failed);
    let done = report.counts.done / 2;
    assert!(
        v.problems.contains(&format!(
            "flow `to_reporting` (verified elsewhere): {done} done partitions have no \
             write-time record, so the run's totals can't be scoped to the flows checked here"
        )),
        "{:#?}",
        v.problems
    );
}
