//! The dead-letter queue (A7) end to end: the golden Mongo `orders` read
//! by the Mongo source, split into `orders` and `order_items`, written by
//! the real Postgres sink with the state store in the same database
//! (`TxnWithState`), and verified.
//!
//! B4's compiled plan isn't on main yet, so `GoldenPlan` stands in for the
//! golden mapping (fixtures/golden/README.md) by hand, and rejects a
//! document whose item `qty` isn't an integer the way the compiled
//! `int(qty)` will. S3 reruns these scenarios with the real plan.

mod common;

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray};
use common::{Db, new_run, verifier};
use dataferry_connector_mongo::MongoSource;
use dataferry_connector_postgres::PostgresConnector;
use dataferry_core::types::{CanonicalField, CanonicalType};
use dataferry_core::{
    Entity, ForeignKey, PartitionInputs, RejectedRecord, Result, Sink, Source, TargetBatch,
    TargetSchema, TargetTable, TransformOutput, TransformPlan, TransformStream, WriteMode,
};
use dataferry_registry::{ConnectorRegistry, RawConnection, Roles, StateStore};
use dataferry_runtime::{Flow, Input, OnError, Runner, Settings};
use dataferry_state::PgStateBackend;
use dataferry_verify::{ChecksumObserver, MemoryWriteLog, Report, Status, WriteLog};
use futures::StreamExt;
use mongodb::bson::{Bson, Document, doc, oid::ObjectId};
use serde_json::json;
use testcontainers_modules::mongo::Mongo;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

const DEAD_LETTERED: &str = "650000000000000000000006";

// ---- the source ----

struct Shop {
    _container: ContainerAsync<Mongo>,
    url: String,
    client: mongodb::Client,
}

impl Shop {
    async fn start() -> Self {
        let container = Mongo::default().start().await.expect("start mongo");
        let port = container.get_host_port_ipv4(27017).await.unwrap();
        let url = format!("mongodb://127.0.0.1:{port}");
        let client = mongodb::Client::with_uri_str(&url).await.unwrap();
        Self {
            _container: container,
            url,
            client,
        }
    }

    async fn insert(&self, docs: Vec<Document>) {
        self.client
            .database("shop")
            .collection::<Document>("orders")
            .insert_many(docs)
            .await
            .unwrap();
    }

    fn source(&self) -> MongoSource {
        MongoSource::new(&self.url, "shop").unwrap()
    }

    async fn orders(&self) -> Entity {
        self.source()
            .discover()
            .await
            .unwrap()
            .into_iter()
            .find(|e| e.name() == "orders")
            .unwrap()
    }
}

fn fixture(path: &str) -> serde_json::Value {
    let path = format!(
        "{}/../../fixtures/golden/{path}",
        env!("CARGO_MANIFEST_DIR")
    );
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

/// The ten golden documents, from Extended JSON.
fn golden() -> Vec<Document> {
    fixture("source/orders.json")
        .as_array()
        .unwrap()
        .iter()
        .map(|d| match Bson::try_from(d.clone()).unwrap() {
            Bson::Document(doc) => doc,
            other => panic!("not a document: {other:?}"),
        })
        .collect()
}

/// One more paid order with one item of quantity `qty`.
fn order(n: u8, qty: Bson) -> Document {
    let id = ObjectId::parse_str(format!("6500000000000000000000{n:02x}")).unwrap();
    doc! { "_id": id, "status": "PAID", "items": [{ "sku": "SKU-9", "qty": qty }] }
}

// ---- the golden mapping, by hand ----

/// `orders` → `orders (id, status)` and `order_items (order_id, line_no,
/// sku, qty)`, one item row per element of `items`; documents with
/// `deleted_at` set are filtered, and a document with an item whose `qty`
/// isn't an integer is rejected as a whole.
struct GoldenPlan {
    inputs: Vec<String>,
    order: Vec<String>,
}

fn golden_target() -> TargetSchema {
    let text = |name: &str, nullable| CanonicalField::new(name, CanonicalType::Utf8, nullable);
    let int = |name: &str| CanonicalField::new(name, CanonicalType::Int64, false);
    TargetSchema::new(vec![
        TargetTable {
            name: "orders".into(),
            fields: vec![text("id", false), text("status", true)],
            primary_key: vec!["id".into()],
            foreign_keys: Vec::new(),
        },
        TargetTable {
            name: "order_items".into(),
            fields: vec![
                text("order_id", false),
                int("line_no"),
                text("sku", true),
                int("qty"),
            ],
            primary_key: vec!["order_id".into(), "line_no".into()],
            foreign_keys: vec![ForeignKey {
                columns: vec!["order_id".into()],
                references_table: "orders".into(),
                references_columns: vec!["id".into()],
            }],
        },
    ])
    .unwrap()
}

impl TransformPlan for GoldenPlan {
    fn inputs(&self) -> &[String] {
        &self.inputs
    }

    fn write_order(&self) -> &[String] {
        &self.order
    }

    fn execute(&self, mut inputs: PartitionInputs) -> Result<TransformStream> {
        Ok(inputs
            .take("orders")?
            .map(|batch| {
                let batch = batch?;
                let col = |name: &str| batch.column_by_name(name).unwrap();
                let ids = col("_id").as_string::<i32>();
                let status = col("status").as_string::<i32>();
                let deleted = col("deleted_at");
                let items = col("items").as_list::<i32>();
                let mut records = Vec::new();
                let mut rejected = Vec::new();
                let (mut orders, mut order_lineage) = (Vec::new(), Vec::new());
                let (mut children, mut child_lineage) = (Vec::new(), Vec::new());
                for row in 0..batch.num_rows() {
                    let index = row as u32;
                    let id = ids.value(row).to_owned();
                    records.push(dataferry_core::SourceRecordId(vec![
                        dataferry_core::KeyValue::Str(id.clone()),
                    ]));
                    if deleted.is_valid(row) {
                        continue; // not_deleted: filtered from every target
                    }
                    let mut rows = Vec::new();
                    let mut error = None;
                    if items.is_valid(row) {
                        let elements = items.value(row);
                        let elements = elements.as_struct();
                        let sku = elements.column_by_name("sku").unwrap().as_string::<i32>();
                        let qty = elements.column_by_name("qty").unwrap().as_string::<i32>();
                        for j in 0..elements.len() {
                            let raw = qty.value(j);
                            match serde_json::from_str::<serde_json::Value>(raw)
                                .ok()
                                .and_then(|v| v.as_i64())
                            {
                                Some(q) => rows.push((
                                    id.clone(),
                                    j as i64,
                                    sku.is_valid(j).then(|| sku.value(j).to_owned()),
                                    q,
                                )),
                                None => {
                                    error =
                                        Some(format!("items[{j}].qty: {raw} is not an integer"));
                                    break;
                                }
                            }
                        }
                    }
                    if let Some(error) = error {
                        // One bad child rejects the whole document.
                        rejected.push(RejectedRecord {
                            index,
                            target: Some("order_items".into()),
                            error,
                        });
                        continue;
                    }
                    orders.push((id, status.value(row).to_lowercase()));
                    order_lineage.push(index);
                    child_lineage.extend(rows.iter().map(|_| index));
                    children.extend(rows);
                }
                let strings =
                    |v: Vec<Option<String>>| -> ArrayRef { Arc::new(StringArray::from(v)) };
                let orders_batch = RecordBatch::try_from_iter([
                    (
                        "id",
                        strings(orders.iter().map(|o| Some(o.0.clone())).collect()),
                    ),
                    (
                        "status",
                        strings(orders.iter().map(|o| Some(o.1.clone())).collect()),
                    ),
                ])
                .unwrap();
                let items_batch = RecordBatch::try_from_iter([
                    (
                        "order_id",
                        strings(children.iter().map(|c| Some(c.0.clone())).collect()),
                    ),
                    (
                        "line_no",
                        Arc::new(Int64Array::from_iter_values(children.iter().map(|c| c.1)))
                            as ArrayRef,
                    ),
                    (
                        "sku",
                        strings(children.iter().map(|c| c.2.clone()).collect()),
                    ),
                    (
                        "qty",
                        Arc::new(Int64Array::from_iter_values(children.iter().map(|c| c.3))),
                    ),
                ])
                .unwrap();
                Ok(TransformOutput {
                    records,
                    targets: vec![
                        TargetBatch {
                            target: "orders".into(),
                            batch: orders_batch,
                            source_index: order_lineage,
                        },
                        TargetBatch {
                            target: "order_items".into(),
                            batch: items_batch,
                            source_index: child_lineage,
                        },
                    ],
                    rejected,
                })
            })
            .boxed())
    }
}

// ---- running ----

fn sink(db: &Db) -> (Arc<dyn Sink>, Arc<dyn StateStore>) {
    let mut registry = ConnectorRegistry::new();
    registry.register_state_capable(PostgresConnector, PgStateBackend);
    let conn: RawConnection = serde_json::from_value(json!({
        "name": "warehouse",
        "type": "postgres",
        "url": db.url,
        "schema": "dst",
    }))
    .unwrap();
    let (built, store) = registry.build_with_state(&conn, Roles::SINK).unwrap();
    (built.endpoints.sink.unwrap(), store)
}

fn flow(source: Arc<dyn Source>, entity: Entity, sink: Arc<dyn Sink>) -> Flow {
    Flow {
        name: "orders".into(),
        inputs: vec![Input {
            source,
            entity,
            co_partition: None,
        }],
        plan: Arc::new(GoldenPlan {
            inputs: vec!["orders".into()],
            order: vec!["orders".into(), "order_items".into()],
        }),
        sink,
        target: golden_target(),
        mode: WriteMode::Insert,
        finalizer: None,
        depends_on: Vec::new(),
    }
}

/// The golden runtime.yaml's: dead-letter, partitions of four documents.
fn settings(max_errors: u64) -> Settings {
    Settings {
        workers: 1,
        partition_rows: 4,
        on_error: OnError::DeadLetter,
        max_errors: Some(max_errors),
        ..common::settings()
    }
}

fn runner(
    db: &Db,
    store: Arc<dyn StateStore>,
    flow: Flow,
    max: u64,
    log: Arc<dyn WriteLog>,
) -> Runner {
    Runner::new(store, vec![flow], settings(max))
        .unwrap()
        .with_observer(Arc::new(ChecksumObserver::new(log, db.target())))
}

async fn strings(db: &Db, sql: &str) -> Vec<String> {
    db.client()
        .await
        .query(sql, &[])
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .iter()
        .map(|r| r.get(0))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn golden_0006_lands_nowhere_but_once_in_the_dead_letter_queue_however_often_it_runs() {
    let shop = Shop::start().await;
    shop.insert(golden()).await;
    let db = Db::start("").await;
    let (sink, store) = sink(&db);
    let flow = flow(Arc::new(shop.source()), shop.orders().await, sink);

    let mut runs = Vec::new();
    for attempt in 0..2 {
        // The second run reads every partition again, …0006's included.
        let log: Arc<dyn WriteLog> = Arc::new(MemoryWriteLog::default());
        let report = runner(&db, Arc::clone(&store), flow.clone(), 10, Arc::clone(&log))
            .start(new_run())
            .await
            .unwrap();
        assert!(report.is_complete(), "attempt {attempt}: {report:?}");
        assert_eq!(report.totals.read, 10);
        assert_eq!(report.totals.dead_lettered, 1);

        // No row of …0006 in either table; everything else as expected.
        let expected: BTreeSet<String> = fixture("expected/orders.json")
            .as_array()
            .unwrap()
            .iter()
            .map(|o| o["id"].as_str().unwrap().to_owned())
            .collect();
        let orders: BTreeSet<String> = strings(&db, "SELECT id FROM dst.orders")
            .await
            .into_iter()
            .collect();
        assert_eq!(orders, expected);
        assert!(!orders.contains(DEAD_LETTERED));
        let expected: Vec<String> = fixture("expected/order_items.json")
            .as_array()
            .unwrap()
            .iter()
            .map(|i| {
                format!(
                    "{}/{}/{}/{}",
                    i["order_id"].as_str().unwrap(),
                    i["line_no"],
                    i["sku"].as_str().unwrap(),
                    i["qty"]
                )
            })
            .collect();
        let items = strings(
            &db,
            "SELECT order_id || '/' || line_no || '/' || sku || '/' || qty FROM dst.order_items \
             ORDER BY order_id, line_no",
        )
        .await;
        assert_eq!(items, expected);

        // Exactly one dead letter, the whole document, as this run's.
        let letters = strings(
            &db,
            "SELECT concat_ws(' | ', flow, source_id, run, target, error, \
                    record->>'_id', record->'items'->1->>'qty') \
             FROM dst._ferry_dlq",
        )
        .await;
        assert_eq!(
            letters,
            vec![format!(
                "orders | [\"{DEAD_LETTERED}\"] | {} | order_items | items[1].qty: \"three\" is \
                 not an integer | {DEAD_LETTERED} | \"three\"",
                report.run.0
            )],
            "attempt {attempt}"
        );

        let v = verifier(&db, Arc::clone(&store), std::slice::from_ref(&flow), log)
            .verify(&report.run)
            .await
            .unwrap();
        assert_eq!(v.status, Status::Passed, "{:#?}", v.problems);
        assert_eq!((v.dead_letters.rows, v.dead_letters.passed), (1, true));
        assert_eq!(
            v.dead_letters.reasons[0].example,
            format!("[\"{DEAD_LETTERED}\"]")
        );
        let md = Report::new(&v).to_markdown();
        assert!(
            md.contains("items[1].qty: \"three\" is not an integer"),
            "{md}"
        );
        runs.push(report.run);
    }
    assert_ne!(runs[0], runs[1]);
}

#[tokio::test(flavor = "multi_thread")]
async fn max_errors_3_with_4_bad_documents_stops_the_run_at_the_4th() {
    let shop = Shop::start().await;
    shop.insert(golden()).await;
    // …0006 plus three more: …000b and …000c share a partition, …000d is
    // alone in the last.
    shop.insert(vec![
        order(0x0b, Bson::String("two".into())),
        order(0x0c, Bson::Double(1.5)),
        order(0x0d, Bson::String("many".into())),
    ])
    .await;
    let db = Db::start("").await;
    let (sink, store) = sink(&db);
    let flow = flow(Arc::new(shop.source()), shop.orders().await, sink);
    let log: Arc<dyn WriteLog> = Arc::new(MemoryWriteLog::default());
    let report = runner(&db, store, flow, 3, log)
        .start(new_run())
        .await
        .unwrap();

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
    assert_eq!(report.totals.dead_lettered, 3);
    // The partition of the 4th rolled back: neither its dead letter nor
    // its rows landed.
    assert_eq!(
        strings(
            &db,
            "SELECT source_id FROM dst._ferry_dlq ORDER BY source_id"
        )
        .await,
        vec![
            format!("[\"{DEAD_LETTERED}\"]"),
            "[\"65000000000000000000000b\"]".into(),
            "[\"65000000000000000000000c\"]".into(),
        ]
    );
    assert_eq!(
        db.count("SELECT count(*) FROM dst.orders WHERE id = '65000000000000000000000d'")
            .await,
        0
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_document_that_no_longer_fits_the_schema_is_dead_lettered_and_the_run_verifies() {
    let shop = Shop::start().await;
    shop.insert(golden()).await;
    let entity = shop.orders().await;
    // Written after discovery, with a field the schema doesn't have. Before
    // A7 it failed its whole partition.
    let late = ObjectId::parse_str("65000000000000000000000e").unwrap();
    shop.insert(vec![
        doc! { "_id": late, "status": "PAID", "coupon": "XMAS" },
    ])
    .await;
    let db = Db::start("").await;
    let (sink, store) = sink(&db);
    let flow = flow(Arc::new(shop.source()), entity, sink);
    let log: Arc<dyn WriteLog> = Arc::new(MemoryWriteLog::default());
    let report = runner(&db, Arc::clone(&store), flow.clone(), 10, Arc::clone(&log))
        .start(new_run())
        .await
        .unwrap();
    assert!(report.is_complete(), "{report:?}");
    assert_eq!((report.totals.read, report.totals.dead_lettered), (11, 2));

    let letters = strings(
        &db,
        "SELECT concat_ws(' | ', source_id, coalesce(target, '-'), error, record->>'coupon') \
         FROM dst._ferry_dlq WHERE source_id = '[\"65000000000000000000000e\"]'",
    )
    .await;
    assert_eq!(letters.len(), 1);
    assert!(
        letters[0].starts_with("[\"65000000000000000000000e\"] | - | ")
            && letters[0].contains("field `coupon` is not in the schema")
            && letters[0].ends_with(" | XMAS"),
        "{}",
        letters[0]
    );
    let v = verifier(&db, store, std::slice::from_ref(&flow), log)
        .verify(&report.run)
        .await
        .unwrap();
    assert_eq!(v.status, Status::Passed, "{:#?}", v.problems);
    assert_eq!(v.counts.flows[0].records.rejected, 2);
}
