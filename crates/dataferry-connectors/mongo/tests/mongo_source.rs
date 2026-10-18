//! The Mongo source against a real MongoDB.

use std::collections::BTreeSet;

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Decimal128Type, Float64Type, Int32Type, Int64Type, TimestampMicrosecondType,
};
use arrow_array::{Array, RecordBatch, StructArray};
use dataferry_connector_mongo::MongoSource;
use dataferry_core::missing::is_missing;
use dataferry_core::testkit::check_source;
use dataferry_core::types::{CanonicalField, CanonicalType, DecimalSpec};
use dataferry_core::{
    CoPartition, Entity, FerryError, KeyValue, Partition, PartitionHint, PartitionId,
    PartitionScope, Source, align,
};
use dataferry_registry::rejected::{is_rejected, rejected_rows};
use futures::StreamExt;
use mongodb::bson::spec::BinarySubtype;
use mongodb::bson::{Binary, Bson, DateTime, Decimal128, Document, doc, oid::ObjectId};
use testcontainers_modules::mongo::Mongo;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

struct Db {
    _container: ContainerAsync<Mongo>,
    url: String,
    client: mongodb::Client,
}

async fn mongo() -> Db {
    let container = Mongo::default().start().await.expect("start mongo");
    let port = container.get_host_port_ipv4(27017).await.unwrap();
    let url = format!("mongodb://127.0.0.1:{port}");
    let client = mongodb::Client::with_uri_str(&url).await.unwrap();
    Db {
        _container: container,
        url,
        client,
    }
}

impl Db {
    async fn insert(&self, collection: &str, docs: Vec<Document>) {
        self.client
            .database("shop")
            .collection::<Document>(collection)
            .insert_many(docs)
            .await
            .unwrap();
    }

    fn source(&self) -> MongoSource {
        MongoSource::new(&self.url, "shop").unwrap()
    }
}

/// The golden source documents, parsed from Extended JSON.
fn golden_orders() -> Vec<Document> {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../fixtures/golden/source/orders.json"
    );
    let json: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    json.as_array()
        .unwrap()
        .iter()
        .map(|d| match Bson::try_from(d.clone()).unwrap() {
            Bson::Document(doc) => doc,
            other => panic!("not a document: {other:?}"),
        })
        .collect()
}

async fn entity(source: &MongoSource, name: &str) -> Entity {
    source
        .discover()
        .await
        .unwrap()
        .into_iter()
        .find(|e| e.name() == name)
        .unwrap_or_else(|| panic!("no entity {name}"))
}

fn whole(name: &str) -> Partition {
    Partition {
        id: PartitionId(format!("{name}-whole")),
        entity: name.into(),
        scope: PartitionScope::Whole,
    }
}

async fn read_all(source: &MongoSource, e: &Entity, p: &Partition) -> RecordBatch {
    let batches: Vec<RecordBatch> =
        futures::TryStreamExt::try_collect(source.read(e, p).await.unwrap())
            .await
            .unwrap();
    arrow_select_concat(&e.arrow_schema(), &batches)
}

fn arrow_select_concat(schema: &arrow_schema::SchemaRef, batches: &[RecordBatch]) -> RecordBatch {
    // One batch keeps assertions simple; the golden set is ten rows.
    assert!(batches.len() <= 1, "expected at most one batch");
    batches
        .first()
        .cloned()
        .unwrap_or_else(|| RecordBatch::new_empty(schema.clone()))
}

fn row_of(batch: &RecordBatch, id: &str) -> usize {
    let ids = batch.column_by_name("_id").unwrap().as_string::<i32>();
    (0..ids.len())
        .find(|&i| ids.value(i) == id)
        .unwrap_or_else(|| panic!("no row {id}"))
}

fn utf8(name: &str) -> CanonicalField {
    CanonicalField::new(name, CanonicalType::Utf8, true)
}

fn money() -> CanonicalType {
    CanonicalType::Decimal(DecimalSpec::new(38, 3).unwrap())
}

#[tokio::test]
async fn golden_orders_keep_every_edge_case() {
    let db = mongo().await;
    db.insert("orders", golden_orders()).await;
    let source = db.source();
    let orders = entity(&source, "orders").await;

    let utc = CanonicalType::Timestamp {
        tz: Some("UTC".into()),
    };
    assert_eq!(
        orders.fields(),
        &[
            CanonicalField::new("_id", CanonicalType::Utf8, false),
            utf8("status"),
            CanonicalField::new(
                "customer",
                CanonicalType::Struct(vec![utf8("id"), utf8("email")]),
                true
            ),
            CanonicalField::new("total", money(), true),
            CanonicalField::new("created_at", utc.clone(), true),
            CanonicalField::new(
                "items",
                CanonicalType::List(Box::new(CanonicalField::new(
                    "item",
                    CanonicalType::Struct(vec![
                        utf8("sku"),
                        // int everywhere but one "three": a variant, not a cast.
                        CanonicalField::new("qty", CanonicalType::Json, true),
                        CanonicalField::new("price", money(), true),
                    ]),
                    true
                ))),
                true
            ),
            CanonicalField::new("deleted_at", utc, true),
        ]
    );
    assert!(orders.tracks_missing());

    let b = read_all(&source, &orders, &whole("orders")).await;
    assert_eq!(b.num_rows(), 10);
    let id = |n: u8| format!("6500000000000000000000{n:02x}");

    // deleted_at: missing in …01, explicitly null in …05, set in …04.
    let deleted = b.column_by_name("deleted_at").unwrap();
    let r1 = row_of(&b, &id(1));
    let r4 = row_of(&b, &id(4));
    let r5 = row_of(&b, &id(5));
    assert!(deleted.is_null(r1) && is_missing(&b, r1, "deleted_at"));
    assert!(deleted.is_null(r5) && !is_missing(&b, r5, "deleted_at"));
    assert!(deleted.is_valid(r4));

    // items: an empty array in …02, missing in …03.
    let items = b.column_by_name("items").unwrap().as_list::<i32>();
    let r2 = row_of(&b, &id(2));
    let r3 = row_of(&b, &id(3));
    assert!(items.is_valid(r2) && items.value(r2).is_empty());
    assert!(items.is_null(r3) && is_missing(&b, r3, "items"));

    // The mixed-type qty survives with its original types.
    let r6 = row_of(&b, &id(6));
    let r6_items = items.value(r6);
    let qty = r6_items
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap()
        .column_by_name("qty")
        .unwrap()
        .as_string::<i32>();
    assert_eq!((qty.value(0), qty.value(1)), ("1", "\"three\""));

    // 1.005 is exact, not 1.00499999…
    let total = b
        .column_by_name("total")
        .unwrap()
        .as_primitive::<Decimal128Type>();
    assert_eq!(total.value(row_of(&b, &id(7))), 1005);
    assert_eq!(total.value(r1), 25500);

    // customer.email missing under a present customer, customer null in …0a.
    let customer = b.column_by_name("customer").unwrap().as_struct();
    let r8 = row_of(&b, &id(8));
    let ra = row_of(&b, &id(10));
    assert!(is_missing(&b, r8, "customer.email"));
    assert!(customer.column_by_name("email").unwrap().is_null(r8));
    assert!(customer.is_null(ra) && !is_missing(&b, ra, "customer"));

    // created_at missing in …09; dates are UTC microseconds.
    let created = b
        .column_by_name("created_at")
        .unwrap()
        .as_primitive::<TimestampMicrosecondType>();
    let r9 = row_of(&b, &id(9));
    assert!(created.is_null(r9) && is_missing(&b, r9, "created_at"));
    assert_eq!(created.value(r1), 1_772_359_200_000_000); // 2026-03-01T10:00:00Z
}

#[tokio::test]
async fn every_collection_passes_the_source_contract() {
    let db = mongo().await;
    db.insert("orders", golden_orders()).await;
    // Sparse integer keys, int32 and int64 mixed.
    db.insert(
        "gappy",
        vec![
            doc! { "_id": 1_i32 },
            doc! { "_id": 2_i64 },
            doc! { "_id": 3_i32 },
            doc! { "_id": 10_i64 },
            doc! { "_id": 11_i32 },
            doc! { "_id": 500_i32 },
            doc! { "_id": 501_i64 },
            doc! { "_id": 502_i32 },
            doc! { "_id": 100_000_i32 },
            doc! { "_id": 9_000_000_000_i64 },
        ],
    )
    .await;
    // String keys whose byte order differs from a case-insensitive one.
    db.insert(
        "tags",
        ["B", "a", "Z", "b", "é", "ab", "A", "zz"]
            .iter()
            .map(|k| doc! { "_id": *k })
            .collect(),
    )
    .await;
    // Mixed key types: read as one partition.
    db.insert(
        "mixed",
        vec![
            doc! { "_id": 1_i32 },
            doc! { "_id": "one" },
            doc! { "_id": ObjectId::new() },
        ],
    )
    .await;

    let source = db.source();
    for target_rows in [1, 3, 1000] {
        let violations = check_source(&source, &PartitionHint::rows(target_rows)).await;
        assert!(
            violations.is_empty(),
            "target_rows {target_rows}: {violations:#?}"
        );
    }
    for (name, expected) in [("orders", 4), ("gappy", 4), ("tags", 3), ("mixed", 1)] {
        let e = entity(&source, name).await;
        let parts = source
            .plan_partitions(&e, &PartitionHint::rows(3))
            .await
            .unwrap();
        assert_eq!(parts.len(), expected, "{name}: {parts:#?}");
    }
}

#[tokio::test]
async fn a_field_that_is_sometimes_a_string_lands_in_a_variant() {
    let db = mongo().await;
    let docs: Vec<Document> = (0..200_i32)
        .map(|i| match i {
            57 | 143 => doc! { "_id": i, "n": "n/a", "small": i, "wide": i },
            _ => doc! { "_id": i, "n": i, "small": i, "wide": i64::from(i) * 1_000_000_000 },
        })
        .collect();
    db.insert("readings", docs).await;
    let source = db.source();
    let e = entity(&source, "readings").await;
    assert_eq!(e.field("n").unwrap().ty, CanonicalType::Json);
    // int32 stays int32; int32 mixed with int64 widens, losslessly.
    assert_eq!(e.field("small").unwrap().ty, CanonicalType::Int32);
    assert_eq!(e.field("wide").unwrap().ty, CanonicalType::Int64);

    let b = read_all(&source, &e, &whole("readings")).await;
    assert_eq!(b.num_rows(), 200);
    let n = b.column_by_name("n").unwrap().as_string::<i32>();
    let ids = b.column_by_name("_id").unwrap().as_primitive::<Int32Type>();
    for row in 0..b.num_rows() {
        let expected = match ids.value(row) {
            57 | 143 => "\"n/a\"".to_owned(),
            i => i.to_string(),
        };
        assert_eq!(n.value(row), expected, "row {row}");
    }
}

#[tokio::test]
async fn a_document_that_no_longer_fits_is_read_as_a_rejected_row_naming_it() {
    let db = mongo().await;
    db.insert(
        "readings",
        (0..10_i32).map(|i| doc! { "_id": i, "n": i }).collect(),
    )
    .await;
    let source = db.source().with_batch_rows(4);
    let e = entity(&source, "readings").await;
    assert_eq!(e.field("n").unwrap().ty, CanonicalType::Int32);

    // Written after discovery: none may be cast or dropped, and none may
    // fail the partition the nine others share.
    db.insert(
        "readings",
        vec![
            doc! { "_id": 100_i32, "n": "late" },
            doc! { "_id": 101_i32, "n": 1_i32, "extra": true },
            doc! { "_id": 102_i32, "n": 1.5 },
        ],
    )
    .await;
    let batches: Vec<RecordBatch> =
        futures::TryStreamExt::try_collect(source.read(&e, &whole("readings")).await.unwrap())
            .await
            .unwrap();
    let (rejected, data): (Vec<_>, Vec<_>) = batches.iter().partition(|b| is_rejected(b));
    assert_eq!(data.iter().map(|b| b.num_rows()).sum::<usize>(), 10);
    assert!(data.iter().all(|b| b.schema() == e.arrow_schema()));
    let rows: Vec<_> = rejected
        .iter()
        .flat_map(|b| rejected_rows(&e, b).unwrap())
        .collect();
    let ids: Vec<_> = rows.iter().map(|r| r.id.0.clone()).collect();
    assert_eq!(
        ids,
        [100, 101, 102].map(|i| vec![KeyValue::Int(i)]).to_vec()
    );
    for (row, (id, expected, raw)) in rows.iter().zip([
        (100, "`n` holds String", r#""n":"late""#),
        (101, "field `extra` is not in the schema", r#""extra":true"#),
        (102, "`n` holds Double", r#""n":1.5"#),
    ]) {
        assert!(
            row.error.contains(&format!("_id {id}")) && row.error.contains(expected),
            "{}",
            row.error
        );
        let record = row.record.as_deref().unwrap();
        assert!(record.contains(raw), "{record}");
        serde_json::from_str::<serde_json::Value>(record).unwrap();
    }
}

#[tokio::test]
async fn an_id_of_another_type_lands_in_the_first_partition_not_in_none() {
    let db = mongo().await;
    db.insert(
        "events",
        (1..=9_i32).map(|i| doc! { "_id": i * 10 }).collect(),
    )
    .await;
    let source = db.source().with_batch_rows(1);
    let e = entity(&source, "events").await;
    let parts = source
        .plan_partitions(&e, &PartitionHint::rows(3))
        .await
        .unwrap();
    assert_eq!(parts.len(), 3);
    // Written after planning, so no boundary knows about them: a string
    // key and a fractional one.
    db.insert("events", vec![doc! { "_id": "late" }, doc! { "_id": 69.5 }])
        .await;

    let mut ids = BTreeSet::new();
    let mut failures = Vec::new();
    for p in &parts {
        let mut stream = source.read(&e, p).await.unwrap();
        while let Some(batch) = stream.next().await {
            match batch {
                Ok(b) => ids.extend(
                    b.column_by_name("_id")
                        .unwrap()
                        .as_primitive::<Int32Type>()
                        .values()
                        .iter()
                        .copied(),
                ),
                Err(e) => failures.push((p.id.0.clone(), e.to_string())),
            }
        }
    }
    assert_eq!(ids, (1..=9).map(|i| i * 10).collect());
    // Each late document is read by exactly one partition, and fails it.
    assert_eq!(failures.len(), 2, "{failures:#?}");
    assert!(
        failures
            .iter()
            .any(|(p, e)| p == "events-0" && e.contains("_id \"late\"")),
        "{failures:#?}"
    );
    assert!(
        failures
            .iter()
            .any(|(p, e)| p == "events-1" && e.contains("_id 69.5")),
        "{failures:#?}"
    );
}

#[tokio::test]
async fn co_partitioned_items_follow_their_orders() {
    let db = mongo().await;
    let orders: Vec<ObjectId> = (1..=8)
        .map(|i| ObjectId::parse_str(format!("6500000000000000000000{i:02x}")).unwrap())
        .collect();
    db.insert(
        "orders",
        orders.iter().map(|o| doc! { "_id": *o }).collect(),
    )
    .await;
    let mut items: Vec<Document> = orders
        .iter()
        .flat_map(|o| (0..3).map(move |line| doc! { "order_id": *o, "line": line }))
        .collect();
    // An item whose order is unknown lands in the first partition.
    items.push(doc! { "order_id": Bson::Null, "line": 0 });
    db.insert("order_items", items).await;

    let source = db.source();
    let order_entity = entity(&source, "orders").await;
    let item_entity = entity(&source, "order_items").await;
    let driver = source
        .plan_partitions(&order_entity, &PartitionHint::rows(3))
        .await
        .unwrap();
    let cp = CoPartition::from_partitions("order_id", &driver).unwrap();
    let hint = PartitionHint {
        target_rows: 3,
        co_partition: Some(cp),
    };
    let children = source.plan_partitions(&item_entity, &hint).await.unwrap();
    let pairs = align(&driver, &children).unwrap();

    let mut seen = 0;
    for (d, c) in &pairs {
        let order_ids: BTreeSet<String> = {
            let b = read_all(&source, &order_entity, d).await;
            let ids = b.column_by_name("_id").unwrap().as_string::<i32>();
            ids.iter().map(|v| v.unwrap().to_owned()).collect()
        };
        let b = read_all(&source, &item_entity, c).await;
        let refs = b.column_by_name("order_id").unwrap().as_string::<i32>();
        for r in refs.iter() {
            match r {
                Some(order) => assert!(order_ids.contains(order), "{order} not with its order"),
                None => assert_eq!(c.id.0, "order_items-0", "null order_id"),
            }
        }
        seen += b.num_rows();
    }
    assert_eq!(seen, 8 * 3 + 1);

    let ints = PartitionHint {
        target_rows: 3,
        co_partition: Some(CoPartition::new("order_id", vec![KeyValue::Int(5)]).unwrap()),
    };
    match source.plan_partitions(&item_entity, &ints).await {
        Err(FerryError::CoPartitionUnsupported { column, .. }) => assert_eq!(column, "order_id"),
        other => panic!("expected CoPartitionUnsupported, got {other:?}"),
    }
}

#[tokio::test]
async fn bson_types_convert_explicitly() {
    let db = mongo().await;
    let uuid = [7_u8; 16];
    let oid = ObjectId::parse_str("650000000000000000000099").unwrap();
    db.insert(
        "kinds",
        vec![doc! {
            "_id": 1_i32,
            "flag": true,
            "i32": i32::MIN,
            "i64": i64::MAX,
            "f64": 0.1,
            "dec": "-12345678901234567890.123".parse::<Decimal128>().unwrap(),
            "oid": oid,
            "at": DateTime::from_millis(-1),
            "uuid": Binary { subtype: BinarySubtype::Uuid, bytes: uuid.to_vec() },
            "bin": Binary { subtype: BinarySubtype::Generic, bytes: vec![0, 255] },
            "grid": [[1_i32, 2_i32], [], Bson::Null],
            "ts": Bson::Timestamp(mongodb::bson::Timestamp { time: 5, increment: 1 }),
        }],
    )
    .await;
    let source = db.source();
    let e = entity(&source, "kinds").await;
    let ty = |name: &str| e.field(name).unwrap().ty.clone();
    assert_eq!(ty("i32"), CanonicalType::Int32);
    assert_eq!(ty("i64"), CanonicalType::Int64);
    assert_eq!(
        ty("dec"),
        CanonicalType::Decimal(DecimalSpec::new(38, 3).unwrap())
    );
    assert_eq!(ty("uuid"), CanonicalType::Uuid);
    assert_eq!(ty("ts"), CanonicalType::Json);

    let b = read_all(&source, &e, &whole("kinds")).await;
    let col = |name: &str| b.column_by_name(name).unwrap().clone();
    assert!(col("flag").as_boolean().value(0));
    assert_eq!(col("i32").as_primitive::<Int32Type>().value(0), i32::MIN);
    assert_eq!(col("i64").as_primitive::<Int64Type>().value(0), i64::MAX);
    assert_eq!(col("f64").as_primitive::<Float64Type>().value(0), 0.1);
    assert_eq!(
        col("dec").as_primitive::<Decimal128Type>().value(0),
        -12_345_678_901_234_567_890_123_i128
    );
    assert_eq!(col("oid").as_string::<i32>().value(0), oid.to_hex());
    assert_eq!(
        col("at")
            .as_primitive::<TimestampMicrosecondType>()
            .value(0),
        -1000
    );
    assert_eq!(col("uuid").as_fixed_size_binary().value(0), uuid);
    assert_eq!(col("bin").as_binary::<i32>().value(0), [0, 255]);
    let grid = col("grid");
    let grid = grid.as_list::<i32>().value(0);
    let grid = grid.as_list::<i32>();
    assert_eq!(grid.len(), 3);
    assert_eq!(grid.value(0).as_primitive::<Int32Type>().values(), &[1, 2]);
    assert!(grid.value(1).is_empty() && grid.is_null(2));
    assert_eq!(
        col("ts").as_string::<i32>().value(0),
        r#"{"$timestamp":{"t":5,"i":1}}"#
    );
}

#[tokio::test]
async fn collections_with_reserved_field_names_are_reported_by_name() {
    let db = mongo().await;
    db.insert("ok", vec![doc! { "_id": 1_i32 }]).await;
    db.insert("bad", vec![doc! { "_id": 1_i32, "_ferry_x": 1_i32 }])
        .await;
    let err = db.source().discover().await.unwrap_err().to_string();
    assert!(err.contains("bad:") && err.contains("_ferry_x"), "{err}");
    assert!(!err.contains("ok:"), "{err}");
}

#[tokio::test]
async fn a_small_sample_still_finds_a_consistent_schema() {
    let db = mongo().await;
    db.insert(
        "big",
        (0..500_i32).map(|i| doc! { "_id": i, "n": i }).collect(),
    )
    .await;
    // Fewer sampled documents than the collection holds: `$sample` path.
    let source = db.source().with_sample_size(20);
    let e = entity(&source, "big").await;
    assert_eq!(e.field("n").unwrap().ty, CanonicalType::Int32);
    let violations = check_source(&source, &PartitionHint::rows(128)).await;
    assert!(violations.is_empty(), "{violations:#?}");
}

#[tokio::test]
async fn an_object_id_key_column_refuses_a_later_string_key() {
    let db = mongo().await;
    let hex = "650000000000000000000001";
    db.insert(
        "orders",
        vec![doc! { "_id": ObjectId::parse_str(hex).unwrap() }],
    )
    .await;
    let source = db.source();
    let e = entity(&source, "orders").await;
    // Same text as the ObjectId: accepting it would give two documents one key.
    db.insert("orders", vec![doc! { "_id": hex }]).await;
    let err = futures::TryStreamExt::try_collect::<Vec<_>>(
        source.read(&e, &whole("orders")).await.unwrap(),
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(
        err.contains(&format!("_id \"{hex}\""))
            && err.contains("holds String but the schema inferred at discovery says ObjectId"),
        "{err}"
    );
}

#[tokio::test]
async fn string_keys_split_by_bytes_whatever_the_default_collation() {
    use mongodb::options::{Collation, CollationStrength};
    let db = mongo().await;
    db.client
        .database("shop")
        .create_collection("tags")
        .collation(
            Collation::builder()
                .locale("en")
                .strength(CollationStrength::Secondary)
                .build(),
        )
        .await
        .unwrap();
    db.insert(
        "tags",
        // Byte order B D F a c e; the collation's a B c D e F.
        ["a", "B", "c", "D", "e", "F", "ab", "Gh", "z", "Y"]
            .iter()
            .map(|k| doc! { "_id": *k })
            .collect(),
    )
    .await;
    let source = db.source();
    for target_rows in [1, 2, 3] {
        let violations = check_source(&source, &PartitionHint::rows(target_rows)).await;
        assert!(
            violations.is_empty(),
            "target_rows {target_rows}: {violations:#?}"
        );
    }
}

#[tokio::test]
async fn time_series_collections_are_refused_by_name_and_views_skipped() {
    use mongodb::options::TimeseriesOptions;
    let db = mongo().await;
    db.insert("orders", vec![doc! { "_id": 1_i32 }]).await;
    let shop = db.client.database("shop");
    shop.create_collection("open_orders")
        .view_on("orders".to_owned())
        .pipeline(vec![doc! { "$match": {} }])
        .await
        .unwrap();
    let source = db.source();
    let names: Vec<_> = source
        .discover()
        .await
        .unwrap()
        .iter()
        .map(|e| e.name().to_owned())
        .collect();
    assert_eq!(names, ["orders"]);

    shop.create_collection("metrics")
        .timeseries(
            TimeseriesOptions::builder()
                .time_field("at".to_owned())
                .build(),
        )
        .await
        .unwrap();
    let err = db.source().discover().await.unwrap_err().to_string();
    assert!(
        err.contains("metrics: time-series collections are not supported yet"),
        "{err}"
    );
}
