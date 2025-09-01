//! The Postgres sink against a real Postgres: the generic sink contract
//! suite under every strategy it offers, DDL, type fidelity, and the
//! checks `prepare` makes on tables that already exist.

mod common;

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::builder::{Int32Builder, ListBuilder, StringBuilder};
use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Decimal256Array,
    FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, RecordBatch, StringArray, StructArray, TimestampMicrosecondArray, UInt64Array,
};
use arrow_buffer::i256;
use arrow_schema::{DataType, Field, Fields, Schema};
use common::{Run, db, int64, lease, orders, partition, sink_fixture, utf8};
use ferry_connector_postgres::{PostgresSink, PostgresSource};
use ferry_core::testkit::check_sink;
use ferry_core::types::{CanonicalField, CanonicalType, DecimalSpec};
use ferry_core::{IdempotencyStrategy, Sink, Source, TargetSchema, TargetTable, WriteMode};
use futures::TryStreamExt;

/// The generic suite on a plain sink. It re-runs a committed partition
/// under made-up epochs, which a real state store never grants once the
/// partition is done, so the sink with the state store is checked by the
/// state-capable suite instead (`pg_connector`).
#[tokio::test]
async fn passes_the_sink_contract_in_every_mode() {
    let db = db().await;
    for (schema, mode) in [
        ("plain_insert", WriteMode::Insert),
        ("plain_upsert", WriteMode::Upsert),
    ] {
        let sink = db.plain_sink(schema);
        let strategy = sink.capabilities().await.unwrap().strategy_for(mode);
        assert_eq!(
            strategy,
            Some(IdempotencyStrategy::DeletePartitionThenInsert),
            "{schema}"
        );
        let violations = check_sink(sink.as_ref(), &db.probe(schema), &sink_fixture(mode)).await;
        assert!(violations.is_empty(), "{schema}: {violations:#?}");
    }
}

#[tokio::test]
async fn only_a_sink_bound_to_a_ledger_offers_txn_with_state() {
    let db = db().await;
    let plain = db.plain_sink("public").capabilities().await.unwrap();
    assert_eq!(
        plain.strategies,
        [
            IdempotencyStrategy::DeletePartitionThenInsert,
            IdempotencyStrategy::UpsertByKey
        ]
    );
    assert!(plain.bulk_copy);
    let (sink, _) = db.sink_with_state("public");
    assert_eq!(
        sink.capabilities().await.unwrap().strategies[0],
        IdempotencyStrategy::TxnWithState
    );

    // Asking a plain sink for TxnWithState anyway is refused up front.
    let err = db
        .plain_sink("public")
        .begin(
            &partition("p"),
            &lease("p", 1),
            IdempotencyStrategy::TxnWithState,
        )
        .await
        .err()
        .expect("refused");
    assert!(err.to_string().contains("TxnWithState"), "{err}");
}

#[tokio::test]
async fn prepare_creates_the_schema_and_tables_with_keys() {
    let db = db().await;
    let sink = db.plain_sink("shop");
    sink.prepare(&common::order_schema(), WriteMode::Insert)
        .await
        .unwrap();
    let columns = db
        .strings(
            "SELECT table_name || '.' || column_name || ' ' || data_type || ' ' || is_nullable \
             FROM information_schema.columns WHERE table_schema = 'shop' AND table_name = 'order_items' \
             ORDER BY ordinal_position",
        )
        .await;
    assert_eq!(
        columns,
        [
            "order_items.order_id text NO",
            "order_items.line_no bigint NO",
            "order_items.sku text YES",
            "order_items.qty bigint YES",
        ]
    );
    let pk = db
        .strings(
            "SELECT string_agg(a.attname, ',' ORDER BY k.ord) FROM pg_index i \
             CROSS JOIN LATERAL unnest(i.indkey::int2[]) WITH ORDINALITY k(attnum, ord) \
             JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum \
             WHERE i.indrelid = 'shop.order_items'::regclass AND i.indisprimary",
        )
        .await;
    assert_eq!(pk, ["order_id,line_no"]);

    // Preparing again validates the tables it created, for any mode.
    sink.prepare(&common::order_schema(), WriteMode::Upsert)
        .await
        .unwrap();
}

fn table(name: &str, fields: Vec<CanonicalField>, key: &[&str]) -> TargetTable {
    TargetTable {
        name: name.into(),
        fields,
        primary_key: key.iter().map(|k| k.to_string()).collect(),
        foreign_keys: Vec::new(),
    }
}

fn schema(tables: Vec<TargetTable>) -> TargetSchema {
    TargetSchema::new(tables).unwrap()
}

#[tokio::test]
async fn prepare_reports_every_incompatible_existing_table_and_creates_nothing() {
    let db = db().await;
    db.client
        .batch_execute(
            "CREATE TABLE public.typed (id text PRIMARY KEY, n integer);
             CREATE TABLE public.extra (id text PRIMARY KEY, required text NOT NULL);
             CREATE TABLE public.loose (id text, v text);",
        )
        .await
        .unwrap();
    let target = schema(vec![
        // `n` is integer in the database, but the target holds int64s.
        table("typed", vec![utf8("id", false), int64("n", true)], &["id"]),
        // `required` would be left null by every insert.
        table("extra", vec![utf8("id", false)], &["id"]),
        // Upsert needs a unique index on the key; `loose` has none.
        table("loose", vec![utf8("id", false), utf8("v", true)], &["id"]),
        // Missing a column the target writes.
        table("fresh", vec![utf8("id", false)], &["id"]),
    ]);
    db.client
        .batch_execute("CREATE TABLE public.fresh (other text)")
        .await
        .unwrap();
    let target_with_new = schema(
        target
            .tables()
            .iter()
            .cloned()
            .chain([table("brand_new", vec![utf8("id", false)], &["id"])])
            .collect(),
    );
    let err = db
        .plain_sink("public")
        .prepare(&target_with_new, WriteMode::Upsert)
        .await
        .unwrap_err()
        .to_string();
    for expected in [
        "`typed`: column `n` is integer, but the target needs bigint",
        "`extra`: column `required` is NOT NULL with no default",
        "`loose`: upsert needs a primary key or unique index on exactly (id)",
        "`fresh`: column `id` is missing",
    ] {
        assert!(err.contains(expected), "missing {expected:?} in:\n{err}");
    }
    let created = db
        .strings("SELECT relname::text FROM pg_class WHERE relname = 'brand_new'")
        .await;
    assert!(created.is_empty(), "a failed prepare must create nothing");
}

#[tokio::test]
async fn prepare_accepts_equivalent_existing_types() {
    let db = db().await;
    db.client
        .batch_execute(
            "CREATE TABLE public.docs (id varchar(40) PRIMARY KEY, body json, \
             created timestamptz DEFAULT now() NOT NULL)",
        )
        .await
        .unwrap();
    let target = schema(vec![table(
        "docs",
        vec![
            utf8("id", false),
            CanonicalField::new("body", CanonicalType::Json, true),
        ],
        &["id"],
    )]);
    let sink = db.plain_sink("public");
    sink.prepare(&target, WriteMode::Upsert).await.unwrap();

    let id: ArrayRef = Arc::new(StringArray::from(vec!["d1"]));
    let body: ArrayRef = Arc::new(StringArray::from(vec![r#"{"a": [1, 2]}"#]));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(
            target.tables()[0]
                .fields
                .iter()
                .map(CanonicalField::to_arrow)
                .collect::<Vec<_>>(),
        )),
        vec![id, body],
    )
    .unwrap();
    let mut txn = sink
        .begin(
            &partition("p"),
            &lease("p", 1),
            IdempotencyStrategy::UpsertByKey,
        )
        .await
        .unwrap();
    txn.write("docs", batch).await.unwrap();
    txn.commit().await.unwrap();
    assert_eq!(
        db.rows("SELECT body FROM public.docs").await,
        [r#"{"a": [1, 2]}"#]
    );
}

#[tokio::test]
async fn a_keyless_table_needs_the_state_store_for_inserts_and_a_key_for_upserts() {
    let db = db().await;
    let keyless = schema(vec![table("events", vec![utf8("what", true)], &[])]);
    let err = db
        .plain_sink("public")
        .prepare(&keyless, WriteMode::Insert)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("`events`: has no key"), "{err}");

    let (sink, store) = db.sink_with_state("public");
    let err = sink
        .prepare(&keyless, WriteMode::Upsert)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("upsert needs a key"), "{err}");

    // Under TxnWithState the ledger records each partition once, so a
    // keyless insert is accepted; DeletePartitionThenInsert still refuses it.
    sink.prepare(&keyless, WriteMode::Insert).await.unwrap();
    let what: ArrayRef = Arc::new(StringArray::from(vec!["x"]));
    let batch = RecordBatch::try_from_iter([("what", what)]).unwrap();
    let run = Run::plan(store, &["p"]).await;
    let mut txn = sink
        .begin(
            &partition("p"),
            &run.lease("p").await,
            IdempotencyStrategy::TxnWithState,
        )
        .await
        .unwrap();
    txn.write("events", batch.clone()).await.unwrap();
    txn.commit().await.unwrap();
    assert_eq!(db.count("public", "events").await, 1);
    assert!(run.is_done("p").await);

    let mut txn = sink
        .begin(
            &partition("q"),
            &lease("q", 1),
            IdempotencyStrategy::DeletePartitionThenInsert,
        )
        .await
        .unwrap();
    let err = txn.write("events", batch).await.unwrap_err().to_string();
    assert!(err.contains("has no key"), "{err}");
    txn.abort().await.unwrap();
}

#[tokio::test]
async fn writes_are_checked_against_the_prepared_table() {
    let db = db().await;
    let sink = db.plain_sink("public");
    sink.prepare(&common::order_schema(), WriteMode::Insert)
        .await
        .unwrap();
    let mut txn = sink
        .begin(
            &partition("p"),
            &lease("p", 1),
            IdempotencyStrategy::DeletePartitionThenInsert,
        )
        .await
        .unwrap();
    let err = txn
        .write("customers", orders(&[("o1", "new")]))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("not prepared"), "{err}");
    let err = txn
        .write("order_items", orders(&[("o1", "new")]))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("expects columns"), "{err}");
    let nullable_id = {
        let id: ArrayRef = Arc::new(StringArray::from(vec![None::<&str>]));
        let status: ArrayRef = Arc::new(StringArray::from(vec![Some("new")]));
        RecordBatch::try_from_iter([("id", id), ("status", status)]).unwrap()
    };
    let err = txn.write("orders", nullable_id).await.unwrap_err();
    assert!(err.to_string().contains("not nullable"), "{err}");
    txn.abort().await.unwrap();
    assert_eq!(db.count("public", "orders").await, 0);
}

fn field(name: &str, ty: CanonicalType) -> CanonicalField {
    CanonicalField::new(name, ty, true)
}

fn list_of(ty: CanonicalType) -> CanonicalType {
    CanonicalType::List(Box::new(CanonicalField::new("item", ty, true)))
}

/// Every canonical type the Postgres source also reads back to itself.
fn round_trip_table() -> TargetTable {
    let utc = CanonicalType::Timestamp {
        tz: Some(Arc::from("UTC")),
    };
    table(
        "all_types",
        vec![
            CanonicalField::new("id", CanonicalType::Int64, false),
            field("i16", CanonicalType::Int16),
            field("i32", CanonicalType::Int32),
            field("f32", CanonicalType::Float32),
            field("f64", CanonicalType::Float64),
            field(
                "dec",
                CanonicalType::Decimal(DecimalSpec::new(12, 3).unwrap()),
            ),
            field(
                "wide",
                CanonicalType::Decimal(DecimalSpec::new(50, 5).unwrap()),
            ),
            field("flag", CanonicalType::Bool),
            field("name", CanonicalType::Utf8),
            field("raw", CanonicalType::Binary),
            field("day", CanonicalType::Date),
            field("local", CanonicalType::Timestamp { tz: None }),
            field("instant", utc),
            field("uid", CanonicalType::Uuid),
            field("doc", CanonicalType::Json),
            field("ints", list_of(CanonicalType::Int32)),
            field("tags", list_of(CanonicalType::Utf8)),
        ],
        &["id"],
    )
}

fn round_trip_batch(t: &TargetTable) -> RecordBatch {
    let mut ints = ListBuilder::new(Int32Builder::new());
    ints.append_value([Some(1), None, Some(-3)]);
    ints.append_value([]);
    ints.append_null();
    let mut tags = ListBuilder::new(StringBuilder::new());
    tags.append_value([Some("a"), Some("ü")]);
    tags.append_null();
    tags.append_value([None::<&str>]);
    let wide = Decimal256Array::from(vec![
        Some(i256::from_string("-123456789012345678901234567890123456789012345").unwrap()),
        Some(i256::ZERO),
        None,
    ])
    .with_precision_and_scale(50, 5)
    .unwrap();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![1, 2, 3])),
        Arc::new(Int16Array::from(vec![Some(i16::MIN), Some(0), None])),
        Arc::new(Int32Array::from(vec![Some(i32::MAX), Some(-1), None])),
        Arc::new(Float32Array::from(vec![Some(1.5), Some(f32::NAN), None])),
        Arc::new(Float64Array::from(vec![
            Some(-0.1),
            Some(f64::INFINITY),
            None,
        ])),
        Arc::new(
            Decimal128Array::from(vec![Some(123_456_789), Some(-5), None])
                .with_precision_and_scale(12, 3)
                .unwrap(),
        ),
        Arc::new(wide),
        Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])),
        Arc::new(StringArray::from(vec![
            Some("plain"),
            Some("quote \" and tab\t"),
            None,
        ])),
        Arc::new(BinaryArray::from(vec![
            Some(&[0u8, 255, 10][..]),
            Some(&[][..]),
            None,
        ])),
        // 1970-01-01, 2000-01-01 (Postgres's epoch), and before both.
        Arc::new(Date32Array::from(vec![Some(0), Some(10_957), Some(-1)])),
        Arc::new(TimestampMicrosecondArray::from(vec![
            Some(1_700_000_000_123_456),
            Some(-1),
            None,
        ])),
        Arc::new(
            TimestampMicrosecondArray::from(vec![Some(946_684_800_000_000), Some(0), None])
                .with_timezone("UTC"),
        ),
        Arc::new(
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                [Some([7u8; 16]), None, Some([0u8; 16])].into_iter(),
                16,
            )
            .unwrap(),
        ),
        // Already in jsonb's normal form, so it reads back unchanged.
        Arc::new(StringArray::from(vec![
            Some(r#"{"a": 1, "b": [true, null]}"#),
            Some("[]"),
            None,
        ])),
        Arc::new(ints.finish()),
        Arc::new(tags.finish()),
    ];
    let schema = Schema::new(
        t.fields
            .iter()
            .map(CanonicalField::to_arrow)
            .collect::<Vec<_>>(),
    );
    RecordBatch::try_new(Arc::new(schema), columns).unwrap()
}

#[tokio::test]
async fn every_canonical_type_round_trips_through_the_postgres_source() {
    let db = db().await;
    let t = round_trip_table();
    let batch = round_trip_batch(&t);
    let sink = PostgresSink::new(&db.url, "public").unwrap();
    sink.prepare(&schema(vec![t]), WriteMode::Insert)
        .await
        .unwrap();
    let mut txn = sink
        .begin(
            &partition("p"),
            &lease("p", 1),
            IdempotencyStrategy::DeletePartitionThenInsert,
        )
        .await
        .unwrap();
    txn.write("all_types", batch.clone()).await.unwrap();
    let summary = txn.commit().await.unwrap();
    assert_eq!(summary.rows_written["all_types"], 3);

    let source = PostgresSource::new(&db.url, "public").unwrap();
    let entity = source
        .discover()
        .await
        .unwrap()
        .into_iter()
        .find(|e| e.name() == "all_types")
        .unwrap();
    let read: Vec<RecordBatch> = source
        .read(&entity, &partition("p"))
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(read.len(), 1);
    // Compared column by column: NaN != NaN, so whole-batch equality would
    // fail on the float columns for the wrong reason.
    for (i, f) in batch.schema().fields().iter().enumerate() {
        let (want, got) = (batch.column(i), read[0].column(i));
        assert_eq!(got.data_type(), want.data_type(), "{}", f.name());
        if f.name().starts_with('f') {
            assert_eq!(format!("{got:?}"), format!("{want:?}"), "{}", f.name());
        } else {
            assert_eq!(got.to_data(), want.to_data(), "{}", f.name());
        }
    }
}

#[tokio::test]
async fn types_without_a_postgres_equivalent_widen_or_become_documents() {
    let db = db().await;
    let doc_fields = Fields::from(vec![
        Field::new("sku", DataType::Utf8, true),
        Field::new("price", DataType::Decimal128(10, 2), true),
    ]);
    let t = table(
        "widened",
        vec![
            CanonicalField::new("id", CanonicalType::Int8, false),
            field("big", CanonicalType::UInt64),
            field(
                "item",
                CanonicalType::Struct(vec![
                    field("sku", CanonicalType::Utf8),
                    field(
                        "price",
                        CanonicalType::Decimal(DecimalSpec::new(10, 2).unwrap()),
                    ),
                ]),
            ),
        ],
        &["id"],
    );
    let item = StructArray::new(
        doc_fields,
        vec![
            Arc::new(StringArray::from(vec![Some("A-1"), None])),
            Arc::new(
                Decimal128Array::from(vec![Some(1999), Some(-5)])
                    .with_precision_and_scale(10, 2)
                    .unwrap(),
            ),
        ],
        None,
    );
    let schema_ = Schema::new(
        t.fields
            .iter()
            .map(CanonicalField::to_arrow)
            .collect::<Vec<_>>(),
    );
    let batch = RecordBatch::try_new(
        Arc::new(schema_),
        vec![
            Arc::new(Int8Array::from(vec![-128, 127])),
            Arc::new(UInt64Array::from(vec![Some(u64::MAX), None])),
            Arc::new(item),
        ],
    )
    .unwrap();
    let sink = db.plain_sink("public");
    sink.prepare(&schema(vec![t]), WriteMode::Upsert)
        .await
        .unwrap();
    let mut txn = sink
        .begin(
            &partition("p"),
            &lease("p", 1),
            IdempotencyStrategy::UpsertByKey,
        )
        .await
        .unwrap();
    txn.write("widened", batch).await.unwrap();
    txn.commit().await.unwrap();

    let types: HashMap<String, String> = db
        .client
        .query(
            "SELECT column_name::text, data_type::text FROM information_schema.columns \
             WHERE table_name = 'widened'",
            &[],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    assert_eq!(types["id"], "smallint");
    assert_eq!(types["big"], "numeric");
    assert_eq!(types["item"], "jsonb");
    assert_eq!(
        db.rows("SELECT row(id, big, item) FROM public.widened ORDER BY id")
            .await,
        [
            r#"(-128,18446744073709551615,"{""sku"": ""A-1"", ""price"": 19.99}")"#,
            r#"(127,,"{""sku"": null, ""price"": -0.05}")"#,
        ]
    );
}
