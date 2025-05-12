//! The Postgres source against a real Postgres.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Decimal128Type, Decimal256Type, Float32Type, Float64Type, Int16Type, Int32Type,
    Int64Type, TimestampMicrosecondType,
};
use arrow_array::{Array, RecordBatch};
use ferry_connector_postgres::PostgresSource;
use ferry_core::testkit::check_source;
use ferry_core::types::{CanonicalType, DecimalSpec};
use ferry_core::{
    CoPartition, Entity, FerryError, KeyValue, Partition, PartitionHint, PartitionId,
    PartitionScope, Source, align,
};
use futures::TryStreamExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

struct Db {
    _container: ContainerAsync<Postgres>,
    _client: tokio_postgres::Client,
    url: String,
}

/// A fresh Postgres with `setup` applied.
async fn db(setup: &str) -> Db {
    let container = Postgres::default().start().await.expect("start postgres");
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(connection);
    client.batch_execute(setup).await.unwrap();
    Db {
        _container: container,
        _client: client,
        url,
    }
}

fn source(db: &Db) -> PostgresSource {
    PostgresSource::new(&db.url, "public").unwrap()
}

async fn entity(source: &PostgresSource, name: &str) -> Entity {
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

async fn read_all(source: &PostgresSource, e: &Entity, p: &Partition) -> Vec<RecordBatch> {
    source
        .read(e, p)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap()
}

const ALL_TYPES: &str = r#"
CREATE TABLE all_types (
    id          int8 PRIMARY KEY,
    small       int2,
    medium      int4,
    big         int8 NOT NULL,
    real4       float4,
    real8       float8,
    money       numeric(38, 10),
    wide        numeric(60, 4),
    flag        bool,
    name        text,
    code        varchar(8),
    blob        bytea,
    born        date,
    seen        timestamp,
    seen_tz     timestamptz,
    uid         uuid,
    doc         json,
    docb        jsonb,
    tags        text[],
    scores      int4[]
);
INSERT INTO all_types VALUES (
    1, -32768, 2147483647, -9223372036854775808, 1.5, -2.25,
    '-1234567890123456789012345678.0123456789',
    '123456789012345678901234567890123456789012345678901234.5678',
    true, 'zoë', 'ab', '\x00ff10', '1999-12-31',
    '2026-03-01 10:00:00.123456', '2026-03-01 10:00:00.000001+05:30',
    'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
    '{"a": [1, 2]}', '{"b": null}',
    ARRAY['x', NULL, 'z'], ARRAY[]::int4[]
);
INSERT INTO all_types (id, big) VALUES (2, 0);
"#;

#[tokio::test]
async fn every_supported_type_round_trips_including_nulls() {
    let db = db(ALL_TYPES).await;
    let source = source(&db);
    let e = entity(&source, "all_types").await;

    let ty = |c: &str| e.field(c).unwrap().ty.clone();
    assert_eq!(
        ty("money"),
        CanonicalType::Decimal(DecimalSpec::new(38, 10).unwrap())
    );
    assert_eq!(
        ty("seen_tz"),
        CanonicalType::Timestamp {
            tz: Some(Arc::from("UTC"))
        }
    );
    assert!(!e.field("big").unwrap().nullable, "NOT NULL is kept");
    assert!(e.field("small").unwrap().nullable);
    assert_eq!(e.primary_key(), ["id"]);

    let batches = read_all(&source, &e, &whole("all_types")).await;
    assert_eq!(batches.len(), 1);
    let b = &batches[0];
    assert_eq!(b.schema(), e.arrow_schema());
    assert_eq!(b.num_rows(), 2);
    let col = |c: &str| b.column_by_name(c).unwrap();

    assert_eq!(col("small").as_primitive::<Int16Type>().value(0), i16::MIN);
    assert_eq!(col("medium").as_primitive::<Int32Type>().value(0), i32::MAX);
    assert_eq!(col("big").as_primitive::<Int64Type>().value(0), i64::MIN);
    assert_eq!(col("real4").as_primitive::<Float32Type>().value(0), 1.5);
    assert_eq!(col("real8").as_primitive::<Float64Type>().value(0), -2.25);
    assert_eq!(
        col("money").as_primitive::<Decimal128Type>().value(0),
        -12_345_678_901_234_567_890_123_456_780_123_456_789
    );
    let wide = col("wide").as_primitive::<Decimal256Type>().value(0);
    assert_eq!(
        wide.to_string(),
        "1234567890123456789012345678901234567890123456789012345678"
    );
    assert!(col("flag").as_boolean().value(0));
    assert_eq!(col("name").as_string::<i32>().value(0), "zoë");
    assert_eq!(col("code").as_string::<i32>().value(0), "ab");
    assert_eq!(col("blob").as_binary::<i32>().value(0), [0x00, 0xff, 0x10]);
    assert_eq!(col("born").as_primitive::<Date32Type>().value(0), 10_956);
    assert_eq!(
        col("seen")
            .as_primitive::<TimestampMicrosecondType>()
            .value(0),
        1_772_359_200_123_456
    );
    // 10:00:00.000001+05:30 is 04:30:00.000001 UTC.
    assert_eq!(
        col("seen_tz")
            .as_primitive::<TimestampMicrosecondType>()
            .value(0),
        1_772_339_400_000_001
    );
    assert_eq!(
        col("uid").as_fixed_size_binary().value(0),
        [
            0xa0, 0xee, 0xbc, 0x99, 0x9c, 0x0b, 0x4e, 0xf8, 0xbb, 0x6d, 0x6b, 0xb9, 0xbd, 0x38,
            0x0a, 0x11
        ]
    );
    assert_eq!(col("doc").as_string::<i32>().value(0), r#"{"a": [1, 2]}"#);
    assert_eq!(col("docb").as_string::<i32>().value(0), r#"{"b": null}"#);

    let tags = col("tags").as_list::<i32>().value(0);
    let tags = tags.as_string::<i32>();
    assert_eq!(tags.len(), 3);
    assert_eq!(tags.value(0), "x");
    assert!(tags.is_null(1), "NULL array elements stay null");
    assert_eq!(
        col("scores").as_list::<i32>().value(0).len(),
        0,
        "empty array"
    );

    // Row 2: every nullable column is NULL.
    for (i, f) in e.fields().iter().enumerate() {
        if f.name != "id" && f.name != "big" {
            assert!(b.column(i).is_null(1), "`{}` should be null", f.name);
        }
    }
    assert!(
        !col("tags").is_valid(1),
        "a NULL array is a null list, not an empty one"
    );
}

#[tokio::test]
async fn source_passes_the_contract_for_each_key_kind() {
    let db = db(r#"
        CREATE TABLE gapped (id int8 PRIMARY KEY, v text);
        INSERT INTO gapped SELECT g * g, 'v' || g FROM generate_series(1, 25) g;
        CREATE TABLE by_text (code text PRIMARY KEY);
        INSERT INTO by_text VALUES ('a'), ('B'), ('_'), ('é'), ('Z'), ('aa'), ('0'), ('ß');
        CREATE TABLE by_uuid (id uuid PRIMARY KEY);
        INSERT INTO by_uuid SELECT md5(g::text)::uuid FROM generate_series(1, 10) g;
        CREATE TABLE composite (a int4, b int4, PRIMARY KEY (a, b));
        INSERT INTO composite VALUES (1, 1), (1, 2), (2, 1);
        CREATE TABLE empty (id int4 PRIMARY KEY);
    "#)
    .await;
    let source = source(&db);

    let violations = check_source(&source, &PartitionHint::rows(3)).await;
    assert_eq!(violations, Vec::<String>::new());

    let parts = |name: &'static str| {
        let source = &source;
        async move {
            let e = entity(source, name).await;
            source
                .plan_partitions(&e, &PartitionHint::rows(3))
                .await
                .unwrap()
        }
    };
    assert_eq!(parts("gapped").await.len(), 9, "25 rows at 3 per partition");
    assert_eq!(parts("by_text").await.len(), 3);
    assert_eq!(parts("by_uuid").await.len(), 4);
    let composite = parts("composite").await;
    assert_eq!(composite.len(), 1);
    assert_eq!(
        composite[0].scope,
        PartitionScope::Whole,
        "composite keys are not range-split yet"
    );
    assert_eq!(
        parts("empty").await.len(),
        1,
        "an empty table still has one partition"
    );
}

#[tokio::test]
async fn text_partition_bounds_follow_byte_order() {
    // Bounds must be increasing under KeyValue's byte ordering, which is what
    // co-partitioning compares, whatever the database's collation.
    let db = db(r#"
        CREATE TABLE by_text (code text PRIMARY KEY);
        INSERT INTO by_text VALUES ('a'), ('B'), ('_'), ('é'), ('Z'), ('aa'), ('0'), ('ß');
    "#)
    .await;
    let source = source(&db);
    let e = entity(&source, "by_text").await;
    let partitions = source
        .plan_partitions(&e, &PartitionHint::rows(2))
        .await
        .unwrap();
    CoPartition::from_partitions("code", &partitions).expect("bounds are strictly increasing");
}

#[tokio::test]
async fn discovery_names_every_unsupported_column_and_keyless_table() {
    let db = db(r#"
        CREATE TABLE places (id int4 PRIMARY KEY, location point, amount numeric);
        CREATE TABLE logs (line text);
        CREATE TABLE fine (id int4 PRIMARY KEY);
    "#)
    .await;
    let err = source(&db).discover().await.unwrap_err();
    let FerryError::Connector { message, .. } = err else {
        panic!("expected a connector error, got {err:?}");
    };
    assert!(message.contains("places.location"), "{message}");
    assert!(
        message.contains("places.amount") && message.contains("numeric(p, s)"),
        "{message}"
    );
    assert!(
        message.contains("logs") && message.contains("primary key"),
        "{message}"
    );
    assert!(!message.contains("fine"), "{message}");
}

#[tokio::test]
async fn co_partitioned_children_land_with_their_parents() {
    let db = db(r#"
        CREATE TABLE orders (id int8 PRIMARY KEY);
        INSERT INTO orders SELECT g * 3 FROM generate_series(1, 20) g;
        CREATE TABLE order_items (id int8 PRIMARY KEY, order_id int8 NOT NULL);
        INSERT INTO order_items SELECT o.id * 100 + n, o.id FROM orders o, generate_series(1, 3) n;
    "#)
    .await;
    let source = source(&db);
    let orders = entity(&source, "orders").await;
    let items = entity(&source, "order_items").await;

    let driver = source
        .plan_partitions(&orders, &PartitionHint::rows(4))
        .await
        .unwrap();
    let hint = PartitionHint {
        target_rows: 4,
        co_partition: Some(CoPartition::from_partitions("order_id", &driver).unwrap()),
    };
    let children = source.plan_partitions(&items, &hint).await.unwrap();
    let pairs = align(&driver, &children).unwrap();

    let mut placed: BTreeMap<i64, usize> = BTreeMap::new();
    for (parent_p, child_p) in &pairs {
        let parents = int_column(&read_all(&source, &orders, parent_p).await, "id");
        for order_id in int_column(&read_all(&source, &items, child_p).await, "order_id") {
            assert!(
                parents.contains(&order_id),
                "child of {order_id} is not with its parent"
            );
            *placed.entry(order_id).or_default() += 1;
        }
    }
    assert_eq!(placed.len(), 20);
    assert!(placed.values().all(|&n| n == 3));
}

#[tokio::test]
async fn co_partitioning_refuses_unknown_columns_and_mismatched_key_kinds() {
    let db = db("CREATE TABLE items (id int8 PRIMARY KEY, sku text);").await;
    let source = source(&db);
    let items = entity(&source, "items").await;
    let hint = |column: &str, bound: KeyValue| PartitionHint {
        target_rows: 4,
        co_partition: Some(CoPartition::new(column, vec![bound]).unwrap()),
    };
    for h in [
        hint("order_id", KeyValue::Int(5)),
        hint("sku", KeyValue::Int(5)),
    ] {
        assert!(
            matches!(
                source.plan_partitions(&items, &h).await,
                Err(FerryError::CoPartitionUnsupported { .. })
            ),
            "{h:?}"
        );
    }
}

#[tokio::test]
async fn reads_are_split_into_batches_of_the_configured_size() {
    let db = db(r#"
        CREATE TABLE t (id int4 PRIMARY KEY);
        INSERT INTO t SELECT generate_series(1, 10);
    "#)
    .await;
    let source = source(&db).with_batch_rows(4);
    let e = entity(&source, "t").await;
    let sizes: Vec<_> = read_all(&source, &e, &whole("t"))
        .await
        .iter()
        .map(|b| b.num_rows())
        .collect();
    assert_eq!(sizes, [4, 4, 2]);
}

fn int_column(batches: &[RecordBatch], column: &str) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|b| {
            let c = b
                .column_by_name(column)
                .unwrap()
                .as_primitive::<Int64Type>();
            (0..c.len()).map(|i| c.value(i)).collect::<Vec<_>>()
        })
        .collect()
}
