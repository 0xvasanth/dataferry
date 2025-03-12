use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch};
use ferry_core::testkit::MemSource;
use ferry_core::types::{CanonicalField, CanonicalType};
use ferry_core::{
    CoPartition, Entity, FerryError, KeyRange, KeyValue, Partition, PartitionHint, PartitionId,
    PartitionScope, Source, align,
};
use futures::TryStreamExt;

fn int(v: i64) -> KeyValue {
    KeyValue::Int(v)
}

fn range_partition(
    name: &str,
    i: usize,
    column: &str,
    lo: Option<i64>,
    hi: Option<i64>,
) -> Partition {
    Partition {
        id: PartitionId(format!("{name}-{i}")),
        entity: name.into(),
        scope: PartitionScope::KeyRange(KeyRange::new(column, lo.map(int), hi.map(int)).unwrap()),
    }
}

fn int_field(name: &str) -> CanonicalField {
    CanonicalField::new(name, CanonicalType::Int64, false)
}

fn entity(name: &str, cols: &[&str]) -> Entity {
    Entity::new(
        name,
        cols.iter().map(|c| int_field(c)).collect(),
        vec![cols[0].into()],
    )
    .unwrap()
}

fn rows(e: &Entity, columns: Vec<Vec<i64>>) -> RecordBatch {
    let arrays = columns
        .into_iter()
        .map(|c| Arc::new(Int64Array::from(c)) as ArrayRef)
        .collect();
    RecordBatch::try_new(e.arrow_schema(), arrays).unwrap()
}

#[test]
fn boundaries_define_ranges_that_cover_every_key_once() {
    let cp = CoPartition::new("order_id", vec![int(10), int(20)]).unwrap();
    let ranges = cp.ranges();
    assert_eq!(ranges.len(), 3);
    for k in [i64::MIN, 9, 10, 19, 20, i64::MAX] {
        let hits = ranges.iter().filter(|r| r.contains(&int(k))).count();
        assert_eq!(hits, 1, "key {k}");
    }
    assert!(ranges.iter().all(|r| r.column() == "order_id"));
}

#[test]
fn no_boundaries_is_one_range_over_everything() {
    let cp = CoPartition::new("order_id", vec![]).unwrap();
    assert_eq!(
        cp.ranges(),
        [KeyRange::new("order_id", None, None).unwrap()]
    );
}

#[test]
fn boundaries_must_be_strictly_increasing_and_of_one_kind() {
    for bad in [
        vec![int(20), int(10)],
        vec![int(10), int(10)],
        vec![int(1), KeyValue::Str("b".into())],
    ] {
        assert!(
            matches!(
                CoPartition::new("k", bad.clone()),
                Err(FerryError::CoPartitionUnsupported { .. })
            ),
            "{bad:?}"
        );
    }
}

#[test]
fn boundaries_derive_from_contiguous_driver_partitions() {
    let driver = [
        range_partition("orders", 0, "id", None, Some(7)),
        range_partition("orders", 1, "id", Some(7), Some(20)),
        range_partition("orders", 2, "id", Some(20), None),
    ];
    let cp = CoPartition::from_partitions("order_id", &driver).unwrap();
    assert_eq!(cp.column(), "order_id");
    assert_eq!(cp.boundaries(), [int(7), int(20)]);
}

#[test]
fn boundaries_cannot_derive_from_partitions_with_gaps_or_open_ends() {
    let gap = [
        range_partition("orders", 0, "id", None, Some(7)),
        range_partition("orders", 1, "id", Some(8), None),
    ];
    let bounded_start = [range_partition("orders", 0, "id", Some(0), None)];
    let bounded_end = [range_partition("orders", 0, "id", None, Some(100))];
    let whole = [Partition {
        id: PartitionId("orders-0".into()),
        entity: "orders".into(),
        scope: PartitionScope::Whole,
    }];
    for (name, driver) in [
        ("gap", &gap[..]),
        ("bounded start", &bounded_start[..]),
        ("bounded end", &bounded_end[..]),
        ("whole", &whole[..]),
        ("empty", &[][..]),
    ] {
        assert!(
            matches!(
                CoPartition::from_partitions("order_id", driver),
                Err(FerryError::CoPartitionUnsupported { .. })
            ),
            "{name}"
        );
    }
}

#[test]
fn a_single_whole_range_driver_partition_has_no_boundaries() {
    let driver = [range_partition("orders", 0, "id", None, None)];
    assert!(
        CoPartition::from_partitions("order_id", &driver)
            .unwrap()
            .boundaries()
            .is_empty()
    );
}

#[test]
fn align_refuses_partition_lists_that_do_not_match() {
    let driver = [
        range_partition("orders", 0, "id", None, Some(7)),
        range_partition("orders", 1, "id", Some(7), None),
    ];
    let shifted = [
        range_partition("order_items", 0, "order_id", None, Some(8)),
        range_partition("order_items", 1, "order_id", Some(8), None),
    ];
    let fewer = [range_partition("order_items", 0, "order_id", None, None)];
    assert!(align(&driver, &shifted).is_err());
    assert!(align(&driver, &fewer).is_err());

    let matching = [
        range_partition("order_items", 0, "order_id", None, Some(7)),
        range_partition("order_items", 1, "order_id", Some(7), None),
    ];
    let pairs = align(&driver, &matching).unwrap();
    assert_eq!(pairs[1].0.id.0, "orders-1");
    assert_eq!(pairs[1].1.id.0, "order_items-1");
}

/// The end-to-end property 0.6 exists for: after co-partitioning, every
/// child row is in the partition paired with its parent's partition.
#[tokio::test]
async fn every_child_lands_in_the_partition_paired_with_its_parent() {
    let orders = entity("orders", &["id"]);
    let items = entity("order_items", &["id", "order_id"]);
    let order_ids: Vec<i64> = vec![1, 2, 3, 5, 8, 13, 21, 34, 55, 89];
    // 3 items per order, ids out of key order relative to order_id.
    let (item_ids, item_orders): (Vec<i64>, Vec<i64>) = order_ids
        .iter()
        .rev()
        .flat_map(|o| (0..3).map(move |j| (o * 100 + j, *o)))
        .unzip();
    let source = MemSource::new()
        .with_entity(orders.clone(), rows(&orders, vec![order_ids.clone()]))
        .unwrap()
        .with_entity(items.clone(), rows(&items, vec![item_ids, item_orders]))
        .unwrap()
        .with_batch_rows(4);

    let driver = source
        .plan_partitions(&orders, &PartitionHint::rows(3))
        .await
        .unwrap();
    assert!(driver.len() > 1, "the test needs several partitions");
    let hint = PartitionHint {
        target_rows: 3,
        co_partition: Some(CoPartition::from_partitions("order_id", &driver).unwrap()),
    };
    let children = source.plan_partitions(&items, &hint).await.unwrap();
    let pairs = align(&driver, &children).unwrap();

    let mut placed: BTreeMap<i64, usize> = BTreeMap::new();
    for (i, (parent_p, child_p)) in pairs.iter().enumerate() {
        let parents = read_column(&source, &orders, parent_p, "id").await;
        for child_order in read_column(&source, &items, child_p, "order_id").await {
            assert!(
                parents.contains(&child_order),
                "pair {i}: child of order {child_order} is not with its parent"
            );
            *placed.entry(child_order).or_default() += 1;
        }
    }
    assert_eq!(placed.len(), order_ids.len());
    assert!(
        placed.values().all(|&n| n == 3),
        "every item placed exactly once: {placed:?}"
    );
}

#[tokio::test]
async fn co_partitioning_on_an_unknown_column_is_unsupported() {
    let orders = entity("orders", &["id"]);
    let source = MemSource::new()
        .with_entity(orders.clone(), rows(&orders, vec![vec![1, 2]]))
        .unwrap();
    let hint = PartitionHint {
        target_rows: 1,
        co_partition: Some(CoPartition::new("order_id", vec![int(2)]).unwrap()),
    };
    match source.plan_partitions(&orders, &hint).await {
        Err(FerryError::CoPartitionUnsupported { column, .. }) => assert_eq!(column, "order_id"),
        other => panic!("expected CoPartitionUnsupported, got {other:?}"),
    }
}

async fn read_column(source: &MemSource, e: &Entity, p: &Partition, column: &str) -> Vec<i64> {
    let batches: Vec<RecordBatch> = source
        .read(e, p)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    batches
        .iter()
        .flat_map(|b| {
            let col = b.column_by_name(column).unwrap();
            let col = col.as_any().downcast_ref::<Int64Array>().unwrap();
            (0..col.len()).map(|i| col.value(i)).collect::<Vec<_>>()
        })
        .collect()
}
