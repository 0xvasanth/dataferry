use dataferry_core::{
    Checkpoint, FerryError, KeyRange, KeyValue, Partition, PartitionId, PartitionScope,
};

fn int(v: i64) -> Option<KeyValue> {
    Some(KeyValue::Int(v))
}

#[test]
fn partition_survives_a_store_and_load_round_trip() {
    let partitions = [
        Partition {
            id: PartitionId("orders-0003".into()),
            entity: "orders".into(),
            scope: PartitionScope::KeyRange(KeyRange::new("id", int(1000), int(2000)).unwrap()),
        },
        Partition {
            id: PartitionId("orders-mongo-1".into()),
            entity: "orders".into(),
            scope: PartitionScope::KeyRange(
                KeyRange::new(
                    "_id",
                    Some(KeyValue::Bytes(vec![0x65, 0x1f, 0, 0xff])),
                    None,
                )
                .unwrap(),
            ),
        },
        Partition {
            id: PartitionId("countries".into()),
            entity: "countries".into(),
            scope: PartitionScope::Whole,
        },
    ];
    for p in partitions {
        let stored = serde_json::to_string(&p).unwrap();
        let loaded: Partition = serde_json::from_str(&stored).unwrap();
        assert_eq!(loaded, p);
    }
}

#[test]
fn checkpoint_bytes_survive_a_store_and_load_round_trip_unchanged() {
    let cp = Checkpoint::new(vec![0u8, 255, 10, 13, 0x7f]);
    let loaded: Checkpoint = serde_json::from_str(&serde_json::to_string(&cp).unwrap()).unwrap();
    assert_eq!(loaded.as_bytes(), [0u8, 255, 10, 13, 0x7f]);
}

#[test]
fn key_range_is_half_open() {
    let r = KeyRange::new("id", int(10), int(20)).unwrap();
    assert!(!r.contains(&KeyValue::Int(9)));
    assert!(r.contains(&KeyValue::Int(10)), "lower bound is inclusive");
    assert!(r.contains(&KeyValue::Int(19)));
    assert!(!r.contains(&KeyValue::Int(20)), "upper bound is exclusive");
}

#[test]
fn adjacent_ranges_cover_every_key_exactly_once() {
    let ranges = [
        KeyRange::new("id", None, int(0)).unwrap(),
        KeyRange::new("id", int(0), int(100)).unwrap(),
        KeyRange::new("id", int(100), None).unwrap(),
    ];
    for k in [i64::MIN, -1, 0, 1, 99, 100, i64::MAX] {
        let hits = ranges
            .iter()
            .filter(|r| r.contains(&KeyValue::Int(k)))
            .count();
        assert_eq!(hits, 1, "key {k} matched {hits} ranges");
    }
}

#[test]
fn bytes_compare_lexicographically() {
    let r = KeyRange::new(
        "_id",
        Some(KeyValue::Bytes(vec![1, 0])),
        Some(KeyValue::Bytes(vec![2])),
    )
    .unwrap();
    assert!(r.contains(&KeyValue::Bytes(vec![1, 255, 255])));
    assert!(!r.contains(&KeyValue::Bytes(vec![2, 0])));
}

#[test]
fn value_of_another_kind_is_never_contained() {
    let r = KeyRange::new("id", None, None).unwrap();
    assert!(
        r.contains(&KeyValue::Int(5)),
        "an unbounded range contains everything"
    );

    let r = KeyRange::new("id", int(0), int(10)).unwrap();
    assert!(!r.contains(&KeyValue::Str("5".into())));
}

#[test]
fn empty_inverted_or_mixed_kind_ranges_are_rejected() {
    let bad = [
        KeyRange::new("id", int(5), int(5)),
        KeyRange::new("id", int(6), int(5)),
        KeyRange::new("id", int(0), Some(KeyValue::Str("z".into()))),
    ];
    for r in bad {
        assert!(
            matches!(r, Err(FerryError::InvalidKeyRange { .. })),
            "{r:?}"
        );
    }
}
