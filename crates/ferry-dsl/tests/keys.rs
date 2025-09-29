//! `$key` (B5, 0XT-263): deterministic, and collision-free at scale.

use ferry_core::partition::KeyValue;
use ferry_core::transform::SourceRecordId;
use ferry_dsl::compile::key::{Step, child_key, name};
use proptest::prelude::*;

/// An ObjectId-like `_id`: 24 hex digits.
fn object_id(n: u64) -> SourceRecordId {
    SourceRecordId(vec![KeyValue::Str(format!("65{n:022x}"))])
}

#[test]
fn ten_million_child_keys_never_collide() {
    // 200 000 documents with 50 items each, the shape of a real unnest,
    // plus 10 000 integer-keyed records with nested paths, so different
    // kinds of record id and path depths share the key space.
    let threads = std::thread::available_parallelism().map_or(4, |n| n.get()) as u64;
    let mut keys: Vec<u128> = std::thread::scope(|scope| {
        let workers: Vec<_> = (0..threads)
            .map(|t| {
                scope.spawn(move || {
                    let mut keys = Vec::new();
                    for doc in (t..200_000).step_by(threads as usize) {
                        let record = object_id(doc);
                        for index in 0..50 {
                            let steps = [Step {
                                path: "items".into(),
                                index,
                            }];
                            keys.push(child_key(&record, &steps).as_u128());
                        }
                    }
                    for id in (t as i64..10_000).step_by(threads as usize) {
                        let record = SourceRecordId(vec![KeyValue::Int(id)]);
                        for outer in 0..2 {
                            for index in 0..5 {
                                let steps = [
                                    Step {
                                        path: "items".into(),
                                        index: outer,
                                    },
                                    Step {
                                        path: "options".into(),
                                        index,
                                    },
                                ];
                                keys.push(child_key(&record, &steps).as_u128());
                            }
                        }
                    }
                    keys
                })
            })
            .collect();
        workers
            .into_iter()
            .flat_map(|w| w.join().unwrap())
            .collect()
    });
    assert_eq!(keys.len(), 10_100_000);
    keys.sort_unstable();
    let collisions = keys.windows(2).filter(|w| w[0] == w[1]).count();
    assert_eq!(collisions, 0);
}

fn key_value() -> impl Strategy<Value = KeyValue> {
    // Tiny alphabets, so near-misses (`ab` vs `a` + `b`, the same bytes as
    // text and as bytes) come up often.
    prop_oneof![
        (0i64..4).prop_map(KeyValue::Int),
        "[ab]{0,3}".prop_map(KeyValue::Str),
        proptest::collection::vec(b'a'..=b'b', 0..3).prop_map(KeyValue::Bytes),
    ]
}

fn input() -> impl Strategy<Value = (SourceRecordId, Vec<Step>)> {
    (
        proptest::collection::vec(key_value(), 1..3).prop_map(SourceRecordId),
        proptest::collection::vec(
            ("[a.]{0,3}", 0u64..3).prop_map(|(path, index)| Step { path, index }),
            1..3,
        ),
    )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(20_000))]

    /// Different inputs encode to different names, so they hash to
    /// different keys; equal inputs always give the same key.
    #[test]
    fn the_encoding_is_injective_and_the_key_deterministic(a in input(), b in input()) {
        let (name_a, name_b) = (name(&a.0, &a.1), name(&b.0, &b.1));
        prop_assert_eq!(a == b, name_a == name_b);
        prop_assert_eq!(a == b, child_key(&a.0, &a.1) == child_key(&b.0, &b.1));
        prop_assert_eq!(child_key(&a.0, &a.1), child_key(&a.0.clone(), &a.1.clone()));
    }
}

#[test]
fn every_part_of_the_input_changes_the_key() {
    let record = object_id(1);
    let base = child_key(
        &record,
        &[Step {
            path: "items".into(),
            index: 0,
        }],
    );
    let other_record = child_key(
        &object_id(2),
        &[Step {
            path: "items".into(),
            index: 0,
        }],
    );
    let other_path = child_key(
        &record,
        &[Step {
            path: "lines".into(),
            index: 0,
        }],
    );
    let other_index = child_key(
        &record,
        &[Step {
            path: "items".into(),
            index: 1,
        }],
    );
    assert_ne!(base, other_record);
    assert_ne!(base, other_path);
    assert_ne!(base, other_index);
    assert_eq!(base.get_version_num(), 5);
}
