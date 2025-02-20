use std::sync::Arc;

use arrow_array::{
    Array, BinaryArray, FixedSizeBinaryArray, Float64Array, Int32Array, Int64Array, StringArray,
    UInt64Array,
};
use ferry_core::{FerryError, KeyValue};

#[test]
fn integer_keys_of_every_width_read_as_int() {
    let i32s = Int32Array::from(vec![-7, 42]);
    assert_eq!(KeyValue::from_array(&i32s, 1).unwrap(), KeyValue::Int(42));
    assert_eq!(KeyValue::from_array(&i32s, 0).unwrap(), KeyValue::Int(-7));

    let i64s = Int64Array::from(vec![i64::MAX]);
    assert_eq!(
        KeyValue::from_array(&i64s, 0).unwrap(),
        KeyValue::Int(i64::MAX)
    );
}

#[test]
fn unsigned_key_beyond_i64_is_rejected_not_wrapped() {
    let big = UInt64Array::from(vec![u64::MAX, 5]);
    assert_eq!(KeyValue::from_array(&big, 1).unwrap(), KeyValue::Int(5));
    assert!(matches!(
        KeyValue::from_array(&big, 0),
        Err(FerryError::InvalidKey { .. })
    ));
}

#[test]
fn string_and_binary_keys_keep_their_kind() {
    let strs = StringArray::from(vec!["ord_1"]);
    assert_eq!(
        KeyValue::from_array(&strs, 0).unwrap(),
        KeyValue::Str("ord_1".into())
    );

    let bin = BinaryArray::from(vec![&[0x65u8, 0x1f][..]]);
    assert_eq!(
        KeyValue::from_array(&bin, 0).unwrap(),
        KeyValue::Bytes(vec![0x65, 0x1f])
    );
}

#[test]
fn uuid_keys_read_as_their_sixteen_bytes() {
    let bytes = [7u8; 16];
    let uuids = FixedSizeBinaryArray::try_from_iter(std::iter::once(bytes)).unwrap();
    assert_eq!(
        KeyValue::from_array(&uuids, 0).unwrap(),
        KeyValue::Bytes(bytes.to_vec())
    );
}

#[test]
fn null_key_is_rejected() {
    let col = Int64Array::from(vec![None, Some(1)]);
    assert!(matches!(
        KeyValue::from_array(&col, 0),
        Err(FerryError::InvalidKey { .. })
    ));
}

#[test]
fn float_key_is_rejected() {
    let col: Arc<dyn Array> = Arc::new(Float64Array::from(vec![1.5]));
    match KeyValue::from_array(col.as_ref(), 0) {
        Err(FerryError::InvalidKey { reason }) => assert!(reason.contains("Float64"), "{reason}"),
        other => panic!("expected InvalidKey, got {other:?}"),
    }
}
