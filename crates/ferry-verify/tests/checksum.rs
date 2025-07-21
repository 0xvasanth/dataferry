//! The multiset checksum over Arrow batches: order independence, no XOR
//! cancellation, normalization, and an unambiguous row encoding.

use std::sync::Arc;

use arrow_array::{
    ArrayRef, Decimal128Array, Int32Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray,
};
use ferry_core::TargetTable;
use ferry_core::types::{CanonicalField, CanonicalType, DecimalSpec};
use ferry_verify::{Checksum, ChecksumSpec};

fn table(fields: Vec<CanonicalField>) -> TargetTable {
    TargetTable {
        name: "t".into(),
        fields,
        primary_key: Vec::new(),
        foreign_keys: Vec::new(),
    }
}

fn batch(columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
    RecordBatch::try_from_iter(columns).unwrap()
}

fn sum(spec: &ChecksumSpec, b: &RecordBatch) -> Checksum {
    let mut c = Checksum::default();
    c.add_batch(spec, b).unwrap();
    c
}

fn ids_and_names() -> (ChecksumSpec, impl Fn(Vec<i64>) -> RecordBatch) {
    let spec = ChecksumSpec::new(&table(vec![
        CanonicalField::new("id", CanonicalType::Int64, false),
        CanonicalField::new("name", CanonicalType::Utf8, true),
    ]));
    let rows = |ids: Vec<i64>| {
        let names: Vec<String> = ids.iter().map(|i| format!("n{i}")).collect();
        batch(vec![
            ("id", Arc::new(Int64Array::from(ids))),
            ("name", Arc::new(StringArray::from(names))),
        ])
    };
    (spec, rows)
}

#[test]
fn row_order_and_batch_boundaries_do_not_matter() {
    let (spec, rows) = ids_and_names();
    let whole = sum(&spec, &rows(vec![1, 2, 3, 4, 5]));
    let mut split = sum(&spec, &rows(vec![5, 3]));
    split.merge(&sum(&spec, &rows(vec![2, 1, 4])));
    assert_eq!(whole, split);
    assert_eq!(whole.rows, 5);
}

#[test]
fn duplicates_never_cancel_the_way_xor_does() {
    let (spec, rows) = ids_and_names();
    let clean = spec.row_hashes(&rows(vec![1, 2, 3])).unwrap();
    // Row 2 twice more: XOR is back where it started, the sum is not.
    let corrupt = spec.row_hashes(&rows(vec![1, 2, 3, 2, 2])).unwrap();
    let xor = |h: &[u128]| h.iter().fold(0, |a, b| a ^ b);
    assert_eq!(xor(&clean), xor(&corrupt));
    let total = |h: &[u128]| {
        let mut c = Checksum::default();
        h.iter().for_each(|h| c.add(*h));
        c
    };
    assert_ne!(total(&clean).sum, total(&corrupt).sum);

    // A table of nothing but pairs: XOR is 0, like an empty table.
    let pairs = total(&spec.row_hashes(&rows(vec![4, 4, 9, 9])).unwrap());
    assert_eq!(xor(&spec.row_hashes(&rows(vec![4, 4, 9, 9])).unwrap()), 0);
    assert_ne!(pairs.sum, 0);
    // Same row count, one row swapped for a copy of another.
    let swapped = total(&spec.row_hashes(&rows(vec![1, 1, 3])).unwrap());
    assert_eq!(swapped.rows, total(&clean).rows);
    assert_ne!(swapped.sum, total(&clean).sum);
}

#[test]
fn values_are_compared_across_integer_widths_and_decimal_scales() {
    let spec = ChecksumSpec::new(&table(vec![CanonicalField::new(
        "n",
        CanonicalType::Int64,
        true,
    )]));
    let int64 = batch(vec![(
        "n",
        Arc::new(Int64Array::from(vec![Some(7), None, Some(-3)])),
    )]);
    let int32 = batch(vec![(
        "n",
        Arc::new(Int32Array::from(vec![Some(7), None, Some(-3)])),
    )]);
    let decimal = Decimal128Array::from(vec![Some(700), None, Some(-300)])
        .with_precision_and_scale(10, 2)
        .unwrap();
    let decimal = batch(vec![("n", Arc::new(decimal))]);
    assert_eq!(sum(&spec, &int64), sum(&spec, &int32));
    assert_eq!(sum(&spec, &int64), sum(&spec, &decimal));
    let off = batch(vec![(
        "n",
        Arc::new(Int64Array::from(vec![Some(7), None, Some(-4)])),
    )]);
    assert_ne!(sum(&spec, &int64), sum(&spec, &off));
}

#[test]
fn timestamps_round_to_the_targets_precision_and_ignore_the_zone_label() {
    let utc = CanonicalType::Timestamp {
        tz: Some("UTC".into()),
    };
    let target = table(vec![CanonicalField::new("at", utc, true)]);
    let micros = |tz: &str, v: Vec<i64>| {
        batch(vec![(
            "at",
            Arc::new(TimestampMicrosecondArray::from(v).with_timezone(tz)),
        )])
    };
    // What the run wrote, and what a timestamptz(3) column kept.
    let written = micros("+00:00", vec![1_000_123_456, 1_000_123_500, -999_500]);
    let stored = batch(vec![(
        "at",
        Arc::new(
            TimestampMillisecondArray::from(vec![1_000_123, 1_000_124, -1_000])
                .with_timezone("UTC"),
        ),
    )]);
    let at_millis = ChecksumSpec::new(&target)
        .with_timestamp_digits("at", 3)
        .unwrap();
    assert_eq!(sum(&at_millis, &written), sum(&at_millis, &stored));
    // At microseconds they differ: the rounding is what makes them match.
    let at_micros = ChecksumSpec::new(&target);
    assert_ne!(sum(&at_micros, &written), sum(&at_micros, &stored));
    // The zone label is not part of the value.
    assert_eq!(
        sum(&at_micros, &written),
        sum(
            &at_micros,
            &micros("UTC", vec![1_000_123_456, 1_000_123_500, -999_500])
        )
    );
    // More than 6 digits, or a column that isn't there, is refused.
    assert!(
        ChecksumSpec::new(&target)
            .with_timestamp_digits("at", 7)
            .is_err()
    );
    assert!(
        ChecksumSpec::new(&target)
            .with_timestamp_digits("nope", 3)
            .is_err()
    );
}

#[test]
fn the_row_encoding_is_unambiguous() {
    let spec = ChecksumSpec::new(&table(vec![
        CanonicalField::new("a", CanonicalType::Utf8, true),
        CanonicalField::new("b", CanonicalType::Utf8, true),
    ]));
    let pair = |a: Option<&str>, b: Option<&str>| {
        spec.row_hashes(&batch(vec![
            ("a", Arc::new(StringArray::from(vec![a]))),
            ("b", Arc::new(StringArray::from(vec![b]))),
        ]))
        .unwrap()[0]
    };
    // Where one column ends and the next begins.
    assert_ne!(pair(Some("ab"), Some("c")), pair(Some("a"), Some("bc")));
    // Null, empty and the text "null" are three values.
    assert_ne!(pair(None, Some("x")), pair(Some(""), Some("x")));
    assert_ne!(pair(None, Some("x")), pair(Some("null"), Some("x")));
    // Column order is the table's, not the batch's.
    let reordered = spec
        .row_hashes(&batch(vec![
            ("b", Arc::new(StringArray::from(vec!["c"]))),
            ("a", Arc::new(StringArray::from(vec!["ab"]))),
        ]))
        .unwrap()[0];
    assert_eq!(reordered, pair(Some("ab"), Some("c")));
}

#[test]
fn json_columns_compare_as_json_and_text_columns_as_text() {
    let fields = |ty| table(vec![CanonicalField::new("doc", ty, true)]);
    let docs = |text: &str| batch(vec![("doc", Arc::new(StringArray::from(vec![text])))]);
    let json = ChecksumSpec::new(&fields(CanonicalType::Json));
    // jsonb hands back sorted keys and its own spacing.
    assert_eq!(
        sum(&json, &docs(r#"{"b": 1, "a": "x"}"#)),
        sum(&json, &docs(r#"{"a": "x", "b": 1}"#))
    );
    let text = ChecksumSpec::new(&fields(CanonicalType::Utf8));
    assert_ne!(
        sum(&text, &docs(r#"{"b": 1, "a": "x"}"#)),
        sum(&text, &docs(r#"{"a": "x", "b": 1}"#))
    );
}

#[test]
fn a_missing_column_or_unsupported_type_is_an_error() {
    let spec = ChecksumSpec::new(&table(vec![
        CanonicalField::new("id", CanonicalType::Int64, false),
        CanonicalField::new(
            "amount",
            CanonicalType::Decimal(DecimalSpec::new(10, 2).unwrap()),
            false,
        ),
    ]));
    let err = spec
        .row_hashes(&batch(vec![("id", Arc::new(Int64Array::from(vec![1])))]))
        .unwrap_err();
    assert!(err.to_string().contains("no column `amount`"), "{err}");
}

#[test]
fn checksums_serialize_their_sum_as_hex() {
    let c = Checksum {
        rows: 3,
        sum: u128::MAX - 1,
    };
    let text = serde_json::to_string(&c).unwrap();
    assert_eq!(
        text,
        r#"{"rows":3,"sum":"fffffffffffffffffffffffffffffffe"}"#
    );
    assert_eq!(serde_json::from_str::<Checksum>(&text).unwrap(), c);
    // The sum wraps modulo 2^128 instead of overflowing.
    let mut wrapping = c;
    wrapping.add(5);
    assert_eq!(wrapping.sum, 3);
}
