use std::sync::Arc;

use arrow_schema::{DataType, Field, TimeUnit};
use dataferry_core::FerryError;
use dataferry_core::types::{
    CanonicalField, CanonicalType, DecimalSpec, EXTENSION_NAME_KEY, JSON_EXTENSION, UUID_EXTENSION,
};

fn decimal(p: u8, s: i8) -> CanonicalType {
    CanonicalType::Decimal(DecimalSpec::new(p, s).unwrap())
}

fn field(name: &str, ty: CanonicalType) -> CanonicalField {
    CanonicalField::new(name, ty, true)
}

/// One of every canonical type, including nesting, so a forgotten variant in
/// either direction of the mapping fails this test.
fn every_type() -> Vec<CanonicalType> {
    use CanonicalType::*;
    vec![
        Int8,
        Int16,
        Int32,
        Int64,
        UInt8,
        UInt16,
        UInt32,
        UInt64,
        Float32,
        Float64,
        decimal(38, 10),
        decimal(76, 0),
        Bool,
        Utf8,
        Binary,
        Date,
        Timestamp { tz: None },
        Timestamp {
            tz: Some(Arc::from("UTC")),
        },
        Uuid,
        Json,
        List(Box::new(field("item", Int64))),
        List(Box::new(field("item", Uuid))),
        Struct(vec![
            field("sku", Utf8),
            CanonicalField::new("qty", Int32, false),
            field("unit", decimal(12, 2)),
            field("tags", List(Box::new(field("item", Json)))),
        ]),
        Map {
            key: Box::new(Utf8),
            value: Box::new(field("value", Uuid)),
        },
    ]
}

#[test]
fn every_canonical_type_round_trips_through_arrow() {
    for ty in every_type() {
        for nullable in [true, false] {
            let original = CanonicalField::new("col", ty.clone(), nullable);
            let back = CanonicalField::from_arrow(&original.to_arrow())
                .unwrap_or_else(|e| panic!("{ty:?} failed to round-trip: {e}"));
            assert_eq!(back, original);
        }
    }
}

#[test]
fn canonical_types_use_the_documented_arrow_representation() {
    let cases = [
        (CanonicalType::Int64, DataType::Int64),
        (CanonicalType::UInt32, DataType::UInt32),
        (decimal(38, 10), DataType::Decimal128(38, 10)),
        (decimal(39, 2), DataType::Decimal256(39, 2)),
        (CanonicalType::Date, DataType::Date32),
        (
            CanonicalType::Timestamp { tz: None },
            DataType::Timestamp(TimeUnit::Microsecond, None),
        ),
        (
            CanonicalType::Timestamp {
                tz: Some(Arc::from("+05:30")),
            },
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("+05:30"))),
        ),
        (CanonicalType::Uuid, DataType::FixedSizeBinary(16)),
        (CanonicalType::Json, DataType::Utf8),
    ];
    for (ty, expected) in cases {
        assert_eq!(
            field("c", ty.clone()).to_arrow().data_type(),
            &expected,
            "{ty:?}"
        );
    }
}

#[test]
fn uuid_and_json_carry_extension_metadata() {
    let uuid = field("id", CanonicalType::Uuid).to_arrow();
    assert_eq!(
        uuid.metadata().get(EXTENSION_NAME_KEY).map(String::as_str),
        Some(UUID_EXTENSION)
    );

    let json = field("doc", CanonicalType::Json).to_arrow();
    assert_eq!(
        json.metadata().get(EXTENSION_NAME_KEY).map(String::as_str),
        Some(JSON_EXTENSION)
    );

    let utf8 = field("name", CanonicalType::Utf8).to_arrow();
    assert!(utf8.metadata().is_empty());
}

#[test]
fn plain_utf8_without_metadata_reads_back_as_utf8_not_json() {
    let f = Field::new("name", DataType::Utf8, true);
    assert_eq!(
        CanonicalField::from_arrow(&f).unwrap().ty,
        CanonicalType::Utf8
    );
}

#[test]
fn fixed_size_binary_without_uuid_tag_is_rejected() {
    let f = Field::new("hash", DataType::FixedSizeBinary(16), true);
    assert_eq!(
        CanonicalField::from_arrow(&f),
        Err(FerryError::UnsupportedArrowType {
            field: "hash".into(),
            arrow_type: "FixedSizeBinary(16)".into(),
        })
    );
}

#[test]
fn non_canonical_arrow_types_are_rejected_not_coerced() {
    let cases = [
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        DataType::Timestamp(TimeUnit::Millisecond, Some(Arc::from("UTC"))),
        DataType::LargeUtf8,
        DataType::Float16,
        DataType::Date64,
        // Decimal256 below 38 digits would give one decimal two Arrow forms.
        DataType::Decimal256(10, 2),
    ];
    for dt in cases {
        let f = Field::new("col", dt.clone(), true);
        match CanonicalField::from_arrow(&f) {
            Err(FerryError::UnsupportedArrowType { field, .. }) => assert_eq!(field, "col"),
            other => panic!("{dt:?} should be rejected, got {other:?}"),
        }
    }
}

#[test]
fn non_canonical_type_nested_in_a_struct_is_rejected_with_the_inner_field_name() {
    let inner = Field::new(
        "placed_at",
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        true,
    );
    let f = Field::new("order", DataType::Struct(vec![inner].into()), true);
    match CanonicalField::from_arrow(&f) {
        Err(FerryError::UnsupportedArrowType { field, .. }) => assert_eq!(field, "placed_at"),
        other => panic!("expected rejection, got {other:?}"),
    }
}

#[test]
fn decimal_spec_rejects_impossible_precision_and_scale() {
    assert!(DecimalSpec::new(1, 0).is_ok());
    assert!(DecimalSpec::new(76, 76).is_ok());
    assert!(
        DecimalSpec::new(10, -2).is_ok(),
        "negative scale is valid (Postgres 15+, Arrow)"
    );

    for (p, s) in [(0, 0), (77, 0), (10, 11)] {
        assert!(
            matches!(
                DecimalSpec::new(p, s),
                Err(FerryError::InvalidDecimal { .. })
            ),
            "decimal({p}, {s}) should be rejected"
        );
    }
}
