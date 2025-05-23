//! Inferring one schema per collection from sampled documents.
//!
//! Each value has a [`Shape`]; shapes of the same path are merged across
//! documents. Merging only widens where no value can change: int32 into
//! int64, int32 into double, integers into decimal. Any other conflict (an
//! int that is sometimes a string, an int64 that is sometimes a double)
//! makes the path a *variant*: a JSON column holding each value as relaxed
//! Extended JSON, so its original type survives and nothing is cast.

use std::collections::HashMap;

use mongodb::bson::spec::BinarySubtype;
use mongodb::bson::{Bson, Document};

use ferry_core::types::{CanonicalField, CanonicalType, DecimalSpec};

use crate::decimal::Finite;

/// Precision of every inferred decimal column: the widest that is still an
/// Arrow `Decimal128`. Values are checked against it when read.
pub(crate) const DECIMAL_PRECISION: u8 = 38;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Shape {
    /// Only nulls so far.
    Unknown,
    Bool,
    Int32,
    Int64,
    Double,
    /// Decimal128 values: the largest scale and integer-digit count seen.
    Decimal {
        scale: u8,
        integer_digits: u8,
    },
    String,
    ObjectId,
    DateTime,
    Uuid,
    Binary,
    /// Fields in order of first appearance.
    Struct(Vec<(String, Shape)>),
    List(Box<Shape>),
    Variant,
}

impl Shape {
    pub(crate) fn of(value: &Bson) -> Shape {
        match value {
            Bson::Null => Shape::Unknown,
            Bson::Boolean(_) => Shape::Bool,
            Bson::Int32(_) => Shape::Int32,
            Bson::Int64(_) => Shape::Int64,
            Bson::Double(_) => Shape::Double,
            Bson::Decimal128(d) => match Finite::parse(d) {
                Some(f) => Shape::Decimal {
                    scale: f.scale(),
                    integer_digits: f.integer_digits(),
                },
                None => Shape::Variant,
            },
            Bson::String(_) => Shape::String,
            Bson::ObjectId(_) => Shape::ObjectId,
            Bson::DateTime(_) => Shape::DateTime,
            Bson::Binary(b) if b.subtype == BinarySubtype::Uuid && b.bytes.len() == 16 => {
                Shape::Uuid
            }
            Bson::Binary(b) if b.subtype == BinarySubtype::Generic => Shape::Binary,
            Bson::Document(doc) => Shape::of_document(doc),
            Bson::Array(items) => Shape::List(Box::new(
                items
                    .iter()
                    .map(Shape::of)
                    .fold(Shape::Unknown, Shape::merge),
            )),
            // Timestamps, regexes, code, min/max keys, other binary subtypes:
            // no canonical equivalent, so kept as JSON.
            _ => Shape::Variant,
        }
    }

    pub(crate) fn of_document(doc: &Document) -> Shape {
        Shape::Struct(doc.iter().map(|(k, v)| (k.clone(), Shape::of(v))).collect())
    }

    pub(crate) fn merge(self, other: Shape) -> Shape {
        use Shape::*;
        match (self, other) {
            (Unknown, x) | (x, Unknown) => x,
            (Variant, _) | (_, Variant) => Variant,
            (Int32, Int64) | (Int64, Int32) => Int64,
            (Int32, Double) | (Double, Int32) => Double,
            (
                Decimal {
                    scale: a,
                    integer_digits: x,
                },
                Decimal {
                    scale: b,
                    integer_digits: y,
                },
            ) => Decimal {
                scale: a.max(b),
                integer_digits: x.max(y),
            },
            // Every int32 has at most 10 digits, every int64 at most 19.
            (
                Decimal {
                    scale,
                    integer_digits,
                },
                Int32,
            )
            | (
                Int32,
                Decimal {
                    scale,
                    integer_digits,
                },
            ) => Decimal {
                scale,
                integer_digits: integer_digits.max(10),
            },
            (
                Decimal {
                    scale,
                    integer_digits,
                },
                Int64,
            )
            | (
                Int64,
                Decimal {
                    scale,
                    integer_digits,
                },
            ) => Decimal {
                scale,
                integer_digits: integer_digits.max(19),
            },
            (Struct(mut fields), Struct(more)) => {
                for (name, shape) in more {
                    match fields.iter_mut().find(|(n, _)| *n == name) {
                        Some((_, existing)) => {
                            *existing = std::mem::replace(existing, Unknown).merge(shape);
                        }
                        None => fields.push((name, shape)),
                    }
                }
                Struct(fields)
            }
            (List(a), List(b)) => List(Box::new(a.merge(*b))),
            (a, b) if a == b => a,
            _ => Variant,
        }
    }

    pub(crate) fn to_canonical(&self) -> CanonicalType {
        match self {
            // Nothing but nulls says nothing about the type: JSON accepts
            // whatever a later document holds.
            Shape::Unknown | Shape::Variant => CanonicalType::Json,
            Shape::Bool => CanonicalType::Bool,
            Shape::Int32 => CanonicalType::Int32,
            Shape::Int64 => CanonicalType::Int64,
            Shape::Double => CanonicalType::Float64,
            Shape::Decimal {
                scale,
                integer_digits,
            } => match (
                u16::from(*scale) + u16::from(*integer_digits) <= u16::from(DECIMAL_PRECISION),
                DecimalSpec::new(DECIMAL_PRECISION, *scale as i8),
            ) {
                (true, Ok(spec)) => CanonicalType::Decimal(spec),
                _ => CanonicalType::Json,
            },
            // An ObjectId is its 24-digit hex string, which sorts like the
            // ObjectId itself.
            Shape::String | Shape::ObjectId => CanonicalType::Utf8,
            Shape::DateTime => CanonicalType::Timestamp {
                tz: Some("UTC".into()),
            },
            Shape::Uuid => CanonicalType::Uuid,
            Shape::Binary => CanonicalType::Binary,
            // `{}` everywhere has no fields to make columns of.
            Shape::Struct(fields) if fields.is_empty() => CanonicalType::Json,
            Shape::Struct(fields) => CanonicalType::Struct(
                fields
                    .iter()
                    .map(|(name, shape)| CanonicalField::new(name, shape.to_canonical(), true))
                    .collect(),
            ),
            Shape::List(item) => CanonicalType::List(Box::new(CanonicalField::new(
                "item",
                item.to_canonical(),
                true,
            ))),
        }
    }
}

/// What a UTF-8 column holds: strings, or ObjectIds as hex. Both read the
/// same, so this is kept beside the schema to refuse the other one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TextKind {
    String,
    ObjectId,
}

/// The [`TextKind`] of every UTF-8 column, by schema path: field names
/// joined by `.`, with list items adding nothing (`items.sku`).
pub(crate) type TextKinds = HashMap<String, TextKind>;

pub(crate) fn text_kinds(fields: &[(String, Shape)]) -> TextKinds {
    fn walk(shape: &Shape, path: &str, out: &mut TextKinds) {
        match shape {
            Shape::String => {
                out.insert(path.to_owned(), TextKind::String);
            }
            Shape::ObjectId => {
                out.insert(path.to_owned(), TextKind::ObjectId);
            }
            Shape::List(item) => walk(item, path, out),
            Shape::Struct(fields) => {
                for (name, shape) in fields {
                    walk(shape, &format!("{path}.{name}"), out);
                }
            }
            _ => {}
        }
    }
    let mut out = TextKinds::new();
    for (name, shape) in fields {
        walk(shape, name, &mut out);
    }
    out
}

#[cfg(test)]
mod tests {
    use mongodb::bson::{Binary, DateTime, Decimal128, doc, oid::ObjectId};

    use super::*;

    fn shape_of(docs: &[Document]) -> Shape {
        docs.iter()
            .map(Shape::of_document)
            .fold(Shape::Unknown, Shape::merge)
    }

    fn field(docs: &[Document], name: &str) -> CanonicalType {
        let Shape::Struct(fields) = shape_of(docs) else {
            panic!("documents have a struct shape");
        };
        fields
            .iter()
            .find(|(n, _)| n == name)
            .unwrap_or_else(|| panic!("no field {name}"))
            .1
            .to_canonical()
    }

    fn dec(s: &str) -> Bson {
        Bson::Decimal128(s.parse::<Decimal128>().unwrap())
    }

    #[test]
    fn scalar_types_map_to_their_canonical_type() {
        let d = doc! {
            "b": true, "i": 1_i32, "l": 1_i64, "f": 1.5, "s": "x",
            "oid": ObjectId::new(), "at": DateTime::now(), "m": dec("1.25"),
            "u": Binary { subtype: BinarySubtype::Uuid, bytes: vec![0; 16] },
            "bin": Binary { subtype: BinarySubtype::Generic, bytes: vec![1, 2] },
        };
        let d = std::slice::from_ref(&d);
        assert_eq!(field(d, "b"), CanonicalType::Bool);
        assert_eq!(field(d, "i"), CanonicalType::Int32);
        assert_eq!(field(d, "l"), CanonicalType::Int64);
        assert_eq!(field(d, "f"), CanonicalType::Float64);
        assert_eq!(field(d, "s"), CanonicalType::Utf8);
        assert_eq!(field(d, "oid"), CanonicalType::Utf8);
        assert_eq!(
            field(d, "at"),
            CanonicalType::Timestamp {
                tz: Some("UTC".into())
            }
        );
        assert_eq!(
            field(d, "m"),
            CanonicalType::Decimal(DecimalSpec::new(38, 2).unwrap())
        );
        assert_eq!(field(d, "u"), CanonicalType::Uuid);
        assert_eq!(field(d, "bin"), CanonicalType::Binary);
    }

    #[test]
    fn only_lossless_widenings_are_merged() {
        let docs = [
            doc! { "a": 1_i32, "b": 1_i32, "c": dec("1.5"), "d": dec("1.5") },
            doc! { "a": 2_i64, "b": 2.5, "c": 3_i64, "d": 2_i32 },
        ];
        assert_eq!(field(&docs, "a"), CanonicalType::Int64);
        assert_eq!(field(&docs, "b"), CanonicalType::Float64);
        assert_eq!(
            field(&docs, "c"),
            CanonicalType::Decimal(DecimalSpec::new(38, 1).unwrap())
        );
        assert_eq!(
            field(&docs, "d"),
            CanonicalType::Decimal(DecimalSpec::new(38, 1).unwrap())
        );
    }

    #[test]
    fn conflicts_that_would_need_a_cast_become_variants() {
        let docs = [
            doc! { "n": 1_i32, "big": 1_i64, "money": dec("1.5"), "id": ObjectId::new() },
            doc! { "n": "one", "big": 1.5, "money": 1.5, "id": "legacy-1" },
        ];
        for name in ["n", "big", "money", "id"] {
            assert_eq!(field(&docs, name), CanonicalType::Json, "{name}");
        }
    }

    #[test]
    fn a_decimal_wider_than_38_digits_is_a_variant() {
        let docs = [
            doc! { "x": dec("1.000000000000000000000000000001") },
            doc! {
                "x": dec("12345678901234567890")
            },
        ];
        // 30 digits of scale plus 20 integer digits don't fit 38.
        assert_eq!(field(&docs, "x"), CanonicalType::Json);
    }

    #[test]
    fn nulls_take_the_type_of_the_values_beside_them() {
        let docs = [
            doc! { "x": Bson::Null, "only_null": Bson::Null },
            doc! { "x": 5_i32 },
        ];
        assert_eq!(field(&docs, "x"), CanonicalType::Int32);
        assert_eq!(field(&docs, "only_null"), CanonicalType::Json);
    }

    #[test]
    fn nested_documents_and_arrays_merge_field_by_field() {
        let docs = [
            doc! { "c": { "id": "c1" }, "items": [{ "qty": 1_i32 }, { "qty": 2_i32, "sku": "a" }] },
            doc! { "c": { "email": "x@y" }, "items": [] },
            doc! { "items": [{ "qty": "three" }] },
        ];
        let utf8 = |name: &str| CanonicalField::new(name, CanonicalType::Utf8, true);
        assert_eq!(
            field(&docs, "c"),
            CanonicalType::Struct(vec![utf8("id"), utf8("email")])
        );
        assert_eq!(
            field(&docs, "items"),
            CanonicalType::List(Box::new(CanonicalField::new(
                "item",
                CanonicalType::Struct(vec![
                    CanonicalField::new("qty", CanonicalType::Json, true),
                    utf8("sku"),
                ]),
                true
            )))
        );
    }

    #[test]
    fn text_kinds_are_recorded_for_every_utf8_column() {
        let Shape::Struct(fields) = shape_of(&[doc! {
            "_id": ObjectId::new(),
            "s": "x",
            "c": { "ref": ObjectId::new() },
            "items": [{ "sku": "a" }],
            "n": 1_i32,
        }]) else {
            panic!("struct");
        };
        let kinds = text_kinds(&fields);
        assert_eq!(
            kinds,
            TextKinds::from([
                ("_id".into(), TextKind::ObjectId),
                ("s".into(), TextKind::String),
                ("c.ref".into(), TextKind::ObjectId),
                ("items.sku".into(), TextKind::String),
            ])
        );
    }

    #[test]
    fn arrays_that_are_always_empty_hold_json() {
        let docs = [doc! { "tags": [] }];
        assert_eq!(
            field(&docs, "tags"),
            CanonicalType::List(Box::new(CanonicalField::new(
                "item",
                CanonicalType::Json,
                true
            )))
        );
    }

    #[test]
    fn a_document_and_a_scalar_at_the_same_path_is_a_variant() {
        let docs = [doc! { "c": { "id": 1_i32 } }, doc! { "c": "c1" }];
        assert_eq!(field(&docs, "c"), CanonicalType::Json);
    }

    #[test]
    fn types_with_no_canonical_equivalent_are_variants() {
        let docs = [doc! {
            "ts": Bson::Timestamp(mongodb::bson::Timestamp { time: 1, increment: 1 }),
            "nan": dec("NaN"),
            "empty": {},
        }];
        for name in ["ts", "nan", "empty"] {
            assert_eq!(field(&docs, name), CanonicalType::Json, "{name}");
        }
    }
}
