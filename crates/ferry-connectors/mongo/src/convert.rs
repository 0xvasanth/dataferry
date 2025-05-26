//! BSON documents to Arrow columns.
//!
//! Every document is first checked against the entity's schema, which also
//! lists the paths it omits for [`MISSING_PATHS_COLUMN`]. A document that
//! doesn't fit (a field that wasn't in the sample, a value of a type the
//! column can't hold without a cast) is an error naming its `_id` and the
//! path: it is never cast, truncated or dropped. Then each column is built
//! from the checked documents in one pass.
//!
//! [`MISSING_PATHS_COLUMN`]: ferry_core::missing::MISSING_PATHS_COLUMN

use std::sync::Arc;

use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Decimal128Builder, FixedSizeBinaryBuilder, Float64Builder,
    Int32Builder, Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
};
use arrow_array::{ArrayRef, ListArray, RecordBatch, StructArray};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{Fields, SchemaRef};
use ferry_core::Entity;
use ferry_core::missing::MissingPathsBuilder;
use ferry_core::types::{CanonicalField, CanonicalType};
use mongodb::bson::spec::BinarySubtype;
use mongodb::bson::{Bson, Document};

use crate::decimal::Finite;
use crate::infer::{TextKind, TextKinds};

/// Builds one batch from `docs`, which must each fit `entity`. `kinds`
/// says which UTF-8 columns hold ObjectIds; a column it doesn't mention
/// takes either.
pub(crate) fn to_batch(
    entity: &Entity,
    schema: SchemaRef,
    docs: &[Document],
    kinds: &TextKinds,
) -> Result<RecordBatch, String> {
    let mut missing = MissingPathsBuilder::new();
    for doc in docs {
        let paths = check_document(doc, entity.fields(), kinds)
            .map_err(|e| format!("document {}: {e}", describe_id(doc)))?;
        missing.append_row(&paths);
    }
    let mut columns = entity
        .fields()
        .iter()
        .map(|f| {
            let values: Vec<Option<&Bson>> = docs.iter().map(|d| present(d.get(&f.name))).collect();
            build(f, &values).map_err(|e| format!("{}: {e}", f.name))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if entity.tracks_missing() {
        columns.push(missing.finish());
    }
    RecordBatch::try_new(schema, columns).map_err(|e| e.to_string())
}

/// The document's `_id` for error messages.
pub(crate) fn describe_id(doc: &Document) -> String {
    match doc.get("_id") {
        Some(id) => format!("_id {id}"),
        None => "without _id".to_owned(),
    }
}

/// A value that is there and not null.
fn present(value: Option<&Bson>) -> Option<&Bson> {
    value.filter(|v| !matches!(v, Bson::Null))
}

/// The paths `doc` omits, or why it doesn't fit `fields`.
pub(crate) fn check_document(
    doc: &Document,
    fields: &[CanonicalField],
    kinds: &TextKinds,
) -> Result<Vec<String>, String> {
    let mut checker = Checker {
        kinds,
        missing: Vec::new(),
    };
    checker.check_struct(doc, fields, "", "")?;
    Ok(checker.missing)
}

struct Checker<'a> {
    kinds: &'a TextKinds,
    missing: Vec<String>,
}

impl Checker<'_> {
    /// `prefix` is the document path so far (`items.0.`), `schema_prefix`
    /// the same without list indexes (`items.`).
    fn check_struct(
        &mut self,
        doc: &Document,
        fields: &[CanonicalField],
        prefix: &str,
        schema_prefix: &str,
    ) -> Result<(), String> {
        if let Some(extra) = doc.keys().find(|k| !fields.iter().any(|f| f.name == **k)) {
            return Err(format!(
                "field `{prefix}{extra}` is not in the schema inferred at discovery"
            ));
        }
        for field in fields {
            let path = format!("{prefix}{}", field.name);
            let schema_path = format!("{schema_prefix}{}", field.name);
            match doc.get(&field.name) {
                None => self.missing.push(path),
                Some(Bson::Null) => {}
                Some(value) => self.check_value(value, &field.ty, &path, &schema_path)?,
            }
        }
        Ok(())
    }

    fn check_value(
        &mut self,
        value: &Bson,
        ty: &CanonicalType,
        path: &str,
        schema_path: &str,
    ) -> Result<(), String> {
        let mismatch = || {
            Err(format!(
                "`{path}` holds {} but the schema inferred at discovery says {}",
                bson_type_name(value),
                match (ty, self.kinds.get(schema_path)) {
                    (CanonicalType::Utf8, Some(TextKind::ObjectId)) => "ObjectId".to_owned(),
                    (CanonicalType::Utf8, Some(TextKind::String)) => "String".to_owned(),
                    _ => format!("{ty:?}"),
                }
            ))
        };
        match (ty, value) {
            (CanonicalType::Struct(fields), Bson::Document(doc)) => {
                self.check_struct(doc, fields, &format!("{path}."), &format!("{schema_path}."))
            }
            (CanonicalType::List(item), Bson::Array(items)) => {
                for (i, v) in items.iter().enumerate() {
                    if !matches!(v, Bson::Null) {
                        self.check_value(v, &item.ty, &format!("{path}.{i}"), schema_path)?;
                    }
                }
                Ok(())
            }
            (CanonicalType::Decimal(spec), Bson::Decimal128(d)) => match Finite::parse(d) {
                Some(f) => f
                    .to_scaled(spec.precision(), spec.scale())
                    .map(drop)
                    .map_err(|e| format!("`{path}`: {e}")),
                None => Err(format!("`{path}`: {d} has no decimal equivalent")),
            },
            (CanonicalType::Decimal(spec), Bson::Int32(_) | Bson::Int64(_)) => {
                let n = match value {
                    Bson::Int32(x) => i64::from(*x),
                    Bson::Int64(x) => *x,
                    _ => unreachable!(),
                };
                let digits = n.unsigned_abs().checked_ilog10().map_or(1, |d| d + 1);
                if i64::from(digits) + i64::from(spec.scale().max(0)) > i64::from(spec.precision())
                {
                    return Err(format!(
                        "`{path}`: {n} does not fit decimal({}, {})",
                        spec.precision(),
                        spec.scale()
                    ));
                }
                Ok(())
            }
            (CanonicalType::Utf8, Bson::String(_)) => match self.kinds.get(schema_path) {
                Some(TextKind::ObjectId) => mismatch(),
                _ => Ok(()),
            },
            (CanonicalType::Utf8, Bson::ObjectId(_)) => match self.kinds.get(schema_path) {
                Some(TextKind::String) => mismatch(),
                _ => Ok(()),
            },
            _ if fits(ty, value) => Ok(()),
            _ => mismatch(),
        }
    }
}

/// Whether a scalar `value` is stored in a `ty` column without a cast.
fn fits(ty: &CanonicalType, value: &Bson) -> bool {
    match (ty, value) {
        (CanonicalType::Json, _)
        | (CanonicalType::Bool, Bson::Boolean(_))
        | (CanonicalType::Int32, Bson::Int32(_))
        | (CanonicalType::Int64, Bson::Int32(_) | Bson::Int64(_))
        | (CanonicalType::Float64, Bson::Double(_) | Bson::Int32(_))
        | (CanonicalType::Timestamp { .. }, Bson::DateTime(_)) => true,
        (CanonicalType::Uuid, Bson::Binary(b)) => {
            b.subtype == BinarySubtype::Uuid && b.bytes.len() == 16
        }
        (CanonicalType::Binary, Bson::Binary(b)) => b.subtype == BinarySubtype::Generic,
        _ => false,
    }
}

fn bson_type_name(value: &Bson) -> String {
    format!("{:?}", value.element_type())
}

fn scale_int(x: i64, pow: i128) -> Result<i128, String> {
    i128::from(x)
        .checked_mul(pow)
        .ok_or_else(|| format!("{x} does not fit the decimal column"))
}

/// One column from values that already passed [`check_document`]; `None`
/// is null or missing.
fn build(field: &CanonicalField, values: &[Option<&Bson>]) -> Result<ArrayRef, String> {
    let unexpected = |v: &Bson| format!("unexpected {} value", bson_type_name(v));
    macro_rules! scalar {
        ($builder:expr, |$v:ident| $convert:expr) => {{
            let mut b = $builder;
            for value in values {
                match value {
                    None => b.append_null(),
                    Some($v) => b.append_value($convert),
                }
            }
            Ok(Arc::new(b.finish()) as ArrayRef)
        }};
    }
    match &field.ty {
        CanonicalType::Bool => scalar!(BooleanBuilder::new(), |v| match v {
            Bson::Boolean(x) => *x,
            other => return Err(unexpected(other)),
        }),
        CanonicalType::Int32 => scalar!(Int32Builder::new(), |v| match v {
            Bson::Int32(x) => *x,
            other => return Err(unexpected(other)),
        }),
        CanonicalType::Int64 => scalar!(Int64Builder::new(), |v| match v {
            Bson::Int32(x) => i64::from(*x),
            Bson::Int64(x) => *x,
            other => return Err(unexpected(other)),
        }),
        CanonicalType::Float64 => scalar!(Float64Builder::new(), |v| match v {
            Bson::Double(x) => *x,
            Bson::Int32(x) => f64::from(*x),
            other => return Err(unexpected(other)),
        }),
        CanonicalType::Decimal(spec) if spec.precision() <= 38 => {
            let (p, s) = (spec.precision(), spec.scale());
            let pow = 10_i128.pow(s.max(0) as u32);
            let mut b = Decimal128Builder::new();
            for value in values {
                match value {
                    None => b.append_null(),
                    Some(Bson::Decimal128(d)) => b.append_value(
                        Finite::parse(d)
                            .ok_or_else(|| format!("{d} has no decimal equivalent"))?
                            .to_scaled(p, s)?,
                    ),
                    Some(Bson::Int32(x)) => b.append_value(scale_int(i64::from(*x), pow)?),
                    Some(Bson::Int64(x)) => b.append_value(scale_int(*x, pow)?),
                    Some(other) => return Err(unexpected(other)),
                }
            }
            let array = b
                .finish()
                .with_precision_and_scale(p, s)
                .map_err(|e| e.to_string())?;
            Ok(Arc::new(array))
        }
        CanonicalType::Utf8 => scalar!(StringBuilder::new(), |v| match v {
            Bson::String(x) => x.clone(),
            Bson::ObjectId(x) => x.to_hex(),
            other => return Err(unexpected(other)),
        }),
        CanonicalType::Json => scalar!(StringBuilder::new(), |v| {
            (*v).clone().into_relaxed_extjson().to_string()
        }),
        CanonicalType::Timestamp { tz } => {
            let mut b = TimestampMicrosecondBuilder::new().with_timezone_opt(tz.clone());
            for value in values {
                match value {
                    None => b.append_null(),
                    Some(Bson::DateTime(at)) => b.append_value(
                        at.timestamp_millis()
                            .checked_mul(1000)
                            .ok_or_else(|| format!("date {at} is out of range"))?,
                    ),
                    Some(other) => return Err(unexpected(other)),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        CanonicalType::Uuid => {
            let mut b = FixedSizeBinaryBuilder::new(16);
            for value in values {
                match value {
                    None => b.append_null(),
                    Some(Bson::Binary(x)) => {
                        b.append_value(&x.bytes).map_err(|e| e.to_string())?;
                    }
                    Some(other) => return Err(unexpected(other)),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        CanonicalType::Binary => scalar!(BinaryBuilder::new(), |v| match v {
            Bson::Binary(x) => &x.bytes,
            other => return Err(unexpected(other)),
        }),
        CanonicalType::Struct(children) => {
            let mut docs = Vec::with_capacity(values.len());
            for value in values {
                docs.push(match value {
                    None => None,
                    Some(Bson::Document(d)) => Some(d),
                    Some(other) => return Err(unexpected(other)),
                });
            }
            let columns = children
                .iter()
                .map(|child| {
                    let values: Vec<_> = docs
                        .iter()
                        .map(|d| d.and_then(|d| present(d.get(&child.name))))
                        .collect();
                    build(child, &values).map_err(|e| format!("{}.{e}", child.name))
                })
                .collect::<Result<Vec<_>, _>>()?;
            let fields: Fields = children.iter().map(CanonicalField::to_arrow).collect();
            let nulls = NullBuffer::from_iter(docs.iter().map(Option::is_some));
            Ok(Arc::new(
                StructArray::try_new(fields, columns, Some(nulls)).map_err(|e| e.to_string())?,
            ))
        }
        CanonicalType::List(item) => {
            let mut lengths = Vec::with_capacity(values.len());
            let mut elements = Vec::new();
            for value in values {
                match value {
                    None => lengths.push(0),
                    Some(Bson::Array(items)) => {
                        lengths.push(items.len());
                        elements.extend(items.iter().map(|v| present(Some(v))));
                    }
                    Some(other) => return Err(unexpected(other)),
                }
            }
            let child = build(item, &elements)?;
            let nulls = NullBuffer::from_iter(values.iter().map(Option::is_some));
            Ok(Arc::new(
                ListArray::try_new(
                    Arc::new(item.to_arrow()),
                    OffsetBuffer::from_lengths(lengths),
                    child,
                    Some(nulls),
                )
                .map_err(|e| e.to_string())?,
            ))
        }
        other => Err(format!("{other:?} columns are not read from Mongo")),
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::cast::AsArray;
    use arrow_array::types::{Decimal128Type, Int64Type};
    use arrow_array::{Array, StringArray};
    use ferry_core::missing::is_missing;
    use ferry_core::types::DecimalSpec;
    use mongodb::bson::{Decimal128, doc, oid::ObjectId};

    use super::*;

    fn utf8(name: &str) -> CanonicalField {
        CanonicalField::new(name, CanonicalType::Utf8, true)
    }

    fn entity(fields: Vec<CanonicalField>) -> Entity {
        let mut all = vec![CanonicalField::new("_id", CanonicalType::Int64, false)];
        all.extend(fields);
        Entity::new("t", all, vec!["_id".into()])
            .unwrap()
            .with_missing_tracking()
    }

    fn batch(entity: &Entity, docs: &[Document]) -> Result<RecordBatch, String> {
        to_batch(entity, entity.arrow_schema(), docs, &TextKinds::new())
    }

    #[test]
    fn missing_and_null_are_told_apart_at_every_depth() {
        let e = entity(vec![
            utf8("a"),
            CanonicalField::new(
                "c",
                CanonicalType::Struct(vec![utf8("id"), utf8("email")]),
                true,
            ),
            CanonicalField::new(
                "items",
                CanonicalType::List(Box::new(CanonicalField::new(
                    "item",
                    CanonicalType::Struct(vec![utf8("sku")]),
                    true,
                ))),
                true,
            ),
        ]);
        let docs = [
            doc! { "_id": 1_i64, "a": Bson::Null, "c": { "id": "c1" }, "items": [{}, { "sku": Bson::Null }] },
            doc! { "_id": 2_i64, "c": Bson::Null },
        ];
        let b = batch(&e, &docs).unwrap();
        let a = b.column_by_name("a").unwrap();
        assert!(a.is_null(0) && a.is_null(1));
        assert!(!is_missing(&b, 0, "a"), "explicit null");
        assert!(is_missing(&b, 1, "a"));
        assert!(is_missing(&b, 0, "c.email"));
        assert!(!is_missing(&b, 0, "c.id"));
        assert!(!is_missing(&b, 1, "c"), "c is null, not missing");
        assert!(
            !is_missing(&b, 1, "c.email"),
            "a null parent has no missing children"
        );
        assert!(is_missing(&b, 0, "items.0.sku"));
        assert!(!is_missing(&b, 0, "items.1.sku"));
        assert!(is_missing(&b, 1, "items"));
    }

    #[test]
    fn values_land_in_their_columns() {
        let money = CanonicalType::Decimal(DecimalSpec::new(38, 3).unwrap());
        let e = entity(vec![
            utf8("oid"),
            CanonicalField::new("money", money, true),
            CanonicalField::new("v", CanonicalType::Json, true),
            CanonicalField::new("n", CanonicalType::Int64, true),
        ]);
        let oid = ObjectId::parse_str("650000000000000000000001").unwrap();
        let docs = [
            doc! { "_id": 1_i64, "oid": oid, "money": "1.005".parse::<Decimal128>().unwrap(), "v": 2_i32, "n": 1_i32 },
            doc! { "_id": 2_i64, "money": 7_i64, "v": "three", "n": 9_000_000_000_i64 },
        ];
        let b = batch(&e, &docs).unwrap();
        let oid = b.column_by_name("oid").unwrap().as_string::<i32>();
        assert_eq!(oid.value(0), "650000000000000000000001");
        let money = b
            .column_by_name("money")
            .unwrap()
            .as_primitive::<Decimal128Type>();
        assert_eq!(money.value(0), 1005);
        assert_eq!(money.value(1), 7000);
        let v = b.column_by_name("v").unwrap().as_string::<i32>();
        assert_eq!(v.value(0), "2");
        assert_eq!(v.value(1), "\"three\"");
        let n = b.column_by_name("n").unwrap().as_primitive::<Int64Type>();
        assert_eq!(n.values(), &[1, 9_000_000_000]);
    }

    #[test]
    fn a_document_that_does_not_fit_names_its_id_and_path() {
        let e = entity(vec![CanonicalField::new("n", CanonicalType::Int32, true)]);
        for (doc, expected) in [
            (doc! { "_id": 7_i64, "n": "seven" }, "`n` holds String"),
            (doc! { "_id": 7_i64, "n": 7_i64 }, "`n` holds Int64"),
            (doc! { "_id": 7_i64, "n": 7.0 }, "`n` holds Double"),
            (
                doc! { "_id": 7_i64, "extra": 1_i32 },
                "field `extra` is not in the schema",
            ),
        ] {
            let err = batch(&e, &[doc]).unwrap_err();
            assert!(err.contains("_id 7") && err.contains(expected), "{err}");
        }
    }

    #[test]
    fn decimals_are_never_rounded_to_fit() {
        let e = entity(vec![CanonicalField::new(
            "m",
            CanonicalType::Decimal(DecimalSpec::new(38, 2).unwrap()),
            true,
        )]);
        let doc = doc! { "_id": 1_i64, "m": "1.005".parse::<Decimal128>().unwrap() };
        let err = batch(&e, &[doc]).unwrap_err();
        assert!(err.contains("more than 2 digits"), "{err}");
    }

    #[test]
    fn an_object_id_column_refuses_strings_and_the_reverse() {
        let e = entity(vec![
            utf8("oid"),
            CanonicalField::new(
                "c",
                CanonicalType::List(Box::new(CanonicalField::new(
                    "item",
                    CanonicalType::Struct(vec![utf8("name")]),
                    true,
                ))),
                true,
            ),
        ]);
        let kinds = TextKinds::from([
            ("oid".to_owned(), TextKind::ObjectId),
            ("c.name".to_owned(), TextKind::String),
        ]);
        let oid = ObjectId::parse_str("650000000000000000000001").unwrap();
        let ok = doc! { "_id": 1_i64, "oid": oid, "c": [{ "name": "x" }] };
        assert!(to_batch(&e, e.arrow_schema(), &[ok], &kinds).is_ok());
        for (doc, expected) in [
            (
                doc! { "_id": 2_i64, "oid": "650000000000000000000001" },
                "`oid` holds String but the schema inferred at discovery says ObjectId",
            ),
            (
                doc! { "_id": 3_i64, "c": [{ "name": "x" }, { "name": oid }] },
                "`c.1.name` holds ObjectId but the schema inferred at discovery says String",
            ),
        ] {
            let err = to_batch(&e, e.arrow_schema(), &[doc], &kinds).unwrap_err();
            assert!(err.contains(expected), "{err}");
        }
    }

    #[test]
    fn integers_must_fit_a_decimal_column_with_its_scale() {
        let e = entity(vec![CanonicalField::new(
            "m",
            CanonicalType::Decimal(DecimalSpec::new(38, 30).unwrap()),
            true,
        )]);
        let fits = doc! { "_id": 1_i64, "m": 12_345_678_i32 };
        let b = batch(&e, &[fits]).unwrap();
        assert_eq!(
            b.column_by_name("m")
                .unwrap()
                .as_primitive::<Decimal128Type>()
                .value(0),
            12_345_678 * 10_i128.pow(30)
        );
        for n in [
            Bson::Int32(123_456_789),
            Bson::Int64(i64::MAX),
            Bson::Int64(i64::MIN),
        ] {
            let err = batch(&e, &[doc! { "_id": 2_i64, "m": n }]).unwrap_err();
            assert!(err.contains("does not fit decimal(38, 30)"), "{err}");
        }
    }

    #[test]
    fn nested_unknown_fields_are_errors_too() {
        let e = entity(vec![CanonicalField::new(
            "c",
            CanonicalType::Struct(vec![utf8("id")]),
            true,
        )]);
        let doc = doc! { "_id": 1_i64, "c": { "id": "c1", "tier": "gold" } };
        let err = batch(&e, &[doc]).unwrap_err();
        assert!(err.contains("`c.tier` is not in the schema"), "{err}");
    }

    #[test]
    fn variant_values_keep_their_bson_type_but_not_their_width() {
        let e = entity(vec![CanonicalField::new("v", CanonicalType::Json, true)]);
        let docs = [
            doc! { "_id": 1_i64, "v": "1.5".parse::<Decimal128>().unwrap() },
            doc! { "_id": 2_i64, "v": { "nested": [1_i32, "x"] } },
            doc! { "_id": 3_i64, "v": Bson::Null },
            doc! { "_id": 4_i64, "v": 5_i64 },
            doc! { "_id": 5_i64, "v": "5" },
        ];
        let b = batch(&e, &docs).unwrap();
        let v = b
            .column_by_name("v")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(v.value(0), r#"{"$numberDecimal":"1.5"}"#);
        assert_eq!(v.value(1), r#"{"nested":[1,"x"]}"#);
        assert!(v.is_null(2));
        assert_eq!((v.value(3), v.value(4)), ("5", "\"5\""));
    }
}
