//! The golden source documents (`fixtures/golden/source/orders.json`) as an
//! entity and an Arrow batch, the way the Mongo source presents them:
//! absent fields are null and listed in `_ferry_missing`.
//!
//! `items[].qty` is inferred as int64. One document has `"three"` there,
//! which the Mongo source makes an error row (the whole document is
//! dead-lettered); here it is null, and tests leave that document out.

use std::sync::Arc;

use arrow_array::builder::{
    Decimal128Builder, Int64Builder, ListBuilder, StringBuilder, StructBuilder,
    TimestampMicrosecondBuilder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field};
use ferry_core::Entity;
use ferry_core::missing::MissingPathsBuilder;
use ferry_core::types::{CanonicalField, CanonicalType, DecimalSpec};
use serde_json::Value;

use super::repo;

pub const SCALE: i8 = 10;

pub fn money() -> CanonicalType {
    CanonicalType::Decimal(DecimalSpec::new(38, SCALE).unwrap())
}

pub fn utc() -> CanonicalType {
    CanonicalType::Timestamp {
        tz: Some("UTC".into()),
    }
}

fn customer_fields() -> Vec<CanonicalField> {
    vec![
        CanonicalField::new("id", CanonicalType::Utf8, true),
        CanonicalField::new("email", CanonicalType::Utf8, true),
    ]
}

fn item_fields() -> Vec<CanonicalField> {
    vec![
        CanonicalField::new("sku", CanonicalType::Utf8, true),
        CanonicalField::new("qty", CanonicalType::Int64, true),
        CanonicalField::new("price", money(), true),
    ]
}

pub fn entity() -> Entity {
    Entity::new(
        "orders",
        vec![
            CanonicalField::new("_id", CanonicalType::Utf8, false),
            CanonicalField::new("status", CanonicalType::Utf8, true),
            CanonicalField::new("customer", CanonicalType::Struct(customer_fields()), true),
            CanonicalField::new("total", money(), true),
            CanonicalField::new("created_at", utc(), true),
            CanonicalField::new("deleted_at", utc(), true),
            CanonicalField::new(
                "items",
                CanonicalType::List(Box::new(CanonicalField::new(
                    "item",
                    CanonicalType::Struct(item_fields()),
                    true,
                ))),
                true,
            ),
        ],
        vec!["_id".into()],
    )
    .unwrap()
    .with_missing_tracking()
}

pub fn documents() -> Vec<Value> {
    let text = std::fs::read_to_string(repo("fixtures/golden/source/orders.json")).unwrap();
    serde_json::from_str(&text).unwrap()
}

pub fn id(doc: &Value) -> String {
    doc["_id"]["$oid"].as_str().unwrap().to_owned()
}

/// `"25.50"` as an integer scaled by 10^SCALE.
pub fn scaled(text: &str) -> i128 {
    let (int, frac) = text.split_once('.').unwrap_or((text, ""));
    let negative = int.starts_with('-');
    let digits = format!(
        "{}{frac:0<width$}",
        int.trim_start_matches('-'),
        width = SCALE as usize
    );
    let v: i128 = digits.parse().unwrap();
    if negative { -v } else { v }
}

/// `2026-03-01T10:00:00Z` in microseconds since the epoch.
fn micros(text: &str) -> i64 {
    let n = |r: std::ops::Range<usize>| text[r].parse::<i64>().unwrap();
    let (y, m, d) = (n(0..4), n(5..7), n(8..10));
    // Days from civil (Howard Hinnant).
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    (days * 86_400 + n(11..13) * 3600 + n(14..16) * 60 + n(17..19)) * 1_000_000
}

fn decimal_builder() -> Decimal128Builder {
    Decimal128Builder::new().with_data_type(DataType::Decimal128(38, SCALE))
}

fn timestamp(builder: &mut TimestampMicrosecondBuilder, doc: &Value, key: &str) {
    match doc.get(key) {
        Some(Value::Object(o)) => builder.append_value(micros(o["$date"].as_str().unwrap())),
        _ => builder.append_null(),
    }
}

pub fn batch() -> RecordBatch {
    let entity = entity();
    let docs = documents();
    let mut ids = StringBuilder::new();
    let mut status = StringBuilder::new();
    let to_fields =
        |f: Vec<CanonicalField>| -> Vec<Field> { f.iter().map(|f| f.to_arrow()).collect() };
    let mut customer = StructBuilder::from_fields(to_fields(customer_fields()), docs.len());
    let mut total = decimal_builder();
    let mut created = TimestampMicrosecondBuilder::new().with_timezone("UTC");
    let mut deleted = TimestampMicrosecondBuilder::new().with_timezone("UTC");
    let item_type = entity.field("items").unwrap().to_arrow();
    let DataType::List(item_field) = item_type.data_type() else {
        unreachable!()
    };
    let mut items = ListBuilder::new(StructBuilder::from_fields(to_fields(item_fields()), 0))
        .with_field(item_field.clone());
    let mut missing = MissingPathsBuilder::new();

    for doc in &docs {
        let mut absent = Vec::new();
        for key in [
            "status",
            "customer",
            "total",
            "created_at",
            "deleted_at",
            "items",
        ] {
            if doc.get(key).is_none() {
                absent.push(key.to_owned());
            }
        }
        ids.append_value(id(doc));
        status.append_option(doc.get("status").and_then(Value::as_str));
        match doc.get("customer") {
            Some(Value::Object(c)) => {
                if !c.contains_key("email") {
                    absent.push("customer.email".into());
                }
                let fields = customer.field_builder::<StringBuilder>(0).unwrap();
                fields.append_option(c.get("id").and_then(Value::as_str));
                let fields = customer.field_builder::<StringBuilder>(1).unwrap();
                fields.append_option(c.get("email").and_then(Value::as_str));
                customer.append(true);
            }
            _ => {
                customer
                    .field_builder::<StringBuilder>(0)
                    .unwrap()
                    .append_null();
                customer
                    .field_builder::<StringBuilder>(1)
                    .unwrap()
                    .append_null();
                customer.append(false);
            }
        }
        total.append_option(
            doc.get("total")
                .map(|t| scaled(t["$numberDecimal"].as_str().unwrap())),
        );
        timestamp(&mut created, doc, "created_at");
        timestamp(&mut deleted, doc, "deleted_at");
        match doc.get("items") {
            Some(Value::Array(list)) => {
                for item in list {
                    let s = items.values();
                    s.field_builder::<StringBuilder>(0)
                        .unwrap()
                        .append_option(item.get("sku").and_then(Value::as_str));
                    s.field_builder::<Int64Builder>(1)
                        .unwrap()
                        .append_option(item.get("qty").and_then(Value::as_i64));
                    s.field_builder::<Decimal128Builder>(2)
                        .unwrap()
                        .append_option(
                            item.get("price")
                                .map(|p| scaled(p["$numberDecimal"].as_str().unwrap())),
                        );
                    s.append(true);
                }
                items.append(true);
            }
            _ => items.append(false),
        }
        missing.append_row(&absent);
    }

    // The struct builders don't take a decimal type, so fix the price
    // column's type by casting.
    let items: ArrayRef = arrow_cast::cast(&items.finish(), item_type.data_type()).unwrap();
    let customer: ArrayRef = Arc::new(customer.finish());
    let columns: Vec<ArrayRef> = vec![
        Arc::new(ids.finish()),
        Arc::new(status.finish()),
        customer,
        Arc::new(total.finish()),
        Arc::new(created.finish()),
        Arc::new(deleted.finish()),
        items,
        missing.finish(),
    ];
    RecordBatch::try_new(entity.arrow_schema(), columns).unwrap()
}

/// The document the source dead-letters (`qty: "three"`).
pub const DEAD_LETTERED: &str = "650000000000000000000006";
