//! The order-independent multiset checksum.
//!
//! Each row's mapped columns are normalized, encoded without ambiguity and
//! hashed to 128 bits (XXH3-128). A table's checksum is the row count and
//! the **sum** of its row hashes modulo 2^128. Addition is commutative, so
//! the order rows are written or read back in doesn't matter, and unlike
//! XOR it never cancels: a row present twice adds its hash twice, where
//! XOR would remove it. The same checksum is computed at write time, over
//! what the runner hands the sink, and over a read-back of the target.
//!
//! Normalization makes values that are equal after the round trip hash
//! equal:
//!
//! - **Integers and decimals** hash as one exact number, whatever the
//!   width or scale: `Int32 7`, `Int64 7` and `Decimal(10, 2) 7.00` agree.
//! - **Timestamps** hash as an instant (the zone label is ignored), rounded
//!   to the fractional-second digits the target keeps, half away from zero
//!   as Postgres rounds. A column stored as `timestamptz(3)` gets the same
//!   checksum at write time as after the database rounded it.
//! - **Floats** are widened to `f64`; `-0.0` is `0.0` and every NaN is one.
//! - **JSON** is re-serialized with sorted object keys and no whitespace,
//!   as `jsonb` returns it. Number spellings (`1e2` vs `100`) are not
//!   normalized.

use std::fmt;

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Decimal128Type, Decimal256Type, Float32Type, Float64Type, Int8Type, Int16Type,
    Int32Type, Int64Type, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{Array, RecordBatch};
use arrow_buffer::i256;
use arrow_schema::{DataType, TimeUnit};
use ferry_core::types::CanonicalType;
use ferry_core::{Result, TargetTable};
use serde::{Deserialize, Serialize};

use crate::verifier_error;

/// Fractional-second digits a timestamp keeps when nothing says otherwise:
/// microseconds, the canonical unit.
pub const MICROS: u8 = 6;

/// A table's rows as a multiset: how many, and the sum of their hashes
/// modulo 2^128.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Checksum {
    pub rows: u64,
    #[serde(with = "hex_u128")]
    pub sum: u128,
}

impl Checksum {
    /// Adds one row by its hash.
    pub fn add(&mut self, row_hash: u128) {
        self.rows += 1;
        self.sum = self.sum.wrapping_add(row_hash);
    }

    /// Adds every row of `batch`, hashed by `spec`.
    pub fn add_batch(&mut self, spec: &ChecksumSpec, batch: &RecordBatch) -> Result<()> {
        for hash in spec.row_hashes(batch)? {
            self.add(hash);
        }
        Ok(())
    }

    /// The checksum of both multisets together.
    pub fn merge(&mut self, other: &Checksum) {
        self.rows += other.rows;
        self.sum = self.sum.wrapping_add(other.sum);
    }
}

impl fmt::Display for Checksum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} rows, sum {:032x}", self.rows, self.sum)
    }
}

/// Which columns of a target table are hashed, in which order, and how.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChecksumSpec {
    table: String,
    columns: Vec<ColumnSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnSpec {
    pub name: String,
    pub ty: CanonicalType,
    /// Fractional-second digits timestamps are rounded to, 0 to 6.
    pub timestamp_digits: u8,
}

impl ChecksumSpec {
    /// Every mapped column of `table`, in DDL order, timestamps at
    /// microseconds.
    pub fn new(table: &TargetTable) -> Self {
        Self {
            table: table.name.clone(),
            columns: table
                .fields
                .iter()
                .map(|f| ColumnSpec {
                    name: f.name.clone(),
                    ty: f.ty.clone(),
                    timestamp_digits: MICROS,
                })
                .collect(),
        }
    }

    /// Rounds `column`'s timestamps to `digits` fractional-second digits,
    /// on both sides, because the target keeps no more than that.
    pub fn with_timestamp_digits(mut self, column: &str, digits: u8) -> Result<Self> {
        if digits > MICROS {
            return Err(verifier_error(format!(
                "`{}.{column}`: timestamps keep at most {MICROS} fractional digits, not {digits}",
                self.table
            )));
        }
        let Some(c) = self.columns.iter_mut().find(|c| c.name == column) else {
            return Err(verifier_error(format!(
                "`{}` has no column `{column}`",
                self.table
            )));
        };
        c.timestamp_digits = digits;
        Ok(self)
    }

    pub fn table(&self) -> &str {
        &self.table
    }

    pub fn columns(&self) -> &[ColumnSpec] {
        &self.columns
    }

    /// The hash of every row of `batch`. Columns are found by name, so a
    /// batch may order them differently or carry extra ones.
    pub fn row_hashes(&self, batch: &RecordBatch) -> Result<Vec<u128>> {
        let columns = self
            .columns
            .iter()
            .map(|c| {
                batch.column_by_name(&c.name).ok_or_else(|| {
                    verifier_error(format!(
                        "`{}` rows have no column `{}` to checksum",
                        self.table, c.name
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut buf = Vec::new();
        let mut hashes = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            buf.clear();
            for (spec, column) in self.columns.iter().zip(&columns) {
                let value = Value {
                    ty: Some(&spec.ty),
                    digits: spec.timestamp_digits,
                };
                value
                    .encode(column.as_ref(), row, &mut buf)
                    .map_err(|e| verifier_error(format!("`{}.{}`: {e}", self.table, spec.name)))?;
            }
            hashes.push(twox_hash::XxHash3_128::oneshot(&buf));
        }
        Ok(hashes)
    }
}

// One tag byte starts every encoded value, and every variable-length value
// carries its length, so two different rows can't encode to the same bytes.
const NULL: u8 = 0;
const BOOL: u8 = 1;
const NUMBER: u8 = 2;
const FLOAT: u8 = 3;
const TEXT: u8 = 4;
const BYTES: u8 = 5;
const DATE: u8 = 6;
const TIMESTAMP: u8 = 7;
const FIXED: u8 = 8;
const JSON: u8 = 9;
const JSON_UNPARSED: u8 = 10;
const LIST: u8 = 11;
const STRUCT: u8 = 12;
const MAP: u8 = 13;

/// How to encode one value: its canonical type when known (to tell JSON
/// from text), and the timestamp precision.
#[derive(Clone, Copy)]
struct Value<'a> {
    ty: Option<&'a CanonicalType>,
    digits: u8,
}

impl Value<'_> {
    fn encode(self, array: &dyn Array, row: usize, out: &mut Vec<u8>) -> Result<(), String> {
        if array.is_null(row) {
            out.push(NULL);
            return Ok(());
        }
        match array.data_type() {
            DataType::Boolean => {
                out.push(BOOL);
                out.push(u8::from(array.as_boolean().value(row)));
            }
            DataType::Int8 => integer(array.as_primitive::<Int8Type>().value(row).into(), out),
            DataType::Int16 => integer(array.as_primitive::<Int16Type>().value(row).into(), out),
            DataType::Int32 => integer(array.as_primitive::<Int32Type>().value(row).into(), out),
            DataType::Int64 => integer(array.as_primitive::<Int64Type>().value(row).into(), out),
            DataType::UInt8 => integer(array.as_primitive::<UInt8Type>().value(row).into(), out),
            DataType::UInt16 => integer(array.as_primitive::<UInt16Type>().value(row).into(), out),
            DataType::UInt32 => integer(array.as_primitive::<UInt32Type>().value(row).into(), out),
            DataType::UInt64 => integer(array.as_primitive::<UInt64Type>().value(row).into(), out),
            DataType::Decimal128(_, scale) => number(
                i256::from_i128(array.as_primitive::<Decimal128Type>().value(row)),
                *scale,
                out,
            ),
            DataType::Decimal256(_, scale) => number(
                array.as_primitive::<Decimal256Type>().value(row),
                *scale,
                out,
            ),
            DataType::Float32 => float(array.as_primitive::<Float32Type>().value(row).into(), out),
            DataType::Float64 => float(array.as_primitive::<Float64Type>().value(row), out),
            DataType::Utf8 | DataType::LargeUtf8 => {
                let text = match array.data_type() {
                    DataType::Utf8 => array.as_string::<i32>().value(row),
                    _ => array.as_string::<i64>().value(row),
                };
                if self.ty == Some(&CanonicalType::Json) {
                    json(text, out);
                } else {
                    bytes(TEXT, text.as_bytes(), out);
                }
            }
            DataType::Binary => bytes(BYTES, array.as_binary::<i32>().value(row), out),
            DataType::LargeBinary => bytes(BYTES, array.as_binary::<i64>().value(row), out),
            DataType::FixedSizeBinary(_) => {
                bytes(FIXED, array.as_fixed_size_binary().value(row), out)
            }
            DataType::Date32 => {
                out.push(DATE);
                out.extend(array.as_primitive::<Date32Type>().value(row).to_le_bytes());
            }
            DataType::Timestamp(unit, _) => {
                let (value, nanos_per) = match unit {
                    TimeUnit::Second => (
                        array.as_primitive::<TimestampSecondType>().value(row),
                        1_000_000_000,
                    ),
                    TimeUnit::Millisecond => (
                        array.as_primitive::<TimestampMillisecondType>().value(row),
                        1_000_000,
                    ),
                    TimeUnit::Microsecond => (
                        array.as_primitive::<TimestampMicrosecondType>().value(row),
                        1_000,
                    ),
                    TimeUnit::Nanosecond => (
                        array.as_primitive::<TimestampNanosecondType>().value(row),
                        1,
                    ),
                };
                out.push(TIMESTAMP);
                let nanos = i128::from(value) * nanos_per;
                out.extend(round_to_digits(nanos, self.digits).to_le_bytes());
            }
            DataType::List(_) | DataType::LargeList(_) => {
                let items = match array.data_type() {
                    DataType::List(_) => array.as_list::<i32>().value(row),
                    _ => array.as_list::<i64>().value(row),
                };
                let item = Value {
                    ty: match self.ty {
                        Some(CanonicalType::List(item)) => Some(&item.ty),
                        _ => None,
                    },
                    ..self
                };
                out.push(LIST);
                out.extend((items.len() as u64).to_le_bytes());
                for i in 0..items.len() {
                    item.encode(items.as_ref(), i, out)?;
                }
            }
            DataType::Struct(_) => {
                let fields = array.as_struct();
                out.push(STRUCT);
                out.extend((fields.num_columns() as u64).to_le_bytes());
                for (i, child) in fields.columns().iter().enumerate() {
                    let ty = match self.ty {
                        Some(CanonicalType::Struct(declared)) => declared.get(i).map(|f| &f.ty),
                        _ => None,
                    };
                    Value { ty, ..self }.encode(child.as_ref(), row, out)?;
                }
            }
            DataType::Map(_, _) => {
                let entries = array.as_map().value(row);
                let (key, value) = match self.ty {
                    Some(CanonicalType::Map { key, value }) => (Some(&**key), Some(&value.ty)),
                    _ => (None, None),
                };
                out.push(MAP);
                out.extend((entries.len() as u64).to_le_bytes());
                for i in 0..entries.len() {
                    Value { ty: key, ..self }.encode(entries.column(0).as_ref(), i, out)?;
                    Value { ty: value, ..self }.encode(entries.column(1).as_ref(), i, out)?;
                }
            }
            other => return Err(format!("{other} values can't be checksummed")),
        }
        Ok(())
    }
}

fn integer(value: i128, out: &mut Vec<u8>) {
    number(i256::from_i128(value), 0, out);
}

/// An exact number as its unscaled value at the smallest scale that keeps
/// it exact (never below 0), so width and scale don't change the bytes.
fn number(mut unscaled: i256, scale: i8, out: &mut Vec<u8>) {
    let ten = i256::from_i128(10);
    let mut scale = i32::from(scale);
    if unscaled == i256::ZERO {
        scale = 0;
    }
    while scale > 0 && unscaled.checked_rem(ten) == Some(i256::ZERO) {
        unscaled = unscaled.checked_div(ten).expect("ten is not zero");
        scale -= 1;
    }
    // A negative scale (`Decimal(5, -2)`) is a whole number with zeros
    // dropped; spell them out, unless that would overflow.
    while scale < 0 {
        match unscaled.checked_mul(ten) {
            Some(v) => {
                unscaled = v;
                scale += 1;
            }
            None => break,
        }
    }
    out.push(NUMBER);
    out.extend(unscaled.to_le_bytes());
    out.extend(scale.to_le_bytes());
}

fn float(value: f64, out: &mut Vec<u8>) {
    let value = if value == 0.0 {
        0.0
    } else if value.is_nan() {
        f64::NAN
    } else {
        value
    };
    out.push(FLOAT);
    out.extend(value.to_bits().to_le_bytes());
}

fn bytes(tag: u8, value: &[u8], out: &mut Vec<u8>) {
    out.push(tag);
    out.extend((value.len() as u64).to_le_bytes());
    out.extend(value);
}

/// JSON with sorted object keys and no whitespace. Text that doesn't parse
/// is hashed as it is, under its own tag.
fn json(text: &str, out: &mut Vec<u8>) {
    match serde_json::from_str::<serde_json::Value>(text) {
        Ok(value) => {
            let mut canonical = String::new();
            write_json(&value, &mut canonical);
            bytes(JSON, canonical.as_bytes(), out);
        }
        Err(_) => bytes(JSON_UNPARSED, text.as_bytes(), out),
    }
}

fn write_json(value: &serde_json::Value, out: &mut String) {
    use serde_json::Value as J;
    match value {
        J::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_json(item, out);
            }
            out.push(']');
        }
        J::Object(map) => {
            let mut entries: Vec<_> = map.iter().collect();
            entries.sort_by(|a, b| a.0.cmp(b.0));
            out.push('{');
            for (i, (key, item)) in entries.into_iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&J::String(key.clone()).to_string());
                out.push(':');
                write_json(item, out);
            }
            out.push('}');
        }
        scalar => out.push_str(&scalar.to_string()),
    }
}

/// `nanos` rounded to `digits` fractional-second digits, half away from
/// zero, the way Postgres applies a `timestamp(p)` typmod.
fn round_to_digits(nanos: i128, digits: u8) -> i128 {
    let step = 10_i128.pow(9 - u32::from(digits.min(9)));
    let (q, r) = (nanos / step, nanos % step);
    let q = if 2 * r.abs() >= step {
        q + nanos.signum()
    } else {
        q
    };
    q * step
}

mod hex_u128 {
    use serde::{Deserialize, Deserializer, Serializer, de};

    pub fn serialize<S: Serializer>(value: &u128, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&format!("{value:032x}"))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<u128, D::Error> {
        let text = String::deserialize(d)?;
        u128::from_str_radix(&text, 16).map_err(de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rounding_matches_postgres_half_away_from_zero() {
        let us = 1_000; // nanos per microsecond
        // 12:00:00.123500 at 3 digits rounds up, .123499 down.
        assert_eq!(round_to_digits(123_500 * us, 3), 124_000 * us);
        assert_eq!(round_to_digits(123_499 * us, 3), 123_000 * us);
        // Before the epoch, a half rounds away from zero too.
        assert_eq!(round_to_digits(-123_500 * us, 3), -124_000 * us);
        assert_eq!(round_to_digits(-123_499 * us, 3), -123_000 * us);
        // 0 digits: whole seconds.
        assert_eq!(round_to_digits(1_500_000 * us, 0), 2_000_000 * us);
        assert_eq!(round_to_digits(1_499_999 * us, 0), 1_000_000 * us);
        // Microseconds are left alone at 6 digits.
        assert_eq!(round_to_digits(123_456 * us, 6), 123_456 * us);
    }

    #[test]
    fn exact_numbers_normalize_width_and_scale() {
        let enc = |unscaled: i128, scale: i8| {
            let mut out = Vec::new();
            number(i256::from_i128(unscaled), scale, &mut out);
            out
        };
        assert_eq!(enc(700, 2), enc(7, 0));
        assert_eq!(enc(0, 4), enc(0, 0));
        assert_eq!(enc(7, -2), enc(700, 0));
        assert_eq!(enc(-1250, 3), enc(-125, 2));
        assert_ne!(enc(7, 1), enc(7, 0));
    }

    #[test]
    fn json_ignores_key_order_and_whitespace_but_not_values() {
        let enc = |text: &str| {
            let mut out = Vec::new();
            json(text, &mut out);
            out
        };
        assert_eq!(
            enc(r#"{"b": [1, {"y": 2, "x": 1}], "a": null}"#),
            enc(r#"{"a":null,"b":[1,{"x":1,"y":2}]}"#)
        );
        assert_ne!(enc(r#"{"a": 1}"#), enc(r#"{"a": 2}"#));
        // Text that isn't JSON still hashes, and differently from JSON.
        assert_ne!(enc("{oops"), enc(r#""{oops""#));
    }
}
