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
//! - **Nested values stored as documents.** A sink may store a struct, a
//!   map or a list of them as a JSON document (Postgres `jsonb`), so it is
//!   read back as JSON text. Such text, in a column whose canonical type is
//!   nested, is decoded by that type and hashed exactly as the Arrow value
//!   it was written from: arrays as lists, objects as structs (fields by
//!   name, in declared order), and the values JSON has no type for as the
//!   sink spells them, dates and timestamps in ISO 8601, UUIDs in their
//!   canonical form, bytes as `\x`-prefixed hex. Numbers go through
//!   `f64`, so a decimal of more than 15 significant digits inside a
//!   document doesn't verify; maps stored as documents can't be checked
//!   (their entry order is lost). Both fail verification, loudly.

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
                match self.ty {
                    Some(CanonicalType::Json) => json(text, out),
                    Some(
                        ty @ (CanonicalType::List(_)
                        | CanonicalType::Struct(_)
                        | CanonicalType::Map { .. }),
                    ) => {
                        let value: serde_json::Value = serde_json::from_str(text)
                            .map_err(|e| format!("a stored document isn't JSON: {e}"))?;
                        document(ty, &value, self.digits, out)?;
                    }
                    _ => bytes(TEXT, text.as_bytes(), out),
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

/// `value`, a document a sink stored for a value of type `ty`, encoded as
/// [`Value::encode`] encodes that value from Arrow.
fn document(
    ty: &CanonicalType,
    value: &serde_json::Value,
    digits: u8,
    out: &mut Vec<u8>,
) -> Result<(), String> {
    use CanonicalType as C;
    use serde_json::Value as J;
    let wrong = || format!("`{value}` in a stored document isn't a {ty:?}");
    match (ty, value) {
        (_, J::Null) => out.push(NULL),
        (C::Bool, J::Bool(b)) => {
            out.push(BOOL);
            out.push(u8::from(*b));
        }
        (
            C::Int8 | C::Int16 | C::Int32 | C::Int64 | C::UInt8 | C::UInt16 | C::UInt32 | C::UInt64,
            J::Number(n),
        ) => {
            let n = n
                .as_i64()
                .map(i128::from)
                .or_else(|| n.as_u64().map(i128::from))
                .ok_or_else(wrong)?;
            integer(n, out);
        }
        (C::Decimal(_), J::Number(n)) => {
            let (unscaled, scale) = decimal_text(&n.to_string()).ok_or_else(wrong)?;
            number(unscaled, scale, out);
        }
        // A float32 is hashed widened from its own bits at write time, and
        // the sink writes it in its shortest float32 spelling: narrow first.
        (C::Float32, J::Number(n)) => float(f64::from(n.as_f64().ok_or_else(wrong)? as f32), out),
        (C::Float64, J::Number(n)) => float(n.as_f64().ok_or_else(wrong)?, out),
        (C::Utf8, J::String(s)) => bytes(TEXT, s.as_bytes(), out),
        (C::Json, _) => {
            let mut canonical = String::new();
            write_json(value, &mut canonical);
            bytes(JSON, canonical.as_bytes(), out);
        }
        (C::Binary, J::String(s)) => {
            let hex = s.strip_prefix("\\x").ok_or_else(wrong)?;
            bytes(BYTES, &unhex(hex).ok_or_else(wrong)?, out);
        }
        (C::Uuid, J::String(s)) => {
            let hex: String = s.chars().filter(|c| *c != '-').collect();
            match unhex(&hex) {
                Some(b) if b.len() == 16 && s.len() == 36 => bytes(FIXED, &b, out),
                _ => return Err(wrong()),
            }
        }
        (C::Date, J::String(s)) => {
            let days = iso_date(s).ok_or_else(wrong)?;
            out.push(DATE);
            out.extend(i32::try_from(days).map_err(|_| wrong())?.to_le_bytes());
        }
        (C::Timestamp { .. }, J::String(s)) => {
            let micros = iso_timestamp(s).ok_or_else(wrong)?;
            out.push(TIMESTAMP);
            out.extend(round_to_digits(micros * 1_000, digits).to_le_bytes());
        }
        (C::List(item), J::Array(items)) => {
            out.push(LIST);
            out.extend((items.len() as u64).to_le_bytes());
            for v in items {
                document(&item.ty, v, digits, out)?;
            }
        }
        (C::Struct(fields), J::Object(map)) => {
            if let Some(extra) = map.keys().find(|k| fields.iter().all(|f| &f.name != *k)) {
                return Err(format!(
                    "a stored document has a field `{extra}` its type doesn't declare"
                ));
            }
            out.push(STRUCT);
            out.extend((fields.len() as u64).to_le_bytes());
            for f in fields {
                document(&f.ty, map.get(&f.name).unwrap_or(&J::Null), digits, out)?;
            }
        }
        (C::Map { .. }, _) => {
            return Err(
                "maps stored as JSON documents can't be checksummed: their entry \
                        order is lost"
                    .into(),
            );
        }
        _ => return Err(wrong()),
    }
    Ok(())
}

/// `-12.50` as (-1250, 2). No exponents: a sink writes decimals plainly.
fn decimal_text(text: &str) -> Option<(i256, i8)> {
    let (whole, fraction) = text.split_once('.').unwrap_or((text, ""));
    let digits = whole.trim_start_matches('-');
    if digits.is_empty()
        || !digits.bytes().all(|b| b.is_ascii_digit())
        || !fraction.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let scale = i8::try_from(fraction.len()).ok()?;
    let unscaled = i256::from_string(&format!("{whole}{fraction}"))?;
    Some((unscaled, scale))
}

fn unhex(hex: &str) -> Option<Vec<u8>> {
    if hex.len() % 2 != 0 {
        return None;
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(hex.get(i..i + 2)?, 16).ok())
        .collect()
}

/// `YYYY-MM-DD` as days since the Unix epoch.
fn iso_date(text: &str) -> Option<i64> {
    let mut parts = text.splitn(3, '-');
    let (y, m, d) = (parts.next()?, parts.next()?, parts.next()?);
    if y.len() != 4 || m.len() != 2 || d.len() != 2 {
        return None;
    }
    let (y, m, d): (i64, i64, i64) = (y.parse().ok()?, m.parse().ok()?, d.parse().ok()?);
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return None;
    }
    // Howard Hinnant's `days_from_civil`.
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    Some(era * 146_097 + doe - 719_468)
}

/// `YYYY-MM-DDTHH:MM:SS[.ffffff][Z]` as microseconds since the epoch.
fn iso_timestamp(text: &str) -> Option<i128> {
    let text = text.strip_suffix('Z').unwrap_or(text);
    let (date, time) = text.split_once('T')?;
    let (hms, fraction) = time.split_once('.').unwrap_or((time, ""));
    let mut parts = hms.splitn(3, ':');
    let (h, m, s) = (parts.next()?, parts.next()?, parts.next()?);
    if [h, m, s].iter().any(|p| p.len() != 2) || fraction.len() > 6 {
        return None;
    }
    let (h, m, s): (i128, i128, i128) = (h.parse().ok()?, m.parse().ok()?, s.parse().ok()?);
    let us: i128 = match fraction {
        "" => 0,
        f => format!("{f:0<6}").parse().ok()?,
    };
    let days = i128::from(iso_date(date)?);
    Some(((days * 86_400 + h * 3_600 + m * 60 + s) * 1_000_000) + us)
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

    /// A `nest:` column, `List(Struct)`, as Arrow at write time and as the
    /// `jsonb` text Postgres returns: the same bytes.
    #[test]
    fn a_nested_value_hashes_the_same_as_arrow_and_as_its_stored_document() {
        use std::sync::Arc;

        use arrow_array::builder::{Int32Builder, ListBuilder, StringBuilder, StructBuilder};
        use arrow_array::{ArrayRef, StringArray};
        use arrow_schema::{Field, Fields};
        use ferry_core::types::CanonicalField;

        let ts = CanonicalType::Timestamp {
            tz: Some("UTC".into()),
        };
        let item = CanonicalType::Struct(vec![
            CanonicalField::new("sku", CanonicalType::Utf8, false),
            CanonicalField::new("qty", CanonicalType::Int32, true),
            CanonicalField::new("at", ts.clone(), true),
        ]);
        let ty = CanonicalType::List(Box::new(CanonicalField::new("item", item.clone(), true)));
        let fields = Fields::from(vec![
            Field::new("sku", DataType::Utf8, false),
            Field::new("qty", DataType::Int32, true),
            Field::new(
                "at",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
        ]);
        let mut list = ListBuilder::new(StructBuilder::new(
            fields.clone(),
            vec![
                Box::new(StringBuilder::new()),
                Box::new(Int32Builder::new()),
                Box::new(
                    arrow_array::builder::TimestampMicrosecondBuilder::new().with_timezone("UTC"),
                ),
            ],
        ));
        // [{SKU-1, 2, 1970-01-01T00:00:01.5Z}, {SKU-2, null, null}], then [].
        for (sku, qty, at) in [("SKU-1", Some(2), Some(1_500_000)), ("SKU-2", None, None)] {
            let s = list.values();
            s.field_builder::<StringBuilder>(0)
                .unwrap()
                .append_value(sku);
            s.field_builder::<Int32Builder>(1)
                .unwrap()
                .append_option(qty);
            s.field_builder::<arrow_array::builder::TimestampMicrosecondBuilder>(2)
                .unwrap()
                .append_option(at);
            s.append(true);
        }
        list.append(true);
        list.append(true);
        let written: ArrayRef = Arc::new(list.finish());
        let stored: ArrayRef = Arc::new(StringArray::from(vec![
            r#"[{"at": "1970-01-01T00:00:01.500000Z", "qty": 2, "sku": "SKU-1"}, {"at": null, "qty": null, "sku": "SKU-2"}]"#,
            "[]",
        ]));
        let enc = |array: &ArrayRef, row: usize| {
            let mut out = Vec::new();
            Value {
                ty: Some(&ty),
                digits: MICROS,
            }
            .encode(array.as_ref(), row, &mut out)
            .unwrap();
            out
        };
        assert_eq!(enc(&written, 0), enc(&stored, 0));
        assert_eq!(enc(&written, 1), enc(&stored, 1));
        assert_ne!(enc(&written, 0), enc(&written, 1));

        // A changed value, a reordered array or an extra field differ.
        let differ = |text: &str| -> Result<Vec<u8>, String> {
            let mut out = Vec::new();
            let array: ArrayRef = Arc::new(StringArray::from(vec![text]));
            Value {
                ty: Some(&ty),
                digits: MICROS,
            }
            .encode(array.as_ref(), 0, &mut out)
            .map(|()| out)
        };
        let original = enc(&written, 0);
        for changed in [
            r#"[{"at": "1970-01-01T00:00:01.500000Z", "qty": 3, "sku": "SKU-1"}, {"sku": "SKU-2"}]"#,
            r#"[{"sku": "SKU-2"}, {"at": "1970-01-01T00:00:01.500000Z", "qty": 2, "sku": "SKU-1"}]"#,
            r#"[{"at": "1970-01-01T00:00:01.400000Z", "qty": 2, "sku": "SKU-1"}, {"sku": "SKU-2"}]"#,
        ] {
            assert_ne!(differ(changed).unwrap(), original, "{changed}");
        }
        // A missing field is null, like the written one.
        assert_eq!(
            differ(
                r#"[{"at": "1970-01-01T00:00:01.5Z", "qty": 2, "sku": "SKU-1"}, {"sku": "SKU-2"}]"#
            )
            .unwrap(),
            original
        );
        assert!(differ(r#"[{"sku": "SKU-1", "colour": "red"}]"#).is_err());
        assert!(differ(r#"[{"sku": 7}]"#).is_err());
        assert!(differ("not json").is_err());
    }

    #[test]
    fn document_scalars_decode_as_the_sink_spells_them() {
        let enc = |ty: CanonicalType, text: &str| {
            let mut out = Vec::new();
            document(&ty, &serde_json::from_str(text).unwrap(), MICROS, &mut out).map(|()| out)
        };
        let mut seven = Vec::new();
        number(i256::from_i128(700), 2, &mut seven);
        let decimal = CanonicalType::Decimal(ferry_core::types::DecimalSpec::new(10, 2).unwrap());
        assert_eq!(enc(decimal.clone(), "7.00").unwrap(), seven);
        assert_eq!(enc(decimal, "7").unwrap(), seven);
        let mut day = Vec::new();
        day.push(DATE);
        day.extend(19_782_i32.to_le_bytes());
        assert_eq!(enc(CanonicalType::Date, r#""2024-02-29""#).unwrap(), day);
        let mut b = Vec::new();
        bytes(BYTES, &[0xde, 0xad], &mut b);
        assert_eq!(enc(CanonicalType::Binary, r#""\\xdead""#).unwrap(), b);
        let mut u = Vec::new();
        bytes(FIXED, &[0x11; 16], &mut u);
        assert_eq!(
            enc(
                CanonicalType::Uuid,
                r#""11111111-1111-1111-1111-111111111111""#
            )
            .unwrap(),
            u
        );
        // `0.1f32` is written `0.1` and hashed as the f32 it was.
        let mut tenth = Vec::new();
        float(f64::from(0.1_f32), &mut tenth);
        assert_eq!(enc(CanonicalType::Float32, "0.1").unwrap(), tenth);
        let mut tenth64 = Vec::new();
        float(0.1, &mut tenth64);
        assert_eq!(enc(CanonicalType::Float64, "0.1").unwrap(), tenth64);
        assert_eq!(iso_timestamp("1969-12-31T23:59:59.999999Z"), Some(-1));
        assert_eq!(iso_date("1970-01-01"), Some(0));
        assert!(enc(CanonicalType::Int64, "1.5").is_err());
        assert!(enc(CanonicalType::Date, r#""2024-13-01""#).is_err());
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
