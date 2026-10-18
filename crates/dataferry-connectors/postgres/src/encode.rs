//! Encoding Arrow columns as rows of a binary `COPY ... FROM STDIN`.
//!
//! Each target column has a [`Wire`] format, chosen from its canonical type
//! and the column's actual Postgres type (see `crate::ddl`). Values are
//! written in Postgres's binary send format for that type, so the server
//! does no text parsing and no value goes through a lossy text round trip.

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Decimal128Type, Decimal256Type, Float32Type, Float64Type, Int8Type, Int16Type,
    Int32Type, Int64Type, TimestampMicrosecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{Array, ArrayRef};
use arrow_schema::{DataType, Field};

use crate::decode::{PG_EPOCH_DAYS, PG_EPOCH_MICROS};
use crate::json;

/// How one column's values are sent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Wire {
    Int2,
    Int4,
    Int8,
    Float4,
    Float8,
    Numeric,
    Bool,
    Text,
    Bytea,
    Date,
    Timestamp,
    Uuid,
    /// A JSON document held as text in Arrow (the `json` canonical type).
    /// `jsonb`'s binary form is a version byte followed by the text; `json`'s
    /// is the bare text.
    JsonText {
        jsonb: bool,
    },
    /// A nested Arrow value (struct, map, list of nested values) rendered as
    /// a JSON document.
    JsonDoc {
        jsonb: bool,
    },
    /// A one-dimensional array of `element`, whose type oid is `oid`.
    Array {
        element: Box<Wire>,
        oid: u32,
    },
}

/// The binary COPY header: signature, flags, header-extension length.
const HEADER: &[u8] = b"PGCOPY\n\xff\r\n\0\0\0\0\0\0\0\0\0";
const TRAILER: &[u8] = &(-1i16).to_be_bytes();

/// One column to encode: its Arrow field (for extension metadata of nested
/// values), its values, and how they are sent.
pub(crate) struct Column<'a> {
    pub field: &'a Field,
    pub values: &'a ArrayRef,
    pub wire: &'a Wire,
}

/// The whole COPY payload for `rows` rows of `columns`. Errors name the
/// column and row of the first value that can't be encoded.
pub(crate) fn copy_data(columns: &[Column<'_>], rows: usize) -> Result<Vec<u8>, String> {
    let mut out = Vec::with_capacity(HEADER.len() + rows * (2 + columns.len() * 12) + 2);
    out.extend_from_slice(HEADER);
    let width = i16::try_from(columns.len()).map_err(|_| "too many columns for COPY")?;
    for row in 0..rows {
        out.extend_from_slice(&width.to_be_bytes());
        for c in columns {
            if c.values.is_null(row) {
                out.extend_from_slice(&(-1i32).to_be_bytes());
                continue;
            }
            with_length(&mut out, |out| {
                encode(c.wire, c.field, c.values.as_ref(), row, out)
            })
            .map_err(|e| format!("column `{}`, row {row}: {e}", c.field.name()))?;
        }
    }
    out.extend_from_slice(TRAILER);
    Ok(out)
}

/// Writes a length-prefixed value.
fn with_length(
    out: &mut Vec<u8>,
    write: impl FnOnce(&mut Vec<u8>) -> Result<(), String>,
) -> Result<(), String> {
    let at = out.len();
    out.extend_from_slice(&[0; 4]);
    write(out)?;
    let len = i32::try_from(out.len() - at - 4).map_err(|_| "value is larger than 2 GiB")?;
    out[at..at + 4].copy_from_slice(&len.to_be_bytes());
    Ok(())
}

/// Encodes the non-null value at `row`.
fn encode(
    wire: &Wire,
    field: &Field,
    array: &dyn Array,
    row: usize,
    out: &mut Vec<u8>,
) -> Result<(), String> {
    let mismatch = || {
        format!(
            "internal error: a {} value can't be sent as {wire:?}",
            array.data_type()
        )
    };
    match wire {
        Wire::Int2 => {
            let v = int_at(array, row).ok_or_else(mismatch)?;
            let v = i16::try_from(v).map_err(|_| format!("{v} does not fit in smallint"))?;
            out.extend_from_slice(&v.to_be_bytes());
        }
        Wire::Int4 => {
            let v = int_at(array, row).ok_or_else(mismatch)?;
            let v = i32::try_from(v).map_err(|_| format!("{v} does not fit in integer"))?;
            out.extend_from_slice(&v.to_be_bytes());
        }
        Wire::Int8 => {
            let v = int_at(array, row).ok_or_else(mismatch)?;
            let v = i64::try_from(v).map_err(|_| format!("{v} does not fit in bigint"))?;
            out.extend_from_slice(&v.to_be_bytes());
        }
        Wire::Float4 => {
            let v = array
                .as_primitive_opt::<Float32Type>()
                .ok_or_else(mismatch)?;
            out.extend_from_slice(&v.value(row).to_be_bytes());
        }
        Wire::Float8 => {
            let v = array
                .as_primitive_opt::<Float64Type>()
                .ok_or_else(mismatch)?;
            out.extend_from_slice(&v.value(row).to_be_bytes());
        }
        Wire::Numeric => {
            let (unscaled, scale) = decimal_at(array, row).ok_or_else(mismatch)?;
            numeric(&unscaled, scale, out);
        }
        Wire::Bool => {
            let v = array.as_boolean_opt().ok_or_else(mismatch)?;
            out.push(u8::from(v.value(row)));
        }
        Wire::Text => {
            let v = array.as_string_opt::<i32>().ok_or_else(mismatch)?;
            out.extend_from_slice(v.value(row).as_bytes());
        }
        Wire::Bytea => {
            let v = array.as_binary_opt::<i32>().ok_or_else(mismatch)?;
            out.extend_from_slice(v.value(row));
        }
        Wire::Date => {
            let v = array
                .as_primitive_opt::<Date32Type>()
                .ok_or_else(mismatch)?;
            let days = v
                .value(row)
                .checked_sub(PG_EPOCH_DAYS)
                .ok_or("date is out of range")?;
            out.extend_from_slice(&days.to_be_bytes());
        }
        Wire::Timestamp => {
            let v = array
                .as_primitive_opt::<TimestampMicrosecondType>()
                .ok_or_else(mismatch)?;
            let micros = v
                .value(row)
                .checked_sub(PG_EPOCH_MICROS)
                .ok_or("timestamp is out of range")?;
            out.extend_from_slice(&micros.to_be_bytes());
        }
        Wire::Uuid => {
            let v = array.as_fixed_size_binary_opt().ok_or_else(mismatch)?;
            let bytes = v.value(row);
            if bytes.len() != 16 {
                return Err(mismatch());
            }
            out.extend_from_slice(bytes);
        }
        Wire::JsonText { jsonb } => {
            let v = array.as_string_opt::<i32>().ok_or_else(mismatch)?;
            if *jsonb {
                out.push(1);
            }
            out.extend_from_slice(v.value(row).as_bytes());
        }
        Wire::JsonDoc { jsonb } => {
            let mut text = String::new();
            json::write(field, array, row, &mut text)?;
            if *jsonb {
                out.push(1);
            }
            out.extend_from_slice(text.as_bytes());
        }
        Wire::Array { element, oid } => {
            let list = array.as_list_opt::<i32>().ok_or_else(mismatch)?;
            let DataType::List(item_field) = array.data_type() else {
                return Err(mismatch());
            };
            let items = list.value(row);
            let len = i32::try_from(items.len()).map_err(|_| "array is too long")?;
            if len == 0 {
                // No dimensions: Postgres's empty array.
                out.extend_from_slice(&0i32.to_be_bytes());
                out.extend_from_slice(&0i32.to_be_bytes());
                out.extend_from_slice(&oid.to_be_bytes());
                return Ok(());
            }
            out.extend_from_slice(&1i32.to_be_bytes());
            out.extend_from_slice(&i32::from(items.null_count() > 0).to_be_bytes());
            out.extend_from_slice(&oid.to_be_bytes());
            out.extend_from_slice(&len.to_be_bytes());
            out.extend_from_slice(&1i32.to_be_bytes());
            for i in 0..items.len() {
                if items.is_null(i) {
                    out.extend_from_slice(&(-1i32).to_be_bytes());
                } else {
                    with_length(out, |out| {
                        encode(element, item_field, items.as_ref(), i, out)
                    })
                    .map_err(|e| format!("element {i}: {e}"))?;
                }
            }
        }
    }
    Ok(())
}

/// Any integer value, widened.
fn int_at(array: &dyn Array, row: usize) -> Option<i128> {
    macro_rules! try_int {
        ($($t:ty),*) => {
            $(if let Some(a) = array.as_primitive_opt::<$t>() {
                return Some(i128::from(a.value(row)));
            })*
        };
    }
    try_int!(
        Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type
    );
    None
}

/// A decimal (or integer, at scale 0) as its unscaled digits with an
/// optional leading `-`, and its scale.
pub(crate) fn decimal_at(array: &dyn Array, row: usize) -> Option<(String, i8)> {
    if let Some(a) = array.as_primitive_opt::<Decimal128Type>() {
        return Some((a.value(row).to_string(), a.scale()));
    }
    if let Some(a) = array.as_primitive_opt::<Decimal256Type>() {
        return Some((a.value(row).to_string(), a.scale()));
    }
    int_at(array, row).map(|v| (v.to_string(), 0))
}

/// The unscaled digits of a decimal as plain decimal text, e.g. `-12345`
/// at scale 2 is `-123.45`, and `12` at scale -2 is `1200`.
pub(crate) fn decimal_text(unscaled: &str, scale: i8) -> String {
    let (negative, digits) = split_sign(unscaled);
    let (int_part, frac_part) = split_point(digits, scale);
    let int_part = int_part.trim_start_matches('0');
    let mut text = String::new();
    if negative && digits.bytes().any(|b| b != b'0') {
        text.push('-');
    }
    text.push_str(if int_part.is_empty() { "0" } else { int_part });
    if !frac_part.is_empty() {
        text.push('.');
        text.push_str(&frac_part);
    }
    text
}

fn split_sign(unscaled: &str) -> (bool, &str) {
    match unscaled.strip_prefix('-') {
        Some(digits) => (true, digits),
        None => (false, unscaled),
    }
}

/// Integer and fractional digits of `digits * 10^-scale`.
fn split_point(digits: &str, scale: i8) -> (String, String) {
    if scale <= 0 {
        let zeros = "0".repeat(usize::from(scale.unsigned_abs()));
        return (format!("{digits}{zeros}"), String::new());
    }
    let scale = usize::from(scale.unsigned_abs());
    if digits.len() > scale {
        let (i, f) = digits.split_at(digits.len() - scale);
        (i.to_owned(), f.to_owned())
    } else {
        (String::new(), format!("{digits:0>scale$}"))
    }
}

/// Writes a `numeric` in binary form: base-10000 digit groups, the weight
/// of the first group, the sign and the display scale.
fn numeric(unscaled: &str, scale: i8, out: &mut Vec<u8>) {
    let (negative, digits) = split_sign(unscaled);
    let (int_part, frac_part) = split_point(digits, scale);
    let int_width = int_part.len().div_ceil(4) * 4;
    let frac_width = frac_part.len().div_ceil(4) * 4;
    let padded = format!("{int_part:0>int_width$}{frac_part:0<frac_width$}");
    let mut groups: Vec<u16> = padded
        .as_bytes()
        .chunks(4)
        .map(|c| c.iter().fold(0u16, |acc, d| acc * 10 + u16::from(d - b'0')))
        .collect();
    let mut weight = (int_width / 4) as i16 - 1;
    let leading = groups.iter().take_while(|g| **g == 0).count();
    groups.drain(..leading);
    weight -= leading as i16;
    while groups.last() == Some(&0) {
        groups.pop();
    }
    let sign: u16 = if negative && !groups.is_empty() {
        0x4000
    } else {
        0
    };
    if groups.is_empty() {
        weight = 0;
    }
    let dscale = u16::from(scale.max(0).unsigned_abs());
    out.extend_from_slice(&(groups.len() as i16).to_be_bytes());
    out.extend_from_slice(&weight.to_be_bytes());
    out.extend_from_slice(&sign.to_be_bytes());
    out.extend_from_slice(&dscale.to_be_bytes());
    for g in groups {
        out.extend_from_slice(&g.to_be_bytes());
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::ArrowNativeTypeOp;
    use arrow_buffer::i256;

    use super::*;
    use crate::decode::numeric_unscaled;

    fn numeric_bytes(unscaled: &str, scale: i8) -> Vec<u8> {
        let mut out = Vec::new();
        numeric(unscaled, scale, &mut out);
        out
    }

    /// Encoding then decoding with the source's decoder gives the value back.
    #[test]
    fn numeric_round_trips_through_the_source_decoder() {
        let cases: &[(&str, i8)] = &[
            ("0", 0),
            ("0", 4),
            ("1", 0),
            ("-1", 0),
            ("12345", 2),
            ("-12345", 2),
            ("5", 3),
            ("-5", 3),
            ("10000", 0),
            ("100000000", 4),
            ("99999999999999999999999999999999999999", 10),
            ("-12", 8),
            ("123", -2),
            ("18446744073709551615", 0),
        ];
        for &(unscaled, scale) in cases {
            let bytes = numeric_bytes(unscaled, scale);
            let decoded = numeric_unscaled(&bytes, scale.max(0)).unwrap();
            let want = if scale < 0 {
                i256::from_string(unscaled).unwrap()
                    * i256::from_i128(10).pow_wrapping(u32::from(scale.unsigned_abs()))
            } else {
                i256::from_string(unscaled).unwrap()
            };
            assert_eq!(decoded, want, "{unscaled} at scale {scale}");
        }
    }

    #[test]
    fn numeric_layout_matches_postgres() {
        // 12.34 at scale 2: one integer group (12), one fractional (3400).
        assert_eq!(
            numeric_bytes("1234", 2),
            [0, 2, 0, 0, 0, 0, 0, 2, 0, 12, 0x0d, 0x48]
        );
        // Zero has no groups, a zero weight and a positive sign.
        assert_eq!(numeric_bytes("-0", 2), [0, 0, 0, 0, 0, 0, 0, 2]);
        // 0.0005: the only group sits at weight -1.
        assert_eq!(
            numeric_bytes("-5", 4),
            [0, 1, 0xff, 0xff, 0x40, 0, 0, 4, 0, 5]
        );
    }

    #[test]
    fn decimal_text_places_the_point() {
        assert_eq!(decimal_text("-12345", 2), "-123.45");
        assert_eq!(decimal_text("5", 3), "0.005");
        assert_eq!(decimal_text("12", -2), "1200");
        assert_eq!(decimal_text("0", 0), "0");
        assert_eq!(decimal_text("-0", 1), "0.0");
    }
}
