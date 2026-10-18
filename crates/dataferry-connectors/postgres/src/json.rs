//! Rendering nested Arrow values (structs, maps, lists of them) as JSON
//! documents, for columns Postgres stores as `jsonb`.
//!
//! Numbers keep their exact digits (decimals are written as JSON numbers
//! from their unscaled value, never through a float). Values JSON has no
//! type for are written as strings: dates and timestamps in ISO 8601,
//! UUIDs in their canonical form, bytes as `\x`-prefixed hex like
//! Postgres's `bytea` output. NaN and infinities have no JSON form and fail.

use std::fmt::Write;

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type,
    TimestampMicrosecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{Array, ArrayRef};
use arrow_schema::{DataType, Field};
use dataferry_core::types::{EXTENSION_NAME_KEY, JSON_EXTENSION, UUID_EXTENSION};

use crate::encode::{decimal_at, decimal_text};

/// Appends the value at `row` of `array` (described by `field`) as JSON.
pub(crate) fn write(
    field: &Field,
    array: &dyn Array,
    row: usize,
    out: &mut String,
) -> Result<(), String> {
    if array.is_null(row) {
        out.push_str("null");
        return Ok(());
    }
    let extension = field.metadata().get(EXTENSION_NAME_KEY).map(String::as_str);
    macro_rules! display {
        ($t:ty) => {{
            let _ = write!(out, "{}", array.as_primitive::<$t>().value(row));
        }};
    }
    match array.data_type() {
        DataType::Boolean => {
            out.push_str(if array.as_boolean().value(row) {
                "true"
            } else {
                "false"
            });
        }
        DataType::Int8 => display!(Int8Type),
        DataType::Int16 => display!(Int16Type),
        DataType::Int32 => display!(Int32Type),
        DataType::Int64 => display!(Int64Type),
        DataType::UInt8 => display!(UInt8Type),
        DataType::UInt16 => display!(UInt16Type),
        DataType::UInt32 => display!(UInt32Type),
        DataType::UInt64 => display!(UInt64Type),
        DataType::Float32 => {
            let v = array.as_primitive::<Float32Type>().value(row);
            float(f64::from(v).is_finite(), v, out)?;
        }
        DataType::Float64 => {
            let v = array.as_primitive::<Float64Type>().value(row);
            float(v.is_finite(), v, out)?;
        }
        DataType::Decimal128(..) | DataType::Decimal256(..) => {
            let (unscaled, scale) = decimal_at(array, row).expect("a decimal array");
            out.push_str(&decimal_text(&unscaled, scale));
        }
        DataType::Utf8 => {
            let text = array.as_string::<i32>().value(row);
            if extension == Some(JSON_EXTENSION) {
                out.push_str(text);
            } else {
                string(text, out);
            }
        }
        DataType::Binary => hex(array.as_binary::<i32>().value(row), out),
        DataType::FixedSizeBinary(16) if extension == Some(UUID_EXTENSION) => {
            string(&uuid(array.as_fixed_size_binary().value(row)), out);
        }
        DataType::FixedSizeBinary(_) => hex(array.as_fixed_size_binary().value(row), out),
        DataType::Date32 => {
            let days = array.as_primitive::<Date32Type>().value(row);
            string(&date(i64::from(days)), out);
        }
        DataType::Timestamp(_, tz) => {
            let micros = array.as_primitive::<TimestampMicrosecondType>().value(row);
            string(&timestamp(micros, tz.is_some()), out);
        }
        DataType::List(item) => {
            let items = array.as_list::<i32>().value(row);
            sequence(item, &items, out)?;
        }
        DataType::Struct(fields) => {
            let s = array.as_struct();
            out.push('{');
            for (i, (f, column)) in fields.iter().zip(s.columns()).enumerate() {
                if i > 0 {
                    out.push(',');
                }
                string(f.name(), out);
                out.push(':');
                write(f, column.as_ref(), row, out)?;
            }
            out.push('}');
        }
        DataType::Map(_, _) => {
            let entries = array.as_map().value(row);
            let (key_field, value_field) = match entries.data_type() {
                DataType::Struct(kv) if kv.len() == 2 => (&kv[0], &kv[1]),
                other => return Err(format!("map entries of type {other} are not supported")),
            };
            let keys = entries.column(0);
            let values = entries.column(1);
            if keys.data_type() == &DataType::Utf8 {
                out.push('{');
                for i in 0..entries.len() {
                    if i > 0 {
                        out.push(',');
                    }
                    string(keys.as_string::<i32>().value(i), out);
                    out.push(':');
                    write(value_field, values.as_ref(), i, out)?;
                }
                out.push('}');
            } else {
                // Only strings can be object keys, so other maps become
                // a list of [key, value] pairs.
                out.push('[');
                for i in 0..entries.len() {
                    if i > 0 {
                        out.push(',');
                    }
                    out.push('[');
                    write(key_field, keys.as_ref(), i, out)?;
                    out.push(',');
                    write(value_field, values.as_ref(), i, out)?;
                    out.push(']');
                }
                out.push(']');
            }
        }
        other => return Err(format!("{other} values can't be written as JSON")),
    }
    Ok(())
}

fn sequence(item: &Field, items: &ArrayRef, out: &mut String) -> Result<(), String> {
    out.push('[');
    for i in 0..items.len() {
        if i > 0 {
            out.push(',');
        }
        write(item, items.as_ref(), i, out)?;
    }
    out.push(']');
    Ok(())
}

fn float(finite: bool, v: impl std::fmt::Display, out: &mut String) -> Result<(), String> {
    if !finite {
        return Err(format!("{v} has no JSON representation"));
    }
    let _ = write!(out, "{v}");
    Ok(())
}

fn string(s: &str, out: &mut String) {
    out.push_str(&serde_json::to_string(s).expect("strings always serialize"));
}

fn hex(bytes: &[u8], out: &mut String) {
    out.push_str("\"\\\\x");
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out.push('"');
}

pub(crate) fn uuid(b: &[u8]) -> String {
    let hex: String = b.iter().map(|x| format!("{x:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

/// Days since the Unix epoch as `YYYY-MM-DD` (proleptic Gregorian).
fn date(days: i64) -> String {
    let (y, m, d) = civil_from_days(days);
    format!("{y:04}-{m:02}-{d:02}")
}

/// Microseconds since the Unix epoch in ISO 8601, with `Z` for a
/// timestamp that is an instant rather than a wall-clock time.
fn timestamp(micros: i64, utc: bool) -> String {
    let days = micros.div_euclid(86_400_000_000);
    let of_day = micros.rem_euclid(86_400_000_000);
    let (h, rest) = (of_day / 3_600_000_000, of_day % 3_600_000_000);
    let (m, rest) = (rest / 60_000_000, rest % 60_000_000);
    let (s, us) = (rest / 1_000_000, rest % 1_000_000);
    format!(
        "{}T{h:02}:{m:02}:{s:02}.{us:06}{}",
        date(days),
        if utc { "Z" } else { "" }
    )
}

/// Howard Hinnant's `civil_from_days`.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_array::builder::{Int32Builder, ListBuilder, MapBuilder, StringBuilder};
    use arrow_array::{
        BinaryArray, Decimal128Array, Float64Array, Int64Array, StringArray, StructArray,
        TimestampMicrosecondArray,
    };
    use arrow_schema::Fields;

    use super::*;

    fn render(field: &Field, array: &dyn Array, row: usize) -> Result<String, String> {
        let mut out = String::new();
        write(field, array, row, &mut out).map(|()| out)
    }

    #[test]
    fn structs_render_as_objects_with_exact_numbers() {
        let price = Decimal128Array::from(vec![Some(100_500)])
            .with_precision_and_scale(10, 3)
            .unwrap();
        let note = StringArray::from(vec![Some("a \"quoted\"\nline")]);
        let raw = StringArray::from(vec![Some(r#"{"x":[1,2]}"#)]);
        let raw_field = Field::new("raw", DataType::Utf8, true).with_metadata(HashMap::from([(
            EXTENSION_NAME_KEY.to_string(),
            JSON_EXTENSION.to_string(),
        )]));
        let fields = Fields::from(vec![
            Field::new("price", price.data_type().clone(), true),
            Field::new("note", DataType::Utf8, true),
            raw_field,
            Field::new("missing", DataType::Int64, true),
        ]);
        let s = StructArray::new(
            fields.clone(),
            vec![
                Arc::new(price),
                Arc::new(note),
                Arc::new(raw),
                Arc::new(Int64Array::from(vec![None])),
            ],
            None,
        );
        let field = Field::new("doc", DataType::Struct(fields), true);
        let text = render(&field, &s, 0).unwrap();
        assert_eq!(
            text,
            r#"{"price":100.500,"note":"a \"quoted\"\nline","raw":{"x":[1,2]},"missing":null}"#
        );
        serde_json::from_str::<serde_json::Value>(&text).expect("valid JSON");
    }

    #[test]
    fn lists_and_string_keyed_maps() {
        let mut list = ListBuilder::new(Int32Builder::new());
        list.append_value([Some(1), None, Some(3)]);
        let list = list.finish();
        let field = Field::new("l", list.data_type().clone(), true);
        assert_eq!(render(&field, &list, 0).unwrap(), "[1,null,3]");

        let mut map = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
        map.keys().append_value("a");
        map.values().append_value(1);
        map.append(true).unwrap();
        let map = map.finish();
        let field = Field::new("m", map.data_type().clone(), true);
        assert_eq!(render(&field, &map, 0).unwrap(), r#"{"a":1}"#);
    }

    #[test]
    fn dates_times_and_bytes_render_as_strings() {
        let ts = TimestampMicrosecondArray::from(vec![Some(-1)]).with_timezone("UTC");
        let field = Field::new("t", ts.data_type().clone(), true);
        assert_eq!(
            render(&field, &ts, 0).unwrap(),
            r#""1969-12-31T23:59:59.999999Z""#
        );
        assert_eq!(date(0), "1970-01-01");
        assert_eq!(date(19_782), "2024-02-29");
        assert_eq!(date(-719_528), "0000-01-01");

        let bytes = BinaryArray::from(vec![Some(&[0xde, 0xad][..])]);
        let field = Field::new("b", DataType::Binary, true);
        let text = render(&field, &bytes, 0).unwrap();
        assert_eq!(text, r#""\\xdead""#);
        assert_eq!(serde_json::from_str::<String>(&text).unwrap(), r"\xdead");
    }

    #[test]
    fn non_finite_floats_are_refused() {
        let f = Float64Array::from(vec![f64::NAN]);
        let field = Field::new("f", DataType::Float64, true);
        assert!(render(&field, &f, 0).unwrap_err().contains("NaN"));
    }
}
