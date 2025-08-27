//! Value conversions: the strict `int()`, `float()` and `string()`
//! functions, and the final conversion of a result to its canonical type.
//!
//! A conversion never silently turns a value into null or truncates it. A
//! value that can't be converted is an error, and the runner makes that row
//! an error row (0003 §5.4).

use std::sync::Arc;

use arrow_array::builder::{FixedSizeBinaryBuilder, Float64Builder, Int64Builder, StringBuilder};
use arrow_array::cast::AsArray;
use arrow_array::types::{Decimal128Type, Float64Type};
use arrow_array::{Array, ArrayRef, new_null_array};
use arrow_cast::{CastOptions, cast_with_options};
use arrow_schema::{ArrowError, DataType};
use ferry_core::types::CanonicalType;

/// What `int()`, `float()` and `string()` convert to.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Target {
    Int,
    Float,
    String,
}

impl Target {
    pub(crate) fn function(self) -> &'static str {
        match self {
            Target::Int => "int",
            Target::Float => "float",
            Target::String => "string",
        }
    }

    pub(crate) fn data_type(self) -> DataType {
        match self {
            Target::Int => DataType::Int64,
            Target::Float => DataType::Float64,
            Target::String => DataType::Utf8,
        }
    }
}

fn is_text(t: &DataType) -> bool {
    matches!(t, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View)
}

/// Whether `from` can be given to the conversion at all (plan time). Values
/// can still fail one by one at run time.
pub(crate) fn check(from: &DataType, target: Target) -> Result<(), String> {
    let ok = match target {
        Target::Int | Target::Float => {
            from.is_numeric() || is_text(from) || matches!(from, DataType::Boolean | DataType::Null)
        }
        Target::String => {
            from.is_numeric()
                || is_text(from)
                || matches!(
                    from,
                    DataType::Boolean
                        | DataType::Null
                        | DataType::FixedSizeBinary(16)
                        | DataType::Date32
                        | DataType::Timestamp(..)
                )
        }
    };
    if ok {
        Ok(())
    } else {
        Err(format!("can't convert {from}"))
    }
}

fn strict() -> CastOptions<'static> {
    CastOptions {
        safe: false,
        ..Default::default()
    }
}

fn cast(array: &ArrayRef, to: &DataType) -> Result<ArrayRef, ArrowError> {
    cast_with_options(array, to, &strict())
}

fn error(message: String) -> ArrowError {
    ArrowError::CastError(message)
}

pub(crate) fn convert(array: &ArrayRef, target: Target) -> Result<ArrayRef, ArrowError> {
    let from = array.data_type();
    if *from == DataType::Null {
        return Ok(new_null_array(&target.data_type(), array.len()));
    }
    match target {
        Target::Int => to_int(array),
        Target::Float => to_float(array),
        Target::String => to_string(array),
    }
}

fn to_int(array: &ArrayRef) -> Result<ArrayRef, ArrowError> {
    let from = array.data_type();
    if from.is_integer() || *from == DataType::Boolean {
        return cast(array, &DataType::Int64);
    }
    let mut out = Int64Builder::with_capacity(array.len());
    match from {
        DataType::Float32 | DataType::Float64 => {
            let floats = cast(array, &DataType::Float64)?;
            for v in floats.as_primitive::<Float64Type>() {
                out.append_option(v.map(whole_float).transpose()?);
            }
        }
        DataType::Decimal128(_, scale) => {
            for v in array.as_primitive::<Decimal128Type>() {
                out.append_option(v.map(|v| whole_decimal(v, *scale)).transpose()?);
            }
        }
        DataType::Decimal32(..) | DataType::Decimal64(..) | DataType::Decimal256(..) => {
            let (_, scale) = decimal_spec(from);
            return to_int(&cast(array, &DataType::Decimal128(38, scale))?);
        }
        t if is_text(t) => {
            let text = cast(array, &DataType::Utf8)?;
            for v in text.as_string::<i32>() {
                out.append_option(
                    v.map(|s| {
                        s.parse::<i64>()
                            .map_err(|_| error(format!("`{s}` is not an integer")))
                    })
                    .transpose()?,
                );
            }
        }
        other => return Err(error(format!("can't convert {other} to an integer"))),
    }
    Ok(Arc::new(out.finish()))
}

fn decimal_spec(t: &DataType) -> (u8, i8) {
    match t {
        DataType::Decimal32(p, s)
        | DataType::Decimal64(p, s)
        | DataType::Decimal128(p, s)
        | DataType::Decimal256(p, s) => (*p, *s),
        _ => unreachable!("not a decimal"),
    }
}

fn whole_float(v: f64) -> Result<i64, ArrowError> {
    // i64::MIN is exactly representable; i64::MAX + 1 is the first float out
    // of range.
    if v.is_finite() && v.fract() == 0.0 && v >= -(2f64.powi(63)) && v < 2f64.powi(63) {
        Ok(v as i64)
    } else {
        Err(error(format!(
            "`{v}` is not a whole number in int64's range (use round() first)"
        )))
    }
}

fn whole_decimal(v: i128, scale: i8) -> Result<i64, ArrowError> {
    let shown = || format_decimal(v, scale);
    let whole = if scale >= 0 {
        let factor = 10i128.pow(u32::from(scale.unsigned_abs()));
        if v % factor != 0 {
            return Err(error(format!(
                "`{}` is not a whole number (use round() first)",
                shown()
            )));
        }
        Some(v / factor)
    } else {
        10i128
            .checked_pow(u32::from(scale.unsigned_abs()))
            .and_then(|f| v.checked_mul(f))
    };
    whole
        .and_then(|w| i64::try_from(w).ok())
        .ok_or_else(|| error(format!("`{}` is out of int64's range", shown())))
}

fn format_decimal(v: i128, scale: i8) -> String {
    if scale <= 0 {
        return format!("{v}{}", "0".repeat(usize::from(scale.unsigned_abs())));
    }
    let digits = v.unsigned_abs().to_string();
    let scale = usize::from(scale.unsigned_abs());
    let digits = format!("{digits:0>width$}", width = scale + 1);
    let (int, frac) = digits.split_at(digits.len() - scale);
    format!("{}{int}.{frac}", if v < 0 { "-" } else { "" })
}

fn to_float(array: &ArrayRef) -> Result<ArrayRef, ArrowError> {
    let from = array.data_type();
    if from.is_numeric() || *from == DataType::Boolean {
        return cast(array, &DataType::Float64);
    }
    let text = cast(array, &DataType::Utf8)?;
    let mut out = Float64Builder::with_capacity(array.len());
    for v in text.as_string::<i32>() {
        out.append_option(
            v.map(|s| {
                // `"NaN"` and `"inf"` parse as floats but aren't numbers.
                s.parse::<f64>()
                    .ok()
                    .filter(|f| f.is_finite())
                    .ok_or_else(|| error(format!("`{s}` is not a number")))
            })
            .transpose()?,
        );
    }
    Ok(Arc::new(out.finish()))
}

fn to_string(array: &ArrayRef) -> Result<ArrayRef, ArrowError> {
    match array.data_type() {
        DataType::FixedSizeBinary(16) => {
            let mut out = StringBuilder::with_capacity(array.len(), array.len() * 36);
            for v in array.as_fixed_size_binary() {
                out.append_option(v.map(format_uuid));
            }
            Ok(Arc::new(out.finish()))
        }
        DataType::Float32 => to_string(&cast(array, &DataType::Float64)?),
        DataType::Float64 => {
            // Shortest text that reads back as the same float.
            let mut out = StringBuilder::new();
            for v in array.as_primitive::<Float64Type>() {
                out.append_option(v.map(|v| v.to_string()));
            }
            Ok(Arc::new(out.finish()))
        }
        _ => cast(array, &DataType::Utf8),
    }
}

fn format_uuid(bytes: &[u8]) -> String {
    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

/// `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` or 32 hex digits. Works on bytes,
/// so any other text (including non-ASCII) is simply not a UUID.
fn parse_uuid(text: &str) -> Option<[u8; 16]> {
    let bytes = text.as_bytes();
    let hex: Vec<u8> = match bytes.len() {
        32 => bytes.to_vec(),
        36 if [8, 13, 18, 23].iter().all(|&i| bytes[i] == b'-') => bytes
            .iter()
            .enumerate()
            .filter(|(i, _)| ![8, 13, 18, 23].contains(i))
            .map(|(_, b)| *b)
            .collect(),
        _ => return None,
    };
    let digit = |b: u8| (b as char).to_digit(16).map(|d| d as u8);
    let mut out = [0u8; 16];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = digit(hex[2 * i])? << 4 | digit(hex[2 * i + 1])?;
    }
    Some(out)
}

/// Converts a result to the Arrow form of `ty`. This is the conversion a
/// column with a `type:` gets (lossy ones only with `allow_lossy`, which the
/// type checker enforces); the result of an expression without one only has
/// its representation normalized (`Utf8View` to `Utf8`, nanoseconds to
/// microseconds).
pub fn to_canonical(array: &ArrayRef, ty: &CanonicalType) -> Result<ArrayRef, ArrowError> {
    let to = ty.to_arrow();
    let from = array.data_type();
    if *from == to {
        if *ty == CanonicalType::Json {
            check_json(array)?;
        }
        return Ok(array.clone());
    }
    match (from, ty) {
        (t, CanonicalType::Uuid) if is_text(t) => {
            let text = cast(array, &DataType::Utf8)?;
            let mut out = FixedSizeBinaryBuilder::with_capacity(array.len(), 16);
            for v in text.as_string::<i32>() {
                match v {
                    Some(s) => out.append_value(
                        parse_uuid(s).ok_or_else(|| error(format!("`{s}` is not a uuid")))?,
                    )?,
                    None => out.append_null(),
                }
            }
            Ok(Arc::new(out.finish()))
        }
        (DataType::FixedSizeBinary(16), CanonicalType::Utf8) => to_string(array),
        (DataType::Float32 | DataType::Float64, CanonicalType::Utf8) => to_string(array),
        (t, CanonicalType::Json) if is_text(t) => {
            let text = cast(array, &DataType::Utf8)?;
            check_json(&text)?;
            Ok(text)
        }
        // Lossy float → int: truncation toward zero is what was opted into,
        // but a value out of range is still an error.
        (DataType::Float32 | DataType::Float64, t) if t.to_arrow().is_integer() => {
            let floats = cast(array, &DataType::Float64)?;
            let truncated: ArrayRef = Arc::new(
                floats
                    .as_primitive::<Float64Type>()
                    .unary::<_, Float64Type>(f64::trunc),
            );
            cast(&truncated, &to)
        }
        _ => cast(array, &to),
    }
}

fn check_json(array: &ArrayRef) -> Result<(), ArrowError> {
    for v in array.as_string::<i32>().iter().flatten() {
        if serde_json::from_str::<serde_json::Value>(v).is_err() {
            return Err(error(format!("`{v}` is not valid JSON")));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Decimal128Array, Float64Array, Int64Array, StringArray};

    fn ints(a: ArrayRef) -> Vec<Option<i64>> {
        a.as_primitive::<arrow_array::types::Int64Type>()
            .iter()
            .collect()
    }

    #[test]
    fn int_accepts_only_exact_values() {
        let text: ArrayRef = Arc::new(StringArray::from(vec![Some("3"), None, Some("-7")]));
        assert_eq!(
            ints(convert(&text, Target::Int).unwrap()),
            [Some(3), None, Some(-7)]
        );
        for bad in ["three", "3.0", " 3", ""] {
            let text: ArrayRef = Arc::new(StringArray::from(vec![bad]));
            assert!(convert(&text, Target::Int).is_err(), "{bad:?}");
        }
        let floats: ArrayRef = Arc::new(Float64Array::from(vec![2.0, -0.0]));
        assert_eq!(
            ints(convert(&floats, Target::Int).unwrap()),
            [Some(2), Some(0)]
        );
        for bad in [1.5, f64::NAN, f64::INFINITY, 1e19] {
            let floats: ArrayRef = Arc::new(Float64Array::from(vec![bad]));
            assert!(convert(&floats, Target::Int).is_err(), "{bad}");
        }
    }

    #[test]
    fn int_of_a_decimal_needs_a_whole_number() {
        let whole: ArrayRef = Arc::new(
            Decimal128Array::from(vec![101_000, -5_000])
                .with_precision_and_scale(10, 3)
                .unwrap(),
        );
        assert_eq!(
            ints(convert(&whole, Target::Int).unwrap()),
            [Some(101), Some(-5)]
        );
        let fraction: ArrayRef = Arc::new(
            Decimal128Array::from(vec![100_500])
                .with_precision_and_scale(10, 3)
                .unwrap(),
        );
        let err = convert(&fraction, Target::Int).unwrap_err().to_string();
        assert!(err.contains("`100.500` is not a whole number"), "{err}");
    }

    #[test]
    fn string_formats_uuids_and_floats_canonically() {
        let mut b = FixedSizeBinaryBuilder::new(16);
        b.append_value([0xab; 16]).unwrap();
        let uuids: ArrayRef = Arc::new(b.finish());
        let s = convert(&uuids, Target::String).unwrap();
        assert_eq!(
            s.as_string::<i32>().value(0),
            "abababab-abab-abab-abab-abababababab"
        );
        let back = to_canonical(&s, &CanonicalType::Uuid).unwrap();
        assert_eq!(back.as_fixed_size_binary().value(0), [0xab; 16]);
        let floats: ArrayRef = Arc::new(Float64Array::from(vec![0.1, 100.0]));
        let s = convert(&floats, Target::String).unwrap();
        assert_eq!(s.as_string::<i32>().value(0), "0.1");
        assert_eq!(s.as_string::<i32>().value(1), "100");
    }

    #[test]
    fn float_of_text_rejects_non_numbers() {
        let ok: ArrayRef = Arc::new(StringArray::from(vec!["1.5", "-2e3"]));
        assert!(convert(&ok, Target::Float).is_ok());
        for bad in ["NaN", "inf", "-infinity", "1,5"] {
            let text: ArrayRef = Arc::new(StringArray::from(vec![bad]));
            assert!(convert(&text, Target::Float).is_err(), "{bad}");
        }
    }

    #[test]
    fn uuid_text_that_is_not_a_uuid_is_an_error_not_a_panic() {
        for bad in [
            "é0123456789abcdef0123456789abcd",     // 32 bytes, non-ASCII
            "0123456é-89ab-cdef-0123-456789abcde", // 36 bytes, non-ASCII
            "0123456789abcdef0123456789abcdeg",
            "01234567+89ab-cdef-0123-456789abcdef",
            "",
        ] {
            let text: ArrayRef = Arc::new(StringArray::from(vec![bad]));
            assert!(
                to_canonical(&text, &CanonicalType::Uuid).is_err(),
                "{bad:?}"
            );
        }
        let plain: ArrayRef = Arc::new(StringArray::from(vec!["0123456789ABCDEF0123456789abcdef"]));
        let uuid = to_canonical(&plain, &CanonicalType::Uuid).unwrap();
        assert_eq!(uuid.as_fixed_size_binary().value(0)[..2], [0x01, 0x23]);
    }

    #[test]
    fn narrowing_to_canonical_errors_out_of_range() {
        let big: ArrayRef = Arc::new(Int64Array::from(vec![1 << 40]));
        assert!(to_canonical(&big, &CanonicalType::Int32).is_err());
        let small: ArrayRef = Arc::new(Int64Array::from(vec![7]));
        assert!(to_canonical(&small, &CanonicalType::Int32).is_ok());
    }

    #[test]
    fn json_text_is_validated() {
        let ok: ArrayRef = Arc::new(StringArray::from(vec![r#"{"a": 1}"#]));
        assert!(to_canonical(&ok, &CanonicalType::Json).is_ok());
        let bad: ArrayRef = Arc::new(StringArray::from(vec!["{a"]));
        assert!(to_canonical(&bad, &CanonicalType::Json).is_err());
    }
}
