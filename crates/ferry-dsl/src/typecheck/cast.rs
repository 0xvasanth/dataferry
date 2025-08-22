//! Which conversions a column's `type:` may ask for, and which of them lose
//! information (B3).
//!
//! A conversion is *safe* when every value of the source type has an exact
//! value in the target type; it happens without asking. It is *lossy* when
//! some values change (precision, scale, time zone) or fail (out of range,
//! text that doesn't parse); the column must say `allow_lossy: true`.
//! Anything else (`bool` to `timestamp`) is not a conversion at all and has
//! to be written in the expression.
//!
//! [`LOSSY`] is the canonical table: one row per kind of loss, each with a
//! test that it is rejected by default and accepted with the opt-in. The
//! classifier is general (`int64 → int16` is lossy like `int64 → int32`);
//! the table names the cases users meet.

use ferry_core::types::CanonicalType;

use crate::types::name;

/// What converting one canonical type to another costs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Conversion {
    Same,
    /// Every value converts exactly.
    Safe,
    /// Some values change or fail; needs `allow_lossy: true`.
    Lossy(String),
    /// Not a conversion: write it in the expression.
    Impossible,
}

/// A row of the canonical lossy-cast table.
#[derive(Clone, Copy, Debug)]
pub struct LossyCast {
    pub from: &'static str,
    pub to: &'static str,
    /// What happens to the values, at run time, once opted in.
    pub effect: &'static str,
}

/// The canonical lossy casts (documented in 0003 §5.4).
pub const LOSSY: &[LossyCast] = &[
    LossyCast {
        from: "int64",
        to: "int32",
        effect: "values outside int32's range are error rows",
    },
    LossyCast {
        from: "int32",
        to: "int16",
        effect: "values outside int16's range are error rows",
    },
    LossyCast {
        from: "uint64",
        to: "int64",
        effect: "values above int64's maximum are error rows",
    },
    LossyCast {
        from: "int64",
        to: "uint64",
        effect: "negative values are error rows",
    },
    LossyCast {
        from: "int64",
        to: "float64",
        effect: "integers beyond ±2^53 are rounded",
    },
    LossyCast {
        from: "float64",
        to: "float32",
        effect: "values are rounded to float32 precision",
    },
    LossyCast {
        from: "float64",
        to: "int64",
        effect: "the fraction is truncated toward zero; out-of-range values are error rows",
    },
    LossyCast {
        from: "decimal(38,10)",
        to: "float64",
        effect: "values are rounded to the nearest float64",
    },
    LossyCast {
        from: "decimal(12,4)",
        to: "decimal(12,2)",
        effect: "the scale shrinks: values are rounded half away from zero",
    },
    LossyCast {
        from: "decimal(12,2)",
        to: "decimal(8,2)",
        effect: "values with more integer digits are error rows",
    },
    LossyCast {
        from: "float64",
        to: "decimal(12,2)",
        effect: "values are rounded to the scale; out-of-range values are error rows",
    },
    LossyCast {
        from: "decimal(12,2)",
        to: "int64",
        effect: "the fraction is dropped",
    },
    LossyCast {
        from: "timestamp(UTC)",
        to: "timestamp",
        effect: "the zone is dropped: the UTC wall-clock time is kept",
    },
    LossyCast {
        from: "timestamp",
        to: "timestamp(UTC)",
        effect: "the wall-clock time is read as UTC",
    },
    LossyCast {
        from: "timestamp(UTC)",
        to: "date",
        effect: "the time of day is dropped",
    },
    LossyCast {
        from: "utf8",
        to: "int64",
        effect: "text that isn't an integer is an error row",
    },
    LossyCast {
        from: "utf8",
        to: "uuid",
        effect: "text that isn't a UUID is an error row",
    },
    LossyCast {
        from: "utf8",
        to: "json",
        effect: "text that isn't JSON is an error row",
    },
    LossyCast {
        from: "binary",
        to: "utf8",
        effect: "bytes that aren't UTF-8 are error rows",
    },
];

/// The inclusive range of an integer type, if it is one.
fn int_range(ty: &CanonicalType) -> Option<(i128, i128)> {
    use CanonicalType::*;
    Some(match ty {
        Int8 => (i8::MIN.into(), i8::MAX.into()),
        Int16 => (i16::MIN.into(), i16::MAX.into()),
        Int32 => (i32::MIN.into(), i32::MAX.into()),
        Int64 => (i64::MIN.into(), i64::MAX.into()),
        UInt8 => (0, u8::MAX.into()),
        UInt16 => (0, u16::MAX.into()),
        UInt32 => (0, u32::MAX.into()),
        UInt64 => (0, u64::MAX.into()),
        _ => return None,
    })
}

/// Decimal digits needed for every value of an integer type.
fn int_digits(range: (i128, i128)) -> u32 {
    range.0.unsigned_abs().max(range.1.unsigned_abs()).ilog10() + 1
}

/// Integers up to this magnitude are exact in a float with `mantissa` bits.
fn exact_in_float(range: (i128, i128), mantissa: u32) -> bool {
    range.0.unsigned_abs().max(range.1.unsigned_abs()) <= 1u128 << mantissa
}

fn is_text_parse_target(ty: &CanonicalType) -> bool {
    use CanonicalType::*;
    int_range(ty).is_some()
        || matches!(
            ty,
            Float32 | Float64 | Decimal(_) | Bool | Date | Timestamp { .. } | Uuid | Json
        )
}

fn lossy(from: &CanonicalType, to: &CanonicalType, why: impl Into<String>) -> Conversion {
    Conversion::Lossy(format!("{} → {}: {}", name(from), name(to), why.into()))
}

/// What converting `from` to `to` costs.
pub fn classify(from: &CanonicalType, to: &CanonicalType) -> Conversion {
    use CanonicalType::*;
    if from == to {
        return Conversion::Same;
    }
    match (from, to) {
        (_, Utf8)
            if int_range(from).is_some()
                || matches!(
                    from,
                    Float32 | Float64 | Decimal(_) | Bool | Date | Timestamp { .. } | Uuid | Json
                ) =>
        {
            // Every one of these has an exact text form.
            Conversion::Safe
        }
        (Utf8, Binary) => Conversion::Safe,
        (Binary, Utf8) => lossy(from, to, "bytes that aren't UTF-8 fail"),
        (Utf8, t) if is_text_parse_target(t) => lossy(
            from,
            to,
            format!("text that isn't a valid {} fails", name(t)),
        ),
        _ => match (int_range(from), int_range(to)) {
            (Some(a), Some(b)) => {
                if b.0 <= a.0 && a.1 <= b.1 {
                    Conversion::Safe
                } else if a.0 < 0 && b.0 == 0 {
                    lossy(from, to, "negative values fail")
                } else {
                    lossy(
                        from,
                        to,
                        format!("values outside {}'s range fail", name(to)),
                    )
                }
            }
            (Some(a), None) => from_int(from, a, to),
            (None, Some(b)) => to_int(from, to, b),
            (None, None) => other(from, to),
        },
    }
}

fn from_int(from: &CanonicalType, range: (i128, i128), to: &CanonicalType) -> Conversion {
    use CanonicalType::*;
    match to {
        Float64 if exact_in_float(range, 53) => Conversion::Safe,
        Float64 => lossy(from, to, "integers beyond ±2^53 are rounded"),
        Float32 if exact_in_float(range, 24) => Conversion::Safe,
        Float32 => lossy(from, to, "integers beyond ±2^24 are rounded"),
        Decimal(d) => {
            let whole = i32::from(d.precision()) - i32::from(d.scale());
            if i32::try_from(int_digits(range)).is_ok_and(|n| n <= whole) {
                Conversion::Safe
            } else {
                lossy(
                    from,
                    to,
                    format!("values with more than {whole} integer digits fail"),
                )
            }
        }
        _ => Conversion::Impossible,
    }
}

fn to_int(from: &CanonicalType, to: &CanonicalType, range: (i128, i128)) -> Conversion {
    use CanonicalType::*;
    match from {
        Float32 | Float64 => lossy(
            from,
            to,
            "the fraction is truncated, and out-of-range values fail",
        ),
        Decimal(d) if d.scale() <= 0 => {
            let digits = i32::from(d.precision()) - i32::from(d.scale());
            // Every decimal with `digits` digits fits if the range holds
            // 10^digits - 1.
            let max = 10i128.checked_pow(digits.unsigned_abs()).map(|p| p - 1);
            match max {
                Some(max) if range.0 <= -max && max <= range.1 => Conversion::Safe,
                _ => lossy(
                    from,
                    to,
                    format!("values outside {}'s range fail", name(to)),
                ),
            }
        }
        Decimal(_) => lossy(from, to, "the fraction is dropped"),
        _ => Conversion::Impossible,
    }
}

fn other(from: &CanonicalType, to: &CanonicalType) -> Conversion {
    use CanonicalType::*;
    match (from, to) {
        (Float32, Float64) => Conversion::Safe,
        (Float64, Float32) => lossy(from, to, "values are rounded to float32"),
        (Decimal(a), Decimal(b)) => {
            let whole = |d: &ferry_core::types::DecimalSpec| {
                i32::from(d.precision()) - i32::from(d.scale())
            };
            match (b.scale() >= a.scale(), whole(b) >= whole(a)) {
                (true, true) => Conversion::Safe,
                (false, _) => lossy(
                    from,
                    to,
                    format!(
                        "scale {} → {}: values are rounded half away from zero",
                        a.scale(),
                        b.scale()
                    ),
                ),
                (true, false) => lossy(
                    from,
                    to,
                    format!("values with more than {} integer digits fail", whole(b)),
                ),
            }
        }
        (Decimal(d), Float64) if d.precision() <= 15 => Conversion::Safe,
        (Decimal(d), Float32) if d.precision() <= 6 => Conversion::Safe,
        (Decimal(_), Float32 | Float64) => {
            lossy(from, to, "values are rounded to the nearest float")
        }
        (Float32 | Float64, Decimal(_)) => lossy(
            from,
            to,
            "floats aren't exact decimals: values are rounded to the scale",
        ),
        (Timestamp { tz: Some(_) }, Timestamp { tz: Some(_) }) => Conversion::Safe,
        (Timestamp { tz: Some(_) }, Timestamp { tz: None }) => {
            lossy(from, to, "the time zone is dropped")
        }
        (Timestamp { tz: None }, Timestamp { tz: Some(tz) }) => {
            lossy(from, to, format!("a wall-clock time is read as {tz}"))
        }
        (Timestamp { .. }, Date) => lossy(from, to, "the time of day is dropped"),
        (Date, Timestamp { tz: None }) => Conversion::Safe,
        (Date, Timestamp { tz: Some(tz) }) => lossy(from, to, format!("midnight is read as {tz}")),
        (List(a), List(b)) => match classify(&a.ty, &b.ty) {
            Conversion::Same | Conversion::Safe => Conversion::Safe,
            Conversion::Lossy(why) => Conversion::Lossy(format!("elements: {why}")),
            Conversion::Impossible => Conversion::Impossible,
        },
        _ => Conversion::Impossible,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::parse;

    fn class(from: &str, to: &str) -> Conversion {
        classify(&parse(from).unwrap(), &parse(to).unwrap())
    }

    #[test]
    fn every_canonical_row_is_lossy() {
        for row in LOSSY {
            assert!(
                matches!(class(row.from, row.to), Conversion::Lossy(_)),
                "{} → {}",
                row.from,
                row.to
            );
        }
    }

    #[test]
    fn widening_is_safe() {
        for (from, to) in [
            ("int32", "int64"),
            ("uint32", "int64"),
            ("int8", "float32"),
            ("int32", "float64"),
            ("float32", "float64"),
            ("int32", "decimal(10,0)"),
            ("int64", "decimal(38,10)"),
            ("decimal(10,2)", "decimal(12,4)"),
            ("decimal(12,2)", "float64"),
            ("decimal(18,0)", "int64"),
            ("timestamp(UTC)", "timestamp(+05:30)"),
            ("date", "timestamp"),
            ("int64", "utf8"),
            ("uuid", "utf8"),
            ("json", "utf8"),
            ("decimal(12,2)", "utf8"),
            ("list(int32)", "list(int64)"),
        ] {
            assert_eq!(class(from, to), Conversion::Safe, "{from} → {to}");
        }
    }

    #[test]
    fn near_misses_are_lossy() {
        for (from, to) in [
            ("int32", "decimal(9,0)"),
            ("int32", "float32"),
            ("decimal(19,0)", "int64"),
            ("decimal(16,2)", "float64"),
            ("uint8", "int8"),
            ("list(int64)", "list(int32)"),
        ] {
            assert!(
                matches!(class(from, to), Conversion::Lossy(_)),
                "{from} → {to}"
            );
        }
    }

    #[test]
    fn unrelated_types_are_not_conversions() {
        for (from, to) in [
            ("bool", "timestamp"),
            ("date", "int64"),
            ("uuid", "int64"),
            ("json", "uuid"),
        ] {
            assert_eq!(class(from, to), Conversion::Impossible, "{from} → {to}");
        }
    }

    #[test]
    fn the_reason_names_both_types() {
        let Conversion::Lossy(why) = class("int64", "int32") else {
            panic!()
        };
        assert_eq!(why, "int64 → int32: values outside int32's range fail");
    }
}
