//! Mapping Postgres column types to canonical types.

use std::sync::Arc;

use dataferry_core::types::{CanonicalField, CanonicalType, DecimalSpec};

/// A column's type as `pg_attribute` and `pg_type` describe it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PgType {
    pub oid: u32,
    /// `atttypmod`; `-1` when the type has no modifier.
    pub typmod: i32,
    /// For array types, the element type's oid.
    pub element: Option<u32>,
}

pub(crate) mod oid {
    pub const BOOL: u32 = 16;
    pub const BYTEA: u32 = 17;
    pub const NAME: u32 = 19;
    pub const INT8: u32 = 20;
    pub const INT2: u32 = 21;
    pub const INT4: u32 = 23;
    pub const TEXT: u32 = 25;
    pub const JSON: u32 = 114;
    pub const FLOAT4: u32 = 700;
    pub const FLOAT8: u32 = 701;
    pub const BPCHAR: u32 = 1042;
    pub const VARCHAR: u32 = 1043;
    pub const DATE: u32 = 1082;
    pub const TIMESTAMP: u32 = 1114;
    pub const TIMESTAMPTZ: u32 = 1184;
    pub const NUMERIC: u32 = 1700;
    pub const UUID: u32 = 2950;
    pub const JSONB: u32 = 3802;
}

/// Postgres stores `timestamptz` as UTC, so that is the canonical zone.
pub(crate) const UTC: &str = "UTC";

/// The canonical type for a Postgres column, or why it has none.
pub(crate) fn to_canonical(t: PgType) -> Result<CanonicalType, String> {
    if let Some(element) = t.element {
        let item = to_canonical(PgType {
            oid: element,
            typmod: t.typmod,
            element: None,
        })?;
        return Ok(CanonicalType::List(Box::new(CanonicalField::new(
            "item", item, true,
        ))));
    }
    use CanonicalType::*;
    Ok(match t.oid {
        oid::BOOL => Bool,
        oid::INT2 => Int16,
        oid::INT4 => Int32,
        oid::INT8 => Int64,
        oid::FLOAT4 => Float32,
        oid::FLOAT8 => Float64,
        oid::TEXT | oid::VARCHAR | oid::BPCHAR | oid::NAME => Utf8,
        oid::BYTEA => Binary,
        oid::DATE => Date,
        oid::TIMESTAMP => Timestamp { tz: None },
        oid::TIMESTAMPTZ => Timestamp {
            tz: Some(Arc::from(UTC)),
        },
        oid::UUID => Uuid,
        oid::JSON | oid::JSONB => Json,
        oid::NUMERIC => Decimal(numeric_spec(t.typmod)?),
        other => return Err(format!("Postgres type oid {other} is not supported")),
    })
}

/// Precision and scale encoded in a `numeric` typmod.
fn numeric_spec(typmod: i32) -> Result<DecimalSpec, String> {
    if typmod < 4 {
        return Err("unconstrained `numeric` has no fixed precision; \
                    declare the column as numeric(p, s) or cast it in a query"
            .into());
    }
    let packed = typmod - 4;
    let precision = (packed >> 16) & 0xffff;
    // The scale is stored as an 11-bit signed value (Postgres 15+ allows
    // negative scales).
    let scale = ((packed & 0x7ff) ^ 0x400) - 0x400;
    let precision = u8::try_from(precision).map_err(|_| too_wide(precision))?;
    let scale =
        i8::try_from(scale).map_err(|_| format!("numeric scale {scale} is out of range"))?;
    DecimalSpec::new(precision, scale).map_err(|_| too_wide(precision.into()))
}

fn too_wide(precision: i32) -> String {
    format!(
        "numeric({precision}, _) exceeds the {} digits a decimal can hold",
        DecimalSpec::MAX_PRECISION
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scalar(oid: u32) -> PgType {
        PgType {
            oid,
            typmod: -1,
            element: None,
        }
    }

    /// `atttypmod` for `numeric(p, s)`.
    fn numeric(p: i32, s: i32) -> PgType {
        PgType {
            oid: oid::NUMERIC,
            typmod: ((p << 16) | s) + 4,
            element: None,
        }
    }

    fn array_of(element: u32, typmod: i32) -> PgType {
        PgType {
            oid: 0,
            typmod,
            element: Some(element),
        }
    }

    #[test]
    fn scalar_types_map_to_their_canonical_type() {
        use CanonicalType::*;
        let cases = [
            (oid::BOOL, Bool),
            (oid::INT2, Int16),
            (oid::INT4, Int32),
            (oid::INT8, Int64),
            (oid::FLOAT4, Float32),
            (oid::FLOAT8, Float64),
            (oid::TEXT, Utf8),
            (oid::VARCHAR, Utf8),
            (oid::BPCHAR, Utf8),
            (oid::NAME, Utf8),
            (oid::BYTEA, Binary),
            (oid::DATE, Date),
            (oid::TIMESTAMP, Timestamp { tz: None }),
            (
                oid::TIMESTAMPTZ,
                Timestamp {
                    tz: Some(Arc::from(UTC)),
                },
            ),
            (oid::UUID, Uuid),
            (oid::JSON, Json),
            (oid::JSONB, Json),
        ];
        for (oid, expected) in cases {
            assert_eq!(to_canonical(scalar(oid)), Ok(expected), "oid {oid}");
        }
    }

    #[test]
    fn numeric_keeps_its_declared_precision_and_scale() {
        let d = |p, s| CanonicalType::Decimal(DecimalSpec::new(p, s).unwrap());
        assert_eq!(to_canonical(numeric(38, 10)), Ok(d(38, 10)));
        assert_eq!(to_canonical(numeric(12, 0)), Ok(d(12, 0)));
        assert_eq!(to_canonical(numeric(60, 4)), Ok(d(60, 4)));
    }

    #[test]
    fn unconstrained_numeric_is_unsupported_with_a_fix() {
        let reason = to_canonical(scalar(oid::NUMERIC)).unwrap_err();
        assert!(reason.contains("numeric(p, s)"), "{reason}");
    }

    #[test]
    fn numeric_beyond_76_digits_is_unsupported() {
        assert!(to_canonical(numeric(100, 0)).is_err());
    }

    #[test]
    fn arrays_map_to_lists_of_nullable_elements() {
        let list = to_canonical(array_of(oid::INT4, -1)).unwrap();
        assert_eq!(
            list,
            CanonicalType::List(Box::new(CanonicalField::new(
                "item",
                CanonicalType::Int32,
                true
            )))
        );
    }

    #[test]
    fn an_array_column_typmod_applies_to_its_elements() {
        let typmod = ((10 << 16) | 2) + 4;
        let list = to_canonical(array_of(oid::NUMERIC, typmod)).unwrap();
        let CanonicalType::List(item) = list else {
            panic!("not a list")
        };
        assert_eq!(
            item.ty,
            CanonicalType::Decimal(DecimalSpec::new(10, 2).unwrap())
        );
    }

    #[test]
    fn unknown_types_are_unsupported_by_oid() {
        // 600 is `point`.
        let reason = to_canonical(scalar(600)).unwrap_err();
        assert!(reason.contains("600"), "{reason}");
        assert!(to_canonical(array_of(600, -1)).is_err());
    }
}
