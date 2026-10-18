//! Canonical type names as the config writes them (`int64`,
//! `decimal(12,2)`, `timestamp(UTC)`), and the lenient mapping from whatever
//! Arrow type an expression produces to the canonical type it denotes.

use std::sync::Arc;

use arrow_schema::{DataType, Field};
use dataferry_core::types::{
    CanonicalField, CanonicalType, DecimalSpec, EXTENSION_NAME_KEY, JSON_EXTENSION,
};

/// The type names `type:` accepts besides `decimal(p,s)`, `timestamp`,
/// `timestamp(<tz>)` and `list(<type>)`.
pub const SCALAR_NAMES: &[&str] = &[
    "int8", "int16", "int32", "int64", "uint8", "uint16", "uint32", "uint64", "float32", "float64",
    "bool", "utf8", "binary", "date", "uuid", "json",
];

/// How a type is written in config and in messages.
pub fn name(ty: &CanonicalType) -> String {
    use CanonicalType::*;
    match ty {
        Int8 => "int8".into(),
        Int16 => "int16".into(),
        Int32 => "int32".into(),
        Int64 => "int64".into(),
        UInt8 => "uint8".into(),
        UInt16 => "uint16".into(),
        UInt32 => "uint32".into(),
        UInt64 => "uint64".into(),
        Float32 => "float32".into(),
        Float64 => "float64".into(),
        Decimal(d) => format!("decimal({},{})", d.precision(), d.scale()),
        Bool => "bool".into(),
        Utf8 => "utf8".into(),
        Binary => "binary".into(),
        Date => "date".into(),
        Timestamp { tz: None } => "timestamp".into(),
        Timestamp { tz: Some(tz) } => format!("timestamp({tz})"),
        Uuid => "uuid".into(),
        Json => "json".into(),
        List(item) => format!("list({})", name(&item.ty)),
        Struct(fields) => format!(
            "struct({})",
            fields
                .iter()
                .map(|f| format!("{}: {}", f.name, name(&f.ty)))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Map { key, value } => format!("map({}, {})", name(key), name(&value.ty)),
    }
}

/// Parses a `type:` value. The error says what is accepted.
pub fn parse(text: &str) -> Result<CanonicalType, String> {
    use CanonicalType::*;
    let text = text.trim();
    let call = |prefix: &str| {
        text.strip_prefix(prefix)
            .and_then(|r| r.trim_start().strip_prefix('('))
            .and_then(|r| r.strip_suffix(')'))
            .map(str::trim)
    };
    Ok(match text {
        "int8" => Int8,
        "int16" => Int16,
        "int32" => Int32,
        "int64" => Int64,
        "uint8" => UInt8,
        "uint16" => UInt16,
        "uint32" => UInt32,
        "uint64" => UInt64,
        "float32" => Float32,
        "float64" => Float64,
        "bool" => Bool,
        "utf8" => Utf8,
        "binary" => Binary,
        "date" => Date,
        "uuid" => Uuid,
        "json" => Json,
        "timestamp" => Timestamp { tz: None },
        _ => {
            if let Some(args) = call("decimal") {
                let (p, s) = args
                    .split_once(',')
                    .ok_or_else(|| format!("`{text}`: write `decimal(<precision>,<scale>)`"))?;
                let p: u8 = p
                    .trim()
                    .parse()
                    .map_err(|_| format!("`{text}`: precision must be a number"))?;
                let s: i8 = s
                    .trim()
                    .parse()
                    .map_err(|_| format!("`{text}`: scale must be a number"))?;
                Decimal(DecimalSpec::new(p, s).map_err(|e| e.to_string())?)
            } else if let Some(tz) = call("timestamp") {
                if tz.is_empty() {
                    return Err(format!(
                        "`{text}`: name the time zone, e.g. `timestamp(UTC)`"
                    ));
                }
                Timestamp {
                    tz: Some(Arc::from(tz)),
                }
            } else if let Some(item) = call("list") {
                List(Box::new(CanonicalField::new("item", parse(item)?, true)))
            } else {
                return Err(format!("`{text}` is not a type"));
            }
        }
    })
}

/// The canonical type an Arrow result denotes. `Ok(None)` is a bare `null`,
/// whose type isn't known. Representations that only differ in layout map
/// to the same type (`Utf8View` is `utf8`, nanosecond timestamps are
/// `timestamp`); [`crate::expr::convert::to_canonical`] normalizes the
/// values.
pub fn from_arrow(field: &Field) -> Result<Option<CanonicalType>, String> {
    let json = field.metadata().get(EXTENSION_NAME_KEY).map(String::as_str) == Some(JSON_EXTENSION);
    from_data_type(field.data_type(), json)
}

fn from_data_type(dt: &DataType, json: bool) -> Result<Option<CanonicalType>, String> {
    use CanonicalType::*;
    let decimal = |p: u8, s: i8| {
        DecimalSpec::new(p, s)
            .map(Decimal)
            .map_err(|e| e.to_string())
    };
    let nested = |item: &Field| -> Result<CanonicalField, String> {
        let ty = from_arrow(item)?.ok_or("a list of nulls has no type")?;
        Ok(CanonicalField::new(item.name(), ty, item.is_nullable()))
    };
    Ok(Some(match dt {
        DataType::Null => return Ok(None),
        DataType::Int8 => Int8,
        DataType::Int16 => Int16,
        DataType::Int32 => Int32,
        DataType::Int64 => Int64,
        DataType::UInt8 => UInt8,
        DataType::UInt16 => UInt16,
        DataType::UInt32 => UInt32,
        DataType::UInt64 => UInt64,
        DataType::Float16 | DataType::Float32 => Float32,
        DataType::Float64 => Float64,
        DataType::Decimal32(p, s)
        | DataType::Decimal64(p, s)
        | DataType::Decimal128(p, s)
        | DataType::Decimal256(p, s) => decimal(*p, *s)?,
        DataType::Boolean => Bool,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View if json => Json,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Utf8,
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => Binary,
        DataType::FixedSizeBinary(16) => Uuid,
        DataType::Date32 | DataType::Date64 => Date,
        DataType::Timestamp(_, tz) => Timestamp { tz: tz.clone() },
        DataType::List(item) | DataType::LargeList(item) | DataType::FixedSizeList(item, _) => {
            List(Box::new(nested(item)?))
        }
        DataType::Struct(fields) => {
            Struct(fields.iter().map(|f| nested(f)).collect::<Result<_, _>>()?)
        }
        DataType::Map(entries, _) => match entries.data_type() {
            DataType::Struct(kv) if kv.len() == 2 => Map {
                key: Box::new(nested(&kv[0])?.ty),
                value: Box::new(nested(&kv[1])?),
            },
            other => return Err(format!("{other} is not a map")),
        },
        DataType::Dictionary(_, value) => return from_data_type(value, json),
        other => return Err(format!("{other} has no canonical type")),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_parse_back() {
        for text in [
            "int8",
            "uint64",
            "float32",
            "decimal(12,2)",
            "decimal(38,10)",
            "timestamp",
            "timestamp(UTC)",
            "timestamp(+05:30)",
            "uuid",
            "json",
            "list(utf8)",
            "list(decimal(10,2))",
        ] {
            assert_eq!(name(&parse(text).unwrap()), text);
        }
        assert_eq!(
            name(&parse(" decimal( 12 , 2 ) ").unwrap()),
            "decimal(12,2)"
        );
    }

    #[test]
    fn bad_names_say_why() {
        assert!(parse("int").unwrap_err().contains("not a type"));
        assert!(parse("decimal(80,2)").unwrap_err().contains("precision"));
        assert!(
            parse("decimal(12)")
                .unwrap_err()
                .contains("decimal(<precision>,<scale>)")
        );
        assert!(parse("timestamp()").unwrap_err().contains("time zone"));
    }

    #[test]
    fn layouts_of_one_type_map_to_it() {
        use arrow_schema::TimeUnit;
        let f = |dt| Field::new("x", dt, true);
        assert_eq!(
            from_arrow(&f(DataType::Utf8View)).unwrap(),
            Some(CanonicalType::Utf8)
        );
        assert_eq!(
            from_arrow(&f(DataType::Timestamp(TimeUnit::Nanosecond, None))).unwrap(),
            Some(CanonicalType::Timestamp { tz: None })
        );
        assert_eq!(from_arrow(&f(DataType::Null)).unwrap(), None);
        assert!(from_arrow(&f(DataType::Time64(TimeUnit::Microsecond))).is_err());
        let json = CanonicalField::new("j", CanonicalType::Json, true).to_arrow();
        assert_eq!(from_arrow(&json).unwrap(), Some(CanonicalType::Json));
    }
}
