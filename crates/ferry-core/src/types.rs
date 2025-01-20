//! The canonical type set every connector maps to and from.
//!
//! Each canonical type has exactly one Arrow representation. Converting back
//! from Arrow is strict: Arrow types outside that set are rejected rather than
//! silently coerced, so connectors must normalize (e.g. nanosecond timestamps
//! to microseconds) when they build batches.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Fields, TimeUnit};

use crate::error::{FerryError, Result};

/// Arrow extension-type metadata key used to tag `uuid` and `json` fields.
pub const EXTENSION_NAME_KEY: &str = "ARROW:extension:name";
pub const UUID_EXTENSION: &str = "arrow.uuid";
pub const JSON_EXTENSION: &str = "arrow.json";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CanonicalType {
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Decimal(DecimalSpec),
    Bool,
    Utf8,
    Binary,
    Date,
    /// Microsecond precision. `tz: None` is a wall-clock timestamp.
    Timestamp {
        tz: Option<Arc<str>>,
    },
    Uuid,
    Json,
    List(Box<CanonicalField>),
    Struct(Vec<CanonicalField>),
    Map {
        key: Box<CanonicalType>,
        value: Box<CanonicalField>,
    },
}

/// Precision and scale of a decimal, validated on construction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecimalSpec {
    precision: u8,
    scale: i8,
}

impl DecimalSpec {
    pub const MAX_PRECISION: u8 = 76;

    pub fn new(precision: u8, scale: i8) -> Result<Self> {
        let invalid = |reason| FerryError::InvalidDecimal {
            precision,
            scale,
            reason,
        };
        if precision == 0 || precision > Self::MAX_PRECISION {
            return Err(invalid("precision must be between 1 and 76"));
        }
        if i16::from(scale) > i16::from(precision) {
            return Err(invalid("scale cannot exceed precision"));
        }
        Ok(Self { precision, scale })
    }

    pub fn precision(&self) -> u8 {
        self.precision
    }

    pub fn scale(&self) -> i8 {
        self.scale
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalField {
    pub name: String,
    pub ty: CanonicalType,
    pub nullable: bool,
}

impl CanonicalField {
    pub fn new(name: impl Into<String>, ty: CanonicalType, nullable: bool) -> Self {
        Self {
            name: name.into(),
            ty,
            nullable,
        }
    }

    pub fn to_arrow(&self) -> Field {
        let field = Field::new(&self.name, self.ty.to_arrow(), self.nullable);
        match self.ty.extension_name() {
            Some(ext) => field.with_metadata(HashMap::from([(
                EXTENSION_NAME_KEY.to_string(),
                ext.to_string(),
            )])),
            None => field,
        }
    }

    pub fn from_arrow(field: &Field) -> Result<Self> {
        Ok(Self {
            name: field.name().clone(),
            ty: CanonicalType::from_arrow(field)?,
            nullable: field.is_nullable(),
        })
    }
}

impl CanonicalType {
    /// Microseconds is the single canonical timestamp unit.
    pub const TIMESTAMP_UNIT: TimeUnit = TimeUnit::Microsecond;

    fn extension_name(&self) -> Option<&'static str> {
        match self {
            CanonicalType::Uuid => Some(UUID_EXTENSION),
            CanonicalType::Json => Some(JSON_EXTENSION),
            _ => None,
        }
    }

    pub fn to_arrow(&self) -> DataType {
        use CanonicalType::*;
        match self {
            Int8 => DataType::Int8,
            Int16 => DataType::Int16,
            Int32 => DataType::Int32,
            Int64 => DataType::Int64,
            UInt8 => DataType::UInt8,
            UInt16 => DataType::UInt16,
            UInt32 => DataType::UInt32,
            UInt64 => DataType::UInt64,
            Float32 => DataType::Float32,
            Float64 => DataType::Float64,
            Decimal(d) if d.precision <= DECIMAL128_MAX_PRECISION => {
                DataType::Decimal128(d.precision, d.scale)
            }
            Decimal(d) => DataType::Decimal256(d.precision, d.scale),
            Bool => DataType::Boolean,
            Utf8 | Json => DataType::Utf8,
            Binary => DataType::Binary,
            Date => DataType::Date32,
            Timestamp { tz } => DataType::Timestamp(Self::TIMESTAMP_UNIT, tz.clone()),
            Uuid => DataType::FixedSizeBinary(16),
            List(item) => DataType::List(Arc::new(item.to_arrow())),
            Struct(fields) => DataType::Struct(fields.iter().map(|f| f.to_arrow()).collect()),
            Map { key, value } => {
                let entries = Fields::from(vec![
                    Field::new(MAP_KEY_FIELD, key.to_arrow(), false),
                    value.to_arrow(),
                ]);
                DataType::Map(
                    Arc::new(Field::new(
                        MAP_ENTRIES_FIELD,
                        DataType::Struct(entries),
                        false,
                    )),
                    false,
                )
            }
        }
    }

    fn from_arrow(field: &Field) -> Result<Self> {
        use CanonicalType::*;
        let extension = field.metadata().get(EXTENSION_NAME_KEY).map(String::as_str);
        let unsupported = || FerryError::UnsupportedArrowType {
            field: field.name().clone(),
            arrow_type: field.data_type().to_string(),
        };
        Ok(match (field.data_type(), extension) {
            (DataType::Utf8, Some(JSON_EXTENSION)) => Json,
            (DataType::FixedSizeBinary(16), Some(UUID_EXTENSION)) => Uuid,
            (_, Some(_)) => return Err(unsupported()),
            (DataType::Int8, None) => Int8,
            (DataType::Int16, None) => Int16,
            (DataType::Int32, None) => Int32,
            (DataType::Int64, None) => Int64,
            (DataType::UInt8, None) => UInt8,
            (DataType::UInt16, None) => UInt16,
            (DataType::UInt32, None) => UInt32,
            (DataType::UInt64, None) => UInt64,
            (DataType::Float32, None) => Float32,
            (DataType::Float64, None) => Float64,
            (DataType::Decimal128(p, s), None) => Decimal(DecimalSpec::new(*p, *s)?),
            // Decimal256 is only canonical above Decimal128's range; below it
            // the same decimal would have two Arrow representations.
            (DataType::Decimal256(p, s), None) if *p > DECIMAL128_MAX_PRECISION => {
                Decimal(DecimalSpec::new(*p, *s)?)
            }
            (DataType::Boolean, None) => Bool,
            (DataType::Utf8, None) => Utf8,
            (DataType::Binary, None) => Binary,
            (DataType::Date32, None) => Date,
            (DataType::Timestamp(unit, tz), None) if *unit == Self::TIMESTAMP_UNIT => {
                Timestamp { tz: tz.clone() }
            }
            (DataType::List(item), None) => List(Box::new(CanonicalField::from_arrow(item)?)),
            (DataType::Struct(fields), None) => Struct(
                fields
                    .iter()
                    .map(|f| CanonicalField::from_arrow(f))
                    .collect::<Result<_>>()?,
            ),
            (DataType::Map(entries, false), None) => match entries.data_type() {
                DataType::Struct(kv) if kv.len() == 2 => Map {
                    key: Box::new(CanonicalType::from_arrow(&kv[0])?),
                    value: Box::new(CanonicalField::from_arrow(&kv[1])?),
                },
                _ => return Err(unsupported()),
            },
            _ => return Err(unsupported()),
        })
    }
}

const DECIMAL128_MAX_PRECISION: u8 = 38;
const MAP_ENTRIES_FIELD: &str = "entries";
const MAP_KEY_FIELD: &str = "key";
