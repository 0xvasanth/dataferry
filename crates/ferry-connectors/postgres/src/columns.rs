//! Building Arrow columns from Postgres binary wire values.

use std::sync::Arc;

use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Decimal256Builder,
    FixedSizeBinaryBuilder, Float32Builder, Float64Builder, Int16Builder, Int32Builder,
    Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
};
use arrow_array::{ArrayRef, ListArray};
use arrow_buffer::{NullBufferBuilder, OffsetBuffer, ScalarBuffer};
use arrow_schema::FieldRef;
use ferry_core::types::{CanonicalField, CanonicalType};
use postgres_protocol::types as wire;
use tokio_postgres::fallible_iterator::FallibleIterator;

use crate::decode::{date_days, numeric_unscaled, timestamp_micros};

/// Accumulates one column of a batch.
///
/// Values arrive in Postgres's binary format. `json`/`jsonb` columns must be
/// selected as `text` (see `select_list`), so they decode like text.
pub(crate) struct ColumnBuilder {
    kind: Kind,
}

enum Kind {
    Bool(BooleanBuilder),
    Int16(Int16Builder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Decimal128 {
        scale: i8,
        values: Decimal128Builder,
    },
    Decimal256 {
        scale: i8,
        values: Decimal256Builder,
    },
    Utf8(StringBuilder),
    Binary(BinaryBuilder),
    Date(Date32Builder),
    Timestamp(TimestampMicrosecondBuilder),
    Uuid(FixedSizeBinaryBuilder),
    List {
        item_field: FieldRef,
        item: Box<ColumnBuilder>,
        offsets: Vec<i32>,
        nulls: NullBufferBuilder,
    },
}

type WireError = Box<dyn std::error::Error + Sync + Send>;

fn wire_err(e: WireError) -> String {
    e.to_string()
}

impl ColumnBuilder {
    pub(crate) fn new(field: &CanonicalField) -> Result<Self, String> {
        use CanonicalType as C;
        let kind = match &field.ty {
            C::Bool => Kind::Bool(BooleanBuilder::new()),
            C::Int16 => Kind::Int16(Int16Builder::new()),
            C::Int32 => Kind::Int32(Int32Builder::new()),
            C::Int64 => Kind::Int64(Int64Builder::new()),
            C::Float32 => Kind::Float32(Float32Builder::new()),
            C::Float64 => Kind::Float64(Float64Builder::new()),
            C::Decimal(d) if d.precision() <= 38 => Kind::Decimal128 {
                scale: d.scale(),
                values: Decimal128Builder::new()
                    .with_precision_and_scale(d.precision(), d.scale())
                    .map_err(|e| e.to_string())?,
            },
            C::Decimal(d) => Kind::Decimal256 {
                scale: d.scale(),
                values: Decimal256Builder::new()
                    .with_precision_and_scale(d.precision(), d.scale())
                    .map_err(|e| e.to_string())?,
            },
            C::Utf8 | C::Json => Kind::Utf8(StringBuilder::new()),
            C::Binary => Kind::Binary(BinaryBuilder::new()),
            C::Date => Kind::Date(Date32Builder::new()),
            C::Timestamp { tz } => {
                Kind::Timestamp(TimestampMicrosecondBuilder::new().with_timezone_opt(tz.clone()))
            }
            C::Uuid => Kind::Uuid(FixedSizeBinaryBuilder::new(16)),
            C::List(item) => Kind::List {
                item_field: Arc::new(item.to_arrow()),
                item: Box::new(ColumnBuilder::new(item)?),
                offsets: vec![0],
                nulls: NullBufferBuilder::new(0),
            },
            other => return Err(format!("{other:?} columns can't be read from Postgres")),
        };
        Ok(Self { kind })
    }

    /// Appends one value; `None` is SQL NULL.
    pub(crate) fn append(&mut self, raw: Option<&[u8]>) -> Result<(), String> {
        let Some(b) = raw else {
            self.append_null();
            return Ok(());
        };
        match &mut self.kind {
            Kind::Bool(v) => v.append_value(wire::bool_from_sql(b).map_err(wire_err)?),
            Kind::Int16(v) => v.append_value(wire::int2_from_sql(b).map_err(wire_err)?),
            Kind::Int32(v) => v.append_value(wire::int4_from_sql(b).map_err(wire_err)?),
            Kind::Int64(v) => v.append_value(wire::int8_from_sql(b).map_err(wire_err)?),
            Kind::Float32(v) => v.append_value(wire::float4_from_sql(b).map_err(wire_err)?),
            Kind::Float64(v) => v.append_value(wire::float8_from_sql(b).map_err(wire_err)?),
            Kind::Decimal128 { scale, values } => {
                let unscaled = numeric_unscaled(b, *scale)?;
                let v = unscaled
                    .to_i128()
                    .ok_or("numeric value exceeds the column's precision")?;
                values.append_value(v);
            }
            Kind::Decimal256 { scale, values } => values.append_value(numeric_unscaled(b, *scale)?),
            Kind::Utf8(v) => v.append_value(wire::text_from_sql(b).map_err(wire_err)?),
            Kind::Binary(v) => v.append_value(wire::bytea_from_sql(b)),
            Kind::Date(v) => v.append_value(date_days(wire::date_from_sql(b).map_err(wire_err)?)?),
            Kind::Timestamp(v) => v.append_value(timestamp_micros(
                wire::timestamp_from_sql(b).map_err(wire_err)?,
            )?),
            Kind::Uuid(v) => v
                .append_value(wire::uuid_from_sql(b).map_err(wire_err)?)
                .map_err(|e| e.to_string())?,
            Kind::List {
                item,
                offsets,
                nulls,
                ..
            } => {
                let array = wire::array_from_sql(b).map_err(wire_err)?;
                if array.dimensions().count().map_err(wire_err)? > 1 {
                    return Err("multi-dimensional arrays are not supported".into());
                }
                let mut values = array.values();
                let mut len = 0;
                while let Some(value) = values.next().map_err(wire_err)? {
                    item.append(value)?;
                    len += 1;
                }
                let end = offsets.last().copied().unwrap_or(0) + len;
                offsets.push(end);
                nulls.append_non_null();
            }
        }
        Ok(())
    }

    fn append_null(&mut self) {
        match &mut self.kind {
            Kind::Bool(v) => v.append_null(),
            Kind::Int16(v) => v.append_null(),
            Kind::Int32(v) => v.append_null(),
            Kind::Int64(v) => v.append_null(),
            Kind::Float32(v) => v.append_null(),
            Kind::Float64(v) => v.append_null(),
            Kind::Decimal128 { values, .. } => values.append_null(),
            Kind::Decimal256 { values, .. } => values.append_null(),
            Kind::Utf8(v) => v.append_null(),
            Kind::Binary(v) => v.append_null(),
            Kind::Date(v) => v.append_null(),
            Kind::Timestamp(v) => v.append_null(),
            Kind::Uuid(v) => v.append_null(),
            Kind::List { offsets, nulls, .. } => {
                offsets.push(offsets.last().copied().unwrap_or(0));
                nulls.append_null();
            }
        }
    }

    /// The values appended so far, leaving the builder empty.
    pub(crate) fn finish(&mut self) -> ArrayRef {
        match &mut self.kind {
            Kind::Bool(v) => Arc::new(v.finish()),
            Kind::Int16(v) => Arc::new(v.finish()),
            Kind::Int32(v) => Arc::new(v.finish()),
            Kind::Int64(v) => Arc::new(v.finish()),
            Kind::Float32(v) => Arc::new(v.finish()),
            Kind::Float64(v) => Arc::new(v.finish()),
            Kind::Decimal128 { values, .. } => Arc::new(values.finish()),
            Kind::Decimal256 { values, .. } => Arc::new(values.finish()),
            Kind::Utf8(v) => Arc::new(v.finish()),
            Kind::Binary(v) => Arc::new(v.finish()),
            Kind::Date(v) => Arc::new(v.finish()),
            Kind::Timestamp(v) => Arc::new(v.finish()),
            Kind::Uuid(v) => Arc::new(v.finish()),
            Kind::List {
                item_field,
                item,
                offsets,
                nulls,
            } => {
                let offsets =
                    OffsetBuffer::new(ScalarBuffer::from(std::mem::replace(offsets, vec![0])));
                Arc::new(ListArray::new(
                    Arc::clone(item_field),
                    offsets,
                    item.finish(),
                    nulls.finish(),
                ))
            }
        }
    }
}
