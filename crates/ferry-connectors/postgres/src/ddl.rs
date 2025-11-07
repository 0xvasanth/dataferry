//! Rendering canonical target tables as Postgres DDL, and checking that a
//! table which already exists can take the rows.
//!
//! Every canonical type has one Postgres type (see [`column_type`]); a
//! table the sink creates uses exactly those. An existing table may also
//! use a few equivalents that take the same binary values (`varchar` for
//! `text`, `json` for `jsonb`). Foreign keys are not created with the
//! tables: finalize adds them once the flow is loaded (A6, see
//! `crate::deferred`). The sink also uses them to find the children of a
//! parent table.

use ferry_core::TargetTable;
use ferry_core::types::{CanonicalField, CanonicalType};

use crate::encode::Wire;
use crate::pg_type::oid;
use crate::source::quote;

/// The Postgres type a canonical type is stored as, and how its values are
/// sent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ColumnType {
    /// As `format_type()` prints it, so it compares with the catalog.
    pub sql: String,
    pub wire: Wire,
}

impl ColumnType {
    fn new(sql: impl Into<String>, wire: Wire) -> Self {
        Self {
            sql: sql.into(),
            wire,
        }
    }
}

pub(crate) fn column_type(ty: &CanonicalType) -> ColumnType {
    use CanonicalType as C;
    match ty {
        // Postgres has no one-byte or unsigned integers: each widens to the
        // smallest type that holds every value.
        C::Int8 | C::Int16 | C::UInt8 => ColumnType::new("smallint", Wire::Int2),
        C::Int32 | C::UInt16 => ColumnType::new("integer", Wire::Int4),
        C::Int64 | C::UInt32 => ColumnType::new("bigint", Wire::Int8),
        C::UInt64 => ColumnType::new("numeric(20,0)", Wire::Numeric),
        C::Float32 => ColumnType::new("real", Wire::Float4),
        C::Float64 => ColumnType::new("double precision", Wire::Float8),
        C::Decimal(d) => ColumnType::new(
            format!("numeric({},{})", d.precision(), d.scale()),
            Wire::Numeric,
        ),
        C::Bool => ColumnType::new("boolean", Wire::Bool),
        C::Utf8 => ColumnType::new("text", Wire::Text),
        C::Binary => ColumnType::new("bytea", Wire::Bytea),
        C::Date => ColumnType::new("date", Wire::Date),
        C::Timestamp { tz: None } => {
            ColumnType::new("timestamp without time zone", Wire::Timestamp)
        }
        C::Timestamp { tz: Some(_) } => {
            ColumnType::new("timestamp with time zone", Wire::Timestamp)
        }
        C::Uuid => ColumnType::new("uuid", Wire::Uuid),
        C::Json => ColumnType::new("jsonb", Wire::JsonText { jsonb: true }),
        C::List(item) if is_scalar(&item.ty) => {
            let element = column_type(&item.ty);
            ColumnType::new(
                format!("{}[]", element.sql),
                Wire::Array {
                    element: Box::new(element.wire),
                    oid: element_oid(&item.ty),
                },
            )
        }
        // Nested values (structs, maps, lists of either, lists of lists)
        // have no faithful column type, so they are stored as documents.
        C::List(_) | C::Struct(_) | C::Map { .. } => {
            ColumnType::new("jsonb", Wire::JsonDoc { jsonb: true })
        }
    }
}

fn is_scalar(ty: &CanonicalType) -> bool {
    !matches!(
        ty,
        CanonicalType::List(_) | CanonicalType::Struct(_) | CanonicalType::Map { .. }
    )
}

/// The type oid of a scalar array element.
fn element_oid(ty: &CanonicalType) -> u32 {
    use CanonicalType as C;
    match ty {
        C::Int8 | C::Int16 | C::UInt8 => oid::INT2,
        C::Int32 | C::UInt16 => oid::INT4,
        C::Int64 | C::UInt32 => oid::INT8,
        C::UInt64 | C::Decimal(_) => oid::NUMERIC,
        C::Float32 => oid::FLOAT4,
        C::Float64 => oid::FLOAT8,
        C::Bool => oid::BOOL,
        C::Utf8 => oid::TEXT,
        C::Binary => oid::BYTEA,
        C::Date => oid::DATE,
        C::Timestamp { tz: None } => oid::TIMESTAMP,
        C::Timestamp { tz: Some(_) } => oid::TIMESTAMPTZ,
        C::Uuid => oid::UUID,
        C::Json => oid::JSONB,
        C::List(_) | C::Struct(_) | C::Map { .. } => unreachable!("only scalars are elements"),
    }
}

/// How to send `field` to an existing column of type `actual` (as
/// `format_type()` prints it), or `None` if the column can't take it.
pub(crate) fn wire_for(field: &CanonicalField, actual: &str) -> Option<Wire> {
    let expected = column_type(&field.ty);
    if actual == expected.sql {
        return Some(expected.wire);
    }
    match expected.wire {
        Wire::Text if actual.starts_with("character varying") => Some(Wire::Text),
        Wire::JsonText { .. } if actual == "json" => Some(Wire::JsonText { jsonb: false }),
        Wire::JsonDoc { .. } if actual == "json" => Some(Wire::JsonDoc { jsonb: false }),
        _ => None,
    }
}

/// `CREATE TABLE` for `table` in `schema`. Key columns are `NOT NULL`.
pub(crate) fn create_table(schema: &str, table: &TargetTable) -> String {
    let mut lines: Vec<String> = table
        .fields
        .iter()
        .map(|f| {
            let not_null = !f.nullable || table.primary_key.contains(&f.name);
            format!(
                "{} {}{}",
                quote(&f.name),
                column_type(&f.ty).sql,
                if not_null { " NOT NULL" } else { "" }
            )
        })
        .collect();
    if !table.primary_key.is_empty() {
        lines.push(format!("PRIMARY KEY ({})", column_list(&table.primary_key)));
    }
    format!(
        "CREATE TABLE {} (\n  {}\n)",
        qualified(schema, &table.name),
        lines.join(",\n  ")
    )
}

pub(crate) fn qualified(schema: &str, table: &str) -> String {
    format!("{}.{}", quote(schema), quote(table))
}

pub(crate) fn column_list<S: AsRef<str>>(columns: &[S]) -> String {
    columns
        .iter()
        .map(|c| quote(c.as_ref()))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use ferry_core::types::DecimalSpec;

    use super::*;

    fn field(name: &str, ty: CanonicalType, nullable: bool) -> CanonicalField {
        CanonicalField::new(name, ty, nullable)
    }

    #[test]
    fn create_table_renders_types_keys_and_nullability() {
        let table = TargetTable {
            name: "order_items".into(),
            fields: vec![
                field("order_id", CanonicalType::Utf8, true),
                field("line_no", CanonicalType::Int64, false),
                field(
                    "price",
                    CanonicalType::Decimal(DecimalSpec::new(12, 2).unwrap()),
                    true,
                ),
                field(
                    "at",
                    CanonicalType::Timestamp {
                        tz: Some(Arc::from("UTC")),
                    },
                    true,
                ),
            ],
            primary_key: vec!["order_id".into(), "line_no".into()],
            foreign_keys: Vec::new(),
        };
        assert_eq!(
            create_table("app", &table),
            "CREATE TABLE \"app\".\"order_items\" (\n  \
             \"order_id\" text NOT NULL,\n  \
             \"line_no\" bigint NOT NULL,\n  \
             \"price\" numeric(12,2),\n  \
             \"at\" timestamp with time zone,\n  \
             PRIMARY KEY (\"order_id\", \"line_no\")\n)"
        );
    }

    #[test]
    fn nested_values_are_documents_and_scalar_lists_are_arrays() {
        let item = |ty| Box::new(field("item", ty, true));
        let ints = CanonicalType::List(item(CanonicalType::Int32));
        assert_eq!(column_type(&ints).sql, "integer[]");
        let nested = CanonicalType::List(item(CanonicalType::List(item(CanonicalType::Int32))));
        assert_eq!(column_type(&nested).sql, "jsonb");
        let doc = CanonicalType::Struct(vec![field("a", CanonicalType::Int32, true)]);
        assert_eq!(column_type(&doc).wire, Wire::JsonDoc { jsonb: true });
    }

    #[test]
    fn existing_columns_accept_only_types_with_the_same_values() {
        let text = field("t", CanonicalType::Utf8, true);
        assert_eq!(wire_for(&text, "text"), Some(Wire::Text));
        assert_eq!(wire_for(&text, "character varying(40)"), Some(Wire::Text));
        assert_eq!(wire_for(&text, "integer"), None);

        let json = field("j", CanonicalType::Json, true);
        assert_eq!(
            wire_for(&json, "json"),
            Some(Wire::JsonText { jsonb: false })
        );
        assert_eq!(wire_for(&json, "text"), None);

        let int = field("i", CanonicalType::Int64, true);
        assert_eq!(wire_for(&int, "integer"), None, "would truncate");
    }

    #[test]
    fn timestamptz_arrays_send_their_own_element_oid() {
        let ty = CanonicalType::List(Box::new(field(
            "item",
            CanonicalType::Timestamp {
                tz: Some(Arc::from("UTC")),
            },
            true,
        )));
        let f = field("ts", ty, true);
        let Some(Wire::Array { oid, .. }) = wire_for(&f, "timestamp with time zone[]") else {
            panic!("not an array");
        };
        assert_eq!(oid, oid::TIMESTAMPTZ);
        assert_eq!(wire_for(&f, "timestamp without time zone[]"), None);
    }
}
