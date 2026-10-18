//! The Postgres target: rows read back through the Postgres source (A1),
//! constraints and aggregates queried on the sink's pool.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use dataferry_connector_postgres::PostgresSource;
use dataferry_core::types::{CanonicalField, CanonicalType};
use dataferry_core::{
    BatchStream, Entity, FerryError, ForeignKey, Partition, PartitionId, PartitionScope, Result,
    Source, TargetTable,
};
use deadpool_postgres::Pool;

use crate::checksum::MICROS;
use crate::target::{Aggregate, DeadLetters, Scalar, Target};

/// One schema of a Postgres database, as a sink wrote it.
pub struct PgTarget {
    source: Arc<dyn Source>,
    pool: Pool,
    schema: String,
}

impl PgTarget {
    /// `url` is the sink connection's (a secret: it is only parsed, as
    /// `PostgresSource::new` does, never kept in a message). `pool` is a
    /// pool on the same database, such as the state-capable connector's
    /// (0004 §8). Does no I/O.
    pub fn new(url: &str, schema: impl Into<String>, pool: Pool) -> Result<Self> {
        let schema = schema.into();
        Ok(Self {
            source: Arc::new(PostgresSource::new(url, schema.clone())?),
            pool,
            schema,
        })
    }

    /// Reads tables back through `source`, a Postgres source on `schema`
    /// of the sink's database, such as the connector built for the
    /// connection, which applies everything the connection sets (a
    /// `password:` beside the `url`, TLS). `pool` is as for
    /// [`new`](PgTarget::new). Does no I/O.
    pub fn from_source(source: Arc<dyn Source>, schema: impl Into<String>, pool: Pool) -> Self {
        Self {
            source,
            pool,
            schema: schema.into(),
        }
    }

    fn table(&self, table: &str) -> String {
        format!("{}.{}", quote(&self.schema), quote(table))
    }

    async fn count(&self, sql: &str) -> Result<u64> {
        let client = self.pool.get().await.map_err(|e| pg_error(e.to_string()))?;
        let row = client
            .query_one(sql, &[])
            .await
            .map_err(|e| pg_error(db_message(&e)))?;
        let n: i64 = row.get(0);
        Ok(n as u64)
    }
}

fn quote(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

fn pg_error(message: String) -> FerryError {
    FerryError::Connector {
        connector: "postgres".into(),
        message,
    }
}

fn db_message(e: &tokio_postgres::Error) -> String {
    e.as_db_error().map_or(e.to_string(), |db| db.to_string())
}

/// The entity the source reads `table` back as. The source orders rows by
/// the primary key; a table without one is ordered by its first column
/// that can be compared (`json` can't), since order doesn't change a
/// checksum.
fn read_back_entity(table: &TargetTable) -> Result<Entity> {
    let key = if table.primary_key.is_empty() {
        let orderable = table
            .fields
            .iter()
            .find(|f| match &f.ty {
                CanonicalType::Json => false,
                CanonicalType::List(item) => item.ty != CanonicalType::Json,
                _ => true,
            })
            .ok_or_else(|| {
                pg_error(format!(
                    "`{}` has no primary key and no column to order it by",
                    table.name
                ))
            })?;
        vec![orderable.name.clone()]
    } else {
        table.primary_key.clone()
    };
    // The Postgres sink stores nested values (structs, maps, lists of
    // them) as `jsonb` documents: read them back as the JSON text they
    // are, which the checksum decodes by the column's own type.
    let fields = table
        .fields
        .iter()
        .map(|f| match document(&f.ty) {
            true => CanonicalField::new(f.name.clone(), CanonicalType::Json, f.nullable),
            false => f.clone(),
        })
        .collect();
    Entity::new(table.name.clone(), fields, key)
}

/// Whether the Postgres sink stores a value of `ty` as a JSON document.
fn document(ty: &CanonicalType) -> bool {
    let nested = |ty: &CanonicalType| {
        matches!(
            ty,
            CanonicalType::List(_) | CanonicalType::Struct(_) | CanonicalType::Map { .. }
        )
    };
    match ty {
        CanonicalType::List(item) => nested(&item.ty),
        other => nested(other),
    }
}

#[async_trait]
impl Target for PgTarget {
    fn describe(&self) -> String {
        format!("postgres, schema {}", self.schema)
    }

    async fn timestamp_digits(&self, table: &TargetTable) -> Result<BTreeMap<String, u8>> {
        let client = self.pool.get().await.map_err(|e| pg_error(e.to_string()))?;
        let rows = client
            .query(
                "SELECT column_name::text, datetime_precision::int4 \
                 FROM information_schema.columns \
                 WHERE table_schema = $1 AND table_name = $2 \
                   AND data_type IN ('timestamp without time zone', 'timestamp with time zone')",
                &[&self.schema, &table.name],
            )
            .await
            .map_err(|e| pg_error(db_message(&e)))?;
        let mut digits = BTreeMap::new();
        for row in rows {
            let column: String = row.get(0);
            let precision: Option<i32> = row.get(1);
            let declared = table
                .fields
                .iter()
                .any(|f| f.name == column && matches!(f.ty, CanonicalType::Timestamp { .. }));
            if let Some(p) = precision.and_then(|p| u8::try_from(p).ok())
                && declared
                && p < MICROS
            {
                digits.insert(column, p);
            }
        }
        Ok(digits)
    }

    async fn read_back(&self, table: &TargetTable) -> Result<BatchStream> {
        let entity = read_back_entity(table)?;
        let whole = Partition {
            id: PartitionId(format!("{}-read-back", table.name)),
            entity: table.name.clone(),
            scope: PartitionScope::Whole,
        };
        self.source.read(&entity, &whole).await
    }

    async fn null_count(&self, table: &str, column: &str) -> Result<u64> {
        self.count(&format!(
            "SELECT count(*) FROM {} WHERE {} IS NULL",
            self.table(table),
            quote(column)
        ))
        .await
    }

    async fn duplicate_keys(&self, table: &str, columns: &[String]) -> Result<u64> {
        let columns = columns
            .iter()
            .map(|c| quote(c))
            .collect::<Vec<_>>()
            .join(", ");
        self.count(&format!(
            "SELECT count(*) FROM (SELECT 1 FROM {} GROUP BY {columns} HAVING count(*) > 1) d",
            self.table(table)
        ))
        .await
    }

    async fn orphans(&self, table: &str, fk: &ForeignKey) -> Result<u64> {
        let present = fk
            .columns
            .iter()
            .map(|c| format!("c.{} IS NOT NULL", quote(c)))
            .collect::<Vec<_>>()
            .join(" AND ");
        let matches = fk
            .columns
            .iter()
            .zip(&fk.references_columns)
            .map(|(c, p)| format!("p.{} = c.{}", quote(p), quote(c)))
            .collect::<Vec<_>>()
            .join(" AND ");
        self.count(&format!(
            "SELECT count(*) FROM {} c WHERE {present} \
             AND NOT EXISTS (SELECT 1 FROM {} p WHERE {matches})",
            self.table(table),
            self.table(&fk.references_table)
        ))
        .await
    }

    async fn dead_letters(&self, run: &str) -> Result<Vec<DeadLetters>> {
        let client = self.pool.get().await.map_err(|e| pg_error(e.to_string()))?;
        let table = self.table(dataferry_runtime::DLQ_TABLE);
        let exists: Option<String> = client
            .query_one("SELECT to_regclass($1)::text", &[&table])
            .await
            .map_err(|e| pg_error(db_message(&e)))?
            .get(0);
        if exists.is_none() {
            return Ok(Vec::new());
        }
        let rows = client
            .query(
                &format!(
                    "SELECT flow, target, error, count(*), min(source_id) FROM {table} \
                     WHERE run = $1 GROUP BY flow, target, error \
                     ORDER BY count(*) DESC, flow, target, error"
                ),
                &[&run],
            )
            .await
            .map_err(|e| pg_error(db_message(&e)))?;
        Ok(rows
            .iter()
            .map(|r| DeadLetters {
                flow: r.get(0),
                target: r.get(1),
                error: r.get(2),
                count: r.get::<_, i64>(3) as u64,
                example: r.get(4),
            })
            .collect())
    }

    async fn aggregate(
        &self,
        table: &str,
        aggregate: Aggregate,
        column: Option<&str>,
    ) -> Result<Scalar> {
        let column = column.map(quote);
        let expr = match (aggregate, column) {
            (Aggregate::Count, None) => "count(*)".to_owned(),
            (Aggregate::Count, Some(c)) => format!("count({c})"),
            (Aggregate::CountDistinct, Some(c)) => format!("count(DISTINCT {c})"),
            (Aggregate::Sum, Some(c)) => format!("sum({c})"),
            (Aggregate::Min, Some(c)) => format!("min({c})"),
            (Aggregate::Max, Some(c)) => format!("max({c})"),
            (other, None) => {
                return Err(pg_error(format!("{other:?} needs a column")));
            }
        };
        let client = self.pool.get().await.map_err(|e| pg_error(e.to_string()))?;
        let row = client
            .query_one(
                &format!("SELECT ({expr})::text FROM {}", self.table(table)),
                &[],
            )
            .await
            .map_err(|e| pg_error(db_message(&e)))?;
        let text: Option<String> = row.get(0);
        Ok(text.map_or(Scalar::Null, |t| Scalar::parse(&t)))
    }
}
