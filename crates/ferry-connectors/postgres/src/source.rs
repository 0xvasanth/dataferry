//! Reading Postgres tables as 0xferry entities.
//!
//! Rows stream through extended-protocol queries, which return binary
//! values and take partition bounds as bind parameters. `COPY ... TO STDOUT`
//! would be faster but can't take parameters, so bounds would have to be
//! spliced into SQL; that trade can be revisited once there are benchmarks.

use std::collections::BTreeMap;
use std::error::Error;

use arrow_array::RecordBatch;
use async_trait::async_trait;
use ferry_core::types::{CanonicalField, CanonicalType};
use ferry_core::{
    BatchStream, Entity, FerryError, KeyRange, KeyValue, Partition, PartitionHint, PartitionId,
    PartitionScope, Result, Source, SourceCaps,
};
use futures::{StreamExt, TryStreamExt, stream};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_postgres::types::{FromSql, ToSql, Type};
use tokio_postgres::{Client, Connection, NoTls};

use crate::columns::ColumnBuilder;
use crate::pg_type::{PgType, to_canonical};
use crate::tls::{self, Tls};

/// Reads the tables of one Postgres schema.
///
/// Tables with a single-column integer, text or uuid primary key are split
/// into key ranges; text keys are compared with `COLLATE "C"` so range
/// bounds follow byte order. Other tables are read as one partition.
pub struct PostgresSource {
    config: tokio_postgres::Config,
    tls: Tls,
    schema: String,
    batch_rows: usize,
}

impl PostgresSource {
    pub const DEFAULT_BATCH_ROWS: usize = 8192;

    /// `url` is a libpq-style connection string or URL. Its `sslmode` and
    /// `sslrootcert` follow the TLS policy in [`crate::tls`]. Does no I/O:
    /// the first trait call connects.
    pub fn new(url: &str, schema: impl Into<String>) -> Result<Self> {
        let (config, tls) = tls::parse(url)?;
        Ok(Self::from_parts(config, tls, schema))
    }

    /// From a connection string already parsed under the TLS policy.
    pub(crate) fn from_parts(
        config: tokio_postgres::Config,
        tls: Tls,
        schema: impl Into<String>,
    ) -> Self {
        Self {
            config,
            tls,
            schema: schema.into(),
            batch_rows: Self::DEFAULT_BATCH_ROWS,
        }
    }

    /// Maximum rows per emitted batch.
    pub fn with_batch_rows(mut self, n: usize) -> Self {
        self.batch_rows = n.max(1);
        self
    }

    async fn connect(&self) -> Result<Client> {
        match self.tls.connector()? {
            Some(tls) => spawn_connection(self.config.connect(tls).await),
            None => spawn_connection(self.config.connect(NoTls).await),
        }
    }

    fn table(&self, entity: &Entity) -> String {
        format!("{}.{}", quote(&self.schema), quote(entity.name()))
    }
}

/// Drives a new connection in the background and returns its client.
fn spawn_connection<S, T>(
    connected: std::result::Result<(Client, Connection<S, T>), tokio_postgres::Error>,
) -> Result<Client>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (client, connection) = connected.map_err(pg_error)?;
    // Connection errors also surface on the client's next request.
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(client)
}

pub(crate) fn connector_error(message: impl Into<String>) -> FerryError {
    FerryError::Connector {
        connector: "postgres".into(),
        message: message.into(),
    }
}

pub(crate) fn pg_error(e: tokio_postgres::Error) -> FerryError {
    if let Some(db) = e.as_db_error() {
        return connector_error(db.to_string());
    }
    connector_error(with_causes(&e))
}

/// An error's message followed by its causes. tokio-postgres keeps the
/// reason (say, why a certificate was refused) in the source chain, not in
/// its own message.
pub(crate) fn with_causes(e: &dyn Error) -> String {
    let mut message = e.to_string();
    let mut source = e.source();
    while let Some(cause) = source {
        message += &format!(": {cause}");
        source = cause.source();
    }
    message
}

/// Quotes an identifier.
pub(crate) fn quote(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

const COLUMNS_SQL: &str = r#"
SELECT c.relname::text, a.attname::text, a.atttypid, a.atttypmod, a.attnotnull,
       CASE WHEN t.typcategory = 'A' THEN t.typelem END
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
JOIN pg_type t ON t.oid = a.atttypid
WHERE n.nspname = $1 AND c.relkind IN ('r', 'p') AND NOT c.relispartition
ORDER BY c.relname, a.attnum
"#;

const PRIMARY_KEYS_SQL: &str = r#"
SELECT c.relname::text, a.attname::text
FROM pg_index i
JOIN pg_class c ON c.oid = i.indrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
CROSS JOIN LATERAL unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord)
JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = k.attnum
WHERE i.indisprimary AND n.nspname = $1
ORDER BY c.relname, k.ord
"#;

/// The kinds of single-column keys that can be range-partitioned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KeyKind {
    Int,
    Text,
    Uuid,
}

impl KeyKind {
    fn of(ty: &CanonicalType) -> Option<Self> {
        match ty {
            CanonicalType::Int16 | CanonicalType::Int32 | CanonicalType::Int64 => Some(Self::Int),
            CanonicalType::Utf8 => Some(Self::Text),
            CanonicalType::Uuid => Some(Self::Uuid),
            _ => None,
        }
    }

    fn accepts(self, value: &KeyValue) -> bool {
        match (self, value) {
            (Self::Int, KeyValue::Int(_)) | (Self::Text, KeyValue::Str(_)) => true,
            (Self::Uuid, KeyValue::Bytes(b)) => b.len() == 16,
            _ => false,
        }
    }

    /// The column as it is compared, so that ordering is byte order.
    fn ordered(self, column: &str) -> String {
        match self {
            Self::Text => format!("{} COLLATE \"C\"", quote(column)),
            Self::Int | Self::Uuid => quote(column),
        }
    }

    /// A placeholder for a bound of this kind, cast so the comparison stays
    /// index-friendly whatever the column's exact type.
    fn placeholder(self, n: usize) -> String {
        match self {
            Self::Int => format!("${n}::int8"),
            Self::Text => format!("${n}::text"),
            Self::Uuid => format!("${n}::text::uuid"),
        }
    }

    fn param(self, value: &KeyValue) -> Result<Box<dyn ToSql + Sync + Send>> {
        Ok(match (self, value) {
            (Self::Int, KeyValue::Int(v)) => Box::new(*v),
            (Self::Text, KeyValue::Str(v)) => Box::new(v.clone()),
            (Self::Uuid, KeyValue::Bytes(b)) if b.len() == 16 => Box::new(uuid_string(b)),
            _ => {
                return Err(connector_error(format!(
                    "key bound {value:?} does not match a {self:?} key"
                )));
            }
        })
    }
}

fn uuid_string(b: &[u8]) -> String {
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

/// Any column value as its raw binary bytes.
struct Raw<'a>(&'a [u8]);

impl<'a> FromSql<'a> for Raw<'a> {
    fn from_sql(
        _: &Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(Raw(raw))
    }

    fn accepts(_: &Type) -> bool {
        true
    }
}

/// The select-list entry for a field. `json`/`jsonb` are read as text
/// because their binary forms differ.
fn select_item(field: &CanonicalField) -> String {
    let column = quote(&field.name);
    match &field.ty {
        CanonicalType::Json => format!("{column}::text"),
        CanonicalType::List(item) if item.ty == CanonicalType::Json => format!("{column}::text[]"),
        _ => column,
    }
}

fn partitions_for(entity: &Entity, ranges: Vec<KeyRange>) -> Vec<Partition> {
    ranges
        .into_iter()
        .enumerate()
        .map(|(i, range)| Partition {
            id: PartitionId(format!("{}-{i}", entity.name())),
            entity: entity.name().to_owned(),
            scope: PartitionScope::KeyRange(range),
        })
        .collect()
}

#[async_trait]
impl Source for PostgresSource {
    async fn discover(&self) -> Result<Vec<Entity>> {
        let client = self.connect().await?;
        let mut columns: BTreeMap<String, Vec<(String, PgType, bool)>> = BTreeMap::new();
        for row in client
            .query(COLUMNS_SQL, &[&self.schema])
            .await
            .map_err(pg_error)?
        {
            let pg = PgType {
                oid: row.get(2),
                typmod: row.get(3),
                element: row.get(5),
            };
            columns
                .entry(row.get(0))
                .or_default()
                .push((row.get(1), pg, row.get(4)));
        }
        let mut primary_keys: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for row in client
            .query(PRIMARY_KEYS_SQL, &[&self.schema])
            .await
            .map_err(pg_error)?
        {
            primary_keys.entry(row.get(0)).or_default().push(row.get(1));
        }

        let mut entities = Vec::new();
        let mut problems = Vec::new();
        for (table, cols) in columns {
            let mut fields = Vec::new();
            for (name, pg, not_null) in cols {
                match to_canonical(pg) {
                    Ok(ty) => fields.push(CanonicalField::new(name, ty, !not_null)),
                    Err(reason) => problems.push(format!("{table}.{name}: {reason}")),
                }
            }
            let Some(pk) = primary_keys.remove(&table) else {
                problems.push(format!(
                    "{table}: has no primary key, which is needed to partition and re-run it safely"
                ));
                continue;
            };
            match Entity::new(table.clone(), fields, pk) {
                Ok(e) => entities.push(e),
                Err(e) => problems.push(format!("{table}: {e}")),
            }
        }
        if !problems.is_empty() {
            return Err(connector_error(format!(
                "schema `{}` has tables that can't be read:\n  - {}",
                self.schema,
                problems.join("\n  - ")
            )));
        }
        Ok(entities)
    }

    async fn plan_partitions(
        &self,
        entity: &Entity,
        hint: &PartitionHint,
    ) -> Result<Vec<Partition>> {
        if let Some(cp) = &hint.co_partition {
            let unsupported = |reason: String| FerryError::CoPartitionUnsupported {
                column: cp.column().to_owned(),
                reason,
            };
            let field = entity
                .field(cp.column())
                .ok_or_else(|| unsupported(format!("`{}` has no such column", entity.name())))?;
            let kind = KeyKind::of(&field.ty).ok_or_else(|| {
                unsupported(format!("{:?} columns can't be range-partitioned", field.ty))
            })?;
            if let Some(bad) = cp.boundaries().iter().find(|b| !kind.accepts(b)) {
                return Err(unsupported(format!(
                    "boundary {bad:?} can't be compared with a {kind:?} column"
                )));
            }
            return Ok(partitions_for(entity, cp.ranges()));
        }

        let whole = || {
            vec![Partition {
                id: PartitionId(format!("{}-0", entity.name())),
                entity: entity.name().to_owned(),
                scope: PartitionScope::Whole,
            }]
        };
        let [pk] = entity.primary_key() else {
            return Ok(whole());
        };
        let Some(kind) = entity.field(pk).and_then(|f| KeyKind::of(&f.ty)) else {
            return Ok(whole());
        };

        let key = match kind {
            KeyKind::Int => "k::int8",
            KeyKind::Text => "k::text",
            KeyKind::Uuid => "uuid_send(k)",
        };
        let sql = format!(
            "SELECT {key} FROM (SELECT {col} AS k, row_number() OVER (ORDER BY {ordered}) AS rn \
             FROM {table}) x WHERE rn > 1 AND (rn - 1) % $1 = 0 ORDER BY rn",
            col = quote(pk),
            ordered = kind.ordered(pk),
            table = self.table(entity),
        );
        let target = i64::try_from(hint.target_rows.max(1)).unwrap_or(i64::MAX);
        let client = self.connect().await?;
        let rows = client.query(&sql, &[&target]).await.map_err(pg_error)?;
        let bounds: Vec<KeyValue> = rows
            .iter()
            .map(|r| match kind {
                KeyKind::Int => KeyValue::Int(r.get(0)),
                KeyKind::Text => KeyValue::Str(r.get(0)),
                KeyKind::Uuid => KeyValue::Bytes(r.get(0)),
            })
            .collect();

        let lowers = std::iter::once(None).chain(bounds.iter().cloned().map(Some));
        let uppers = bounds
            .iter()
            .cloned()
            .map(Some)
            .chain(std::iter::once(None));
        let ranges = lowers
            .zip(uppers)
            .map(|(lo, hi)| KeyRange::new(pk.clone(), lo, hi))
            .collect::<Result<Vec<_>>>()?;
        Ok(partitions_for(entity, ranges))
    }

    async fn read(&self, entity: &Entity, partition: &Partition) -> Result<BatchStream> {
        let mut sql = format!(
            "SELECT {} FROM {}",
            entity
                .fields()
                .iter()
                .map(select_item)
                .collect::<Vec<_>>()
                .join(", "),
            self.table(entity)
        );
        let mut params: Vec<Box<dyn ToSql + Sync + Send>> = Vec::new();
        if let PartitionScope::KeyRange(range) = &partition.scope {
            let kind = entity
                .field(range.column())
                .and_then(|f| KeyKind::of(&f.ty))
                .ok_or_else(|| {
                    connector_error(format!(
                        "`{}` can't be range-partitioned on `{}`",
                        entity.name(),
                        range.column()
                    ))
                })?;
            let column = kind.ordered(range.column());
            let mut predicates = Vec::new();
            if let Some(lo) = range.lower() {
                params.push(kind.param(lo)?);
                predicates.push(format!("{column} >= {}", kind.placeholder(params.len())));
            }
            if let Some(hi) = range.upper() {
                params.push(kind.param(hi)?);
                predicates.push(format!("{column} < {}", kind.placeholder(params.len())));
            }
            if !predicates.is_empty() {
                sql += &format!(" WHERE {}", predicates.join(" AND "));
            }
        }
        sql += &format!(
            " ORDER BY {}",
            entity
                .primary_key()
                .iter()
                .map(|c| quote(c))
                .collect::<Vec<_>>()
                .join(", ")
        );

        let client = self.connect().await?;
        let rows = client
            .query_raw(
                &sql,
                params.iter().map(|p| p.as_ref() as &(dyn ToSql + Sync)),
            )
            .await
            .map_err(pg_error)?;

        struct State {
            // Keeps the connection open while rows stream.
            _client: Client,
            rows: std::pin::Pin<Box<tokio_postgres::RowStream>>,
            done: bool,
        }
        let schema = entity.arrow_schema();
        let fields = entity.fields().to_vec();
        let table = entity.name().to_owned();
        let batch_rows = self.batch_rows;
        let state = State {
            _client: client,
            rows: Box::pin(rows),
            done: false,
        };
        Ok(stream::try_unfold(state, move |mut st| {
            let schema = schema.clone();
            let fields = fields.clone();
            let table = table.clone();
            async move {
                if st.done {
                    return Ok(None);
                }
                let mut builders = fields
                    .iter()
                    .map(ColumnBuilder::new)
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(connector_error)?;
                let mut n = 0;
                while n < batch_rows {
                    let Some(row) = st.rows.try_next().await.map_err(pg_error)? else {
                        st.done = true;
                        break;
                    };
                    for (i, builder) in builders.iter_mut().enumerate() {
                        let raw: Option<Raw> = row.try_get(i).map_err(pg_error)?;
                        builder.append(raw.map(|r| r.0)).map_err(|e| {
                            connector_error(format!("{table}.{}: {e}", fields[i].name))
                        })?;
                    }
                    n += 1;
                }
                if n == 0 {
                    return Ok(None);
                }
                let columns = builders.iter_mut().map(ColumnBuilder::finish).collect();
                let batch = RecordBatch::try_new(schema, columns)
                    .map_err(|e| connector_error(format!("{table}: {e}")))?;
                Ok(Some((batch, st)))
            }
        })
        .boxed())
    }

    async fn capabilities(&self) -> Result<SourceCaps> {
        Ok(SourceCaps {
            range_scan: true,
            predicate_pushdown: false,
            consistent_snapshot: false,
        })
    }
}
