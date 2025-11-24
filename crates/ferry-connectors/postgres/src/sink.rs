//! Writing partitions into Postgres tables.
//!
//! One [`SinkTxn`] is one Postgres transaction on one pooled connection,
//! opened with an explicit `BEGIN` and ended by `COMMIT` or `ROLLBACK`. Every
//! target table of the partition is written inside it.
//!
//! # How a batch is written
//!
//! Rows travel as binary `COPY` (see `crate::encode`) into a temporary
//! stage, and from there into the target:
//!
//! - **Insert**: the target rows with the same keys as the stage are
//!   deleted, then the stage is inserted. A re-run of a partition that
//!   already committed (its done-record was lost, or it was retried)
//!   produces the same keys, so it replaces its own rows instead of
//!   duplicating them. That is what `DeletePartitionThenInsert` means here,
//!   and it is why that strategy needs every target to have a key.
//! - **Upsert**: `INSERT ... ON CONFLICT (key) DO UPDATE`.
//! - **A keyless table**, which only `TxnWithState` accepts, is copied
//!   straight into the target. Its partitions can't replace earlier rows, so
//!   exactly-once rests on the ledger never recording a partition twice.
//!
//! On the staged path a parent's children are replaced, not merged: before
//! a parent batch lands, the rows of every table that references it (by a
//! [`ForeignKey`](ferry_core::ForeignKey) of the same target schema), and
//! their own children, are deleted for the parents in the batch. The child
//! batches that follow write the current set, so an item dropped from a
//! source document leaves no stale row behind. Children of parents that are
//! not in the batch are untouched.
//!
//! # Loading into tables that already exist
//!
//! `prepare` drops the foreign keys of target tables that already exist,
//! and with `drop_indexes` their secondary indexes, recording each so
//! [`Finalizer::finalize`] recreates it once the flow is loaded; see
//! `crate::deferred` (A6). Finalize also creates the foreign keys the
//! target schema declares, which tables are created without.
//!
//! # Exactly-once
//!
//! A sink built with a [`Ledger`] (through `StateCapable`) also offers
//! `TxnWithState`: its commit calls `ledger.record_done` on the
//! transaction's own connection after the last write and before `COMMIT`,
//! so the rows and the done-record commit together or not at all. The
//! ledger refuses a superseded lease, which is the epoch fencing check.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arrow_array::RecordBatch;
use arrow_schema::Field;
use async_trait::async_trait;
use bytes::Bytes;
use deadpool_postgres::{Object, Pool};
use ferry_core::types::CanonicalField;
use ferry_core::{
    FerryError, IdempotencyStrategy, Lease, Partition, Result, Sink, SinkCaps, SinkTxn,
    TargetSchema, TargetTable, WriteMode, WriteSummary,
};
use ferry_registry::{Finalized, Finalizer, Ledger};
use futures::SinkExt;
use tokio_postgres::Client;

use crate::ddl::{self, column_list, qualified};
use crate::deferred::{self, LoadOptions};
use crate::encode::{self, Wire};
use crate::pool;
use crate::source::{connector_error, pg_error, quote};
use crate::tls;

/// The temporary table a staged batch is copied into. It lives in the
/// transaction's own `pg_temp` schema and is dropped after each batch.
const STAGE: &str = "pg_temp.\"_ferry_stage\"";

/// Writes into the tables of one Postgres schema.
pub struct PostgresSink {
    pool: Pool,
    schema: String,
    ledger: Option<Arc<dyn Ledger<Client>>>,
    load: LoadOptions,
    prepared: Arc<Mutex<HashMap<String, Arc<Prepared>>>>,
}

/// A target table after `prepare`: how to send each column, and which
/// tables reference it.
struct Prepared {
    table: TargetTable,
    mode: WriteMode,
    fields: Vec<Field>,
    wires: Vec<Wire>,
    children: Vec<Child>,
}

/// A table whose `columns` reference `references` of a parent table.
#[derive(Clone)]
struct Child {
    table: String,
    columns: Vec<String>,
    references: Vec<String>,
}

impl PostgresSink {
    /// `url` is a libpq-style connection string or URL, under the same TLS
    /// policy as the source. Does no I/O: the first trait call connects.
    pub fn new(url: &str, schema: impl Into<String>) -> Result<Self> {
        let (config, tls) = tls::parse(url)?;
        Ok(Self::from_pool(pool::build(config, tls)?, schema, None))
    }

    /// What `prepare` drops from target tables that already exist, for
    /// finalize to put back. By default foreign keys are dropped and
    /// indexes kept.
    pub fn with_load(mut self, load: LoadOptions) -> Self {
        self.load = load;
        self
    }

    pub(crate) fn from_pool(
        pool: Pool,
        schema: impl Into<String>,
        ledger: Option<Arc<dyn Ledger<Client>>>,
    ) -> Self {
        Self {
            pool,
            schema: schema.into(),
            ledger,
            load: LoadOptions::default(),
            prepared: Arc::default(),
        }
    }

    fn strategies(&self) -> Vec<IdempotencyStrategy> {
        let mut strategies = Vec::new();
        if self.ledger.is_some() {
            strategies.push(IdempotencyStrategy::TxnWithState);
        }
        strategies.push(IdempotencyStrategy::DeletePartitionThenInsert);
        strategies.push(IdempotencyStrategy::UpsertByKey);
        strategies
    }

    /// The problems that stop `table` from being written in `mode`, before
    /// looking at the database.
    fn key_problem(&self, table: &TargetTable, mode: WriteMode) -> Option<String> {
        if !table.primary_key.is_empty() {
            return None;
        }
        match mode {
            WriteMode::Upsert => Some("upsert needs a key to match existing rows on".into()),
            // Without a ledger the only insert strategy is
            // DeletePartitionThenInsert, which finds a partition's earlier
            // rows by their keys.
            WriteMode::Insert if self.ledger.is_none() => Some(
                "has no key, so re-running a partition can't replace the rows it wrote before; \
                 declare a key, or keep the state store on this connection"
                    .into(),
            ),
            WriteMode::Insert => None,
        }
    }
}

const TABLE_EXISTS_SQL: &str = r#"
SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind IN ('r', 'p')
"#;

const COLUMNS_SQL: &str = r#"
SELECT a.attname::text, format_type(a.atttypid, a.atttypmod), a.attnotnull,
       a.atthasdef OR a.attidentity <> ''
FROM pg_attribute a
JOIN pg_class c ON c.oid = a.attrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = $1 AND c.relname = $2 AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY a.attnum
"#;

/// Column-name sets of the unique indexes (the primary key included).
const UNIQUE_SQL: &str = r#"
SELECT array_agg(a.attname::text ORDER BY a.attname)
FROM pg_index i
JOIN pg_class c ON c.oid = i.indrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
CROSS JOIN LATERAL unnest(i.indkey::int2[]) AS k(attnum)
JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = k.attnum
WHERE n.nspname = $1 AND c.relname = $2 AND i.indisunique AND i.indpred IS NULL
GROUP BY i.indexrelid
"#;

#[async_trait]
impl Sink for PostgresSink {
    /// Creates the schema and every missing table, and checks that tables
    /// which already exist can take the rows. Every problem is reported at
    /// once. Nothing is created unless every table is fine.
    async fn prepare(&self, target: &TargetSchema, mode: WriteMode) -> Result<()> {
        let mut problems = Vec::new();
        for t in target.tables() {
            if t.name == crate::deferred::DROPPED {
                problems.push(format!(
                    "`{}`: this name is ferry's record of dropped indexes and keys \
                     and can't be a target",
                    t.name
                ));
                continue;
            }
            if let Some(p) = self.key_problem(t, mode) {
                problems.push(format!("`{}`: {p}", t.name));
            }
        }
        if !problems.is_empty() {
            return Err(prepare_error(&self.schema, problems));
        }

        let mut client = pool::get(&self.pool).await?;
        let txn = client.transaction().await.map_err(pg_error)?;
        txn.batch_execute(&format!(
            "CREATE SCHEMA IF NOT EXISTS {}",
            quote(&self.schema)
        ))
        .await
        .map_err(pg_error)?;

        let mut prepared = Vec::new();
        let mut existing = Vec::new();
        for t in target.tables() {
            let exists = !txn
                .query(TABLE_EXISTS_SQL, &[&self.schema, &t.name])
                .await
                .map_err(pg_error)?
                .is_empty();
            if exists {
                existing.push(t.name.as_str());
            } else {
                txn.batch_execute(&ddl::create_table(&self.schema, t))
                    .await
                    .map_err(pg_error)?;
            }
            let columns: Vec<(String, String, bool, bool)> = txn
                .query(COLUMNS_SQL, &[&self.schema, &t.name])
                .await
                .map_err(pg_error)?
                .iter()
                .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3)))
                .collect();

            let mut wires = Vec::new();
            for f in &t.fields {
                match columns.iter().find(|c| c.0 == f.name) {
                    None => problems.push(format!("`{}`: column `{}` is missing", t.name, f.name)),
                    Some((_, actual, ..)) => match ddl::wire_for(f, actual) {
                        Some(wire) => wires.push(wire),
                        None => problems.push(format!(
                            "`{}`: column `{}` is {actual}, but the target needs {}",
                            t.name,
                            f.name,
                            ddl::column_type(&f.ty).sql
                        )),
                    },
                }
            }
            for (name, _, not_null, has_default) in &columns {
                if *not_null && !has_default && !t.fields.iter().any(|f| &f.name == name) {
                    problems.push(format!(
                        "`{}`: column `{name}` is NOT NULL with no default, and the target \
                         doesn't write it",
                        t.name
                    ));
                }
            }
            if mode == WriteMode::Upsert && !t.primary_key.is_empty() {
                let mut key = t.primary_key.clone();
                key.sort();
                let unique: Vec<Vec<String>> = txn
                    .query(UNIQUE_SQL, &[&self.schema, &t.name])
                    .await
                    .map_err(pg_error)?
                    .iter()
                    .map(|r| r.get(0))
                    .collect();
                if !unique.contains(&key) {
                    problems.push(format!(
                        "`{}`: upsert needs a primary key or unique index on exactly ({})",
                        t.name,
                        t.primary_key.join(", ")
                    ));
                }
            }
            prepared.push((t, wires));
        }
        if !problems.is_empty() {
            // Dropping the transaction rolls back any table created above.
            return Err(prepare_error(&self.schema, problems));
        }
        // With the tables, so a crash leaves each object or its record.
        deferred::drop_for_load(&txn, &self.schema, &existing, self.load).await?;
        txn.commit().await.map_err(pg_error)?;

        let mut map = self.prepared.lock().unwrap_or_else(|e| e.into_inner());
        for (t, wires) in prepared {
            let children = target
                .tables()
                .iter()
                .flat_map(|child| {
                    child
                        .foreign_keys
                        .iter()
                        .filter(|fk| fk.references_table == t.name && child.name != t.name)
                        .map(|fk| Child {
                            table: child.name.clone(),
                            columns: fk.columns.clone(),
                            references: fk.references_columns.clone(),
                        })
                })
                .collect();
            map.insert(
                t.name.clone(),
                Arc::new(Prepared {
                    table: t.clone(),
                    mode,
                    fields: t.fields.iter().map(CanonicalField::to_arrow).collect(),
                    wires,
                    children,
                }),
            );
        }
        Ok(())
    }

    async fn begin(
        &self,
        _partition: &Partition,
        lease: &Lease,
        strategy: IdempotencyStrategy,
    ) -> Result<Box<dyn SinkTxn>> {
        if !self.strategies().contains(&strategy) {
            return Err(connector_error(format!(
                "this sink can't write under {strategy:?}; it offers {:?}",
                self.strategies()
            )));
        }
        let client = pool::get(&self.pool).await?;
        client.batch_execute("BEGIN").await.map_err(pg_error)?;
        Ok(Box::new(PgTxn {
            client: Some(client),
            schema: self.schema.clone(),
            prepared: Arc::clone(&self.prepared),
            ledger: match strategy {
                IdempotencyStrategy::TxnWithState => self.ledger.clone(),
                _ => None,
            },
            lease: lease.clone(),
            strategy,
            summary: WriteSummary::default(),
        }))
    }

    async fn capabilities(&self) -> Result<SinkCaps> {
        Ok(SinkCaps {
            strategies: self.strategies(),
            bulk_copy: true,
        })
    }
}

#[async_trait]
impl Finalizer for PostgresSink {
    async fn finalize(&self, target: &TargetSchema) -> Result<Finalized> {
        let mut client = pool::get(&self.pool).await?;
        deferred::restore(&mut client, &self.schema, target).await
    }
}

fn prepare_error(schema: &str, problems: Vec<String>) -> FerryError {
    connector_error(format!(
        "schema `{schema}` can't take the target tables:\n  - {}",
        problems.join("\n  - ")
    ))
}

/// One partition's open transaction.
struct PgTxn {
    /// `None` once committed or rolled back.
    client: Option<Object>,
    schema: String,
    prepared: Arc<Mutex<HashMap<String, Arc<Prepared>>>>,
    /// Set only under `TxnWithState`.
    ledger: Option<Arc<dyn Ledger<Client>>>,
    lease: Lease,
    strategy: IdempotencyStrategy,
    summary: WriteSummary,
}

impl PgTxn {
    fn client(&mut self) -> Result<&mut Client> {
        match self.client.as_mut() {
            Some(object) => Ok(object),
            None => Err(connector_error("the transaction has already ended")),
        }
    }

    fn prepared(&self, target: &str) -> Result<Arc<Prepared>> {
        let map = self.prepared.lock().unwrap_or_else(|e| e.into_inner());
        map.get(target).cloned().ok_or_else(|| {
            connector_error(format!("target `{target}` was not prepared on this sink"))
        })
    }

    async fn copy(&mut self, into: &str, columns: &str, data: Vec<u8>) -> Result<u64> {
        let sql = format!("COPY {into} ({columns}) FROM STDIN (FORMAT binary)");
        let client = self.client()?;
        let sink = client.copy_in::<_, Bytes>(&sql).await.map_err(pg_error)?;
        futures::pin_mut!(sink);
        sink.send(Bytes::from(data)).await.map_err(pg_error)?;
        sink.finish().await.map_err(pg_error)
    }

    /// Deletes, deepest first, every row that descends from the parent rows
    /// `from` selects (a `FROM ... [WHERE ...]` clause over `parent`).
    fn descendant_deletes(
        &self,
        parent: &Prepared,
        from: &str,
        path: &mut Vec<String>,
        out: &mut Vec<String>,
    ) -> Result<()> {
        path.push(parent.table.name.clone());
        for child in &parent.children {
            if path.contains(&child.table) {
                continue;
            }
            let table = qualified(&self.schema, &child.table);
            let condition = format!(
                "({}) IN (SELECT {} {from})",
                column_list(&child.columns),
                column_list(&child.references)
            );
            let child_from = format!("FROM {table} WHERE {condition}");
            let prepared = self.prepared(&child.table)?;
            self.descendant_deletes(&prepared, &child_from, path, out)?;
            out.push(format!("DELETE FROM {table} WHERE {condition}"));
        }
        path.pop();
        Ok(())
    }

    /// Rolls back. If that fails the connection's state is unknown, so it is
    /// closed instead of going back to the pool.
    async fn rollback(&mut self) -> Result<()> {
        let Some(object) = self.client.take() else {
            return Ok(());
        };
        match object.batch_execute("ROLLBACK").await {
            Ok(()) => Ok(()),
            Err(e) => {
                drop(Object::take(object));
                Err(pg_error(e))
            }
        }
    }
}

#[async_trait]
impl SinkTxn for PgTxn {
    async fn write(&mut self, target: &str, batch: RecordBatch) -> Result<()> {
        let p = self.prepared(target)?;
        check_batch(&p, &batch)?;
        if p.table.primary_key.is_empty() && self.strategy != IdempotencyStrategy::TxnWithState {
            return Err(connector_error(format!(
                "`{target}` has no key, so {:?} can't replace the rows a previous attempt wrote",
                self.strategy
            )));
        }
        *self
            .summary
            .rows_written
            .entry(target.to_owned())
            .or_default() += batch.num_rows() as u64;
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let columns: Vec<encode::Column<'_>> = p
            .fields
            .iter()
            .zip(batch.columns())
            .zip(&p.wires)
            .map(|((field, values), wire)| encode::Column {
                field,
                values,
                wire,
            })
            .collect();
        let data = encode::copy_data(&columns, batch.num_rows())
            .map_err(|e| connector_error(format!("`{target}`: {e}")))?;
        let names: Vec<&str> = p.table.fields.iter().map(|f| f.name.as_str()).collect();
        let column_names = column_list(&names);
        let table = qualified(&self.schema, &p.table.name);

        let key = &p.table.primary_key;
        if key.is_empty() {
            self.copy(&table, &column_names, data).await?;
            return Ok(());
        }
        self.client()?
            .batch_execute(&format!(
                "CREATE TEMP TABLE {STAGE} AS SELECT {column_names} FROM {table} WITH NO DATA"
            ))
            .await
            .map_err(pg_error)?;
        self.copy(STAGE, &column_names, data).await?;

        let mut statements = Vec::new();
        self.descendant_deletes(
            &p,
            &format!("FROM {STAGE}"),
            &mut Vec::new(),
            &mut statements,
        )?;
        let keys = column_list(key);
        match p.mode {
            WriteMode::Insert => {
                statements.push(format!(
                    "DELETE FROM {table} WHERE ({keys}) IN (SELECT {keys} FROM {STAGE})"
                ));
                statements.push(format!(
                    "INSERT INTO {table} ({column_names}) SELECT {column_names} FROM {STAGE}"
                ));
            }
            WriteMode::Upsert => {
                let updates: Vec<String> = names
                    .iter()
                    .filter(|n| !key.iter().any(|k| k == *n))
                    .map(|n| format!("{0} = EXCLUDED.{0}", quote(n)))
                    .collect();
                let action = if updates.is_empty() {
                    "DO NOTHING".to_owned()
                } else {
                    format!("DO UPDATE SET {}", updates.join(", "))
                };
                statements.push(format!(
                    "INSERT INTO {table} ({column_names}) SELECT {column_names} FROM {STAGE} \
                     ON CONFLICT ({keys}) {action}"
                ));
            }
        }
        statements.push(format!("DROP TABLE {STAGE}"));
        let client = self.client()?;
        for sql in statements {
            client.batch_execute(&sql).await.map_err(pg_error)?;
        }
        Ok(())
    }

    async fn commit(mut self: Box<Self>) -> Result<WriteSummary> {
        if let Some(ledger) = self.ledger.clone() {
            let lease = self.lease.clone();
            let summary = self.summary.clone();
            let recorded = ledger.record_done(self.client()?, &lease, &summary).await;
            if let Err(e) = recorded {
                // The ledger's error (say, StaleLease) is the one to report.
                let _ = self.rollback().await;
                return Err(e);
            }
        }
        let object = self
            .client
            .take()
            .ok_or_else(|| connector_error("the transaction has already ended"))?;
        if let Err(e) = object.batch_execute("COMMIT").await {
            drop(Object::take(object));
            return Err(pg_error(e));
        }
        Ok(std::mem::take(&mut self.summary))
    }

    async fn abort(mut self: Box<Self>) -> Result<()> {
        self.rollback().await
    }
}

impl Drop for PgTxn {
    /// A transaction dropped without `commit` or `abort` (a failed write, a
    /// cancelled task) must not go back to the pool still open: its
    /// connection is closed instead, and the server rolls it back.
    fn drop(&mut self) {
        if let Some(object) = self.client.take() {
            drop(Object::take(object));
        }
    }
}

/// Checks a batch against the prepared table: the same columns, in order,
/// with the canonical Arrow types, and no nulls where the target has none.
fn check_batch(p: &Prepared, batch: &RecordBatch) -> Result<()> {
    let schema = batch.schema();
    let got: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    let want: Vec<&str> = p.fields.iter().map(|f| f.name().as_str()).collect();
    let name = &p.table.name;
    if got != want {
        return Err(connector_error(format!(
            "`{name}` expects columns {want:?}, got {got:?}"
        )));
    }
    for (field, column) in p.fields.iter().zip(batch.columns()) {
        if column.data_type() != field.data_type() {
            return Err(connector_error(format!(
                "`{name}.{}` expects {}, got {}",
                field.name(),
                field.data_type(),
                column.data_type()
            )));
        }
        if !field.is_nullable() && column.null_count() > 0 {
            return Err(connector_error(format!(
                "`{name}.{}` is not nullable but the batch has {} nulls",
                field.name(),
                column.null_count()
            )));
        }
    }
    Ok(())
}
