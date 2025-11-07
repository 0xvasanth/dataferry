//! What a load drops from its target tables, and finalize puts back (A6).
//!
//! Before a load, `prepare` drops the foreign keys of every target table
//! (unless the connection says `foreign_keys: enforce`) and, with
//! `drop_indexes: true`, the indexes that enforce nothing: not unique, and
//! not backing a constraint. Rows then land in any order and without index
//! maintenance. Write order stays an optimization, not the integrity
//! mechanism.
//!
//! Each dropped object's definition, as `pg_get_constraintdef` and
//! `pg_get_indexdef` print it, goes to `_ferry_dropped` in the target
//! schema in the same transaction as the drop, so after a crash at any
//! point either the object or its record exists. The record lives next to
//! the tables rather than with one run, so any later run on the sink, a
//! resume or a fresh start, restores it.
//!
//! Finalize recreates every recorded object of the flow's tables, then
//! creates the foreign keys the target schema declares (the sink creates
//! tables without them). Each object is restored in its own transaction,
//! together with the change to its record, so finalize can stop anywhere
//! and run again. A foreign key is added `NOT VALID` and then validated. If
//! the loaded rows break it, it stays `NOT VALID` (checked for new rows,
//! never silently off), the rows are reported by key, and its record is
//! kept so the next finalize validates it again.

use std::collections::BTreeSet;

use ferry_core::{ForeignKey, Result, TargetSchema, TargetTable};
use ferry_registry::finalize::MAX_ORPHANS;
use ferry_registry::{FinalizeProblem, Finalized, Orphans};
use schemars::JsonSchema;
use serde::Deserialize;
use tokio_postgres::{Client, Transaction};

use crate::ddl::{column_list, qualified};
use crate::source::{pg_error, quote};

/// The table that holds what a load dropped.
pub(crate) const DROPPED: &str = "_ferry_dropped";

/// How a load treats the constraints and indexes of target tables that
/// already exist.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LoadOptions {
    pub drop_indexes: bool,
    pub foreign_keys: ForeignKeys,
}

/// What a load does with the foreign keys of its target tables.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ForeignKeys {
    /// Dropped for the load, recreated and validated after it. Rows that
    /// break one are reported.
    #[default]
    Defer,
    /// Kept, and checked on every write. Parents must land before their
    /// children.
    Enforce,
}

impl LoadOptions {
    fn drops_anything(&self) -> bool {
        self.drop_indexes || self.foreign_keys == ForeignKeys::Defer
    }
}

const FOREIGN_KEYS_SQL: &str = r#"
SELECT c.conname::text, pg_get_constraintdef(c.oid), c.convalidated
FROM pg_constraint c
WHERE c.conrelid = to_regclass($1) AND c.contype = 'f'
ORDER BY c.conname
"#;

/// Indexes that enforce nothing: not unique, and not backing one of the
/// table's constraints (an exclusion constraint's index isn't unique).
const LOOSE_INDEXES_SQL: &str = r#"
SELECT i.relname::text, pg_get_indexdef(i.oid)
FROM pg_index x
JOIN pg_class i ON i.oid = x.indexrelid
WHERE x.indrelid = to_regclass($1) AND NOT x.indisunique AND NOT x.indisprimary
  AND NOT EXISTS (
      SELECT 1 FROM pg_constraint c
      WHERE c.conrelid = x.indrelid AND c.conindid = x.indexrelid
  )
ORDER BY i.relname
"#;

/// The columns of a foreign key, and the table and columns it references.
const FOREIGN_KEY_SQL: &str = r#"
SELECT c.confrelid::regclass::text,
       ARRAY(SELECT a.attname::text
             FROM unnest(c.conkey) WITH ORDINALITY AS k(n, o)
             JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.n
             ORDER BY k.o),
       ARRAY(SELECT a.attname::text
             FROM unnest(c.confkey) WITH ORDINALITY AS k(n, o)
             JOIN pg_attribute a ON a.attrelid = c.confrelid AND a.attnum = k.n
             ORDER BY k.o),
       pg_get_constraintdef(c.oid)
FROM pg_constraint c
WHERE c.conrelid = to_regclass($1) AND c.conname = $2 AND c.contype = 'f'
"#;

/// Foreign keys of a table, by the columns they map.
const FOREIGN_KEY_NAMES_SQL: &str = r#"
SELECT c.conname::text
FROM pg_constraint c
WHERE c.conrelid = to_regclass($1) AND c.contype = 'f' AND c.confrelid = to_regclass($2)
  AND ARRAY(SELECT a.attname::text
            FROM unnest(c.conkey) WITH ORDINALITY AS k(n, o)
            JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.n
            ORDER BY k.o) = $3::text[]
  AND ARRAY(SELECT a.attname::text
            FROM unnest(c.confkey) WITH ORDINALITY AS k(n, o)
            JOIN pg_attribute a ON a.attrelid = c.confrelid AND a.attnum = k.n
            ORDER BY k.o) = $4::text[]
"#;

const CONSTRAINT_EXISTS_SQL: &str =
    "SELECT 1 FROM pg_constraint WHERE conrelid = to_regclass($1) AND conname = $2";

fn dropped_table(schema: &str) -> String {
    qualified(schema, DROPPED)
}

/// One row per dropped object of a target table. `validated` is whether a
/// foreign key was valid before the load, so finalize validates only those.
fn create_dropped_sql(schema: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {} (
             table_name text NOT NULL,
             kind text NOT NULL CHECK (kind IN ('index', 'foreign_key')),
             name text NOT NULL,
             definition text NOT NULL,
             validated boolean NOT NULL,
             PRIMARY KEY (table_name, kind, name)
         )",
        dropped_table(schema)
    )
}

/// Drops what `options` asks for from `tables` (which exist), recording
/// each object on `txn`, the prepare transaction.
pub(crate) async fn drop_for_load(
    txn: &Transaction<'_>,
    schema: &str,
    tables: &[&str],
    options: LoadOptions,
) -> Result<()> {
    if !options.drops_anything() || tables.is_empty() {
        return Ok(());
    }
    let dropped = dropped_table(schema);
    txn.batch_execute(&create_dropped_sql(schema))
        .await
        .map_err(pg_error)?;
    // An object recorded by an earlier load that never finished keeps its
    // first record: it is the definition from before any load touched it.
    let record = format!(
        "INSERT INTO {dropped} (table_name, kind, name, definition, validated)
         VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING"
    );
    for table in tables {
        let regclass = qualified(schema, table);
        if options.foreign_keys == ForeignKeys::Defer {
            for row in txn
                .query(FOREIGN_KEYS_SQL, &[&regclass])
                .await
                .map_err(pg_error)?
            {
                let (name, definition, validated): (String, String, bool) =
                    (row.get(0), row.get(1), row.get(2));
                let definition = definition.strip_suffix(" NOT VALID").unwrap_or(&definition);
                txn.execute(
                    &record,
                    &[table, &"foreign_key", &name, &definition, &validated],
                )
                .await
                .map_err(pg_error)?;
                txn.batch_execute(&format!(
                    "ALTER TABLE {regclass} DROP CONSTRAINT {}",
                    quote(&name)
                ))
                .await
                .map_err(pg_error)?;
            }
        }
        if options.drop_indexes {
            for row in txn
                .query(LOOSE_INDEXES_SQL, &[&regclass])
                .await
                .map_err(pg_error)?
            {
                let (name, definition): (String, String) = (row.get(0), row.get(1));
                txn.execute(&record, &[table, &"index", &name, &definition, &true])
                    .await
                    .map_err(pg_error)?;
                txn.batch_execute(&format!("DROP INDEX {}", qualified(schema, &name)))
                    .await
                    .map_err(pg_error)?;
            }
        }
    }
    Ok(())
}

/// One row of `_ferry_dropped`.
struct Dropped {
    table: String,
    kind: String,
    name: String,
    definition: String,
    validated: bool,
}

/// Restores what loads dropped from `target`'s tables, and creates the
/// foreign keys it declares.
pub(crate) async fn restore(
    client: &mut Client,
    schema: &str,
    target: &TargetSchema,
) -> Result<Finalized> {
    let mut done = Finalized::default();
    let tables: Vec<String> = target.tables().iter().map(|t| t.name.clone()).collect();
    let mut records = load_records(client, schema, &tables).await?;

    // Indexes first: they speed up validating the foreign keys.
    for r in records.iter().filter(|r| r.kind == "index") {
        match restore_index(client, schema, r).await {
            Ok(()) => done.restored.push(format!("{}.{}", r.table, r.name)),
            Err(e) => done.problems.push(problem(
                r,
                format!(
                    "couldn't recreate the index ({e}); its definition stays in {DROPPED} \
                     for the next finalize: {}",
                    r.definition
                ),
            )),
        }
    }
    records.retain(|r| r.kind == "foreign_key");

    let mut added = BTreeSet::new();
    for r in &records {
        match add_foreign_key(client, schema, r).await {
            Ok(()) => {
                added.insert((r.table.clone(), r.name.clone()));
            }
            Err(e) => done.problems.push(problem(
                r,
                format!(
                    "couldn't recreate the foreign key ({e}); its definition stays in \
                     {DROPPED} for the next finalize: {}",
                    r.definition
                ),
            )),
        }
    }
    for t in target.tables() {
        for fk in &t.foreign_keys {
            match declare_foreign_key(client, schema, t, fk).await {
                Ok(Some(r)) => {
                    added.insert((r.table.clone(), r.name.clone()));
                    records.push(r);
                }
                Ok(None) => {}
                Err(e) => done.problems.push(FinalizeProblem {
                    table: t.name.clone(),
                    object: declared_name(t, fk),
                    message: format!(
                        "couldn't create the foreign key ({}) referencing `{}`: {e}",
                        fk.columns.join(", "),
                        fk.references_table
                    ),
                    orphans: None,
                }),
            }
        }
    }

    // Every added foreign key is NOT VALID until here.
    for r in records
        .iter()
        .filter(|r| added.contains(&(r.table.clone(), r.name.clone())))
    {
        if !r.validated {
            // It wasn't valid before the load either: as it was.
            done.restored.push(format!("{}.{}", r.table, r.name));
            continue;
        }
        let table = target
            .table(&r.table)
            .expect("records are of target tables");
        match validate(client, schema, table, r).await? {
            None => done.restored.push(format!("{}.{}", r.table, r.name)),
            Some(problem) => done.problems.push(problem),
        }
    }
    Ok(done)
}

fn problem(r: &Dropped, message: String) -> FinalizeProblem {
    FinalizeProblem {
        table: r.table.clone(),
        object: r.name.clone(),
        message,
        orphans: None,
    }
}

async fn load_records(client: &Client, schema: &str, tables: &[String]) -> Result<Vec<Dropped>> {
    let dropped = dropped_table(schema);
    let exists: Option<String> = client
        .query_one("SELECT to_regclass($1)::text", &[&dropped])
        .await
        .map_err(pg_error)?
        .get(0);
    if exists.is_none() {
        return Ok(Vec::new());
    }
    Ok(client
        .query(
            &format!(
                "SELECT table_name, kind, name, definition, validated FROM {dropped}
                 WHERE table_name = ANY($1) ORDER BY table_name, kind, name"
            ),
            &[&tables],
        )
        .await
        .map_err(pg_error)?
        .iter()
        .map(|row| Dropped {
            table: row.get(0),
            kind: row.get(1),
            name: row.get(2),
            definition: row.get(3),
            validated: row.get(4),
        })
        .collect())
}

fn forget_sql(schema: &str) -> String {
    format!(
        "DELETE FROM {} WHERE table_name = $1 AND kind = $2 AND name = $3",
        dropped_table(schema)
    )
}

/// Recreates the index unless it already exists (a finalize that stopped
/// after creating it), and forgets its record.
async fn restore_index(client: &mut Client, schema: &str, r: &Dropped) -> Result<()> {
    let txn = client.transaction().await.map_err(pg_error)?;
    let exists: Option<String> = txn
        .query_one(
            "SELECT to_regclass($1)::text",
            &[&qualified(schema, &r.name)],
        )
        .await
        .map_err(pg_error)?
        .get(0);
    if exists.is_none() {
        txn.batch_execute(&r.definition).await.map_err(pg_error)?;
    }
    txn.execute(&forget_sql(schema), &[&r.table, &r.kind, &r.name])
        .await
        .map_err(pg_error)?;
    txn.commit().await.map_err(pg_error)
}

/// Adds the foreign key `NOT VALID` unless a constraint of its name is
/// already there. A key that was valid before the load keeps its record
/// until it validates.
async fn add_foreign_key(client: &mut Client, schema: &str, r: &Dropped) -> Result<()> {
    let regclass = qualified(schema, &r.table);
    let txn = client.transaction().await.map_err(pg_error)?;
    let exists = !txn
        .query(CONSTRAINT_EXISTS_SQL, &[&regclass, &r.name])
        .await
        .map_err(pg_error)?
        .is_empty();
    if !exists {
        txn.batch_execute(&format!(
            "ALTER TABLE {regclass} ADD CONSTRAINT {} {} NOT VALID",
            quote(&r.name),
            r.definition
        ))
        .await
        .map_err(pg_error)?;
    }
    if !r.validated {
        txn.execute(&forget_sql(schema), &[&r.table, &r.kind, &r.name])
            .await
            .map_err(pg_error)?;
    }
    txn.commit().await.map_err(pg_error)
}

/// The name a declared foreign key gets: Postgres's own default,
/// `<table>_<columns>_fkey`, cut to the 63 bytes of an identifier.
fn declared_name(table: &TargetTable, fk: &ForeignKey) -> String {
    let mut name = format!("{}_{}_fkey", table.name, fk.columns.join("_"));
    if name.len() > 63 {
        let mut cut = 63;
        while !name.is_char_boundary(cut) {
            cut -= 1;
        }
        name.truncate(cut);
    }
    name
}

/// Creates a foreign key `table` declares, `NOT VALID` and recorded so it
/// is validated like a restored one, unless one on the same columns and
/// referenced columns exists. Returns its record if it was created.
async fn declare_foreign_key(
    client: &mut Client,
    schema: &str,
    table: &TargetTable,
    fk: &ForeignKey,
) -> Result<Option<Dropped>> {
    let regclass = qualified(schema, &table.name);
    let parent = qualified(schema, &fk.references_table);
    let txn = client.transaction().await.map_err(pg_error)?;
    let existing = txn
        .query(
            FOREIGN_KEY_NAMES_SQL,
            &[&regclass, &parent, &fk.columns, &fk.references_columns],
        )
        .await
        .map_err(pg_error)?;
    if !existing.is_empty() {
        return Ok(None);
    }
    let record = Dropped {
        table: table.name.clone(),
        kind: "foreign_key".into(),
        name: declared_name(table, fk),
        definition: format!(
            "FOREIGN KEY ({}) REFERENCES {parent}({})",
            column_list(&fk.columns),
            column_list(&fk.references_columns)
        ),
        validated: true,
    };
    let dropped = dropped_table(schema);
    txn.batch_execute(&create_dropped_sql(schema))
        .await
        .map_err(pg_error)?;
    txn.batch_execute(&format!(
        "ALTER TABLE {regclass} ADD CONSTRAINT {} {} NOT VALID",
        quote(&record.name),
        record.definition
    ))
    .await
    .map_err(pg_error)?;
    txn.execute(
        &format!(
            "INSERT INTO {dropped} (table_name, kind, name, definition, validated)
             VALUES ($1, $2, $3, $4, true) ON CONFLICT DO NOTHING"
        ),
        &[
            &record.table,
            &record.kind,
            &record.name,
            &record.definition,
        ],
    )
    .await
    .map_err(pg_error)?;
    txn.commit().await.map_err(pg_error)?;
    Ok(Some(record))
}

/// Validates a foreign key and forgets its record. If rows break it, it
/// stays `NOT VALID` with its record, and the problem names the rows.
async fn validate(
    client: &mut Client,
    schema: &str,
    table: &TargetTable,
    r: &Dropped,
) -> Result<Option<FinalizeProblem>> {
    let regclass = qualified(schema, &r.table);
    let txn = client.transaction().await.map_err(pg_error)?;
    let validated = txn
        .batch_execute(&format!(
            "ALTER TABLE {regclass} VALIDATE CONSTRAINT {}",
            quote(&r.name)
        ))
        .await;
    if let Err(e) = validated {
        drop(txn);
        let orphans = orphans(client, &regclass, table, &r.name).await?;
        let message = match &orphans {
            Some(o) => format!(
                "{} rows reference no parent row, so `{}` ({}) stays NOT VALID: new rows are \
                 checked, these are not",
                o.count, r.name, r.definition
            ),
            None => format!(
                "`{}` ({}) couldn't be validated and stays NOT VALID: {}",
                r.name,
                r.definition,
                pg_error(e)
            ),
        };
        return Ok(Some(FinalizeProblem {
            table: r.table.clone(),
            object: r.name.clone(),
            message,
            orphans,
        }));
    }
    txn.execute(&forget_sql(schema), &[&r.table, &r.kind, &r.name])
        .await
        .map_err(pg_error)?;
    txn.commit().await.map_err(pg_error)?;
    Ok(None)
}

/// The rows of `regclass` the foreign key `name` finds no parent for, by
/// the table's primary key (or the key's own columns if it has none).
/// `None` if there are none.
async fn orphans(
    client: &Client,
    regclass: &str,
    table: &TargetTable,
    name: &str,
) -> Result<Option<Orphans>> {
    let row = client
        .query_one(FOREIGN_KEY_SQL, &[&regclass, &name])
        .await
        .map_err(pg_error)?;
    let (parent, columns, references): (String, Vec<String>, Vec<String>) =
        (row.get(0), row.get(1), row.get(2));
    let key = if table.primary_key.is_empty() {
        &columns
    } else {
        &table.primary_key
    };
    // MATCH SIMPLE: a row with a null in any key column references nothing.
    let mut conditions: Vec<String> = columns
        .iter()
        .map(|c| format!("c.{} IS NOT NULL", quote(c)))
        .collect();
    let matches: Vec<String> = columns
        .iter()
        .zip(&references)
        .map(|(c, p)| format!("p.{} = c.{}", quote(p), quote(c)))
        .collect();
    conditions.push(format!(
        "NOT EXISTS (SELECT 1 FROM {parent} AS p WHERE {})",
        matches.join(" AND ")
    ));
    let from = format!("FROM {regclass} AS c WHERE {}", conditions.join(" AND "));
    let count: i64 = client
        .query_one(&format!("SELECT count(*) {from}"), &[])
        .await
        .map_err(pg_error)?
        .get(0);
    if count == 0 {
        return Ok(None);
    }
    let label: Vec<String> = key
        .iter()
        .map(|k| {
            format!(
                "'{}=' || coalesce(c.{}::text, 'null')",
                k.replace('\'', "''"),
                quote(k)
            )
        })
        .collect();
    let order: Vec<String> = key.iter().map(|k| format!("c.{}", quote(k))).collect();
    let keys = client
        .query(
            &format!(
                "SELECT {} {from} ORDER BY {} LIMIT {MAX_ORPHANS}",
                label.join(" || ', ' || "),
                order.join(", ")
            ),
            &[],
        )
        .await
        .map_err(pg_error)?
        .iter()
        .map(|row| row.get(0))
        .collect();
    Ok(Some(Orphans {
        count: count as u64,
        keys,
    }))
}

#[cfg(test)]
mod tests {
    use ferry_core::types::{CanonicalField, CanonicalType};

    use super::*;

    #[test]
    fn declared_foreign_keys_get_postgres_default_names_cut_to_63_bytes() {
        let fk = |columns: &[&str]| ForeignKey {
            columns: columns.iter().map(|c| c.to_string()).collect(),
            references_table: "orders".into(),
            references_columns: vec!["id".into()],
        };
        let table = |name: &str| TargetTable {
            name: name.into(),
            fields: vec![CanonicalField::new("order_id", CanonicalType::Int64, false)],
            primary_key: Vec::new(),
            foreign_keys: Vec::new(),
        };
        assert_eq!(
            declared_name(&table("order_items"), &fk(&["order_id"])),
            "order_items_order_id_fkey"
        );
        assert_eq!(
            declared_name(&table("items"), &fk(&["order_id", "line_no"])),
            "items_order_id_line_no_fkey"
        );
        // Cut on a character boundary, never inside one.
        let long = declared_name(&table(&"é".repeat(40)), &fk(&["order_id"]));
        assert!(long.len() <= 63 && long.starts_with('é'), "{long}");
    }
}
