# PostgreSQL

`type: postgres` is a source and a sink, and can hold the state store.

## Connection options

```yaml
{{#include ../examples/postgres-options/ferry/connections.yaml}}
```

| Option | Default | Meaning |
| --- | --- | --- |
| `url` | required, secret | A libpq-style URL, `postgres://user@host:5432/db?sslmode=require`, or a `key=value` string (`host=db user=app dbname=shop`). TLS settings go here ([TLS](tls.md#postgresql)). |
| `password` | none, secret | Overrides a password in `url`, so the URL can be shared and the password kept apart. |
| `schema` | `public` | The schema read from, as a source, and written to, as a sink. Can't be empty. |
| `drop_indexes` | `false` | As a sink: drop the secondary indexes of target tables that already exist (not unique, not backing a constraint) before the load, and recreate them after it. |
| `foreign_keys` | `defer` | As a sink: `defer` drops the foreign keys of target tables that already exist for the load, then recreates and validates them; `enforce` keeps them, so parents must land before their children. |

`url` and `password` are secret: their values, and a password embedded in
the URL, are replaced with `***` in everything dataferry prints. `plan`
shows the connection as `postgres://user@host:port/db, schema public`,
without the password.

## As a source

**What it reads.** The tables of the connection's `schema:`: ordinary
tables and partitioned tables (as a whole), not views, materialized views,
foreign tables or the partitions of a partitioned table. A flow names a
table as `<connection>.<table>` or `<connection>.<schema>.<table>` with
the connection's own schema; `app.public.orders` and `app.orders` are the
same table on a connection with `schema: public`.

**Every table needs a primary key**, and every column a supported type.
Discovery reads the whole schema, so one table that breaks either rule
fails discovery, even one no flow reads: `plan` and `run` stop (exit `5`)
with a message listing every such table and why
(`has no primary key, which is needed to partition and re-run it safely`).
Fix the table, or point `schema:` at a schema holding only what you
migrate.

**Partitions.** A table with a single-column primary key of type
`smallint`, `integer`, `bigint`, `text`-like or `uuid` is split into
ranges of that key, about `partition_rows` rows each. Text keys are
ordered bytewise (`COLLATE "C"`). Tables with a composite key or another
key type are read as one partition. Planning the ranges reads every key of
the table once.

**Reading.** Each partition is one `SELECT ... WHERE key >= $1 AND key <
$2 ORDER BY key` on its own connection, with binary results. Filters are
not pushed down to the database: `where:` is evaluated by dataferry. There
is no snapshot across partitions.

**Co-partitioning** (for a merge's `many` join) works on any column of
integer, text or UUID type, not only the key. Rows whose co-partition
column is null match no range.

### Types

| PostgreSQL | Canonical |
| --- | --- |
| `boolean` | `bool` |
| `smallint`, `integer`, `bigint` | `int16`, `int32`, `int64` |
| `real`, `double precision` | `float32`, `float64` |
| `numeric(p,s)` | `decimal(p,s)` |
| `text`, `varchar(n)`, `char(n)`, `name` | `utf8` |
| `bytea` | `binary` |
| `date` | `date` |
| `timestamp` | `timestamp` |
| `timestamptz` | `timestamp(UTC)` |
| `uuid` | `uuid` |
| `json`, `jsonb` | `json` |
| a one-dimensional array of any of these | `list(<type>)` |

Not supported, and failing discovery: any other type (`point`,
`interval`, `money`, enums, every domain, even one over a supported type,
…) and `numeric` without a
precision ("declare the column as numeric(p, s) or cast it in a query"),
or with a precision above 76. Values that can't be represented fail the
read with the table and column: a `numeric` `NaN` or infinity, an infinite
date or timestamp, a multi-dimensional array.

## As a sink

### Tables it creates

Before a flow's first partition, the sink **prepares** its targets, in
one transaction: `CREATE SCHEMA IF NOT EXISTS` the connection's schema,
then `CREATE TABLE` for each target that doesn't exist, with the columns
in `columns:` order, `NOT NULL` where the column isn't nullable (key
columns always), and the `PRIMARY KEY` from `key:`. Foreign keys are added
at the end of the load, `NOT VALID` and then validated. `plan` prints this
DDL.

| Canonical | PostgreSQL column |
| --- | --- |
| `bool` | `boolean` |
| `int8`, `int16`, `uint8` | `smallint` |
| `int32`, `uint16` | `integer` |
| `int64`, `uint32` | `bigint` |
| `uint64` | `numeric(20,0)` |
| `float32`, `float64` | `real`, `double precision` |
| `decimal(p,s)` | `numeric(p,s)` |
| `utf8` | `text` |
| `binary` | `bytea` |
| `date` | `date` |
| `timestamp` | `timestamp without time zone` |
| `timestamp(<zone>)` | `timestamp with time zone` |
| `uuid` | `uuid` |
| `json` | `jsonb` |
| `list(<scalar>)` | an array of the scalar's type, `<type>[]` |
| a nested list, a struct, a nest column | `jsonb` |

Nested values in `jsonb` keep decimals exact, write dates as
`YYYY-MM-DD`, timestamps as ISO 8601 with microseconds and `Z`, UUIDs in
their usual form and bytes as `"\\x..."` hex.

### Tables that already exist

A target table that exists is used as it is, if it fits. Prepare checks,
for every target, and reports **every** problem before writing anything:

- each column the flow writes exists, with the type the table above gives,
  or an accepted equivalent (`varchar(n)` for `text`, `json` for `jsonb`).
  A different type is refused, for example an `integer` column for an
  `int64` value ("would truncate");
- each column the flow doesn't write is nullable or has a default;
- for `mode: upsert`, a primary key or unique index covers exactly the
  `key:` columns.

Extra nullable columns are fine. A `char(n)` column can't be written (the
sink writes `text`), even though the source reads `char(n)` as `utf8`.

### How a partition is written

All of a partition's rows, in every target table of the flow, are written
in one transaction, with binary `COPY` through a temporary staging table,
then merged into the target:

- `mode: insert`: rows of the target with the same keys as the partition's
  are deleted, then the partition's rows inserted. A re-run partition
  therefore replaces its own rows, and a run into a table that already
  holds those keys overwrites them.
- `mode: upsert`: `INSERT ... ON CONFLICT (<key>) DO UPDATE` (or `DO
  NOTHING` when every column is in the key).
- In both modes, before a parent's rows land, the child rows that
  reference them through a foreign key the plan declared (and their own
  children) are deleted, so children are replaced with their parent.
- A table without `key:` is written with plain `COPY`, which only
  `TxnWithState` makes safe to re-run; under any other strategy the sink
  refuses a keyless target.

The strategies the sink offers are `TxnWithState`, when the run's state
store is on this connection, then `DeletePartitionThenInsert` and
`UpsertByKey`. The planner prefers them in that order, so `UpsertByKey` is
never chosen: an `upsert` flow without the state store gets
`DeletePartitionThenInsert`, and is still written with `INSERT ... ON
CONFLICT`, as above. `plan` shows which strategy each flow gets.

### Existing tables: foreign keys and indexes

Loading into tables that exist could trip over their constraints, or be
slowed by their indexes. When it prepares a flow whose targets already
exist, the sink:

- with `foreign_keys: defer` (the default), drops the foreign keys defined
  **on** those tables (not foreign keys of other tables pointing at them);
- with `drop_indexes: true`, drops their secondary indexes (not unique,
  not primary, not backing a constraint);

and records each definition in `_ferry_dropped`, in the sink's schema.
When the flow's partitions are all done, it **finalizes**: indexes are
recreated, foreign keys added back `NOT VALID` and then validated. A
foreign key whose validation fails stays `NOT VALID`, keeps its record,
and the report lists it with the number of orphan rows and some of their
keys. Run `ALTER TABLE ... VALIDATE CONSTRAINT` once the data is fixed.
Finalize runs again on every `resume` or `retry`, so an interrupted
finalize completes later.

With `foreign_keys: enforce`, foreign keys that already exist on target
tables stay in place, every write is checked, and parents must be written
before their children: use `depends_on:` between flows. The setting
doesn't change the foreign keys the plan declares between a flow's own
targets: those are always added at finalize, after the load.

## As the state store

```yaml
{{#include ../../../examples/golden/ferry/runtime.yaml:1:8}}
```

The state store's tables (`_ferry_runs`, `_ferry_partitions`,
`_ferry_leases` and the sequence `_ferry_epochs`) are created on first
use. They are created without a schema name, so they land in the first
schema of the connection's `search_path` (usually `public`), whatever the
connection's `schema:` says. When the state connection is also a flow's
sink, that flow gets `TxnWithState`.

## Connections and load

The sink (with the state store, when they share the connection) and the
verifier each keep a pool of connections, of the pool library's default
size: twice the number of CPU cores. The source doesn't pool: it opens a
new connection for each discovery, partition plan and partition read.
Leave the server room for both during a run.
