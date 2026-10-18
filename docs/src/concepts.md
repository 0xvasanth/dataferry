# Concepts

This page names the moving parts and shows how a run uses them. Later
chapters go into each one.

## Connections, sources and sinks

A **connection** is a database dataferry talks to, declared in
`connections.yaml` with a `name`, a `type` and that type's own options
(usually a `url` from an environment variable). The type is a
**connector**: this release has `postgres` and `mongo`.

A connection is used as a **source** (read from), a **sink** (written to),
or both. Each connector says which roles it can play:

| Connector | Source | Sink | Can hold the state store |
| --- | --- | --- | --- |
| `postgres` | yes | yes | yes |
| `mongo` | yes | no | no |

`dataferry validate` checks that every connection a flow reads from is a
source and every one it writes to is a sink.

## Entities and records

An **entity** is what a source holds rows of: a MongoDB collection, or a
PostgreSQL table in the connection's schema. Flows name it as
`<connection>.<entity>`, for example `shop.orders`. For Postgres, the
entity may also carry the connection's schema, as `psql` would write it:
on a connection with `schema: public`, `app.public.orders` and
`app.orders` are the same table.

Each row an entity yields is a **record**. dataferry learns an entity's
fields and their types by asking the source to **discover** it: Postgres
reads its catalog, MongoDB samples documents
([Connectors](connectors.md)). Every entity needs a primary key (Mongo's
`_id`, a Postgres primary key): it identifies a record in reports and
dead letters, and it is what partitions are cut on.

## Flows, targets and rows

A **flow** is one unit of migration, written in a file under `flows/`:

```yaml
{{#include ../../examples/golden/ferry/flows/orders.yaml:1:5}}
{{#include ../../examples/golden/ferry/flows/orders.yaml:19:29}}
```

- `from:` is the **driver** entity. Every row a flow writes is attributed
  to one driver record. A flow can also join other entities to its driver
  (a merge).
- `sink:` is the one connection the flow writes to.
- `to:` lists the flow's **targets**: the tables it writes. A record can
  become rows in several targets (a split), and several rows in one target
  (an unnest).
- `columns:` says how each target column is computed from the record, in
  the [expression language](expressions.md).

Each driver record ends a run in exactly one of three states, and the
report counts each:

- **written**: its rows are in the targets;
- **filtered**: a `where:` you wrote excluded it, so it has no rows
  anywhere;
- **rejected**: computing one of its rows failed (`int("three")`, a null in
  a key column, …), so none of its rows are written. Depending on
  `on_error`, the run either stops, or keeps the record in the dead-letter
  table `_ferry_dlq` and carries on ([Errors](errors.md)).

A record is never half-written: all its rows in all targets land together,
or none do.

## Partitions

A flow's driver entity is cut into **partitions**: slices of about
`partition_rows` records (default 100,000), each a range of the primary
key. A partition is the unit of work and of commit: one worker reads it,
transforms it and writes every target row it produces in **one
transaction**, committed once
([decision 0001](reference/decisions/0001-commit-per-partition.md)).

That is what makes a run safe to stop. A partition is either committed,
with all its rows, or not at all; a run that stops part-way loses at most
the partitions in flight, and running them again can't duplicate rows.

Partitions are planned once, when a run starts, and stored with the run.
A resumed run uses the same partitions. Entities whose key can't be cut
into ranges (a composite key, or a key type a connector can't range over)
are read as one partition.

## Plans

Before anything is written, dataferry **plans** the config:

1. It loads and validates every file (this alone is `dataferry validate`).
2. It discovers every entity the flows read, and asks each sink what it
   can do.
3. It type-checks and compiles every expression against the discovered
   types.
4. It orders the flows: a flow waits for the flows in its `depends_on:`,
   and for any flow writing a table it reads.
5. For each flow, it picks an **idempotency strategy** (how a partition
   is made safe to write twice), the join strategies of a merge, and the
   DDL the sink would run.

`dataferry plan` prints all of this and stops. `dataferry run` plans the
same way, then runs.

## Runs and the state store

A **run** is one execution of the plan, with a **run id** (32 hex
digits) printed first. The run, its partitions and their status are kept
in the **state store**:

- `backend: file`: files in the work directory, the default;
- `backend: postgres`: tables in a Postgres connection you name.

The run pins two things when it starts: the **config hash** (a SHA-256 of
the config's meaning, not its layout) and the **engine version**.
`dataferry resume <run-id>` continues a run from its stored partitions,
and refuses if either changed, so a resumed run computes exactly what the
interrupted one would have ([Running migrations](running.md)).

## Leases and epochs

A worker takes a partition by **leasing** it from the state store. A lease
lasts 30 seconds and the worker renews it every 10 while it works. Each
lease has an **epoch**, a number that grows with every lease ever granted.

If a process dies, its leases expire, and a resumed run can lease those
partitions again, at a higher epoch. Writes are **fenced** by epoch: a
worker whose lease was superseded can't record its partition done. Under
the exactly-once strategy it can't commit its rows either, since the epoch
is checked inside the commit. Under the other strategy the epoch is checked
just before the commit, and a stale write that slips past that check
writes the same rows the new lease writes, which replace them by key.

## Idempotency strategies

How a partition is made safe to write twice depends on where the state
store is:

- **`TxnWithState`**, exactly-once: when the state store is on the flow's
  sink connection, the partition's rows and the record that it is done
  commit in the same transaction. Either both are there or neither is.
- **`DeletePartitionThenInsert`**: otherwise, the sink first deletes the
  target rows whose keys the partition is about to write, then writes
  them. A partition interrupted between its commit and the state store
  update is written again, and the second write replaces the first.

The config never names a strategy: `dataferry plan` shows which one each
flow gets and why. Keep the state store on the sink
(`runtime.state.connection: <sink>`) to get `TxnWithState`.

## Verification and the report

After a run, dataferry **verifies** each sink: it reads the target tables
back and compares them with checksums it took of every row as it wrote
them, checks keys, not-nulls and foreign keys, accounts for every record
read, checks the dead-letter table, and evaluates the flows' `expect:`
assertions. The result is a **report**, `report.md` and `report.json`
([Verification](verification.md)).

## Where things are kept

**The work directory**, `.ferry/` next to the config directory by default
(`--work-dir` or `FERRY_WORK_DIR` to move it), holds what a run keeps
outside the databases:

| Path | What |
| --- | --- |
| `runs/<run-id>/report.md`, `report.json` | The run report. With several sinks, one `report-<sink>.*` pair per sink. |
| `writes/`, `sources/` | Write-time records: per partition, the checksums and counts of what was written, and the source-side `expect:` aggregates. Verification needs them, including after a resume. |
| `stage/` | Spill files of staged merge joins, while a run is going. |
| `state/` | The state store, under `backend: file` without a `path:`. |

Use one work directory per process: two runs at the same time must not
share it.

**In your databases**, dataferry creates tables whose names start with
`_ferry_` (a target table can't use that prefix):

| Table | Where | What |
| --- | --- | --- |
| `_ferry_runs`, `_ferry_partitions`, `_ferry_leases`, sequence `_ferry_epochs` | the state connection, under `backend: postgres` | The state store. Created unqualified, so they land in the schema the connection's `search_path` puts first (usually `public`), not necessarily the connection's `schema:`. |
| `_ferry_dlq` | each sink's schema, for flows with `on_error: dead_letter` | Dead-lettered records ([Errors](errors.md)). |
| `_ferry_dropped` | each sink's schema, when a load into existing tables drops constraints or indexes | Foreign keys and indexes dropped for a load, until they are restored ([PostgreSQL](connectors/postgres.md#existing-tables-foreign-keys-and-indexes)). |

## How a run proceeds

```text
 dataferry run
   │
   ├─ plan ─────────── load + validate config ─► discover sources ─► type-check
   │                   ─► order flows ─► strategies, partitions, DDL
   │
   ├─ create the run ─ pin config hash + engine version in the state store
   │                   print "run <run-id>"
   │
   ├─ for every flow, up front:
   │     prepare the sink (CREATE TABLE ..., drop deferred constraints)
   │     plan and store the flow's partitions (first attempt only)
   │
   ├─ workers, in parallel, each flow once the flows it waits for are done:
   │     lease a partition ─► read ─► transform ─► write every target
   │     ─► commit once ─► partition done
   │
   ├─ once every flow has stopped: finalize the flows that are done
   │     (restore constraints and indexes, validate foreign keys)
   │
   ├─ verify each sink ─ checksums, counts, constraints, dead letters, expect:
   │
   └─ write report.md + report.json, exit 0 / 6 / 7
```

The first failed partition stops the run from leasing new ones; the ones
in flight finish. `dataferry retry --failed <run-id>` puts the failed
partitions back and runs them again.
