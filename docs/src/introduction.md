# Introduction

**dataferry** is an open-source data migration engine written in Rust. You
describe a migration in a few YAML files: which databases to read from and
write to, and how each source record becomes rows in the target tables.
dataferry plans it, copies the data in partitions that each commit on their
own, and then checks that what landed is what it computed.

This release, **0.1.0-beta.1**, moves data from **MongoDB or PostgreSQL into
PostgreSQL**, as a single-process command-line tool. It is a beta: the
config grammar and the exit codes are meant to stay, but expect rough
edges, and read [Limitations and roadmap](limitations.md) before you point
it at data you care about.

## What it does

- **Reshapes while it copies.** One flow can copy a table 1:1, split one
  record into several tables, unnest an array into a child table, or merge
  several entities into one row with joins and nested arrays
  ([Transforms](transforms.md)). Values are computed with a small, pure,
  SQL-like expression language that is type-checked before anything runs
  ([Expressions](expressions.md)).
- **Survives being stopped.** Each partition (a slice of a source entity)
  is written in one transaction and committed once. A run that is killed,
  interrupted or loses its connection resumes from the partitions that
  committed and writes nothing twice
  ([Running migrations](running.md)).
- **Tells you what happened.** Every record a run reads ends up written,
  filtered by a `where:` you wrote, or rejected with a reason (and, if you
  ask for it, kept in a dead-letter table). After the run, the target is
  read back and compared with checksums taken at write time, constraints
  are checked, and your own `expect:` assertions are evaluated. The result
  is a report in Markdown and JSON ([Verification](verification.md)).
- **Fails before it starts.** `dataferry validate` checks a config without
  connecting to anything, and `dataferry plan` discovers the sources,
  type-checks every expression and prints the DDL it would run, without
  writing a byte. Errors point at the file, line and column you wrote.

## Who it is for

Engineers who need to move a database's data, once or a few times, into a
differently shaped PostgreSQL schema: a MongoDB-to-Postgres migration, a
consolidation of legacy tables into a reporting schema, a re-modelling
between two Postgres databases. The config is plain files you can review
and commit, and `validate` and `plan` run in CI without credentials to
production.

## What it is not

- **Not a sync or CDC tool.** A run copies what the source holds when it
  reads it. It doesn't follow changes afterwards, and `mode: upsert` never
  deletes target rows.
- **Not a general ETL scheduler.** There are no triggers, schedules or
  web UI. Each run is one command.
- **Not distributed.** One process runs every flow, with as many workers
  as you configure.
- **Not (yet) a many-to-many connector hub.** The built-in connectors are a
  PostgreSQL source and sink and a MongoDB source. You can
  [add your own connector](extending.md) in Rust and ship it in your own
  binary.

## A migration in 60 seconds

A config is a directory, conventionally called `ferry/`:

```text
ferry/
  ferry.yaml          # version: 1 and the migration's name
  connections.yaml    # the databases, with credentials from ${ENV}
  runtime.yaml        # where run state lives, workers, error policy
  flows/
    orders.yaml       # one or more flows: from, sink, to, columns
```

A flow reads one source entity (here, a Mongo collection) and writes rows
into one or more tables of one sink. This one, from the
[quickstart](quickstart.md), turns each order document into a row in
`orders` and one row per element of its `items` array in `order_items`:

```yaml
{{#include ../../examples/golden/ferry/flows/orders.yaml:1:5}}
    # ...
{{#include ../../examples/golden/ferry/flows/orders.yaml:19:42}}
```

Then:

```sh
dataferry validate      # check the config, offline
dataferry plan          # discover sources, type-check, show DDL and strategy
dataferry run           # copy, verify, write the report
```

`run` prints a run id first. If it is stopped for any reason,
`dataferry resume <run-id>` continues from the last committed partitions.
When it finishes, the report says how many records were read, written,
filtered and dead-lettered, whether every table's checksum matched, and
whether your assertions held. The exit code says the same thing to a
script: `0` for success, `6` when partitions failed, `7` when verification
failed, and so on ([CLI reference](reference/cli.md#exit-codes)).

## How this guide is organised

- **Getting started**: [install](installation.md) dataferry and run the
  [quickstart](quickstart.md), a real Mongo-to-Postgres migration on
  Docker, in about five minutes.
- **User guide**: the [concepts](concepts.md), then each part of a config
  and a run in the order you meet them.
- **Reference**: every [command and flag](reference/cli.md), every
  [config key](reference/config.md), the [JSON Schemas](reference/schemas.md)
  for editor completion, and the [decision records](reference/decisions.md)
  that explain why things are the way they are.

A note on names: the project, the crates and the command are **dataferry**.
The config directory (`ferry/`, with `ferry.yaml`), the local work
directory (`.ferry/`), the tables dataferry creates in your database
(`_ferry_*`) and the environment variables it reads (`FERRY_*`) keep their
shorter names.
