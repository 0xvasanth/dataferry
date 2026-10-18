# Limitations and roadmap

**0.1.0-beta.1 is a beta.** What it does, it does carefully: every
partition commits once, every run is verified, every record is accounted
for, and the connectors pass a conformance suite of 50 runs killed
mid-partition and resumed. But it does fewer things than a 1.0 will, and
some behaviour may change before then. The config grammar
(`version: 1`) and the exit codes are meant to stay.

## What this release doesn't do

**Connectors and sources**

- Sources are MongoDB and PostgreSQL; the only sink is PostgreSQL. There
  is no MongoDB sink.
- PostgreSQL sources read tables, not views or queries, and every table
  of the connection's schema needs a primary key and supported types: one
  table with an enum, a domain, an unconstrained `numeric` or no primary
  key blocks discovery of the whole schema.
  Composite and non-integer, non-text, non-UUID keys are read as one
  partition.
- MongoDB schemas are inferred from a sample of 1,000 documents.
- Reads have no consistent snapshot: migrate from a quiet source or a
  snapshot.
- Client TLS certificates aren't supported for PostgreSQL.
- Connectors are compiled in: there is no plugin loading, and a
  third-party sink can't be verified after a run.

**Runs**

- One process runs a run. There is no distributed mode.
- `on_error` and `max_errors` apply to the whole run, not per flow.
- A partition commits once, at its end: there are no checkpoints inside a
  partition, so a crash redoes the partitions in flight.
- Flows are ordered by `depends_on:` and by the tables they read; foreign
  keys between flows aren't discovered.
- No progress display, and no command to replay dead letters.
- `resume` needs the same dataferry version that started the run.
- Every flow is prepared and partitioned when the run starts, so a flow
  reading a table another flow writes needs that table to exist already.
- A run that stops before finalizing leaves the foreign keys and indexes
  it dropped for the load dropped, until a `resume` or `retry` finalizes
  it.
- The state store's tables are created in the first schema of the state
  connection's `search_path`, not in its `schema:`.

**Mapping**

- Expressions have no date and time functions, and `missing()` works on
  the current record only, not on joined entries.
- Integer arithmetic isn't checked for overflow: it wraps around
  ([Expressions](expressions.md#operators)).
- `columns: "*"` can't be combined with other columns, and a nest column
  can't contain another nest.
- Flows can't be generated from a source schema yet: every flow is
  written by hand.

**Verification**

- It assumes full loads into empty targets: rows already in a table that
  the run doesn't rewrite fail the checks.
- The write-time checksums it compares against are kept in the work
  directory only: a run resumed on another machine, or with another
  `--work-dir`, can't be verified.
- There is no standalone `verify` command; `resume` on a finished run
  verifies it again.

**The CLI**

- No shell completions, no progress output, no log files.

## Roadmap

The next release, v0.2, is planned in four themes. They are plans, not
promises, and their order may change:

- **Hardening**: correctness first, such as more precise redaction, the
  verifier's records in the state store instead of the work directory,
  and lease and dead-letter hygiene.
- **Features** for real-world migrations: foreign-key discovery with
  automatic flow ordering, generating flows from a source schema,
  per-flow `on_error` and `max_errors`, partitions on composite keys, date
  and time functions, `missing()` on joins, and a MongoDB sink.
- **Performance**: a benchmark harness first, then faster paths where it
  shows they matter.
- **Release**: regular tagged releases, and editor completion from the
  JSON Schemas out of the box.

The [issue tracker](https://github.com/0xvasanth/dataferry/issues) is the place
to ask for what you need, or to report what doesn't work.
