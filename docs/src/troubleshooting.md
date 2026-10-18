# Troubleshooting and FAQ

Start with the exit code ([CLI](reference/cli.md#exit-codes)) and the
diagnostic's code: every message names the file and line it is about, and
most say what to do in their `help:` line.

## The config

**`no config directory at ...`, `... is a file; --config expects the
ferry/ directory` (F0001), `no ferry.yaml in ...` (F0103).** `--config`
points at the directory holding `ferry.yaml`: not at the file, and not at
its parent. Without `--config`, commands look for `./ferry`.

**`environment variable not set: NAME` (F0108).** Export it in the shell
that runs dataferry. Variables are read only from the environment, and
only in connections and `runtime.state.path`.

**`` `${` in a flow `` (F0108).** Flows can't use environment variables:
write the value into the flow. It then becomes part of the config hash,
which is what keeps a resumed run identical to the first attempt.

**`unknown key` (F0105).** A typo, or a key in the wrong file or at the
wrong level. The help line suggests the closest key.

**`expected an expression (a string) ... found boolean`.** An unquoted
`true`, `null` or number where an expression belongs: quote it.

**`` `index` is a reserved name `` (F0110).** Write `$index` for the
position, or `"index"` for a field of that name. The same goes for
`parent`, `item` and `key`.

## Planning

**`could not discover the entities of ...` (exit 5).** Either the
database can't be reached (check the URL, the network, credentials and
[TLS](connectors/tls.md)), or the source's schema can't be read. For
PostgreSQL, every table of the connection's `schema:` must have a primary
key and supported column types, even tables no flow reads; the message
lists each offending table. Point `schema:` at a schema holding only what
you migrate, or fix the tables. For MongoDB, a time-series collection or
a field named `_ferry_...` in the database blocks discovery.

**A remote database without TLS fails to connect.** Remote hosts require
TLS unless you turn it off. For a server reached by name without TLS (a
CI service container), add `sslmode=disable` (Postgres) or `tls=false`
(MongoDB) to the URL.

**A certificate isn't trusted.** Certificates are always verified. For a
private CA, give its PEM file: `sslrootcert=` (Postgres) or `tlsCAFile=`
(MongoDB).

**`no field ... in entity ...` (F0110) for a field that exists.** Names
are case-sensitive. For MongoDB, the schema comes from a sample of 1,000
documents: a field none of them has isn't in the schema. Check what
`plan` reports for the entity, and see
[schema inference](connectors/mongo.md#schema-inference).

**`unconstrained numeric has no fixed precision`.** Declare the Postgres
column as `numeric(p, s)`. The message also suggests casting it in a
query, but the source reads tables only, not views or queries, so the
column type itself must change (or migrate from a copy of the table that
has one).

**A type error (F0111) on a typed column.** The conversion from the
expression's type to `type:` can lose information. Convert explicitly in
the expression (`int(x)`, `int(round(x))`), or add `allow_lossy: true` if
you accept what the [lossy table](expressions.md#types-and-conversions)
says will happen.

**`flow ... sets on_error ... but this runner applies on_error to the
whole run` (F0112).** Per-flow `on_error` and `max_errors` aren't
supported yet: set them in `runtime.yaml`.

## Running

**The run stopped with failed partitions (exit 6).** The stderr lines
`F0602` say which partition and why. Fix the cause, then
`dataferry retry --failed <run-id>`. If the fix is a change to the config,
the hash changes: start a new run instead.

**Records are rejected that look fine.** Read the reason in the
dead-letter table or in the error. Common ones: `int()` of text that isn't
exactly an integer (`' 3'`, `'3.0'`); a MongoDB field with mixed types,
read as `json`, so `int()` sees `"7"` with its quotes; a `NOT NULL` key
column with a null; a MongoDB document with a field the sample didn't see.

**`prepare` fails on a table that exists.** The existing table doesn't
fit what the flow writes: a column type that differs (an `integer`
column for an `int64` value), a `NOT NULL` column the flow doesn't write,
or, for `upsert`, no unique index on exactly the key columns. The message
lists every problem. Change the table, or the flow's types.

**`has no key, so re-running a partition can't replace the rows it wrote
before`.** Give the target a `key:`, or keep the state store on the sink
(`runtime.state.connection: <sink>`), the only case where a keyless
target can be written safely.

**`resume` is refused (exit 8).** The config's meaning or the dataferry
version changed since the run started. Tuning settings (`workers`,
`memory_budget`, `partition_rows`, the source limits) don't count, so you
can change those. Anything else: restore it, or start a new run.

**`resume` waits before doing anything.** After a crash or `kill -9`, the
dead process's leases have to expire first, at most 30 seconds. After a
Ctrl-C there is no wait.

**A run uses too much memory.** Lower `memory_budget` (it sizes batches
and read-ahead) or `workers`, and resume: both are tuning settings. A
merge that stages a large joined entity sorts it in a quarter of the
budget, spilling the rest to the work directory.

**A run is too slow, or too hard on the source.** More `workers` read more
partitions at once. `source_max_connections` and `source_rows_per_sec`
cap what the run asks of the sources. Keep the state store on the sink so
partitions aren't written twice after a crash.

## Verification

**Verification fails on tables that had rows before the run.** It
compares whole tables with what the run wrote, so rows it didn't write
count as differences. That is by design: it is meant for full loads into
empty tables. Load into empty tables, or read the report's Tables section
knowing that.

**`done partitions have no write-time record`.** The work directory of
the run was lost or changed (a different `--work-dir`, a new container).
Verification needs the records each partition wrote there: keep the work
directory for the life of a run, including across `resume`.

**An assertion fails.** The report shows both values. If the mapping is
right and the assertion is wrong, fix the assertion: that changes the
config hash, so start a new run.

## FAQ

**Which databases does it support?** MongoDB and PostgreSQL as sources,
PostgreSQL as the sink, in this release. Others can be added as
[connectors](extending.md) in your own build.

**Is it safe to stop a run?** Yes. Ctrl-C, a crash or a lost connection
loses at most the partitions in flight; `resume` continues from the
committed ones and writes nothing twice
([Running migrations](running.md)).

**Is it exactly-once?** For flows whose sink is the state store's
connection, yes: a partition's rows and its done-record commit in one
transaction. Other flows may write a partition twice after a crash, and
the second write replaces the first.

**Does it keep the target in sync with the source?** No. A run copies
what the source holds when it reads it. There is no change data capture,
and upserts never delete.

**Can several machines share a run?** No: one process runs a run. Use
`workers` for parallelism within it.

**Can I run it in CI or a container?** Yes. `validate` needs no database.
For `run`, keep the state store in Postgres (so it survives the container)
and the work directory on a persistent volume (verification needs it).

**Where are the logs?** There are none beyond what the commands print:
stdout for output, stderr for diagnostics. The report in
`.ferry/runs/<run-id>/` is the record of a run.

**How do I see what a run will do without running it?** `dataferry plan`.
It shows the DDL, the strategies and every type, and writes nothing.
