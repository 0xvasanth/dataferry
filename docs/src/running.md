# Running migrations

This chapter is about `plan`, `run`, `resume` and `retry`: what they do,
what they print, and how a run survives being stopped.

## Before a run: `validate` and `plan`

`dataferry validate` reads the config and checks it without connecting to
anything; it is safe to run in CI with placeholder URLs.

`dataferry plan` goes further, read-only: it discovers every entity the
flows read, asks each sink for its capabilities, type-checks every
expression and prints, per flow, the driver and its key, the strategy,
the partition size, the joins and their strategies, the targets with their
columns and types, the assertions, and the DDL the sink would run. It
creates and writes nothing. Its output goes to stdout and its diagnostics
to stderr, so `dataferry plan > plan.txt` captures just the plan for a
review. The [quickstart](quickstart.md#5-plan) shows a full plan.

`run` plans exactly the same way first, so anything `plan` refuses, `run`
refuses before writing.

## `run`

```sh
dataferry run --config ferry
```

1. Plans the config.
2. Creates the run in the state store, pinning the config hash and the
   engine version, and prints `run <run-id>` (before any partition starts,
   so the id is on screen whatever happens next), then one line per flow
   with its strategy.
3. Prepares every flow, up front: its sink creates the missing tables and
   drops deferred constraints, and its partitions are planned and stored
   (on the first attempt only).
4. Runs the flows, in the order `plan` printed: a flow's partitions are
   leased once every flow it waits for has committed all of its own. Each
   partition is read, transformed and written in one transaction,
   committed once.
5. Once every flow has stopped, finalizes the flows whose partitions are
   all done (constraints and indexes dropped for the load are restored,
   foreign keys validated). A flow that didn't finish is finalized by a
   later `resume` or `retry`: until then, what its load dropped stays
   dropped.
6. Verifies each sink and writes the report.
7. Prints a summary and exits.

```text
{{#include examples/pg-merge/expected.txt:1:5}}
```

The summary line counts partitions, driver records read and dead-lettered,
and rows written per table (the dead-letter table aside). The exit code
says how it went: `0` complete and verified, `6` partitions failed, `7`
verification failed, `130` interrupted
([all exit codes](reference/cli.md#exit-codes)).

There is no progress display during a run in this release. `-q` hides the
per-flow strategy lines and the plan's warnings; the run id, the summary,
the verification lines and the warnings `W0601` and `W0701` are always
printed.

## Interrupting and resuming

Press Ctrl-C during a run and it stops at once: partitions in flight are
abandoned without committing, and their leases are released, so they can
be taken again immediately. The run exits `130` and says how to continue.
From a real run of 400,000 Mongo documents in 20 partitions, interrupted
after 5 had committed (a transcript: the guide's tests don't re-run this
one, since where an interrupt lands depends on timing):

```text
run 2b82553a45d04191abfb245b3d6de94c
  flow `events`: TxnWithState
error[F0603]: interrupted: in-flight partitions of run `2b82553a45d04191abfb245b3d6de94c` were abandoned, and 2 leases were released
   = help: continue it with `dataferry resume 2b82553a45d04191abfb245b3d6de94c`
```

`resume` continues from the committed partitions:

```sh
dataferry resume --config ferry 2b82553a45d04191abfb245b3d6de94c
```

```text
resuming run 2b82553a45d04191abfb245b3d6de94c
  flow `events`: TxnWithState
run 2b82553a45d04191abfb245b3d6de94c: complete; 20 partitions; read 400000, dead-lettered 0; wrote events 400000
verification of `wh` passed; report: ./.ferry/runs/2b82553a45d04191abfb245b3d6de94c/report.md
```

The target then held 400,000 rows with 400,000 distinct keys: the 100,000
rows of the first five partitions were not written again, and nothing of
the abandoned partitions had landed.

What `resume` does:

- It **plans again**, so the sources are discovered again and the config
  is checked again, then compares the config hash and the engine version
  with the ones the run pinned. If either changed, it refuses, exit `8`
  (from the same transcript):

  ```text
  error[F0801]: run `2b82553a45d04191abfb245b3d6de94c` was started with config hash sha256:71a5e90f626f0d56a4747328e38776672550b31f7e8d9aab3dacc5bbfce0e733, but the config now hashes to sha256:ec9f013ec3da5b9a274d39663467bb3f430af917de7435ff6aa8eba4fee8cf16: the config changed since the run started. Start a new run, or restore the config the run was started with
  ```

  Undo the change, or start a new run. Resume a run with the same
  dataferry version that started it.
- It **reuses the stored partitions**: partitions are planned once per
  run, so a resumed run covers exactly the key ranges the first attempt
  did, whatever `partition_rows` says now.
- It runs every partition that isn't done. **Failed partitions stay
  failed**: use `retry` for those.
- A run id the state store doesn't have is a usage error, exit `2`.
- Resuming a run that is already complete leases nothing, but still
  prepares every flow again (on tables that exist, deferred foreign keys
  and indexes are dropped and then restored by the finalize that follows),
  verifies again, and rewrites the report.

Interrupted while verifying (after the data is written), the run exits
`130` too, and `resume` verifies it again once there is nothing left to
run. Interrupted while still planning, nothing has been created yet: run
again.

### After a crash or `kill -9`

A process that dies without Ctrl-C can't release its leases. They expire
30 seconds after their last renewal, and a `resume` started before that
waits for them, checking every second, then leases those partitions at a
new epoch. Whatever the dead process had written for them but not
committed was rolled back by the database. If, by any chance, the dead
worker were still alive somewhere (a paused VM), epoch fencing stops it
from recording its partition done, and under `TxnWithState` from
committing its rows at all.

## `retry`

A run that finished with failed partitions exits `6` and says so:

```text
{{#include examples/fail-fast/expected.txt:3:6}}
```

Fix the cause (the data, the config's mapping if the hash allows it, a
permission), then:

```sh
dataferry retry --config ferry --failed <run-id>
```

`retry` checks the pins like `resume`, puts the failed partitions back to
pending, and resumes the run. Flows that didn't start because a flow they
wait for failed start now. Partitions that were done are not touched,
including the records they dead-lettered.

A failure in the mapping (an expression that rejects records under
`on_error: fail`) can't be fixed by a `retry` alone: changing the flow
changes the hash. Start a new run with the fixed config instead; with the
same keys, it overwrites the rows the first one wrote.

## Idempotency and exactly-once

Every way a run can stop leaves the target correct, because of how each
partition is written:

- **One transaction per partition.** A partition's rows in all its target
  tables (and its dead letters) are written in one sink transaction,
  committed once. A stopped run loses at most the partitions in flight,
  and those leave nothing behind.
- **`TxnWithState`** (when the state store is on the flow's sink): the
  record that a partition is done is written in that same transaction,
  after the epoch check. Rows and done-record commit together or not at
  all, so a partition is written **exactly once**.
- **`DeletePartitionThenInsert`** (any other placement of the state
  store): the done-record is written after the commit. A crash between
  the two re-runs the partition, and the sink first deletes the rows with
  the keys it is about to write, so the second write replaces the first.
  The result is the same; the work is done twice.

`plan` and `run` print each flow's strategy, and why when it isn't
`TxnWithState`. Keep `runtime.state.connection` on the sink you write to
for exactly-once. A run with flows into two sinks can have exactly-once
for the flows into the state store's connection only; the others fall back.

## Concurrency, memory and pacing

- **`workers`** (default: one per CPU core) is how many partitions are
  worked on at once, across all flows. Flows that don't wait for each other
  run at the same time and share the workers.
- **`partition_rows`** sizes partitions, and therefore transactions. Larger
  partitions mean fewer, longer transactions; a crash loses at most one
  partition per worker. A merge's co-partitioned child is cut at the
  driver's boundaries, whatever its own size.
- **`memory_budget`** (default 1 GiB) is what the runner sizes its writes
  and read-ahead from: each write batch is about an eighth of the budget
  divided by the workers (at least 64 KiB), and a quarter is shared out as
  read-ahead. Staged merge joins get a quarter of the budget for sorting,
  taken off the rest. A batch the source returns isn't split, so a run can
  exceed the budget by about one source batch.
- **`source_max_connections`** caps the read streams open at once across
  all sources; **`source_rows_per_sec`** caps the rows read per second
  across all sources. Use them to protect a production source.

All of these are tuning settings, left out of the config hash: change them
between attempts and `resume` still accepts the run
([Configuration](configuration.md#runtimeyaml)).

## Running it again

`dataferry run` always starts a **new run**, with a new id, new partitions
and a new report. On tables the previous run filled, `insert` replaces the
rows with the same keys, and verification fails if the tables then hold
rows this run didn't write (rows deleted from the source since, for
example). For a clean re-run, empty or drop the target tables first.

`run`, `resume` and `retry` are meant for one process per run and per
work directory. Don't run two processes on the same run, or two runs
writing the same tables, at once.

## Leases, for tests only

Leases last 30 seconds, renewed every 10. The environment variable
`FERRY_LEASE_TTL_MS` sets their length in milliseconds, from 1000 to
600000, with renewal at a third of it; any other value is a usage error
(exit `2`, `F0601`). It exists so tests can resume right after a kill, and
`run`, `resume` and `retry` warn (`W0601`) whenever it is set. Don't use it in production: a lease shorter than a slow commit or a
network stall expires under a live worker, which then loses its partition
(fenced, so nothing is written twice) and the run redoes or fails work it
had done.
