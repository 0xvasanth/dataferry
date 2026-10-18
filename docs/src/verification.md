# Verification

After the flows of a run have committed, `run` (and `resume`, and `retry`)
verifies every sink it wrote to, and writes a report. There is no separate
`verify` command in this release: to verify a finished run again, `resume`
it, which finds nothing left to run and verifies.

## What it proves, and what it doesn't

The report says it in its first paragraphs, and it is worth reading once:

> Verification proves that what the run computed is what landed: every row
> the runner handed the sink for a done partition is in the target,
> exactly once, with the same values after normalization, and the target
> holds no rows the run didn't write. Counts are what the run read,
> recorded in the state store at read time, so changes to the source after
> it was read don't affect them.
>
> It does not prove the mapping is correct: a wrong expression computes
> wrong values, and they land faithfully. Only `expect:` assertions check
> the mapping.

Verification never queries the sources again. It compares the targets
with what the run recorded while it wrote: per partition, a checksum and a
count of the rows of each table, kept in the work directory (`writes/`)
and in the state store.

## The checks

| Report section | What is checked |
| --- | --- |
| **Counts** | Per flow, every driver record read is accounted for: written + filtered + dead-lettered = read. Any difference is reported as *unaccounted*. The state store's counts must match the write-time records. |
| **Tables** | Per table: the rows the sink reported committed equal the rows handed to it; the table holds as many rows as were written; and the checksum of the rows read back equals the checksum taken at write time. |
| **Constraints** | On the target: every non-nullable column has no nulls, every key is unique, and every foreign key the plan declared has no orphans. |
| **Finalize** | Constraints and indexes dropped for the load were all restored. Shown only when something wasn't. |
| **Assertions** | Every `expect:` assertion holds, with both values shown. |
| **Dead letters** | `_ferry_dlq` holds exactly as many rows for this run as the state store counted dead-lettered. |

The checksum is a 128-bit hash per row, over the mapped columns, added up
per table, so it doesn't depend on row order or partitioning. Values are
normalized before hashing, so representation differences don't count:
integers and decimals of equal value hash alike, timestamps are compared as
instants at the target column's precision, `-0.0` equals `0.0`, and JSON
is compared with its keys sorted, whitespace aside.

The result is **passed** when there are no problems, **failed** when there
are any, and **incomplete** when some partition isn't done (the run failed
or was interrupted): the report is still written, with the counts so far.

`run` exits `7` when verification failed: the data is written, and the
report says what doesn't match. A run with failed partitions exits `6`
instead, which takes precedence.

## `expect:` assertions

Verification shows that the data landed as computed. Assertions check
that what was computed is right. They are named, and compare two
aggregates:

```yaml
{{#include ../../examples/golden/ferry/flows/orders.yaml:44:47}}
```

An assertion is `<side> <op> <side>`, with `==`, `!=`, `<`, `<=`, `>` or
`>=`. Each side is a number, a text in single quotes, an aggregate, or
arithmetic on those (`+ - * /`, `abs(...)`, `round(..., digits)`,
parentheses): `abs(sum(orders.total_cents) - source.sum(int(round(total * 100)))) <= 1`.

**Target side**, computed by querying the target after the run:

| Aggregate | Meaning |
| --- | --- |
| `count(<table>)` | Rows in the table. `count(*)` when the flow has one target. |
| `count(<table>.<column>)` | Non-null values. |
| `sum`, `min`, `max`, `count_distinct` of `<table>.<column>` | As in SQL. `sum` takes numbers; `min` and `max` numbers or text. |

When a flow has several targets, columns must be qualified with their
table (`orders.total_cents`); with one target the table may be left out.

**Source side**, accumulated while the run reads:

| Aggregate | Meaning |
| --- | --- |
| `source.count()` | Driver records written. |
| `source.count(<expr>)`, `source.sum(<expr>)`, `source.min(<expr>)`, `source.max(<expr>)` | Over the driver records written, with an expression in the driver's scope, as the source read them (before any join). |

Source aggregates cover the driver records the run **wrote**: not those it
filtered or rejected. That makes an assertion a check of the mapping (did
the cents add up for the orders we wrote?), while the Counts section
accounts for filtered and dead-lettered records separately. A partition
that is retried counts once.

Aggregates skip nulls: a `sum` of no values is null, a `count` is 0. A
comparison with a null side holds only for `!=`. Integers and decimals are
compared exactly; `/` gives a float.

`plan` checks every assertion against the targets and the source's
fields, so a misspelled column or table fails before the run, with a
suggestion. Not allowed, each with its own error: `and`/`or` between
comparisons, `avg` (divide a `sum` by a `count`), comparing with `null`,
`source.count_distinct`, and a bare column where an aggregate belongs.

### A failing assertion

The [dead letters example](errors.md#on_error-dead_letter), with two
assertions added:

```yaml
{{#include examples/expect-fails/ferry/flows/accounts.yaml:expect}}
```

Two of the five signups are dead-lettered, so the first fails, and the run
exits `7` though everything that was written verified:

```text
{{#include examples/expect-fails/expected.txt}}
```

The second holds: 8 seats were written, and the source side adds up the
seats of the three records written.

## What it assumes

Verification and assertions look at **whole tables**: the checksum covers
every row of a target, and `count(orders)` counts every row. That is right
for the job dataferry is built for, a full load into empty targets, and it
is why the report says *the target holds no rows the run didn't write*.

Loading into tables that already hold rows the run doesn't rewrite (an
`upsert` into a live table, a second run after rows were deleted from the
source) makes the table checks fail, by design: those rows are rows this
run didn't write. A second run over an unchanged source verifies, since it
rewrites every key the first one wrote. Assertions count whole tables too.
Scoping verification to the keys a run wrote is planned, not built.

## Several sinks

Each sink is verified on its own, against its flows' share of the run.
A run with one sink writes `report.md` and `report.json`; a run writing
to several writes `report-<sink>.md` and `report-<sink>.json` for each.

A sink whose type this binary can't read back (a third-party connector
without a verifier) is reported as not verified (`W0701`), which doesn't
change the exit code. What finalize couldn't restore on it still fails the
run.

## The report files

In the work directory, under `runs/<run-id>/`:

- **`report.md`**, for people: the verdict (`# Run <run-id>: verification
  PASSED`, `FAILED` or `INCOMPLETE`), what it proves, then the sections
  **Run** (config hash, versions, connections by name and type, never
  their URLs, timings, partition counts), **Problems** (when there are
  any), **Counts**, **Tables**, **Constraints**, **Assertions**,
  **Finalize** (when needed) and **Dead letters**. Values are escaped and
  cut to 200 characters. The [quickstart](quickstart.md#8-read-the-report)
  shows a complete one.
- **`report.json`**, for tools, format `ferry-report/1`: `status`
  (`passed`, `failed`, `incomplete`), `run` (`id`, `config_hash`,
  `engine_version`, `connections`), `versions`, `target`, `timings`,
  `partitions` (`pending`, `leased`, `done`, `failed`), `counts` (`read`,
  `dead_lettered`, `written` per table, and per flow `read`, `records`
  written/filtered/rejected and `unaccounted`), `tables`, `constraints`,
  `assertions`, `finalize`, `dead_letter` and `problems`. Values are kept
  whole.

Everything in both files goes through the same redaction as the console:
no secret appears in a report.
