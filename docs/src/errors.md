# Errors and dead letters

Real data has records that don't fit the mapping: a quantity of
`"three"`, a missing email in a column that can't be null. dataferry never
drops such a record silently. It **rejects** it, with a reason, and the
run's `on_error` policy decides what happens next: stop, or keep the
record in a dead-letter table and carry on.

## What rejects a record

A record is rejected, as a whole (none of its rows in any target are
written), when:

- an expression fails for it: a strict conversion (`int('ten')`), a value
  that doesn't fit a lossy typed column, an error in a `where:`
  predicate, a `one` join that finds two matches, or an `unnest:` of a
  value that isn't an array;
- a value is null in a column that can't be null (a key column, or one
  declared `nullable: false`);
- the source can't read it as its entity: for MongoDB, a document whose
  field has a type the sampled schema doesn't allow, or a field the sample
  didn't see ([MongoDB](connectors/mongo.md#schema-inference)).

A record that a `where:` excludes is **filtered**, not rejected: that is
the mapping working as written, and it is only counted.

## `on_error: fail`

The default. The first rejected record fails its partition: nothing of
that partition is committed, the run leases no new partitions, the ones in
flight finish, and the run exits `6`.

```yaml
{{#include examples/fail-fast/ferry/runtime.yaml:policy}}
```

With the guide's `signups` example (five documents, two of which can't be
mapped):

```text
{{#include examples/fail-fast/expected.txt}}
```

Fix the data or the mapping, then start again. A changed flow is a new
config hash, so it needs a new run; with the data fixed and the config
unchanged, `dataferry retry --failed <run-id>` re-runs only the failed
partitions.

## `on_error: dead_letter`

```yaml
{{#include examples/dead-letters/ferry/runtime.yaml:policy}}
```

A rejected record is written to the table `_ferry_dlq` on the flow's sink,
**in the partition's own transaction**, and the run carries on. The same
five signups:

```text
{{#include examples/dead-letters/expected.txt}}
```

The run is complete and exits `0`: three accounts are written, and two
records are dead-lettered with the reason. The summary line and the report
count them.

Only **driver** records can be dead-lettered. If a record of a joined
entity can't be read, its partition fails, whatever `on_error` says.

### `max_errors`

`max_errors` is how many records the whole run may dead-letter. Once
committing a partition would bring the run's total **above** it, that
partition is rolled back (its rows and its dead letters), marked failed,
and the run stops with exit `6`:

```yaml
{{#include examples/max-errors/ferry/runtime.yaml:policy}}
```

```text
{{#include examples/max-errors/expected.txt}}
```

- The count is run-wide and includes the partitions committed by earlier
  attempts of the same run: a resumed run doesn't get a fresh budget.
- `max_errors` is in the config hash, so you can't raise it to resume a
  run: that is a new run.
- Without `max_errors`, nothing stops a dead-lettering run, and `plan`
  warns about it (`W0401`).
- In this release, `on_error` and `max_errors` are **run-wide**. A flow
  may repeat them in its `runtime:` only with the run's values; a
  different value is refused by `plan`, so it is never silently ignored.
  Per-flow policies and budgets are planned for a later release.

A run that dead-letters records but stays within `max_errors` still
exits `0`. To fail a CI job when any record didn't land, assert the count
you expect: `count(accounts) == 5` fails, with exit `7`, as soon as one of
the five signups is missing
([Verification](verification.md#a-failing-assertion)).
`count(accounts) == source.count()` can't catch it, because `source.`
aggregates cover only the records the run wrote.

## The `_ferry_dlq` table

Created in each sink's schema for flows with `on_error: dead_letter`, with
the DDL `plan` shows:

| Column | What it holds |
| --- | --- |
| `flow` | The flow that rejected the record. |
| `source_id` | The record's key, as a JSON array of its key values: `[4]`, `["650000000000000000000006"]`. |
| `run` | The run id. |
| `partition` | The partition, as stored (`orders/orders-1`). |
| `target` | The target whose row failed, or null when the record failed as a whole. |
| `error` | Why, in the words the report uses. |
| `record` | The source record as it was read, as JSON (`jsonb`). |

The primary key is `(flow, source_id)`: one row per record and flow. A
record dead-lettered again (by a re-run partition, or a later run)
replaces its earlier row, so the table never fills up with duplicates.

### Inspecting dead letters

It is an ordinary table:

```sql
-- what went wrong in a run, grouped
select flow, target, error, count(*)
from _ferry_dlq
where run = '<run-id>'
group by flow, target, error
order by count(*) desc;

-- the records themselves
select source_id, record
from _ferry_dlq
where flow = 'accounts' and error like '%seats%';
```

The report's **Dead letters** section shows the same grouping, with an
example `source_id` per reason, and checks that `_ferry_dlq` holds exactly
the number of records the state store counted for the run.

Three things to know about `record`:

- For a record the mapping rejected, it includes the column
  `_ferry_missing`: the list of fields that were absent from a MongoDB
  document (as opposed to null). It is how the engine tracks
  [missing values](expressions.md#null-and-missing).
- For a MongoDB document the source itself rejected (it doesn't fit the
  inferred schema), it is the document as relaxed Extended JSON, without
  `_ferry_missing`, and `target` is null.
- A MongoDB field read as `json` (because its type varies between
  documents) holds its value's JSON text, as a string. In the
  [quickstart](quickstart.md)'s dead letter, `qty: "three"` shows up as
  `"qty": "\"three\""`.

### Re-running dead-lettered records

There is no command to replay dead letters in this release. Fix the
records in the source (or the mapping), then run again: a new run reads
every record again, writes those that now map, and dead-letters the rest
again, replacing their rows in `_ferry_dlq`. Rows of records that now
map stay in `_ferry_dlq` from the earlier run until you delete them:

```sql
delete from _ferry_dlq where run = '<old-run-id>';
```

## Other failures

Not every failure is a record's fault:

| What | Exit | What to do |
| --- | --- | --- |
| A config error | `3` | Fix the config; `validate` lists every problem. |
| A plan refusal: an unknown field, a type mismatch, no strategy for a flow's mode | `4` | Fix the config, or the source schema. |
| A source that can't be reached or whose schema can't be read, or a state store that can't be reached | `5` | Check the URL and credentials; for schema problems, the message names the table. |
| A sink that can't be reached: it is first contacted when the run prepares its flows | `6` | Check the URL and credentials, then `resume <run-id>`. |
| A partition fails for another reason (a sink rejects a write, a lost connection mid-run) | `6` | Fix the cause, then `retry --failed <run-id>`. |
| Verification fails | `7` | The data is written: read the report. |
| `resume`/`retry` refused because the config or version changed | `8` | Restore the config, or start a new run. |

[Troubleshooting](troubleshooting.md) goes through the common ones.
