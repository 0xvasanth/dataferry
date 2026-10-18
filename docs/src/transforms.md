# Transforms

A flow reshapes records while it copies them. There are no `split:` or
`merge:` keywords: each shape is a way of writing `from:` and `to:`.

| Shape | How it is written | Page |
| --- | --- | --- |
| 1:1 copy | one `from:` entity, one target, often `columns: "*"` | [Copy a table](transforms/copy.md) |
| split | one `from:` entity, several targets under `to:` | [Split and unnest](transforms/split-unnest.md) |
| unnest | a target with `unnest:`: one row per array element | [Split and unnest](transforms/split-unnest.md) |
| merge | `from:` as a list: a driver, then entities joined to it | [Merge and nest](transforms/merge-nest.md) |
| nest | a column built from a `many` join, as an ordered array | [Merge and nest](transforms/merge-nest.md) |

They combine: one flow can merge three entities, write a summary table
and unnest a child table from the same driver record.

This page covers what every shape shares. Each shape's page has a worked
example that the project's tests run against real databases, with the
rows it writes.

## Anatomy of a flow

```yaml
{{#include examples/catalog/ferry/flows/products.yaml:1:5}}
{{#include examples/catalog/ferry/flows/products.yaml:products}}
```

| Key | Meaning |
| --- | --- |
| `name` | Unique across the config. |
| `from` | The driver entity, `<connection>.<entity>`, or a merge list. |
| `sink` | The one connection the flow writes to. |
| `mode` | `insert` (default) or `upsert`. |
| `where` | A filter on the driver record. |
| `depends_on` | Flows that must finish first ([Configuration](configuration.md#ordering-flows)). |
| `runtime` | Per-flow settings (`partition_rows`). |
| `to` | The targets: table name → target. |
| `expect` | Assertions checked after the run ([Verification](verification.md)). |

A target (a key under `to:`) is a table name, without a schema: the
sink connection decides the schema (Postgres `schema:`, default `public`).
One table can be written by only one flow.

| Target key | Meaning |
| --- | --- |
| `columns` | `"*"`, or target column → column spec, in DDL order. Required. |
| `key` | The table's primary key columns. |
| `parent` | Another target of this flow that this one is a child of. |
| `unnest` | An array path, or a `many` join alias: one row per element. |
| `where` | A filter on this target's rows. |

## Columns

`columns:` is an ordered map from target column to a **column spec**. Its
order is the column order of the table dataferry creates. A spec is one
of:

- **An expression**, as a string: `status: lower(status)`. Its type is
  inferred, and becomes the column's type.
- **A typed column**, `{expr, type, nullable, allow_lossy}`, when the type
  matters. `type:` is a [canonical type](expressions.md#types-and-conversions)
  (`int64`, `decimal(12,2)`, `utf8`, `timestamp(UTC)`, `uuid`, `json`, …).
  `nullable: false` makes the column `NOT NULL`. If converting the
  expression's type to `type:` can lose information, the plan refuses it
  unless the column says `allow_lossy: true`.
- **A nest column**, `{nest: <many alias>, columns: ...}`, built from a
  `many` join ([Merge and nest](transforms/merge-nest.md)).
- **`columns: "*"`**, instead of a map: every field of the entity,
  unchanged ([Copy a table](transforms/copy.md)).

A typed column from the [catalog example](transforms/split-unnest.md): the
source's `price` is a `decimal(38,3)`, and the column narrows it to two
decimal places, rounding half away from zero (`7.125` becomes `7.13`):

```yaml
{{#include examples/catalog/ferry/flows/products.yaml:typed}}
```

## Scopes: what a bare name means

Every target evaluates its expressions in one scope:

| Target | A bare name is a field of |
| --- | --- |
| no `unnest:` | the driver record (in a merge: driver fields, plus `alias.field` for `one` joins) |
| `unnest: <path>` | the current array element |
| `unnest: <many alias>` | the current joined row |
| inside a nest column | the current nested row |

Sigils reach the rest: `$parent` is the parent target's row, `$index` the
element's position, `$item` the element itself and `$key` a generated key
([Expressions](expressions.md#sigils)). An unnested target sees the
element, not the driver record: it reaches values of the record through
`$parent`, the parent's mapped columns, so a value parent and child share
is mapped once, in one place.

## Keys and modes

`key:` lists target columns. They become the table's primary key, and
are `NOT NULL`. There is no hidden or generated key: a child that needs a
surrogate key maps one, for example `id: $key`.

`mode:` is set per flow:

- **`insert`** (the default): the run creates the target rows. Re-running
  a partition is made safe by the flow's
  [idempotency strategy](concepts.md#idempotency-strategies). On
  PostgreSQL, a partition's rows replace any rows already in the table
  with the same key (the sink deletes them first), so a run into tables
  that already hold those keys overwrites them rather than failing.
- **`upsert`**: rows may already exist and are updated by key (`INSERT ...
  ON CONFLICT ... DO UPDATE`). Every target must have `key:`. Children of
  a parent are replaced: for each parent row in a batch, its existing
  child rows are deleted and the new ones written. Upsert never deletes
  rows for records a filter excludes, and never propagates deletes from the
  source.

Keys matter for re-runs. In `insert` mode a target without `key:` is
allowed, and `plan` warns about a child target without one, but a
PostgreSQL sink can only write a keyless table under `TxnWithState`
(state store on the sink): with any other strategy, a re-run couldn't
tell which rows to replace, and the sink refuses the flow. Give every
target a key.

The replacement of child rows relies on the foreign key the planner
derives from `parent:` (below): it exists only when the child maps each of
its parent's key columns as exactly `$parent.<column>`.

## Parents and write order

`parent: <table>` makes a target the child of another target of the same
flow. That one declaration drives three things:

- **Write order.** Parents are written before their children (ordered by
  `parent:`, ties broken by table name), whatever the order of keys in
  the file. `plan` prints it: `write order: products → product_dimensions
  → product_tags → variants → variant_options`.
- **Foreign keys.** If a child maps every key column of its parent from
  `$parent.<column>`, the planner adds a foreign key from those columns to
  the parent's key, created and validated at the end of the load.
- **Filter propagation**, below.

A `parent:` cycle, or a parent that isn't a target of the flow, is an
error.

## Filters: `where:`

`where:` is one expression, or an ordered map of named predicates that
must all hold:

```yaml
{{#include ../../examples/golden/ferry/flows/orders.yaml:10:11}}
```

- A record or row is kept only when every predicate is **true**. A
  predicate that is null counts as false, so `discontinued != true` drops
  rows where `discontinued` is null or missing; write
  `not coalesce(discontinued, false)` to keep them.
- A predicate that **fails** to evaluate (an error, not false) doesn't
  filter: the record is rejected.
- Reports name the first predicate that failed (`not_deleted`), or the
  expression text of an unnamed `where:`.

There are two levels, and one rule connects them:

- **A flow `where:`** applies to the driver record (in a merge, with its
  `one` joins). A record that fails it is **filtered**: it has no rows in
  any target, and is counted once.
- **A target `where:`** applies to that target's rows, in the target's
  scope, and is evaluated on the source values, before the columns are
  mapped.
- **Propagation:** when a row is filtered out of a target, every row that
  reaches it through `parent:` (children, grandchildren) is dropped with
  it. Filters never travel upward or sideways: a sibling target keeps its
  rows, and the record isn't rejected.

## Errors

Any error computing any row of a record (a failed conversion, a null in a
`NOT NULL` column, an array that isn't one) **rejects the whole record**:
none of its rows in any target are written. Other records are not
affected. What happens next is the run's `on_error` policy
([Errors and dead letters](errors.md)).
