# Copy a table 1:1

The simplest flow copies one entity into one table, unchanged. In a large
migration most tables are like this, and each takes a few lines.

## `columns: "*"`

From the [`pg-merge` example](https://github.com/0xvasanth/dataferry/tree/main/examples/pg-merge),
which copies a Postgres `customers` table into a reporting database:

```yaml
{{#include ../../../examples/pg-merge/ferry/legacy/customers.yaml}}
```

`columns: "*"` writes every field of the entity, with its name and type,
in the order the source declares them. `dataferry plan` shows the result:

```text
{{#include ../examples/pg-merge-plan/expected.txt:13:31}}
```

The rules:

- `"*"` is allowed only in a flow with a single `from:` entity, on a
  target with no `parent:` and no `unnest:`.
- It can't be combined with other columns in this release: to rename,
  convert or add one column, list them all.
- Types map from the source's to the sink's through dataferry's canonical
  types ([PostgreSQL](../connectors/postgres.md#types),
  [MongoDB](../connectors/mongo.md#types)). Copying from MongoDB works the
  same way, with the fields schema inference found.

## Renaming, converting, filtering

Listing the columns gives you control over each:

```yaml
{{#include ../examples/catalog/ferry/flows/products.yaml:products}}
```

Here `_id` becomes `id`, `price` is narrowed to two decimal places, and
two columns are computed. A flow `where:` would drop records, and a
target `where:` rows ([Transforms](../transforms.md#filters-where)).

## Copying into tables that exist

dataferry creates each target table that doesn't exist, with the columns,
types and primary key the plan shows. If a table already exists, it must
fit: every column the flow writes must exist with a compatible type, and
any column the flow doesn't write must be nullable or have a default. A
mismatch fails before anything is written, with every problem listed
([PostgreSQL](../connectors/postgres.md#tables-that-already-exist)).

Two things to know before loading into a table that already holds rows:

- **Rows with the same key are replaced**, in either mode: under `insert`
  the sink deletes the target rows whose keys a partition writes, then
  inserts; under `upsert` it updates them in place (`INSERT ... ON
  CONFLICT DO UPDATE`). Rows with other keys are left alone. A target
  without `key:` can only be written under `TxnWithState`.
- **Verification assumes empty targets.** It compares whole tables with
  what the run wrote, so rows the run didn't rewrite make it fail
  ([Verification](../verification.md#what-it-assumes)).

For repeated loads into the same tables, use `mode: upsert` and expect
verification to report the rows already there that the run doesn't rewrite.
