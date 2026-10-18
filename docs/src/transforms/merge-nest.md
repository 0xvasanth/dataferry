# Merge and nest

A **merge** reads several entities and writes rows built from all of them:
`from:` is a list whose first entry is the **driver** and whose other
entries are **joined** to it. A **nest** turns the rows of a `many` join
into an array column. An unnest of a `many` join writes them as child
rows instead.

## The worked example

The [`pg-merge` example](https://github.com/0xvasanth/dataferry/tree/main/examples/pg-merge)
builds one `order_summaries` row per order from three Postgres tables:

```sql
{{#include ../../../examples/pg-merge/source.sql}}
```

The flow:

```yaml
{{#include ../../../examples/pg-merge/ferry/flows/order_summaries.yaml}}
```

What `dataferry run` prints, and what lands in `reporting` (`customers` is
the 1:1 copy from [Copy a table](copy.md), which this flow waits for):

```text
{{#include ../examples/pg-merge/expected.txt}}
```

- Order 11 is cancelled, so the flow `where:` filters it, items and all.
- Order 12 has no items: its `items` is `[]`, never null.
- Order 13 has no customer (`customer_id` is null), so the `one` join
  finds nothing and `customer_name` is null. Its item with `qty: 0` is
  dropped from the array by the nest's `where:`.
- Order 10's items were inserted out of order, and come out sorted by
  `line_no`.

## `from:` as a list

```yaml
{{#include ../../../examples/pg-merge/ferry/flows/order_summaries.yaml:7:22}}
```

- **The driver** is the first entry, and has no `join:`. Every other entry
  has one. Every row the flow writes is attributed to one driver record,
  and partitions are cut on the driver.
- **`as:`** is the alias expressions use. It defaults to the last
  dot-separated part of the entity name (`orders` for
  `app.public.orders`), and must be unique in the flow. In the flow's
  columns, a bare name is a driver field: `status` and `o.status` are the
  same.
- **`join.keys:`** maps a column of *this* entry to a column of an
  earlier one, `{<column>: <alias>.<column>}`; each pair is an equality,
  and several pairs make a composite key. A join may reference the driver
  or an earlier `one` entry (orders → customers → regions), but not a
  `many` entry. Key types must be comparable (the same type, or a safe
  widening; text only with text).
- **Every join is a left join** from the driver: a driver record with no
  match is kept, with null joined fields. To drop it, filter on a joined
  field: `where: c.id != null`.
- Entries may come from different connections.

## Cardinality

`join.cardinality:` is required, and has no default:

- **`one`**: at most one match per driver record. Its fields are available
  as `<alias>.<field>` everywhere in the driver's scope, including the flow
  `where:`. If a second match turns up at run time, the driver record is
  rejected (an error row, then `on_error`): a `one` join never fans out.
- **`many`**: zero or more matches. A `many` alias can only be the source
  of a `nest:` column or of an `unnest:` target. Used anywhere else it would
  multiply the driver record, and the plan refuses it:
  `` `i` is a `many` join: one record has many rows of it ``.

## Join strategies

How a join is executed is chosen per join, and `plan` shows it with the
reason:

```text
{{#include ../examples/pg-merge-plan/expected.txt:38:44}}
```

- **Co-partitioned** (streamed): the joined entity is partitioned on its
  join column at exactly the driver's partition boundaries, and each pair
  of partitions is read and joined together, as one unit of work. Nothing
  is held in memory beyond the partition
  ([decision 0002](../reference/decisions/0002-co-partitioning.md)).
- **Staged**: the joined entity is read once, sorted and spilled to disk
  (in the work directory's `stage/`), and each driver partition looks its
  rows up there. Staged joins share a quarter of `memory_budget` for
  sorting.

`join.strategy:` picks one:

- `auto` (the default) co-partitions when all of these hold: the join has
  exactly one key pair; its right side is written exactly as the driver's
  alias and its single-column primary key (`keys: {order_id: o.id}`); the
  entry is on the driver's connection; and both columns are of the same
  kind that partitions can be cut on (integer, text or bytes). Otherwise
  it stages, and `plan` says which condition failed. If the source can't
  split a joined entity on the driver's boundaries, every co-partitioned
  join of that flow that `auto` chose is staged instead.
- `co_partition` insists on streaming, and fails the plan instead of
  falling back.
- `stage` always stages.

A cross-connection join can only be staged. Co-partitioned child
partitions can be very uneven (one order with 100,000 items), and that is
accepted in this release.

## `nest:` columns

```yaml
{{#include ../../../examples/pg-merge/ferry/flows/order_summaries.yaml:40:47}}
```

A nest column builds an array of objects from a `many` join:

- `nest:` names the `many` alias.
- `columns:` are the fields of each element, evaluated in the joined row's
  scope. Each is an expression or a typed column; a nest can't contain
  another nest in this release.
- `where:` drops joined rows from the array (a filter, not an error).
- `order_by:` is a list of expressions in the joined row's scope; add
  ` desc` (or ` DESC`) to sort one descending. The joined entity's primary
  key columns not already listed are always appended as the tie-breaker,
  so the array's order is deterministic under any partitioning, and `plan`
  prints the full ordering (`ordered by line_no, order_id`). Nulls sort
  last ascending and first descending.
- A driver record with no matches gets `[]`, never null.

The column's type is `list(struct(...))`. PostgreSQL stores it as `jsonb`
(keys in the order Postgres normalizes them, as the output above shows).

## Unnest a `many` join

`unnest: <many alias>` writes one row per joined row: the relational
version of unnesting an array. The
[`golden-pg` example](https://github.com/0xvasanth/dataferry/tree/main/examples/golden-pg)
reads the quickstart's orders as two Postgres tables and writes the same
`orders` and `order_items`:

```yaml
{{#include ../../../examples/golden-pg/ferry/flows/orders.yaml}}
```

```text
{{#include ../examples/golden-pg/expected.txt}}
```

In an unnest of a `many` alias, bare names are the joined row's fields,
rows come in the joined entity's primary-key order, and `$index` and
`$key` are available (`$item` is not: there is no element value, only a
row). Order `…0006` has an item whose `qty` is the text `three`, so
`int(qty)` rejects the whole order, which is dead-lettered.
