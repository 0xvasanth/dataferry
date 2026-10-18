# Split and unnest

A **split** writes one source record into several tables. An **unnest**
writes one row per element of an array. Together they turn a nested
document into a set of related tables.

## The worked example

Three MongoDB product documents, each with a nested `dimensions` object,
an array of `tags` (strings) and an array of `variants` (objects), each
variant with an array of `options`:

```json
{{#include ../examples/catalog/products.json}}
```

The edge cases: `p-2` has an empty `tags` array and a variant marked
`discontinued: false`; `p-3` has `dimensions: null` and no `variants` at
all; the second variant of `p-1` is discontinued; `p-3`'s price has three
decimal places.

The flow, [`docs/src/examples/catalog/ferry/flows/products.yaml`](https://github.com/0xvasanth/dataferry/blob/main/docs/src/examples/catalog/ferry/flows/products.yaml),
writes five tables. Taken a target at a time:

### One row per record

```yaml
{{#include ../examples/catalog/ferry/flows/products.yaml:1:5}}
{{#include ../examples/catalog/ferry/flows/products.yaml:products}}
```

A target with no `parent:` and no `unnest:` gets one row per record. Bare
names are the document's fields.

### A split: another table from the same record

```yaml
{{#include ../examples/catalog/ferry/flows/products.yaml:dimensions}}
```

`parent: products` makes this a child of `products`: it is written after
it, it sees the parent's mapped row as `$parent` (`$parent.id` is the
`products.id` column, already computed), and since it maps the parent's
whole key from `$parent`, it gets a foreign key to `products`. Without an
`unnest:`, a child has one row per parent row, and its scope is still the
document (`dimensions.width_cm`). Its `where:` keeps only products that
have dimensions: `p-3`'s `null` and `p-2`'s missing field both fail it.

### Unnest an array of scalars

```yaml
{{#include ../examples/catalog/ferry/flows/products.yaml:tags}}
```

`unnest: tags` writes one row per element of `tags`. `$index` is the
element's 0-based position and `$item` the element itself, for arrays of
scalars. An empty array, a missing field and `null` all give no rows, and
the parent row is still written.

### Unnest an array of objects

```yaml
{{#include ../examples/catalog/ferry/flows/products.yaml:variants}}
```

When the elements are objects, bare names are the element's fields
(`sku` is short for `$item.sku`). The target's `where:` drops the
discontinued variant. `discontinued` is missing in most variants, and a
missing value compares as null, so `coalesce()` gives it a default: a
plain `discontinued != true` would drop every variant without the field.

### Unnest inside an unnest

```yaml
{{#include ../examples/catalog/ferry/flows/products.yaml:options}}
```

The parent is `variants`, so `unnest: options` is resolved in the scope of
each variant, and `$parent` is the mapped variant row. `$index` is always
the innermost position. `$key` is a deterministic UUID for the row:
version 5, over the driver record's key, the unnest path with every outer
index, and `$index`. The same document always gets the same keys, however
it is partitioned and however often it is re-run, so `$key` is safe as a
primary key for rows that have no natural one.

Because the discontinued variant was filtered out of `variants`, its
options are dropped with it: a filter propagates to children through
`parent:`.

### Assertions

```yaml
{{#include ../examples/catalog/ferry/flows/products.yaml:expect}}
```

`source.sum(...)` adds up an expression over the documents the run
**wrote**, as it read them; the target side is queried after the run
([Verification](../verification.md#expect-assertions)).

### The result

`dataferry run`, then the five tables, as `psql` shows them:

```text
{{#include ../examples/catalog/expected.txt}}
```

`p-3`'s price of 7.125 is 7.13 after narrowing to `decimal(10,2)`, and
713 cents. `p-2` is in `products` with no tags and no dimensions. The
discontinued `LAMP-WHT` and its option are nowhere, and `p-3`, which has no
`variants` field, simply has no variant rows.

## The rules

### Split

- Each key under `to:` is a table. Write order follows `parent:`; tables
  with no relation are ordered by name.
- A record is written in full or not at all: if computing any row of any
  target fails, none of the record's rows are written.

### Unnest

- `unnest: <path>` is resolved in the scope of the target's parent: the
  parent's element if the parent is itself unnested, otherwise the driver
  record. A target with `unnest:` but no `parent:` unnests from the driver
  record and has no `$parent`.
- Elements of an empty array, a missing field or `null`: none, and the
  parent is still written. This isn't configurable.
- A value that isn't an array (`tags: "x"`) is an error: the record is
  rejected. If the source can't give a field one type (a MongoDB field
  that is an array in some documents and a string in others), it is read
  as `json`, and an unnest of it takes JSON arrays element by element and
  rejects anything else.
- A `null` element still gives a row, with a null `$item`.
- Paths use dots for nested fields (`unnest: shipping.parcels`). There is
  no indexing into an array (`items[0]`); `items[*].sku` in an expression
  is the list of every element's `sku`.
- `$index`, `$item` and `$key` exist only in unnest targets (`$item` only
  in one that unnests a path, not a `many` join), and `$parent` only in a
  target with `parent:`.

### The golden example

The [quickstart](../quickstart.md)'s flow is a split and an unnest in one:
`orders` from each document and `order_items` from its `items` array,
with `id: $key`, `order_id: $parent.id` and `line_no: $index`:

```yaml
{{#include ../../../examples/golden/ferry/flows/orders.yaml:31:42}}
```
