# Golden dataset: Mongo `orders` → Postgres `orders` + `order_items`

Ten source documents, each exercising one edge case, and the exact output a
correct run must produce. Connector, DSL and runner tests all check against
these files, so when a semantic decision changes, update the expected files
and this README together.

## The mapping

The spec's split/unnest example. It is written in the 0003 grammar in
[`examples/golden/ferry/flows/orders.yaml`](../../examples/golden/ferry/flows/orders.yaml);
each rule below names the line that implements it, and a test in
`ferry-dsl` (`tests/examples.rs`) checks that the line still says so.

| Rule | Line | Text on that line |
| --- | --- | --- |
| Keep a document only if `deleted_at == null` | 11 | `not_deleted: deleted_at == null` |
| `orders.id = string(_id)` | 23 | `id: string(_id)` |
| `orders.customer_id = customer.id` | 24 | `customer_id: customer.id` |
| `orders.status = lower(status)` | 25 | `status: lower(status)` |
| `orders.placed_at = created_at` | 27 | `expr: created_at` |
| `orders.total_cents = round(total * 100)` | 29 | `total_cents: int(round(total * 100))` |
| `order_items`: one row per element of `items[]` | 34 | `unnest: items` |
| `order_items.order_id = string(_id)` of the parent | 37 | `order_id: $parent.id` |
| `order_items.line_no` = array index | 38 | `line_no: $index` |
| `order_items.sku = sku` | 39 | `sku: sku` |
| `order_items.qty = int(qty)` | 40 | `qty: int(qty)` |
| `order_items.unit_cents = round(price * 100)` | 41 | `unit_cents: int(round(price * 100))` |

## Semantics these files pin down (provisional, owned by Track B)

| Decision | Chosen behavior | Affects |
| --- | --- | --- |
| `x == null` | true for an explicit `null` **and** for a missing field | `…0001`, `…0005` |
| Decimal rounding | half-up (away from zero), on decimals, never via float | `…0007` |
| Filtered parent | its children are dropped too (filters propagate) | `…0004` |
| Filter reason | the name of the first `where:` predicate that failed (`not_deleted`), or the expression text of an unnamed one (0003 §5.6) | `expected/filtered.json` |
| A child fails | the whole source document is dead-lettered, no rows written | `…0006` |
| Missing or empty array | no children, parent still written | `…0002`, `…0003` |
| Null or missing nested parent | the derived column is null | `…0008`, `…000a` |
| Generated child key | not asserted here; B5 adds `id` once UUIDv5 is settled. Rows are identified by `(order_id, line_no)` | all items |

Timestamps are UTC, microsecond precision.

## Records

| `_id` | Edge case | Outcome |
| --- | --- | --- |
| `650000000000000000000001` | Baseline: two items, no `deleted_at` field at all | order + 2 items |
| `650000000000000000000002` | `items` is an empty array | order, no items |
| `650000000000000000000003` | `items` field is missing | order, no items |
| `650000000000000000000004` | Soft-deleted (`deleted_at` set) with an item | filtered by `not_deleted`: no order, no item |
| `650000000000000000000005` | `deleted_at` explicitly `null` | order + 1 item |
| `650000000000000000000006` | Second item has `qty: "three"` (mixed type) | dead-lettered as a whole: no order, no items |
| `650000000000000000000007` | Money `1.005` as Decimal128: half-up gives 101 (float math gives 100) | order + 1 item |
| `650000000000000000000008` | `status: "Shipped"` (mixed case), `customer.email` missing | order + 1 item |
| `650000000000000000000009` | `created_at` missing | order with null `placed_at` + 1 item |
| `65000000000000000000000a` | `customer` is `null` | order with null `customer_id` + 1 item |
