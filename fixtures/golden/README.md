# Golden dataset: Mongo `orders` → Postgres `orders` + `order_items`

Ten source documents, each exercising one edge case, and the exact output a
correct run must produce. Connector, DSL and runner tests all check against
these files, so when a semantic decision changes, update the expected files
and this README together.

## The mapping

This is the spec's split/unnest example, described in words so the fixture
doesn't depend on DSL syntax that Track B hasn't settled yet:

- Keep a document only if `deleted_at == null`.
- `orders`: `id = string(_id)`, `customer_id = customer.id`,
  `status = lower(status)`, `placed_at = created_at`,
  `total_cents = round(total * 100)`.
- `order_items`: one row per element of `items[]`, with
  `order_id = string(_id)` of the parent, `line_no` = array index,
  `sku`, `qty = int(qty)`, `unit_cents = round(price * 100)`.

## Semantics these files pin down (provisional, owned by Track B)

| Decision | Chosen behavior | Affects |
| --- | --- | --- |
| `x == null` | true for an explicit `null` **and** for a missing field | `…0001`, `…0005` |
| Decimal rounding | half-up (away from zero), on decimals, never via float | `…0007` |
| Filtered parent | its children are dropped too (filters propagate) | `…0004` |
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
| `650000000000000000000004` | Soft-deleted (`deleted_at` set) with an item | filtered: no order, no item |
| `650000000000000000000005` | `deleted_at` explicitly `null` | order + 1 item |
| `650000000000000000000006` | Second item has `qty: "three"` (mixed type) | dead-lettered as a whole: no order, no items |
| `650000000000000000000007` | Money `1.005` as Decimal128: half-up gives 101 (float math gives 100) | order + 1 item |
| `650000000000000000000008` | `status: "Shipped"` (mixed case), `customer.email` missing | order + 1 item |
| `650000000000000000000009` | `created_at` missing | order with null `placed_at` + 1 item |
| `65000000000000000000000a` | `customer` is `null` | order with null `customer_id` + 1 item |
