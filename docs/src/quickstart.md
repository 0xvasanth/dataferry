# Quickstart

This walks through a real migration on your machine: ten MongoDB order
documents become rows in two PostgreSQL tables, `orders` and
`order_items`. It takes about five minutes and needs:

- `dataferry` on your `PATH` ([Installation](installation.md)),
- Docker, to run a throwaway MongoDB and PostgreSQL,
- `git`, to fetch the example config and data.

Every command and every output on this page is checked by the project's
tests against real containers, so what you see here is what you should
get. Run ids, times and the order of a few lines in `docker` output will
differ.

## 1. Get the example

The example lives in the repository: the config in
`examples/golden/ferry/` and the ten documents in
`fixtures/golden/source/orders.json`.

```sh
git clone --depth 1 https://github.com/0xvasanth/ferry
cd ferry
```

## 2. Start the databases

A MongoDB 7 and an empty PostgreSQL 16, on their usual ports. The password
is a throwaway for a container on your machine:

```sh
docker run -d --name dataferry-mongo -p 27017:27017 mongo:7
docker run -d --name dataferry-pg -p 5432:5432 -e POSTGRES_PASSWORD=quickstart-pw postgres:16
```

Wait until both accept connections, then load the documents into the
`orders` collection of a `shop` database:

```sh
until docker exec dataferry-mongo mongosh --quiet --eval 'db.runCommand({ ping: 1 })' >/dev/null 2>&1; do sleep 1; done
until docker exec dataferry-pg pg_isready -h 127.0.0.1 -U postgres >/dev/null 2>&1; do sleep 1; done
docker exec -i dataferry-mongo mongoimport --quiet --db shop --collection orders --jsonArray < fixtures/golden/source/orders.json
```

If ports 5432 or 27017 are taken on your machine, publish different ones
(`-p 15432:5432`) and use them in the URLs below.

## 3. Point the config at them

The config never holds credentials. Its `connections.yaml` names two
environment variables:

```yaml
{{#include ../../examples/golden/ferry/connections.yaml}}
```

Set them:

```sh
export FERRY_GOLDEN_MONGO_URL=mongodb://localhost:27017
export FERRY_GOLDEN_PG_URL=postgres://postgres:quickstart-pw@localhost:5432/postgres
```

## 4. Validate, offline

`validate` loads every file of the config, substitutes the variables and
checks each connection's options and every flow, without connecting to
anything:

```sh
dataferry validate --config examples/golden/ferry
```

```text
`golden_orders` is valid: 2 connections, 1 flow
```

With `-v`, it also says how it understood each connection. Secrets never
show: the Mongo URL came from the environment, so all of it is `***`, and
the Postgres one is shown without its password:

```sh
dataferry -v validate --config examples/golden/ferry
```

```text
`golden_orders` is valid: 2 connections, 1 flow
  shop (mongo, used as source): ***, database shop
  warehouse (postgres, used as sink): postgres://postgres@localhost:5432/postgres, schema public
```

The flow it found, `examples/golden/ferry/flows/orders.yaml`, is the whole
migration. Each key is explained in [Configuration](configuration.md) and
[Transforms](transforms.md); the comments say what each line does to the
ten documents:

```yaml
{{#include ../../examples/golden/ferry/flows/orders.yaml}}
```

## 5. Plan

`plan` connects read-only: it samples the Mongo collection to learn its
fields and types, asks Postgres what it can do, type-checks every
expression and prints what a run would do, including the DDL. It writes
nothing:

```sh
dataferry plan --config examples/golden/ferry
```

```text
plan for `golden_orders`, config sha256:c4e1f41657f0c914abccab48976f2122ca2041b6769bd83a031f758b787109e4
state store: postgres, on connection `warehouse`
on_error: dead_letter, max_errors: 10

connections
  shop       mongo (source): ***, database shop
  warehouse  postgres (sink, state store): postgres://postgres@localhost:5432/postgres, schema public

run order
  1. orders

flow `orders`
  from:        shop.orders (driver, key _id)
  sink:        warehouse, mode insert
  strategy:    TxnWithState
  partitions:  about 4 rows each (the flow's runtime:), stored as `orders/<partition>`
  where:       not_deleted: deleted_at == null
  write order: orders → order_items
  targets:
    orders (key id)
      id           utf8
      customer_id  utf8, nullable
      status       utf8, nullable
      placed_at    timestamp(UTC), nullable
      total_cents  int64, nullable
    order_items (key order_id, line_no; parent orders; unnest items)
      id          uuid
      order_id    utf8
      line_no     int64
      sku         utf8, nullable
      qty         int64, nullable
      unit_cents  int64, nullable
      foreign key (order_id) → orders (id)
  expect:
    order_count: count(orders) == source.count()
    order_total: sum(orders.total_cents) == source.sum(int(round(total * 100)))
    item_count: count(order_items) == source.sum(coalesce(cardinality(items), 0))
  ddl (not run by plan):
    CREATE SCHEMA IF NOT EXISTS "public";
    CREATE TABLE "public"."orders" (
      "id" text NOT NULL,
      "customer_id" text,
      "status" text,
      "placed_at" timestamp with time zone,
      "total_cents" bigint,
      PRIMARY KEY ("id")
    );
    CREATE TABLE "public"."order_items" (
      "id" uuid NOT NULL,
      "order_id" text NOT NULL,
      "line_no" bigint NOT NULL,
      "sku" text,
      "qty" bigint,
      "unit_cents" bigint,
      PRIMARY KEY ("order_id", "line_no")
    );
    CREATE TABLE "public"."_ferry_dlq" (
      "flow" text NOT NULL,
      "source_id" text NOT NULL,
      "run" text NOT NULL,
      "partition" text NOT NULL,
      "target" text,
      "error" text NOT NULL,
      "record" jsonb,
      PRIMARY KEY ("flow", "source_id")
    );
    ALTER TABLE "public"."order_items" ADD CONSTRAINT "order_items_order_id_fkey" FOREIGN KEY ("order_id") REFERENCES "public"."orders"("id") NOT VALID;
    ALTER TABLE "public"."order_items" VALIDATE CONSTRAINT "order_items_order_id_fkey";
```

A few things to notice:

- The Mongo URL shows as `***`: every value that came from the environment
  is redacted from everything dataferry prints. The Postgres one is shown
  without its password.
- `strategy: TxnWithState` is the exactly-once strategy: each partition's
  rows and the record that it is done commit in one transaction, because
  the run's state store is on the same connection as the sink
  (`runtime.yaml`, [Running migrations](running.md#idempotency-and-exactly-once)).
- The flow is split into partitions of about four documents, so ten
  documents make three partitions, each committed on its own.
- `_ferry_dlq` is the dead-letter table, created because the config says
  `on_error: dead_letter` ([Errors and dead letters](errors.md)).

## 6. Run

```sh
dataferry run --config examples/golden/ferry
```

```text
run ef3a454bec5a4a0ca03e74dc1ffcb6e0
  flow `orders`: TxnWithState
run ef3a454bec5a4a0ca03e74dc1ffcb6e0: complete; 3 partitions; read 10, dead-lettered 1; wrote order_items 7, orders 8
verification of `warehouse` passed; report: examples/golden/.ferry/runs/ef3a454bec5a4a0ca03e74dc1ffcb6e0/report.md
```

The first line is the run id. Keep it: `resume` and `retry` take it. The
exit code is `0`.

Ten documents were read. One (`…0004`) was filtered by the flow's `where:`
because it is soft-deleted, one (`…0006`) was dead-lettered because an item
has `qty: "three"`, and eight were written, with seven items between them.
Then the target was read back and verified.

## 7. Look at the result

```sh
docker exec dataferry-pg psql -U postgres -c 'select * from orders order by id'
```

```text
            id            | customer_id | status  |       placed_at        | total_cents 
--------------------------+-------------+---------+------------------------+-------------
 650000000000000000000001 | c1          | paid    | 2026-03-01 10:00:00+00 |        2550
 650000000000000000000002 | c1          | paid    | 2026-03-02 10:00:00+00 |           0
 650000000000000000000003 | c2          | pending | 2026-03-03 10:00:00+00 |        1200
 650000000000000000000005 | c2          | paid    | 2026-03-05 10:00:00+00 |         725
 650000000000000000000007 | c1          | paid    | 2026-03-07 10:00:00+00 |         101
 650000000000000000000008 | c3          | shipped | 2026-03-08 10:00:00+00 |         300
 650000000000000000000009 | c2          | paid    |                        |         400
 65000000000000000000000a |             | pending | 2026-03-10 10:00:00+00 |          50
(8 rows)

```

Order `…0007` cost `1.005`: `int(round(total * 100))` rounds the decimal
half away from zero, so it is 101 cents, where floating-point arithmetic
would give 100. `…0009` has no `created_at`, so `placed_at` is null, and
`…000a` has `customer: null`, so `customer_id` is null.

```sh
docker exec dataferry-pg psql -U postgres -c 'select order_id, line_no, sku, qty, unit_cents from order_items order by order_id, line_no'
```

```text
         order_id         | line_no |  sku  | qty | unit_cents 
--------------------------+---------+-------+-----+------------
 650000000000000000000001 |       0 | SKU-1 |   2 |       1000
 650000000000000000000001 |       1 | SKU-2 |   1 |        550
 650000000000000000000005 |       0 | SKU-3 |   1 |        725
 650000000000000000000007 |       0 | SKU-4 |   1 |        101
 650000000000000000000008 |       0 | SKU-5 |   3 |        100
 650000000000000000000009 |       0 | SKU-1 |   4 |        100
 65000000000000000000000a |       0 | SKU-6 |   1 |         50
(7 rows)

```

Orders `…0002` and `…0003` have no items (an empty and a missing `items`
array), so they have no rows here, and the filtered order's item is gone
with it. The dead-lettered document is in `_ferry_dlq`, with the reason:

```sh
docker exec dataferry-pg psql -U postgres -c 'select source_id, target, error from _ferry_dlq'
```

```text
          source_id           |   target    |                                     error                                      
------------------------------+-------------+--------------------------------------------------------------------------------
 ["650000000000000000000006"] | order_items | order_items.qty = `int(qty)`: int(): `"three"` is not an integer (at items[1])
(1 row)

```

## 8. Read the report

The run wrote `report.md` and `report.json` next to the config, under
`.ferry/runs/<run-id>/` (the path `run` printed):

```sh
cat examples/golden/.ferry/runs/<run-id>/report.md
```

```text
# Run ef3a454bec5a4a0ca03e74dc1ffcb6e0: verification PASSED

Verification proves that what the run computed is what landed: every row the runner handed the sink for a done partition is in the target, exactly once, with the same values after normalization, and the target holds no rows the run didn't write. Counts are what the run read, recorded in the state store at read time, so changes to the source after it was read don't affect them.

It does not prove the mapping is correct: a wrong expression computes wrong values, and they land faithfully. Only `expect:` assertions check the mapping. Differences below a target column's timestamp precision, between equal numbers of different width or scale, and in JSON key order or whitespace are normalized away. The checksum covers whole tables, so it assumes a full load into empty targets: rows already in a table count as rows the run didn't write.

## Run

- Config hash: `sha256:c4e1f41657f0c914abccab48976f2122ca2041b6769bd83a031f758b787109e4`
- Engine version: 0.1.0-beta.1
- Verifier version: 0.1.0-beta.1
- Target: postgres, schema public
- Connection `shop`: mongo
- Connection `warehouse`: postgres
- Run: 2026-10-18T06:39:47.378Z to 2026-10-18T06:39:49.010Z (1.6s)
- Verification: 2026-10-18T06:39:49.010Z to 2026-10-18T06:39:49.472Z (0.5s)
- Partitions: 3 done, 0 failed, 0 pending, 0 leased

## Counts

10 driver rows read, 1 dead-lettered (from the state store).

| Flow | Read | Written | Filtered | Dead-lettered | Unaccounted |
|---|---:|---:|---:|---:|---:|
| orders | 10 | 8 | 1 | 1 | 0 |

## Tables

| Table | Committed | Written | Read back | Checksum | Result |
|---|---:|---:|---:|---|---|
| orders | 8 | 8 | 8 | matches | pass |
| order_items | 7 | 7 | 7 | matches | pass |

## Constraints

| Table | Constraint | Violations | Result |
|---|---|---:|---|
| orders | `id` not null | 0 | pass |
| orders | unique (id) | 0 | pass |
| order_items | `id` not null | 0 | pass |
| order_items | `order_id` not null | 0 | pass |
| order_items | `line_no` not null | 0 | pass |
| order_items | unique (order_id, line_no) | 0 | pass |
| order_items | (order_id) references orders (id) | 0 | pass |

## Assertions

| Flow | Name | Assertion | Values | Result |
|---|---|---|---|---|
| orders | order_count | `count(orders) == source.count()` | 8 == 8 | pass |
| orders | order_total | `sum(orders.total_cents) == source.sum(int(round(total * 100)))` | 5326 == 5326 | pass |
| orders | item_count | `count(order_items) == source.sum(coalesce(cardinality(items), 0))` | 7 == 7 | pass |

## Dead letters

1 records dead-lettered (from the state store); `_ferry_dlq` holds 1 rows for this run: pass.

| Flow | Target | Error | Records | Example `source_id` |
|---|---|---|---|---|
| orders | order_items | order_items.qty = \`int(qty)\`: int(): \`"three"\` is not an integer (at items[1]) | 1 | `["650000000000000000000006"]` |
```

[Verification](verification.md) explains each section. In short: every
row the run computed is in the target exactly once with the same values
(**Tables**), every record read is accounted for (**Counts**), the keys and
the foreign key hold (**Constraints**), and the three `expect:` assertions
in the flow hold (**Assertions**).

## 9. Resume and retry

A run that stopped part-way (Ctrl-C, a crash, a lost connection) continues
from the partitions that committed. A run that finished with failed
partitions re-runs only those. Both take the run id. On this finished run,
they find nothing left to do, write nothing twice, and verify again:

```sh
dataferry resume --config examples/golden/ferry <run-id>
```

```text
resuming run ef3a454bec5a4a0ca03e74dc1ffcb6e0
  flow `orders`: TxnWithState
run ef3a454bec5a4a0ca03e74dc1ffcb6e0: complete; 3 partitions; read 10, dead-lettered 1; wrote order_items 7, orders 8
verification of `warehouse` passed; report: examples/golden/.ferry/runs/ef3a454bec5a4a0ca03e74dc1ffcb6e0/report.md
```

```sh
dataferry retry --config examples/golden/ferry --failed <run-id>
```

```text
retrying the failed partitions of run ef3a454bec5a4a0ca03e74dc1ffcb6e0
  flow `orders`: TxnWithState
run ef3a454bec5a4a0ca03e74dc1ffcb6e0: complete; 3 partitions; read 10, dead-lettered 1; wrote order_items 7, orders 8
verification of `warehouse` passed; report: examples/golden/.ferry/runs/ef3a454bec5a4a0ca03e74dc1ffcb6e0/report.md
```

[Running migrations](running.md) shows what happens when a run really is
interrupted.

## 10. Clean up

```sh
docker rm -f dataferry-mongo dataferry-pg
```

The run's work directory, `examples/golden/.ferry/`, holds the reports and
the write-time records verification used; delete it when you no longer
need them.

## Try this

- **Break the mapping.** In `examples/golden/ferry/flows/orders.yaml`,
  change `total_cents: int(round(total * 100))` to
  `int(round(total * 100)) * 10`, start again from an empty Postgres and
  run. The tables still verify, because the wrong values land faithfully,
  but the `order_total` assertion fails with `53260 == 5326`, the report
  says `verification FAILED`, and `run` exits `7`.
- **Interrupt it.** Press Ctrl-C during a `run`: it exits `130` and tells
  you the `resume` command to type.
- **Write your own.** [Configuration](configuration.md) explains the
  `ferry/` directory, and [Transforms](transforms.md) every shape a flow
  can take.
