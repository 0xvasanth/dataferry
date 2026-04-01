# The golden demo

Ten Mongo documents, each carrying one edge case
([`fixtures/golden/README.md`](../../fixtures/golden/README.md)), become
Postgres `orders` and `order_items`. Then the run is verified, and the
report says what landed.

## Run it

You need Docker and a Rust toolchain. From the repository root:

```sh
examples/golden/demo.sh
```

The script:

1. starts `mongo:7` and `postgres:16` in Docker, on random local ports
   (they are removed when it exits, unless `KEEP=1`),
2. loads [`fixtures/golden/source/orders.json`](../../fixtures/golden/source/orders.json)
   into `shop.orders` with `mongoimport`,
3. runs `ferry validate`, which checks the config without connecting,
4. runs `ferry plan`, which shows the flow, the target tables and their DDL,
   the write strategy, and the `expect:` assertions, from the live schemas,
5. runs `ferry run`, which copies the data, verifies it and writes the report,
6. prints `report.md`.

It builds `ferry` with `cargo run` unless `FERRY` names a binary. It uses
existing databases instead of containers when `FERRY_GOLDEN_MONGO_URL`
(holding the golden documents) and `FERRY_GOLDEN_PG_URL` (empty) are both
set. The run's state, write records and report go to `FERRY_WORK_DIR`, or
to a new temporary directory.

## What to expect

`ferry run` prints the run id first, then:

```text
run <id>: complete; 3 partitions; read 10, dead-lettered 1; wrote order_items 7, orders 8
verification of `warehouse` passed; report: <work dir>/runs/<id>/report.md
```

and exits 0. `report.md` opens with `# Run <id>: verification PASSED`.
Its sections:

- **Run**: the config hash, versions, the connections by name and type
  (never their URLs), and the timings.
- **Counts**: per flow, the records read, written, filtered (`…0004`, soft
  deleted) and dead-lettered (`…0006`, a bad `qty`).
- **Tables**: per target table, the rows committed, the rows written and
  the rows read back, and whether the checksum taken at write time matches
  the one read back from Postgres.
- **Constraints**: the keys and the foreign key, checked on the target.
- **Assertions**: each `expect:` assertion with both of its values, e.g.
  `5326 == 5326`.
- **Dead letters**: what went to `_ferry_dlq`, and why.

## When the mapping is wrong

Verification proves that what the run computed is what landed. It can't
know a mapping is wrong; the `expect:` assertions can. Change
`total_cents: int(round(total * 100))` to `int(round(total * 100)) * 10` in
`ferry/flows/orders.yaml` and run again (on an empty Postgres): the tables
still verify, because the wrong values land faithfully, but `order_total`
fails with `53260 == 5326`, the report says `verification FAILED`, and
`ferry run` exits 7.

`ferry-cli`'s `tests/golden_demo.rs` runs this script and checks both.
