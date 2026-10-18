# Example configs

Configs written in the grammar of
[`docs/decisions/0003-config-shape.md`](../docs/decisions/0003-config-shape.md).
The type-specific connection fields (`url`, `database`, `schema`) follow
[`0004-connector-registry.md`](../docs/decisions/0004-connector-registry.md) §2.
`dataferry-dsl` loads every one of them in its tests (`tests/examples.rs`), and
CI runs `dataferry validate` on each (`scripts/check-examples.sh`): every
`ferry/` directory outside `bad/` must pass, and every `bad/<case>/ferry`
must be rejected with exit 3. New examples are found, not listed.

| Directory | What it shows |
| --- | --- |
| `golden/ferry/` | Mongo `orders` → Postgres `orders` + `order_items`: split, unnest, flow `where:`, dead-lettering, `expect:`, state on the sink connection (`TxnWithState`). Expresses `fixtures/golden/`, and is what S2 runs end to end. [`golden/demo.sh`](golden/README.md) runs it on Docker and prints the report. |
| `golden-pg/ferry/` | The golden mapping from a Postgres source: `golden-pg/source.sql` holds the golden documents as `orders` and `order_items` rows, joined back (`many`) and unnested from the join. Gives the golden expected rows, less the generated `order_items.id`. The conformance suite certifies Postgres → Postgres on it. |
| `pg-merge/ferry/` | Postgres → Postgres merge: a driver plus a `one` join and a `many` join, `nest:` into an ordered array, upsert, `depends_on:`, `include:`. Source schema in `pg-merge/source.sql`. |
| `edge/layout/ferry/` | Valid, and only there for the loader: flows in nested directories and in a `.yml` file, a `README.md` under `flows/`, and `include:` globs that overlap each other and `flows/`. Every reached file loads once. |
| `bad/<case>/ferry/` | The golden config with exactly one defect each. [`bad/README.md`](bad/README.md) lists every case, the error it must give and the ticket that owns the check. |

The golden config has a single flow, because the golden data is a single
collection, so the layout cases (several flow files, nesting, `.yml`,
`include:`) live in `edge/layout/` and `pg-merge/` instead of being bolted
onto it.

## Environment variables

No example holds a credential. Every connection `url` is a `${ENV}`
reference, and the tests supply these variables themselves (they never read
your environment):

| Variable | Used by |
| --- | --- |
| `FERRY_GOLDEN_MONGO_URL` | `golden/`, every `bad/` case |
| `FERRY_GOLDEN_PG_SOURCE_URL` | `golden-pg/` (the source, holding `golden-pg/source.sql`) |
| `FERRY_GOLDEN_PG_URL` | `golden/`, `golden-pg/` (the target), every `bad/` case. `bad/missing-env` sets it to a secret-looking value and checks the value is never printed |
| `APP_DATABASE_URL`, `REPORTING_DATABASE_URL` | `pg-merge/` |
| `FERRY_EDGE_APP_URL`, `FERRY_EDGE_REPORTING_URL` | `edge/layout/` |

CI's values for them, all unroutable, are in [`ci.env`](ci.env). A new
example's variables go there too, or the check fails naming them.

To run the golden config yourself, point the first two at a Mongo holding
`fixtures/golden/source/orders.json` and an empty Postgres.
