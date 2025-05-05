# Example configs

Configs written in the grammar of
[`docs/decisions/0003-config-shape.md`](../docs/decisions/0003-config-shape.md).
The type-specific connection fields (`url`, `database`, `schema`) follow
[`0004-connector-registry.md`](../docs/decisions/0004-connector-registry.md) §2.
`ferry-dsl` loads every one of them in its tests (`tests/examples.rs`).

| Directory | What it shows |
| --- | --- |
| `golden/ferry/` | Mongo `orders` → Postgres `orders` + `order_items`: split, unnest, flow `where:`, dead-lettering, `expect:`, state on the sink connection (`TxnWithState`). Expresses `fixtures/golden/`, and is what S2 runs end to end. |
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
| `FERRY_GOLDEN_PG_URL` | `golden/`, every `bad/` case. `bad/missing-env` sets it to a secret-looking value and checks the value is never printed |
| `APP_DATABASE_URL`, `REPORTING_DATABASE_URL` | `pg-merge/` |
| `FERRY_EDGE_APP_URL`, `FERRY_EDGE_REPORTING_URL` | `edge/layout/` |

To run the golden config yourself, point the first two at a Mongo holding
`fixtures/golden/source/orders.json` and an empty Postgres.
