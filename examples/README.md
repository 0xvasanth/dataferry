# Example configs

Draft configs written in the grammar proposed by
[`docs/decisions/0003-config-shape.md`](../docs/decisions/0003-config-shape.md).
Nothing loads them yet: B1 (0XT-254) turns them into its first passing tests.
The type-specific connection fields (`url`, `database`, `schema`) follow
[`0004-connector-registry.md`](../docs/decisions/0004-connector-registry.md) §2.

| Directory | What it shows |
| --- | --- |
| `golden/ferry/` | Mongo `orders` → Postgres `orders` + `order_items`: split, unnest, flow `where:`, dead-lettering, `expect:`, state on the sink connection (`TxnWithState`). Expresses `fixtures/golden/`, and is what S2 runs end to end. |
| `pg-merge/ferry/` | Postgres → Postgres merge: a driver plus a `one` join and a `many` join, `nest:` into an ordered array, upsert, `depends_on:`, `include:`. Source schema in `pg-merge/source.sql`. |
