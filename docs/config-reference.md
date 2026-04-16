# Config reference

Every key of every file in a `ferry/` directory, for grammar `version: 1`.
The grammar and the reasons behind it are in
[decision 0003](decisions/0003-config-shape.md), and the connector fields in
[decision 0004](decisions/0004-connector-registry.md). The JSON Schemas are
in [`schemas/v1`](../schemas/README.md), and `ferry schema` prints them for a
binary, connection types included.

Unknown keys are errors at every level, so a typo fails `ferry validate`
instead of being ignored.

<!--
This file is checked against `ferry schema` by
crates/ferry-cli/tests/config_reference.rs: every key the schemas have must
have a row here, and every row must be a key the schemas have.

- A `## ` heading in backticks names the file a section documents.
- A `### `type: <type>`` heading under `connections.yaml` holds one
  connector type's own fields.
- A key row is a table row whose first cell is the key's path in backticks,
  from the top of the file: `.` between keys, `[]` for a list's items, and
  a `<placeholder>` where the key is yours to choose (a table, a column, a
  predicate name).
- Every value a key's schema enumerates appears in its row, in backticks.
-->

Paths read from the top of the file: `runtime.state.backend` is `backend:`
under `state:` under `runtime:`, `flows[]` is each entry of the `flows:`
list, and `<table>` stands for a name you choose.

## `ferry.yaml`

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `version` | integer | required | Version of the config grammar. Only `1` exists. |
| `name` | identifier | required | Names the migration in state and reports. Lowercase letters, digits and `_`, starting with a letter. |
| `include` | list of paths or globs | none | Extra files to load, relative to the `ferry/` directory. Each holds `flows:`, `connections:` or both. Only `ferry.yaml` may include. A file reached twice loads once (0003 §8). |

Flows are not written here: they live under `flows/`.

## `connections.yaml`

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `connections` | list | required | Every database the flows read from or write to. |
| `connections[].name` | identifier | required | Unique across every file. Flows use it in `from: <name>.<entity>` and `sink: <name>`. No dots. |
| `connections[].type` | string | required | A connector type registered with this binary. The stock `ferry` has `postgres` and `mongo`; `ferry schema` lists a binary's types. |

`description:` is reserved and rejected in v0.1. Every other key belongs to
the connection's type. A field marked *secret* should be a `${ENV}`
reference: a literal value is warned about (W0301), and its value is shown as
`***` in everything ferry prints.

### `type: postgres`

Source and sink, and can hold the state store (`runtime.state.backend:
postgres`).

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `connections[].url` | string, secret | required | libpq URL (`postgres://user@host:5432/db?sslmode=require`) or key=value string. TLS settings (`sslmode`, `sslrootcert`) go here. |
| `connections[].password` | string, secret | none | Overrides a password in `url`. |
| `connections[].schema` | string | `public` | The schema read from as a source and written to as a sink. |
| `connections[].drop_indexes` | boolean or `${ENV}` | `false` | As a sink: drop the secondary indexes of target tables that already exist (not unique, not backing a constraint) before a load, and recreate them after it. |
| `connections[].foreign_keys` | `defer` or `enforce` | `defer` | As a sink: `defer` drops the foreign keys of existing target tables for the load, then recreates and validates them, reporting rows that break one. `enforce` keeps them and checks every write, so parents must land before their children. |

### `type: mongo`

Source only.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `connections[].url` | string, secret | required | `mongodb://` or `mongodb+srv://` connection string. TLS settings (`tls`, `tlsCAFile`) go here. |
| `connections[].database` | string | required | The database whose collections are read. |

## `runtime.yaml`

Every key is optional, and `runtime: {}` is valid.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `runtime` | object | required | The process-wide settings of a run. |
| `runtime.state` | object | `{backend: file}` | Where run state (runs, partitions, leases) lives. |
| `runtime.state.backend` | string | required | `file`, or a state-capable connector type such as `postgres`. When the state is on a flow's sink connection, data and each partition's done-record commit in one transaction (`TxnWithState`). |
| `runtime.state.connection` | string | none | For any backend but `file`: the connection that holds the state. It must be of the `backend:` type. |
| `runtime.state.path` | string | `<work-dir>/state` | For `backend: file`: the state directory. |
| `runtime.workers` | `auto` or a positive integer | `auto` | Worker count. `auto` is one per core. |
| `runtime.memory_budget` | size | `1GiB` | Memory the runner may use: `512MiB` (units `B`, `KiB`, `MiB`, `GiB`, `TiB`) or a number of bytes. |
| `runtime.partition_rows` | positive integer | `100000` | Target rows per partition. |
| `runtime.on_error` | `fail` or `dead_letter` | `fail` | `fail` stops at the first error row. `dead_letter` writes the rejected record to `_ferry_dlq` on the sink and carries on. |
| `runtime.max_errors` | integer | unbounded | How many records may be dead-lettered before the run fails (exit 6). |
| `runtime.source_max_connections` | positive integer | none | Connection cap across every source. |
| `runtime.source_rows_per_sec` | positive integer | none | Read rate cap across every source. |

## `flows/**/*.yaml`

Every `.yaml` and `.yml` file under `flows/`, at any depth, holds `flows:`
(`flows: []` keeps a placeholder file). Names are unique across every file,
and the order flows are written in doesn't matter.

### A flow

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `flows` | list | required | The flows in this file. |
| `flows[].name` | identifier | required | Unique across every file. |
| `flows[].from` | `<connection>.<entity>` or a list | required | One entity (`shop.orders`, `app.public.customers`), or a merge: the driver first, then the entries joined to it. |
| `flows[].sink` | connection name | required | The one connection this flow writes to. |
| `flows[].mode` | `insert` or `upsert` | `insert` | `insert` creates the target rows, and re-runs are made safe by the sink's strategy. `upsert` replaces rows by key, so every target needs `key:`; children with a `parent:` are replaced per parent. |
| `flows[].where` | expression or map | none | Filter on the driver record. A record that fails it has no rows in any target. |
| `flows[].where.<name>` | expression | | One named predicate. All must hold, and reports name the first that fails. |
| `flows[].depends_on` | list of flow names | none | Flows that must finish before this one starts. A flow also waits for any flow writing a table it reads. |
| `flows[].runtime` | object | none | Overrides the runtime settings whose effect is limited to this flow. |
| `flows[].runtime.partition_rows` | positive integer | `runtime.partition_rows` | Target rows per partition, for this flow. |
| `flows[].runtime.on_error` | `fail` or `dead_letter` | `runtime.on_error` | The error policy, for this flow. |
| `flows[].runtime.max_errors` | integer | `runtime.max_errors` | The dead-letter limit, for this flow. |
| `flows[].to` | map | required | The target tables, one or more (a split). Write order follows `parent:`, not the order of the keys. |
| `flows[].expect` | map | none | Named assertions checked after the run (exit 7 when one fails). |
| `flows[].expect.<name>` | assertion | | Two aggregates compared with `==`, `!=`, `<`, `<=`, `>` or `>=`: target side `count(<table>)`, `sum`, `min`, `max`, `count_distinct` over `<table>.<column>`; source side `source.<agg>(<expr>)` over the driver records the run wrote. Meant for full loads into empty targets. |

### A merge: `from:` as a list

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `flows[].from[].entity` | `<connection>.<entity>` | required | The entity this entry reads. |
| `flows[].from[].as` | identifier | last part of `entity` | The alias expressions use (`c.name`). Unique within the flow. Bare names are driver fields. |
| `flows[].from[].join` | object | required, except on the driver | How this entry joins the ones before it. The first entry is the driver and has no `join:`. Every join is a left join from the driver. |
| `flows[].from[].join.cardinality` | `one` or `many` | required | `one`: at most one match per driver record, and a second match at run time rejects the record. `many`: zero or more, usable only as the source of `nest:` or `unnest:`. |
| `flows[].from[].join.keys` | map | required | `{<column of this entry>: <alias>.<column>}`, each pair an equality. It may reference the driver or an earlier `one` entry. |
| `flows[].from[].join.keys.<column>` | `<alias>.<column>` | | One equality of the join key. |
| `flows[].from[].join.strategy` | `auto`, `co_partition` or `stage` | `auto` | `auto` lets the planner choose between co-partitioned streaming and staging, and `ferry plan` shows the choice. `co_partition` fails the plan instead of falling back. `stage` forces staging. |

### A target: `to.<table>`

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `flows[].to.<table>` | object | | One target table. |
| `flows[].to.<table>.columns` | `"*"` or map | required | `"*"` copies every declared field of the entity unchanged (single-entity flows, a target with no `parent:` or `unnest:`). Otherwise target column → column spec, in DDL order. |
| `flows[].to.<table>.key` | list of columns | none | The target's primary key. Required when `mode: upsert`; recommended otherwise. Key columns are not nullable. |
| `flows[].to.<table>.parent` | table name | none | Another target of this flow that this one is a child of: parents are written first, filters propagate to children, and a child mapping the parent's whole key from `$parent` gets a foreign key. |
| `flows[].to.<table>.unnest` | path or `many` alias | none | One row per element of an array (`items`), or per joined row of a `many` join. An empty, missing or null array gives no rows. `$index`, `$item` and `$key` are in scope. |
| `flows[].to.<table>.where` | expression or map | none | Filter on this target's rows, in its scope. Rows reaching it through `parent:` are dropped with it. |
| `flows[].to.<table>.where.<name>` | expression | | One named predicate. |

### A column: `to.<table>.columns.<column>`

A column spec is an expression string (`status: lower(status)`), a typed
column or a nest column. Values must be YAML strings: quote `"null"`,
`"true"` or `"5"`.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `flows[].to.<table>.columns.<column>` | expression or object | | One target column. |
| `flows[].to.<table>.columns.<column>.expr` | expression | required | Typed column: the value. |
| `flows[].to.<table>.columns.<column>.type` | canonical type | inferred | Typed column: `int64`, `decimal(12,2)`, `utf8`, `timestamp(UTC)`, `uuid`, `json`, … |
| `flows[].to.<table>.columns.<column>.nullable` | boolean | inferred | Typed column: whether the target column allows null. |
| `flows[].to.<table>.columns.<column>.allow_lossy` | boolean | `false` | Typed column: allows a lossy conversion of `expr` to `type`. Needs `type:`. |
| `flows[].to.<table>.columns.<column>.nest` | `many` alias | required | Nest column: builds an array column from a `many` join. A driver with no matches gets `[]`. |
| `flows[].to.<table>.columns.<column>.columns` | map | required | Nest column: the nested row's columns, with the same column specs (nests can nest). |
| `flows[].to.<table>.columns.<column>.columns.<column>` | column spec | | One column of the nested row. |
| `flows[].to.<table>.columns.<column>.order_by` | list of expressions | the nested primary key | Nest column: the array's order; `"<expr> desc"` sorts descending. The nested entity's primary key is always appended as the tie-breaker. |
| `flows[].to.<table>.columns.<column>.where` | expression or map | none | Nest column: drops nested rows from the array. |
| `flows[].to.<table>.columns.<column>.where.<name>` | expression | | One named predicate. |

### Expressions and sigils

Expressions are SQL-like (`lower(status)`, `int(round(total * 100))`,
`deleted_at == null`). A bare name is a field of the current scope: the
driver record, the current array element under `unnest: <path>`, the
current joined row under `unnest: <alias>`, or the current nested row.

| Sigil | Meaning |
| --- | --- |
| `$parent` | The mapped row of the `parent:` target: `$parent.id` is the parent's `id` column. |
| `$index` | 0-based position of the current element in the array being unnested. |
| `$item` | The current element itself, for arrays of scalars. |
| `$key` | A deterministic UUIDv5 over the driver record id, the unnest path and `$index`. |

## `${ENV}`

`${NAME}` in a string value is replaced from the environment, in
`connections.yaml` entries (included `connections:` too) and in
`runtime.state` only. A variable that isn't set is an error (exit 3) that
names it, and substituted values are shown as `***` in everything ferry
prints. `${NAME}` is the only form: `${file:...}` is an error. A `${` in a
flow is an error too, so what a flow writes can't change between a run and
its resume.

## The config hash

A run pins a hash of the config, and `ferry resume` refuses (exit 8) when it
changed. It is taken before `${ENV}` substitution, with defaults filled in,
and doesn't depend on file layout or the order flows are written in. The
settings that only change how fast a run goes are left out, so a run can be
resumed with more memory: `workers`, `memory_budget`, `partition_rows`,
`source_max_connections` and `source_rows_per_sec`, in `runtime.yaml` and in
a flow's `runtime:` (0003 §10).

## Test only: `FERRY_LEASE_TTL_MS`

Not a config key, and not for production. Leases last 30 seconds without
a heartbeat (every 10), set by the binary, not the config. The
`FERRY_LEASE_TTL_MS` environment variable overrides that, in milliseconds
from 1000 to 600000, with the heartbeat at a third of it, so the chaos and
conformance suites can resume at once after a kill. Any other value is a
usage error (exit 2, F0601), and `run`, `resume` and `retry` warn (W0601)
whenever it is set. A short lease expires under a live worker on a slow
commit, a database pause or a network stall: the worker loses its
partition (fenced, so nothing is written twice) and the run redoes or
fails work it had done.
