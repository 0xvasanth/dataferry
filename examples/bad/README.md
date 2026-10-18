# Bad configs

Each directory is `examples/golden/ferry/` with **exactly one** defect, in
the one file the table names. A test in `dataferry-dsl` (`tests/examples.rs`)
diffs every case against the golden config, so a case can't fail for a
reason other than the one documented here.

Columns:

- **File**: the only file that differs from the golden config (changed or
  added).
- **Error at**: where the error points. It is usually the defect's own
  line. For duplicates it is the second occurrence, in load order (0003 §2).
- **Field path**: the path the error names.
- **Checked by**: which check rejects it.
  - `yaml`: the YAML reader, before any model exists.
  - `schema`: the file on its own. Both the serde model and the JSON Schema
    reject it, and a test checks that they agree.
  - `load`: needs the whole directory (several files, names defined
    elsewhere, `include:`, `depends_on:`). The JSON Schema can't see it.
  - `env`: `${ENV}` rules (0003 §9).
  - `registry`: needs the connector registry (0004). `dataferry-dsl` loads these
    without error, and `dataferry validate` (C1) rejects them.
- **Owner**: the ticket that owns the check.

| Case | Defect | File | Error at | Field path | Checked by | Owner |
| --- | --- | --- | --- | --- | --- | --- |
| `yaml-syntax` | `sink:` indented one space less than its siblings | `flows/orders.yaml` | `flows/orders.yaml:4` | | yaml | B1a |
| `duplicate-key` | `mode:` twice in one flow, must not be last-wins | `flows/orders.yaml` | `flows/orders.yaml:6` | `flows[orders].mode` | yaml | B1a |
| `flows-in-ferry-yaml` | `flows:` in `ferry.yaml` instead of `flows/` | `ferry.yaml` | `ferry.yaml:5` | `flows` | schema | B1a |
| `unknown-runtime-key` | `wrokers:` typo in `runtime.yaml` | `runtime.yaml` | `runtime.yaml:10` | `runtime.wrokers` | schema | B1b |
| `version-missing` | no `version:` | `ferry.yaml` | `ferry.yaml:3` | `version` | schema | B1b |
| `version-unsupported` | `version: 2` | `ferry.yaml` | `ferry.yaml:3` | `version` | schema | B1b |
| `duplicate-flow-name` | a second flow named `orders`, in a nested `.yml` file | `flows/archive/orders_copy.yml` | `flows/orders.yaml:2` | `flows[orders].name` | load | B1a |
| `duplicate-connection-name` | a second connection named `shop` | `connections.yaml` | `connections.yaml:13` | `connections[shop].name` | load | B1a |
| `include-outside-ferry-yaml` | `include:` in a flow file (only `ferry.yaml` may include, 0003 §8) | `flows/orders.yaml` | `flows/orders.yaml:1` | `include` | schema | B1a |
| `include-missing` | `include:` of a file that doesn't exist | `ferry.yaml` | `ferry.yaml:6` | `include[0]` | load | B1a |
| `undefined-connection` | `sink: warehous`, no such connection | `flows/orders.yaml` | `flows/orders.yaml:4` | `flows[orders].sink` | load | B1b |
| `entity-without-connection` | `from: orders`, no `<conn>.` part | `flows/orders.yaml` | `flows/orders.yaml:3` | `flows[orders].from` | schema | B1b |
| `unknown-connection-type` | `type: postgresql`, not a registered type | `connections.yaml` | `connections.yaml:10` | `connections[warehouse].type` | registry | C1 |
| `wrong-value-type` | `max_errors: ten`, a string where a number is expected | `runtime.yaml` | `runtime.yaml:16` | `runtime.max_errors` | schema | B1b |
| `upsert-without-key` | `mode: upsert` with no `key:` on `order_items` | `flows/orders.yaml` | `flows/orders.yaml:32` | `flows[orders].to.order_items.key` | load | B1b |
| `join-without-cardinality` | a merge join with no `cardinality:` | `flows/orders.yaml` | `flows/orders.yaml:7` | `flows[orders].from[1].join.cardinality` | schema | B1b |
| `missing-env` | `${FERRY_GOLDEN_MONGO_URI}` is not set. Tests set `FERRY_GOLDEN_PG_URL` to a secret and check it is never printed | `connections.yaml` | `connections.yaml:6` | `connections[shop].url` | env | B1c |
| `env-file-reference` | `${file:/run/secrets/pg_url}`, a form v0.1 doesn't have | `connections.yaml` | `connections.yaml:11` | `connections[warehouse].url` | env | B1c |
| `env-in-flow` | `${CUTOFF}` in a flow `where:` | `flows/orders.yaml` | `flows/orders.yaml:12` | `flows[orders].where.recent` | env | B1c |
| `state-not-state-capable` | `runtime.state.connection` names the Mongo connection | `runtime.yaml` | `runtime.yaml:8` | `runtime.state.connection` | registry | C1 |
| `depends-on-unknown` | `depends_on: [customer]`, no such flow | `flows/orders.yaml` | `flows/orders.yaml:6` | `flows[orders].depends_on[0]` | load | B1d |
| `depends-on-self` | `depends_on: [orders]` on the `orders` flow | `flows/orders.yaml` | `flows/orders.yaml:6` | `flows[orders].depends_on[0]` | load | B1d |
| `depends-on-cycle` | two added flows that depend on each other | `flows/audit.yaml` | `flows/audit.yaml:6` | `flows[order_audit].depends_on[0]` | load | B1d |
| `empty-flow-file` | a flow file with only a comment. Decided: an error, because a flow file must contain `flows:` (0003 §2); `flows: []` is the way to keep a placeholder | `flows/returns.yaml` | `flows/returns.yaml:1` | | schema | B1a |
| `connection-description` | `description:` is reserved and has no meaning in v0.1 (0003 §3) | `connections.yaml` | `connections.yaml:6` | `connections[shop].description` | schema | B1b |
| `flow-runtime-not-overridable` | `workers:` in a flow's `runtime:` (process-wide, 0003 §6) | `flows/orders.yaml` | `flows/orders.yaml:18` | `flows[orders].runtime.workers` | schema | B1b |

The draft ticket also asks for `include:` inside an *included* file. A case
here can only change one file, and `include:` in any file but `ferry.yaml` is
the same rule, so `include-outside-ferry-yaml` covers it here and the loader
tests cover the included-file form.
