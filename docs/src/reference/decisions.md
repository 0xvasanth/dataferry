# Decision records

The architecture decisions behind dataferry, each with its context, the
decision, the alternatives rejected and the consequences. They are kept
in the repository under
[`docs/decisions/`](https://github.com/0xvasanth/dataferry/tree/main/docs/decisions)
and included here as written, with their amendments.

| Record | Decides |
| --- | --- |
| [0001: Commit once per partition](decisions/0001-commit-per-partition.md) | One sink transaction per partition, committed once; no checkpoints inside a partition; every sink declares an idempotency strategy, and the epoch fences commits under `TxnWithState`. |
| [0002: Co-partitioning](decisions/0002-co-partitioning.md) | How a merge streams: the joined entity is partitioned at the driver's boundaries, or the planner falls back to staging the join. |
| [0003: Config shape](decisions/0003-config-shape.md) | The `ferry/` directory and how it loads, the flow grammar, scopes and sigils, `mode:` and `key:`, `where:` and its propagation, `unnest:`, `nest:`, `expect:`, per-flow settings, `depends_on:`, `include:`, `${ENV}`, the config hash, and the lossy-conversion table. |
| [0004: Connector registry](decisions/0004-connector-registry.md) | The `Connector` trait and registry, typed options and their JSON Schema, secrets and redaction, the CLI as a library, the commands and exit codes, and state-capable connectors for exactly-once. |

They were written while the project was named ferry, so they use its
crate and command names: `ferry-core` is `dataferry-core`, and `ferry
plan` is `dataferry plan`. Names that didn't change: the `ferry/` config
directory, the `.ferry/` work directory, the `_ferry_*` tables and the
`FERRY_*` variables.

Where a record and this guide disagree, the guide describes what this
release does. The one point where the release does less than the records
describe: per-flow `on_error` and `max_errors` (0003 §6) are accepted only
with the run-wide values, as the amendment in §6 explains.
