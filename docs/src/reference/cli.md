# CLI

```text
dataferry [OPTIONS] <COMMAND>
```

| Command | What it does | Connects? |
| --- | --- | --- |
| [`validate`](#validate) | Loads the config, substitutes `${ENV}`, checks every file, flow and connection. Reports every error. | No |
| [`schema`](#schema) | Prints the JSON Schema of each config file, for this binary's connection types. | No |
| [`plan`](#plan) | Discovers the sources, type-checks and compiles every flow, prints run order, strategies, targets and DDL. Writes nothing. | Read-only |
| [`run`](#run) | Plans, then runs every flow, verifies the sinks and writes the report. | Yes |
| [`resume`](#resume) | Continues a run from its committed partitions. | Yes |
| [`retry`](#retry) | Puts a run's failed partitions back to pending, then resumes it. | Yes |

The help texts below are the binary's own (`dataferry <command> --help`);
a test checks them against it.

## Global options

The options are the same for every command, and may come before or after
it (`dataferry --config ferry plan` and `dataferry plan --config ferry`).

| Option | Environment | Default | Meaning |
| --- | --- | --- | --- |
| `-c`, `--config <DIR>` | `FERRY_CONFIG` | `ferry` | The `ferry/` config directory: the one holding `ferry.yaml`. |
| `--format <FORMAT>` | | `text` | `json` prints diagnostics as JSON lines, for CI and editors. |
| `--color <WHEN>` | `NO_COLOR` (for `auto`) | `auto` | Colour diagnostics: `auto` colours a terminal unless `NO_COLOR` is set; `always`, `never`. |
| `-v`, `--verbose` | | | More detail. `validate -v` shows how each connection was understood. |
| `-q`, `--quiet` | | | Fewer messages: no warnings from `validate` and `plan`, and for `validate` no summary. `run` still prints its run id, summary and verification lines, and the warnings `W0601` and `W0701`. Can't be combined with `-v`. |
| `--work-dir <DIR>` | `FERRY_WORK_DIR` | `.ferry` next to the config directory | Where runs keep write-time records, reports, staged joins and, under `backend: file`, the state. |
| `-h`, `--help` | | | Help; `-h` for a summary. |
| `-V`, `--version` | | | The version. |

```text
{{#include cli/dataferry.txt}}
```

## Output

- **stdout** carries the command's output: `validate`'s summary, the
  schemas, the plan, the run id and run summaries. `dataferry plan >
  plan.txt` captures only the plan.
- **stderr** carries diagnostics and notes.
- Every value substituted from the environment, and every value of a
  secret connection field, is replaced with `***` in both, and in every
  file a run writes.

A **diagnostic** has a severity, a stable code, a message, the file, line
and column it points at (after includes), the field path, and often a
help line:

```text
{{#include ../examples/bad/wrong-role/validate.txt}}
```

With `--format json`, each diagnostic is one JSON object per line on
stderr, with the keys `severity`, `code`, `message`, `file`, `line`,
`column`, `path` and `help`. `validate --format json` also prints a summary
object on stdout:

```text
{{#include ../examples/bad/unknown-key/validate.json}}
```

## `validate`

```text
{{#include cli/validate.txt}}
```

Loads every file of the config, substitutes `${ENV}`, builds every
connection through its connector with the roles its flows use (without
connecting), and checks: the files, keys and values; names and
references; `depends_on:`; connection options; that each connection is
used in a role its connector has; that `runtime.state.connection` is a
connection of the `backend:` type that can hold the state store; and the
expressions' syntax and reserved names. Reports every problem, then
`` `<name>` is valid: <n> connections, <n> flows `` (exit `0`) or
`config is invalid: <n> errors` (exit `3`).

What it can't check without connecting (entities, fields, types) is
`plan`'s.

## `schema`

```text
{{#include cli/schema.txt}}
```

Prints one JSON object keyed by file name (`ferry.schema.json`,
`connections.schema.json`, `runtime.schema.json`, `flows.schema.json`), or
with `--out <dir>` writes those four files there. The connections schema
has one branch per connection type registered in the binary. See
[JSON Schemas](schemas.md).

## `plan`

```text
{{#include cli/plan.txt}}
```

`validate`, then discovers every source the flows read, asks every sink
for its capabilities, type-checks and compiles every flow and every
`expect:` assertion, and prints: the config hash, the state store and the
error policy; the connections; the run order; and per flow its driver and
key, sink and mode, strategy (and why, when it isn't `TxnWithState`),
partition size, joins and their strategies, nest orderings, filters,
write order, targets with their columns and types, assertions, and the DDL
the sink would run. Runs no DDL and writes nothing.

Exit `0`; `3` for a config error; `4` when the plan is refused (an unknown
entity or field, a type error, a flow with no idempotency strategy, a
cycle that only appears through the tables flows read, a per-flow setting
the runner can't apply; a `depends_on:` cycle is a config error, exit `3`); `5` when a source or
sink can't be reached or a source's schema can't be read.

## `run`

```text
{{#include cli/run.txt}}
```

Plans, creates the run, prints `run <run-id>` and each flow's strategy,
runs every flow, verifies each sink, writes the report and prints the
summary ([Running migrations](../running.md)).

## `resume`

```text
{{#include cli/resume.txt}}
```

Plans again, checks that the config hash and the engine version are the
ones the run pinned (exit `8` if not), and runs every partition that isn't
done or failed. Then verifies and reports, like `run`.

## `retry`

```text
{{#include cli/retry.txt}}
```

Like `resume`, after putting the run's failed partitions back to pending.

## Environment variables

| Variable | Meaning |
| --- | --- |
| `FERRY_CONFIG` | Default for `--config`. |
| `FERRY_WORK_DIR` | Default for `--work-dir`. |
| `NO_COLOR` | When set and not empty, `--color auto` doesn't colour. |
| `FERRY_LEASE_TTL_MS` | **Tests only.** Lease length in milliseconds, 1000 to 600000 (heartbeat a third of it); anything else is a usage error (exit `2`, `F0601`). `run`, `resume` and `retry` warn (`W0601`) whenever it is set. |
| any name in `${NAME}` | Read by the config's connections and `runtime.state.path` ([Configuration](../configuration.md#environment-variables-and-secrets)). |

## Exit codes

Scripts and CI can rely on these: a code never changes meaning once
released.

| Code | Meaning | What to do |
| --- | --- | --- |
| 0 | Success. Warnings, and dead-lettered records under `max_errors`, still exit 0. | |
| 1 | Internal error, or a miswired binary (a connector type registered twice). | Report a bug. |
| 2 | Usage error: bad flags, `resume`/`retry` without a run id or with one the state store doesn't have, a bad `FERRY_LEASE_TTL_MS`. | Fix the command. |
| 3 | Config invalid: files, keys, values, references, missing `${ENV}`, a `depends_on:` cycle, connection options or roles, a state connection that can't hold the state. | Fix the config. |
| 4 | Plan refused: unknown entity or field, type errors, no idempotency strategy, a cycle between flows through the tables they read. | Fix the config or the source schema. |
| 5 | Could not connect or authenticate, or a source's schema can't be read (a table without a primary key, an unsupported type). | Check the connection; read the message. |
| 6 | The run ended with failed or unfinished partitions, exceeded `max_errors`, or a flow couldn't be prepared or finalized (a sink first contacted in `prepare` that can't be reached ends here too). | Once the cause is fixed: `retry --failed <run-id>` for failed partitions, `resume <run-id>` otherwise; the message says which. |
| 7 | Verification failed: an assertion, a checksum, a count, a constraint or the dead-letter count. The data was written. | Read the report. |
| 8 | Resume refused: the config hash or the engine version changed since the run started. | Restore the config, or start a new run. |
| 130 | Interrupted (Ctrl-C). In-flight partitions were abandoned uncommitted, and the run's leases released. | `resume <run-id>`. |

## Diagnostic codes

| Code | Meaning |
| --- | --- |
| `F0001` | `--config` names nothing, or a file instead of the `ferry/` directory |
| `F0002` | The binary registered its connectors wrongly |
| `F0003` | Reserved: a command not implemented yet (none is now) |
| `F0004` | Output could not be written |
| `F0100–F0109` | Config loading: other (F0100), I/O (F0101), YAML (F0102), layout (F0103), `include:` (F0104), keys and values (F0105), duplicates (F0106), invalid references (F0107), `${ENV}` (F0108), `depends_on:` (F0109) |
| `F0110` | An expression doesn't parse, or names nothing in scope |
| `F0111` | An expression's type doesn't fit where it is used |
| `F0112` | The planner refused the config: a cycle between flows, no idempotency strategy for a flow's mode, a per-flow setting the runner can't apply |
| `F0301` | Bad connection options (unknown field, wrong type, bad value) |
| `F0302` | Unknown connection type |
| `F0303` | A flow uses a connection in a role its connector doesn't provide |
| `F0304` | The state connection's type isn't `backend:` |
| `F0305` | The state backend can't hold the state store |
| `F0306` | A connector couldn't build a connection |
| `W0301` | A secret field holds a literal value instead of `${ENV}` |
| `W0401` | Something the plan points out that doesn't stop a run |
| `F0501` | A source couldn't be discovered, a sink's capabilities read, or the state store reached |
| `F0601` | The runner couldn't run the flows at all |
| `F0602` | A partition failed, or a flow didn't start because one it waits for failed |
| `F0603` | Interrupted (Ctrl-C) |
| `W0601` | `FERRY_LEASE_TTL_MS` is set: leases are shortened for a test |
| `F0701` | Verification failed, or couldn't run |
| `W0701` | A sink this binary can't read back: its flows are not verified |
| `F0801` | `resume`/`retry`: no such run, or it is pinned to another config or engine version |
