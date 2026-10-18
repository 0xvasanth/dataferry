# Configuration

A dataferry config is a directory of YAML files, conventionally named
`ferry/`. This chapter explains how the directory is read, what each file
holds, how environment variables and secrets work, and which settings
change what a run writes. Every key is listed in the
[config reference](reference/config.md).

## The `ferry/` directory

```text
ferry/
  ferry.yaml          # required: version and name, optional include:
  connections.yaml    # required once there is a flow: the databases
  runtime.yaml        # optional: state store, workers, memory, error policy
  flows/              # every *.yaml / *.yml below here, at any depth
    orders.yaml
    sales/
      refunds.yml
```

Commands read the directory given by `--config <dir>` (or the
`FERRY_CONFIG` environment variable), and `./ferry` by default. The
directory must contain `ferry.yaml`: that file is what marks it as a
config. Pointing `--config` at a file instead is an error that suggests
its directory.

How the files are loaded:

- **Each file holds only its own top-level key**: `ferry.yaml` holds
  `version:`, `name:` and `include:`; `connections.yaml` only
  `connections:`; `runtime.yaml` only `runtime:`; a flow file only
  `flows:`.
- **`.yaml` and `.yml` are both accepted.** Having `connections.yaml` and
  `connections.yml` side by side is an error.
- **Every `*.yaml` and `*.yml` file under `flows/` is a flow file**, at any
  depth. Other files (a `README.md`) are ignored, and so are files and
  directories whose names start with `.` (editor swap files). A flow file
  must contain `flows:`; write `flows: []` to keep a placeholder. A config
  with no flows at all is an error.
- **Order doesn't matter.** Files are read sorted by path, which only
  decides which error is reported first. Moving a flow to another file
  changes nothing.
- **One YAML document per file.** A second document (`---`) is an error,
  which keeps every error's file and line exact.
- **Names** of connections and flows are lowercase letters, digits and
  `_`, starting with a letter. Flow names are unique across all files, and
  a duplicate is reported with both places.
- **Unknown keys are errors at every level**, so a typo fails `validate`
  instead of being ignored.

```text
{{#include examples/bad/unknown-key/validate.txt}}
```

### YAML details that matter

The loader reads YAML 1.2, where `on`, `yes` and `no` are strings, not
booleans (many editors still assume YAML 1.1). The grammar never uses
them as keys: joins say `keys:`, not `on:`.

Expressions and other values must be YAML **strings**. A bare `true`,
`null` or `5` where an expression is expected is an error, so YAML's type
guessing never silently changes an expression. Quote it (`"true"`):

```text
{{#include examples/bad/unquoted-value/validate.txt}}
```

For the same reason, quote any expression that starts with `*`, `[`, `{`,
`&`, `!`, `|`, `>`, `'`, `"`, `%` or `@`, or that contains `: ` (a colon
and a space). `status: lower(status)` needs no quotes;
`label: "'id: ' || id"` does.

## `ferry.yaml`

```yaml
{{#include ../../examples/pg-merge/ferry/ferry.yaml}}
```

- `version: 1` is the version of the config grammar, and the only one.
- `name:` names the migration in the state store and in reports.
- `include:` is an escape hatch for files outside the convention.

### `include:`

`include:` is a list of paths or globs (`*`, `**`), relative to the
`ferry/` directory. They may point outside it
(`../shared/connections.yaml`), so several configs can share one file of
connections. A config with flows still needs its own `connections.yaml`:
when all its connections come from an include, keep a local one holding
`connections: []`.

- An included file holds `flows:`, `connections:` or both, never
  `runtime:` (all runtime settings live in `runtime.yaml`).
- Its entries merge with the conventional files' under the same rules: a
  duplicate flow or connection name is an error naming both files.
- A path that doesn't exist, or a glob that matches nothing, is an error.
- A file reached twice (by the `flows/` convention and a glob, or by two
  globs) is loaded once.
- Only `ferry.yaml` may have `include:`, so includes can't nest.

## `connections.yaml`

```yaml
{{#include ../../examples/golden/ferry/connections.yaml}}
```

Each entry has a `name` and a `type`, and every other key belongs to the
type: [PostgreSQL](connectors/postgres.md#connection-options) has `url`,
`password`, `schema`, `drop_indexes` and `foreign_keys`;
[MongoDB](connectors/mongo.md#connection-options) has `url` and
`database`. An unknown type is an error that lists the types the binary
has. `name`, `type` and `description` are reserved in every entry
(`description` has no meaning yet and is rejected).

`validate` also checks that each connection is used in a role its
connector has:

```text
{{#include examples/bad/wrong-role/validate.txt}}
```

Flows refer to a connection in two ways: an entity as
`<connection>.<entity>` (split at the first dot, so `app.public.orders` is
entity `public.orders` on connection `app`, which Postgres also resolves as
table `orders` in the connection's `schema:`), and a connection on its own
in `sink:` and `runtime.state.connection`.

## Environment variables and secrets

A config never needs to hold a credential. `${NAME}` in a string value is
replaced with the environment variable `NAME`:

- **Only in `connections.yaml`** entries (including connections from
  included files) **and in `runtime.state.path`**. `runtime.state`'s
  `backend:` and `connection:` are checked as written, before
  substitution, so `${...}` there is an error too. A `${` anywhere in a flow is
  an error, because the [config hash](#the-config-hash) is taken before
  substitution: an environment-driven filter could change between a run and
  its resume, and the resumed run would write different rows.
- `NAME` is letters, digits and `_`, not starting with a digit. `${NAME}`
  is the only form: there is no default value syntax and no
  `${file:...}`. To use a secret mounted as a file, export it first:
  `export PG_URL=$(cat /run/secrets/pg_url)`.
- A variable that isn't set is an error that names it, without printing
  any other value.
- A variable set to the empty string is a value, the empty string. There
  is no way to write a literal `${` in a connection value.
- Substitution works on parsed values, so a variable can't add YAML
  structure, and its value is never expanded again.

A `${` in a flow, and a variable that isn't set:

```text
{{#include examples/bad/env-in-flow/validate.txt}}
```

```text
{{#include examples/bad/missing-env/validate.txt}}
```

Every substituted value, and the value of every secret field (Postgres
`url` and `password`, Mongo `url`), is replaced with `***` in everything
dataferry prints and writes: diagnostics, `plan` output, logs and reports.
That is a backstop for driver errors that echo a connection string. A
secret field that holds a literal value is accepted with a warning, since
config directories get committed:

```text
{{#include examples/literal-secret/validate.txt}}
```

## `runtime.yaml`

Every key is optional; without the file, every default applies.

```yaml
{{#include ../../examples/golden/ferry/runtime.yaml}}
```

| Key | Default | Kind | What it does |
| --- | --- | --- | --- |
| `state` | `{backend: file}` | behavioural | Where run state lives: `{backend: file}` (in the work directory, or `path:`, relative to the config directory) or `{backend: postgres, connection: <name>}`. |
| `on_error` | `fail` | behavioural | `fail` stops at the first rejected record; `dead_letter` keeps it in `_ferry_dlq` and carries on ([Errors](errors.md)). |
| `max_errors` | unbounded | behavioural | How many records may be dead-lettered in the run before it fails. |
| `workers` | `auto` | tuning | Partitions worked on at once, across all flows. `auto` is one per CPU core. |
| `memory_budget` | `1GiB` | tuning | Memory the runner sizes its batches and read-ahead from (units `B`, `KiB`, `MiB`, `GiB`, `TiB`, or a number of bytes). |
| `partition_rows` | `100000` | tuning | Target records per partition, so per transaction. |
| `source_max_connections` | none | tuning | Cap on read streams open at once, across every source. |
| `source_rows_per_sec` | none | tuning | Cap on rows read per second, across every source. |

**Behavioural** settings change what a run writes or how it treats
errors, so they are part of the [config hash](#the-config-hash): change
one and `resume` refuses the run. **Tuning** settings only change how
fast, or how much at a time, so they are left out of the hash: a run that
ran out of memory can be resumed with a bigger `memory_budget`, or with
more `workers`. Partitions are stored when a run starts, so a changed
`partition_rows` only applies to new runs.

### The state store and `TxnWithState`

Where the state store is decides each flow's
[idempotency strategy](concepts.md#idempotency-strategies). A flow gets
exactly-once `TxnWithState` only when `state.connection` is the flow's
`sink:`; every other flow falls back to `DeletePartitionThenInsert`, and
`plan` and `run` say why. The [catalog example](transforms/split-unnest.md)
with `state: {backend: file}` runs as:

```text
{{#include examples/file-state/expected.txt:2}}
```

`validate` checks that `state.connection` names a connection whose type is
`backend:` and that this type can hold the state store (only `postgres` in
this release).

### Per-flow settings

A flow may override a setting only if its effect is limited to that flow:

```yaml
{{#include ../../examples/golden/ferry/flows/orders.yaml:13:17}}
```

- `partition_rows` is honoured per flow.
- `on_error` and `max_errors` are accepted in a flow's `runtime:` only
  with the same value as in `runtime.yaml`. The runner applies them to the
  whole run in this release, so `plan` refuses a flow that sets a
  different value (exit 4), rather than silently ignoring it. Per-flow
  budgets are planned for a later release.
- `state`, `workers`, `memory_budget` and the two source limits are
  process-wide, and setting them on a flow is an error.

## Ordering flows

Flows with no relation between them run at the same time, sharing the
workers. A flow starts only after every flow it **waits for** has
committed all its partitions:

- the flows it names in `depends_on:`;
- any flow that writes a table it reads, on the same connection. When
  flow `b` reads `reporting.customers` and flow `a` writes `customers` to
  `reporting`, `b` waits for `a`.

Only the reading waits. Every flow's entities are discovered, and its
partitions planned, when the run starts, so a table one flow reads from
another must exist by then, and its partitions are cut on the rows it
holds at that moment (the first and last ranges are open-ended, so rows
added later are still read, in uneven partitions).

```yaml
{{#include ../../examples/pg-merge/ferry/flows/order_summaries.yaml:1:5}}
```

`depends_on:` names must exist, appear once, not name the flow itself and
not form a cycle. `validate` checks all of this without connecting. If a
flow fails, the flows that wait for it don't start, and the run reports
them as blocked. Within one flow, the order of its tables comes from
`parent:` ([Transforms](transforms.md)), not from `depends_on:`.

You rarely need `depends_on:` for foreign keys: a Postgres sink drops the
foreign keys of existing target tables during a load and validates them at
the end ([PostgreSQL](connectors/postgres.md#existing-tables-foreign-keys-and-indexes)),
so write order is an optimization, not what keeps the data consistent.

## The config hash

A run pins a hash of the config, and `resume` and `retry` refuse to
continue (exit 8) when it has changed. The hash is a SHA-256 of the
config's **meaning**:

- It is taken **after** defaults are filled in, so writing a default out
  (`mode: insert`) doesn't change it.
- It is taken **before** `${ENV}` substitution: `url: ${PG_URL}` is hashed
  as that text, so rotating a password doesn't block a resume, and secret
  values never reach the hash.
- **File layout doesn't count**: moving a flow to another file, renaming
  files, reordering flows or connections, editing comments or changing
  `include:` leaves it unchanged, as long as the model is the same.
- What has an order keeps it: `columns:` (the DDL column order), merge
  `from:` entries, named `where:` predicates and `expect:`.
- **Tuning settings are left out**: `workers`, `memory_budget`,
  `partition_rows`, `source_max_connections` and `source_rows_per_sec`, in
  `runtime.yaml` and in flows.

`plan` prints the hash on its first line, and the report records it.

## Editor support

`dataferry schema --out schemas/` writes a JSON Schema for each kind of
file, including the options of every connector the binary has, so YAML
editors can complete keys and flag mistakes as you type. See
[JSON Schemas](reference/schemas.md).
