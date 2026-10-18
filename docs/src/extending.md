# Writing a connector

dataferry has no plugin loading. A connector is a Rust crate, and you ship
it by building **your own binary**: the `dataferry` crate is a library as
well as a command, and a three-line `main` gives your binary every
subcommand, the built-in connectors, and yours. The built-in `postgres`
and `mongo` connectors are registered exactly this way, through the same
public calls.

The design is [decision 0004](reference/decisions/0004-connector-registry.md).
This chapter walks through it with code from the repository that its tests
compile and run.

## The pieces

| Crate | What you use from it |
| --- | --- |
| `dataferry-core` | The data contracts: the `Source`, `Sink` and `SinkTxn` traits, `Entity`, `Partition`, the canonical types. Batches are Apache Arrow `RecordBatch`es. |
| `dataferry-registry` | The connector author's API: the `Connector` trait, `Roles`, `Secret`, `BuildContext`, `Endpoints`, `BuildError`; the state-capable extension; the contract suites (`testkit` feature). |
| `dataferry` | `Ferry`, the command line as a library. Only your binary needs it. |

A connector crate needs only the first two (plus `serde` and `schemars`
for its options, and usually `async-trait`), so it doesn't pull in the
planner or DataFusion.

## The `Connector` trait

A connector declares its `type:` name, the roles it provides, and an
options struct for its fields in `connections.yaml`, and builds endpoints
from them. The repository's README example, which a test compiles and
registers:

```rust,ignore
{{#include ../../crates/dataferry/tests/readme_connector.rs:9:47}}
```

- **`TYPE`** is the `type:` value. **`ROLES`** says whether it can be a
  source, a sink or both (`Roles::SOURCE`, `Roles::SINK`, `Roles::BOTH`):
  `validate` refuses a flow that uses a connection in another role.
- **`Options`** is your connection fields, minus `name` and `type`. Derive
  `Deserialize` with `#[serde(deny_unknown_fields)]`, so a typo is an
  error, and `JsonSchema`, so `dataferry schema` describes it: doc comments
  become editor hover text, and defaults and enums carry through. Don't
  use the reserved keys `name`, `type` or `description`. Options are
  validated by deserializing them: the Rust type is the source of truth,
  and the schema is derived from it.
- **`Secret`** for every credential-bearing field. It prints as `***`, its
  schema is marked `x-ferry-secret`, `validate` warns when it holds a
  literal instead of `${ENV}`, and its value is redacted from everything
  the CLI prints. Secrets inside another value (a password in a URL) are
  returned by `derived_secrets` (it defaults to none) and redacted too.
- **`build`** returns the `Source` and/or `Sink` that `wants` asks for
  (always a subset of `ROLES`), in an `Endpoints`. It must **not do I/O**:
  `validate` calls it to catch bad options without connecting. Endpoints
  connect on their first trait call. Report bad options as
  `BuildError::option("<field>", "<message>")`; the CLI turns the field
  into a file, line and column.
- **`describe`** is the one line `plan` and `validate -v` show for a
  connection. Never put a secret in it.

## Your binary

```rust,ignore
{{#include ../../crates/dataferry/tests/readme_connector.rs:52:57}}
```

`Ferry` is a builder:

| Method | Does |
| --- | --- |
| `Ferry::with_builtins()` | Starts with `postgres` and `mongo` (each behind a default-on Cargo feature of the same name). `Ferry::new()` starts empty. |
| `.register(connector)` | Adds a connector type. |
| `.register_state_capable(connector, backend)` | Adds a connector that can hold the state store, with its backend (below). |
| `.replace(connector)`, `.replace_state_capable(...)` | Replaces a registered type on purpose, a patched `postgres` for example. |
| `.name("acme-dataferry")` | The name in help and usage text. |
| `.version("1.2.0")` | What `--version` prints. |
| `.main()` | Parses the process arguments, runs the command and returns its exit code. |
| `.run_with(args, out, err)` | The same with explicit arguments and output, for tests. |

Registering a type twice, or a connector whose options use a reserved key,
is a bug in the binary: every command then fails at start-up with exit `1`
and a diagnostic (`F0002`) naming the type.

The subcommands and these builder methods are the library's whole stable
surface in this release.

## The acme test connector

[`crates/dataferry/tests/acme-dataferry`](https://github.com/0xvasanth/ferry/tree/main/crates/dataferry/tests/acme-dataferry)
is a complete third-party binary, written against the public API only and
used by the tests: it registers its own `postgres` and `mongo` (with the
built-ins' options, and endpoints that panic when used, which proves
`validate` never connects) and a `fake` type the stock binary doesn't
have.

Its command line:

```rust,ignore
{{#include ../../crates/dataferry/tests/acme-dataferry/src/lib.rs:31:38}}
```

A source-only connector, with a URL check in `build` that stands in for
parsing a connection string, and no I/O:

```rust,ignore
{{#include ../../crates/dataferry/tests/acme-dataferry/src/lib.rs:273:304}}
```

`endpoints` builds whatever `wants` asks for. `Endpoints` also has two
optional fields for sinks: a `finalizer` (restores what `prepare`
loosened for the load, such as dropped foreign keys) and `ddl` (renders
the DDL `prepare` would run, which `plan` prints):

```rust,ignore
{{#include ../../crates/dataferry/tests/acme-dataferry/src/lib.rs:87:98}}
```

Its tests run the binary as a real process (`validate` against the
repository's example configs and its bad-config set, with every secret
checked to never appear in the output), and in-process through `run_with`
(registration mistakes, and the JSON Schema its registered types compose).

## Implementing the source and sink

The traits are in `dataferry-core`. All are `async_trait`, `Send + Sync`,
and the runner holds them as `Arc<dyn Source>` / `Arc<dyn Sink>`.

**`Source`**:

- `discover()` returns the entities: name, fields with canonical types and
  nullability, and the primary key. For a schemaless store, infer the
  schema; records that don't fit it are surfaced at read time as rejected
  rows (`dataferry_registry::rejected`), not errors.
- `plan_partitions(entity, hint)` splits an entity into partitions that
  together cover every row exactly once, sized by `hint.target_rows`.
  Called once per run; the result is stored, never re-planned. When the
  hint carries a co-partition (a merge's joined entity), produce exactly
  its ranges on its column, or return `CoPartitionUnsupported`
  ([decision 0002](reference/decisions/0002-co-partitioning.md)).
- `read(entity, partition)` streams the partition's rows as Arrow batches
  matching the entity's schema. Every source must also accept a `Whole`
  partition.
- `capabilities()` says what the source can do (range scans, predicate
  pushdown, snapshots).

**`Sink`**:

- `prepare(target_schema, mode)` creates or checks the targets, once per
  flow.
- `begin(partition, lease, strategy)` opens a `SinkTxn` for one partition:
  `write(target, batch)` any number of times, then `commit()` once, or
  `abort()`. Everything a partition writes commits together.
- `capabilities()` lists the idempotency strategies the sink supports.
  The planner takes the first one compatible with the flow's mode, in the
  order `TxnWithState`, `DeletePartitionThenInsert`, `UpsertByKey`,
  `StagingSwap`; a flow with none is refused. Whatever you offer must make
  re-running a partition safe ([decision 0001](reference/decisions/0001-commit-per-partition.md)).

### Exactly-once: state-capable connectors

`TxnWithState` commits a partition's rows and its done-record in one
transaction. A connector offers it by implementing `StateCapable` (a
`pool()` that connects lazily, and `build_with_ledger`, which builds a
sink that calls the given `Ledger` on its own transaction just before
`COMMIT`), paired with a `StateBackend` that keeps the state store in the
same database, and is registered with `register_state_capable`. The
compiler checks that the connector's and the backend's pool and connection
types are the same. A sink built with plain `build` must not list
`TxnWithState`. The acme connector's version, which does nothing:

```rust,ignore
{{#include ../../crates/dataferry/tests/acme-dataferry/src/lib.rs:165:203}}
```

## Testing a connector

With the `testkit` features:

| Suite | Checks |
| --- | --- |
| `dataferry_registry::testkit::check_connector(connector, &fixture)` | The options schema generates and is closed, an unknown field is rejected at its path, `build` returns exactly the roles asked for, and the fixture's secret never shows in `describe()`, `Debug` or errors. |
| `dataferry_registry::testkit::check_state_capable(...)` | A plain-built sink doesn't offer `TxnWithState` and a ledger-built one does; a crash before `COMMIT` commits neither rows nor done-record; a clean commit makes both durable. |
| `dataferry_core::testkit::check_source`, `check_sink` | The source and sink contracts, against your real database. `MemSource` and `MemSink` are in-memory references. |
| `dataferry_runtime::testkit::conformance::certify` | The certification: a golden migration through your binary with a green report (the config needs at least one `expect:` assertion), then 50 runs killed with SIGKILL mid-partition and resumed, each ending with the expected rows and the uninterrupted run's checksums. Its module docs show the `Pair` trait to implement. |

The contract suites return every violation they find, so a test asserts
the list is empty; `certify` returns a certificate, or the first reason it
refused one. The built-in connectors pass all four.

## What a third-party connector can't do yet

- **Be verified.** After a run, dataferry reads a sink back to verify it
  only for the types it knows how to read: `postgres`. A third-party sink's
  flows are reported as not verified (`W0701`), which doesn't change the
  exit code; what its finalizer couldn't restore still fails the run.
- **Qualify entity names with a namespace**, as `app.public.orders` does
  for Postgres: that is also registered only for the built-ins.
- **Be loaded at run time.** Connectors are compiled in. A
  language-agnostic subprocess protocol is designed to fit later
  ([decision 0004 §7](reference/decisions/0004-connector-registry.md)), not
  built.

## Using dataferry as a library

Beyond `Ferry`, the command functions (`plan`, `run`, …) are private in
this release: embedding dataferry means running its commands through
`Ferry::run_with`, with your own arguments and output buffers, and reading
the exit code and the run report. The crates below the CLI (the DSL, the
runner, the state store, the verifier) are published and documented on
docs.rs, but their APIs are not stable yet.
