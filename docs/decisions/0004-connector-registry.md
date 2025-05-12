# 0004: Connector registry, connection options and the CLI layer

Status: proposed. The points that were open were decided on 2026-10-16 (see
Open questions). Ticket: [D4] (drafted, gap analysis §3 and §6). Needs review
from both tracks: Track A owns the connector crates and the state store that
implement it, and Track B owns the loader that feeds it.

## Context

Already decided, and not reopened here:

- The config is a `ferry/` directory. `connections.yaml` holds
  `connections:`, and each entry is `{name, type, ...type-specific fields}`.
  0003 owns `name`, `type` and everything outside connections. This record
  owns the type-specific fields.
- `connections[].type` maps to a constructor through a `ConnectorRegistry`.
  Built-in `postgres` and `mongo` register exactly the way a third-party
  connector would.
- `ferry-cli` becomes lib+bin. No `unsafe`, and no dynamic plugin loading in
  v0.1.

What exists today:

- `Source`, `Sink` and `SinkTxn` in `ferry-core` are object-safe
  (`async_trait`, `Send + Sync`). The runner can hold `Arc<dyn Source>` and
  `Arc<dyn Sink>` without knowing the concrete connector.
- `PostgresSource::new(url, schema) -> Result<Self>` is synchronous and does
  no I/O. It parses the connection string, and the first trait call connects.
  TLS policy is read from the parsed config (`sslmode` inside the URL).
- Connector failures already have a home: `FerryError::Connector { connector,
  message }`.
- `ferry-core` is frozen. Its dependencies are Arrow, serde, futures and
  thiserror, with no JSON or schema crates.
- Nobody owns the CLI layer: argument parsing, the config path, exit codes,
  error rendering, or `ferry validate`. B7 owns the `plan` handler, S2 wires
  `run`, and A5 owns `resume`/`retry` entry points ("wiring can be thin").

## Decision

### 1. The registry lives in a new crate, `ferry-registry`

```
ferry-core  ←  ferry-registry  ←  ferry-connector-postgres, ferry-connector-mongo, third-party connectors
     ↑               ↑                          ↑
 ferry-dsl      ferry-state                     │
     ↑               ↑                          │
     └──────── ferry-cli (lib + bin) ───────────┘
```

`ferry-registry` holds the connector author's API: the `Connector` trait, the
`ConnectorRegistry`, `Secret`, `Roles`, the schema composition, the
state-capable extension (§8), and a `testkit` contract suite. It depends on
`ferry-core`, `serde`, `serde_json`, `schemars` and `async-trait`.
`ferry-state` depends on it for the `StateStore` and `StateBackend` traits
(§8), which is the edge drawn above.

Rejected:

- **`ferry-core`.** Needs a cross-track PR against a frozen crate, and it
  would push `schemars` and `serde_json` into the crate every other crate
  depends on. The registry deals with configuration and wiring, not the data
  contracts `ferry-core` exists for. Nothing in it needs to change a core
  type.
- **`ferry-cli`.** The CLI depends on the built-in connectors to register
  them. If the trait lived in the CLI, `ferry-connector-postgres` would depend
  on `ferry-cli`: a cycle. The only way around the cycle is to write the
  built-in impls inside the CLI, which breaks "built-ins register the way a
  third party would". It would also make every third-party connector crate
  depend on clap and the whole engine.
- **`ferry-dsl`.** Connector crates would then depend on the planner and
  DataFusion.

`ferry-dsl` does **not** depend on `ferry-registry`, and the reverse is also
true. The loader (B1) parses each connection as a raw entry (`name`, `type`,
the remaining fields as a `serde_json::Value`, plus source spans). The CLI
passes those entries to the registry and hands the `Source`/`Sink` it gets
back to the planner. JSON is the interchange format because JSON Schema
already speaks it, a future subprocess protocol would too (§7), and YAML-only
constructs (tags, non-string keys) are rejected by the loader anyway.

Ownership: `ferry-registry` is shared, like `ferry-cli`. Once merged, changes
need review from both tracks, because every connector depends on it. Each
connector's `impl Connector` lives in that connector's own crate (Track A),
exactly where a third party's would.

### 2. Registration API

Connector authors implement one typed trait:

```rust
// ferry-registry

pub trait Connector: Send + Sync + 'static {
    /// The `type:` value in `connections.yaml`, e.g. "postgres".
    const TYPE: &'static str;

    /// What this connector can be used as. Checked by `ferry validate`
    /// without building anything: a connection named in `from:` must be a
    /// source, one named in `sink:` must be a sink.
    const ROLES: Roles;

    /// The type-specific fields: the connection entry minus `name` and
    /// `type`. Must use `#[serde(deny_unknown_fields)]`, and must not use a
    /// reserved key (`name`, `type`, `description`, 0003 §3); the contract
    /// suite checks both. Doc comments become editor hover text.
    type Options: DeserializeOwned + JsonSchema;

    /// Builds the endpoints `wants` asks for (always a subset of `ROLES`).
    /// Must not do I/O: `ferry validate` calls it to catch malformed
    /// options, and connecting happens on the first trait call.
    fn build(
        &self,
        ctx: &BuildContext<'_>,
        options: Self::Options,
        wants: Roles,
    ) -> Result<Endpoints, BuildError>;

    /// One line for `ferry plan` and the run report, with no secrets,
    /// e.g. `postgres://app@db.example.com:5432/shop, schema public`.
    fn describe(&self, options: &Self::Options) -> String;

    /// Secret values that are not whole `Secret` fields, such as a password
    /// embedded in a URL. They are added to the redaction set (§4).
    fn derived_secrets(&self, options: &Self::Options) -> Vec<String> {
        Vec::new()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Roles {
    pub source: bool,
    pub sink: bool,
}

impl Roles {
    /// Only for the state connection when no flow reads or writes it (§8).
    pub const NONE: Roles = Roles { source: false, sink: false };
    pub const SOURCE: Roles = Roles { source: true, sink: false };
    pub const SINK: Roles = Roles { source: false, sink: true };
    pub const BOTH: Roles = Roles { source: true, sink: true };
}

/// What `build` returns. The registry checks it matches `wants`, and treats
/// a mismatch as a connector bug, not a user error.
#[derive(Default)]
pub struct Endpoints {
    pub source: Option<Arc<dyn Source>>,
    pub sink: Option<Arc<dyn Sink>>,
}

#[non_exhaustive]
pub struct BuildContext<'a> {
    /// The connection's `name:`, for error messages.
    pub connection: &'a str,
    // Room for runtime hints later (e.g. a pool size derived from A5's
    // `source_max_connections`) without breaking connectors.
}

pub enum BuildError {
    /// Bad options, each with a path relative to the connection entry
    /// (e.g. `url`, `tls.ca_file`). The CLI maps the path to file:line.
    Options(Vec<OptionsError>),
    Other(FerryError),
}

pub struct OptionsError {
    pub path: String,
    pub message: String,
}
```

How one type provides a source, a sink or both: `ROLES` declares it, and
`build` is called **once per connection** with the roles the config actually
uses. A Postgres connection that is both read from and written to gets one
`build(.., Roles::BOTH)`, so the connector may share a client or pool between
its two halves if it wants to. A connection used only in `from:` never builds
a sink. Mongo declares `Roles::SOURCE` until the Mongo sink decision (S3)
says otherwise, and Postgres declares `BOTH` once A3 lands. Until then,
`sink: <pg connection>` fails validation with "the postgres connector in this
build has no sink", which is the same message a third-party read-only
connector would produce.

The registry stores an object-safe, type-erased form of the trait, so
connectors with different `Options` types can live in one map:

```rust
pub trait DynConnector: Send + Sync {
    fn type_name(&self) -> &str;
    fn roles(&self) -> Roles;
    fn options_schema(&self, generator: &mut SchemaGenerator) -> Schema;
    fn build(
        &self,
        ctx: &BuildContext<'_>,
        options: serde_json::Value,
        wants: Roles,
    ) -> Result<Built, BuildError>;

    /// §8. Only connectors registered with `register_state_capable`
    /// override these.
    fn state_capable(&self) -> bool {
        false
    }
    fn build_with_state(
        &self,
        ctx: &BuildContext<'_>,
        options: serde_json::Value,
        wants: Roles,
    ) -> Result<(Built, Arc<dyn StateStore>), BuildError> {
        Err(not_state_capable(self.type_name()))
    }
}

/// Endpoints plus everything the CLI needs to display and redact them.
pub struct Built {
    pub endpoints: Endpoints,
    pub description: String,
    pub secrets: Vec<String>,
}

pub struct ConnectorRegistry { /* BTreeMap<String, Box<dyn DynConnector>> */ }

impl ConnectorRegistry {
    pub fn new() -> Self;
    /// Panics if `C::TYPE` is already registered, or if `C::Options`'s
    /// schema has a property named after a reserved key (0003 §3): both are
    /// wiring bugs in the binary, found the first time it runs.
    pub fn register<C: Connector>(&mut self, connector: C) -> &mut Self;
    /// Replaces a registered type on purpose, e.g. a patched postgres.
    pub fn replace<C: Connector>(&mut self, connector: C) -> &mut Self;
    /// For connectors that aren't a Rust `Options` type (see §7). Same
    /// panics as `register`.
    pub fn register_dyn(&mut self, connector: Box<dyn DynConnector>) -> &mut Self;
    /// Registers a connector that can host the state store, paired with
    /// its backend (§8). Same panics as `register`.
    pub fn register_state_capable<C, B>(&mut self, connector: C, backend: B) -> &mut Self
    where
        C: StateCapable,
        B: StateBackend<Pool = C::Pool, Conn = C::Conn>;

    pub fn types(&self) -> impl Iterator<Item = (&str, Roles)>;
    pub fn build(&self, conn: &RawConnection, wants: Roles) -> Result<Built, BuildError>;
    /// The state connection: `wants` plus the state store, on one pool (§8).
    pub fn build_with_state(
        &self,
        conn: &RawConnection,
        wants: Roles,
    ) -> Result<(Built, Arc<dyn StateStore>), BuildError>;
    /// The `connections[]` item schema for every registered type (§3).
    pub fn connections_schema(&self, generator: &mut SchemaGenerator) -> Schema;
}
```

`register` wraps `C` in an internal `Typed<C>`, which implements
`DynConnector`. Deserialization goes through `serde_path_to_error`, so a
wrong type or unknown field comes back as an `OptionsError` with a path
instead of a bare serde message.

`RawConnection` (name, type, fields as `serde_json::Value`) is the
`ferry-registry` view of a connection entry. The CLI converts B1's parsed
entry into it and keeps the span map, so it can turn
`connections[1].schema` back into `ferry/connections.yaml:14:5`.

Built-in options, as an example of the shape this record owns:

```yaml
connections:
  - name: legacy
    type: postgres
    url: ${LEGACY_PG_URL}      # libpq URL or key=value string; sslmode goes here
    password: ${LEGACY_PG_PW}  # optional; overrides a password in `url`
    schema: public             # default: public. Read from as a source, written to as a sink.

  - name: shop
    type: mongo
    url: ${SHOP_MONGO_URL}     # mongodb:// or mongodb+srv://
    database: shop
```

- Both types use `url`, not `uri` for Mongo: one word across types is
  easier to remember than each ecosystem's own.
- `sslmode` stays inside the Postgres URL, where Track A's TLS policy
  already reads it. It is not duplicated as a separate field.
- Batch size is a runtime concern (A5's memory budget), not a connection
  field.
- A2 may add fields, such as a schema-inference sample size, as fields on
  `MongoOptions`. That needs no registry change.
- Because `build` must not do I/O, `mongodb+srv://` DNS resolution happens on
  first connect, not in `build`.

### 3. Type-specific fields: per-connector schema fragments, checked by serde

Each connector's `Options` derives `JsonSchema`. `connections_schema()`
composes one complete object schema per registered type and puts them in a
`oneOf` under `connections.items`:

```json
{ "oneOf": [
  { "type": "object",
    "properties": {
      "name": { "type": "string" },
      "type": { "const": "postgres" },
      "url":  { "type": "string", "writeOnly": true, "x-ferry-secret": true,
                "description": "libpq URL or key=value string. ..." },
      "password": { "...": "..." },
      "schema": { "type": "string", "default": "public" } },
    "required": ["name", "type", "url"],
    "additionalProperties": false },
  { "...": "mongo, then each third-party type" }
] }
```

Every branch repeats `name` and `type`, because `additionalProperties: false`
doesn't see properties declared in sibling subschemas. B1 generates the rest
of the schema with `connections.items` left open, and the registry fills it
in. The JSON Schema therefore depends on which connectors a binary has, so
it is generated by the binary (`ferry schema`, §6), not shipped as a static
file. The schema published for the stock `ferry` is just that command's
output.

`${ENV}` vs typed fields: `${VAR}` is substituted before deserialization, so
`port: ${PG_PORT}` is valid config, but the plain derived schema would flag it
in the editor as "expected integer". The composition step therefore widens
every non-string scalar leaf in connection fragments to
`anyOf: [<original>, {type: string, pattern: "^\\$\\{[A-Za-z_][A-Za-z0-9_]*\\}$"}]`.

Validation itself never goes through JSON Schema. After `${ENV}`
substitution, the entry deserializes into the connector's `Options` and then
`build` runs. The Rust type is the single source of truth, and the schema is
derived from it, so the two can't drift.

Editors (VS Code YAML / yaml-language-server, JetBrains) with this schema can
autocomplete:

- `type:` values (from the `const` in each branch)
- each type's field names, with doc-comment descriptions, defaults and enum
  values
- required fields and unknown fields (e.g. a typo like `shcema:`), flagged as
  you type

Not covered by the schema:

- connection names referenced in `from:`, `sink:` and
  `runtime.state.connection`. The schema is static and
  can't see names defined elsewhere in the config.
- entity names, which need a live database.

`ferry validate` catches the first, and `ferry plan` the second.

Rejected: **a free-form `options` map checked when the connector is built.**
Simpler for connector authors, but it gives no autocomplete, catches typos
only at run time, and lets every connector invent its own error format. The
cost of fragments is one `#[derive(Deserialize, JsonSchema)]` per connector.
A connector that really is free-form can declare
`Options = serde_json::Map<String, Value>` and opt out.

### 4. Secrets: `Secret`, schema markers and one redaction pass

- `ferry-registry` provides `Secret`, a string newtype. `Debug` and
  `Display` print `***`, the only way to read the value is `expose()`, and
  its `JsonSchema` is `{type: string, writeOnly: true, x-ferry-secret: true}`.
  Connectors must use it for every credential-bearing field. Postgres `url`
  and `password`, and Mongo `url`, are all `Secret`.
- `${ENV}` is the way to supply secrets. Substitution happens before
  deserialization (B1), so a connector never knows whether a value came from
  the environment.
- **Redaction set.** On build, the registry returns the value of every
  `x-ferry-secret` field (whatever its origin, literal or `${ENV}`) plus
  `derived_secrets()` (e.g. the password parsed out of a Postgres URL). The
  CLI's renderer replaces each occurrence with `***` in **everything** it
  prints: diagnostics, plan output, logs and run reports. This is a backstop
  for driver errors that echo a connection string. Secret values substituted
  into fields not marked secret are not redacted. That is documented, and it
  is why connectors must mark their fields.
- `ferry plan` and the run report show connections only through
  `describe()`. The state store records each connection's `name` and `type`,
  never its fields.
- `ferry validate` warns (it doesn't fail) when a secret field holds a
  literal instead of `${ENV}`, because config directories get committed.
  Local runs against testcontainers legitimately use literals.
- Config hash (A4): the registry only ever sees substituted values, so it
  can't compute the hash. The loader hashes the parsed model **before**
  substitution, by the rules in 0003 §10 (which also leave runtime tuning
  settings out). Rotating a password then doesn't block `resume`, and secret
  values never reach the hash.
- `${NAME}` from the environment is the only substitution form (0003 §9).
  Secrets mounted as files are exported to the environment by whatever starts
  `ferry`.

### 5. `ferry-cli` as lib + bin

The library exposes one builder and an entry point. That is the whole
stable surface in v0.1:

```rust
// ferry-cli (lib)

pub use ferry_registry as registry;

pub struct Ferry { /* registry, binary name, version */ }

impl Ferry {
    /// No connectors registered.
    pub fn new() -> Self;
    /// Built-in postgres and mongo, each behind a default-on cargo feature
    /// (`postgres`, `mongo`), so a slim binary can drop the Mongo driver.
    pub fn with_builtins() -> Self;
    pub fn register<C: registry::Connector>(self, connector: C) -> Self;
    /// See §8. `with_builtins` pairs postgres with ferry-state's backend.
    pub fn register_state_capable<C, B>(self, connector: C, backend: B) -> Self
    where
        C: registry::StateCapable,
        B: registry::StateBackend<Pool = C::Pool, Conn = C::Conn>;
    /// Name used in help text and error messages; defaults to "ferry".
    pub fn name(self, name: &'static str) -> Self;
    /// Parses `std::env::args`, builds the Tokio runtime, runs the command
    /// and renders errors. Never panics on user error.
    pub fn main(self) -> ExitCode;
    /// Same, with explicit arguments and output, for tests.
    pub fn run_with(
        self,
        args: impl IntoIterator<Item = OsString>,
        out: &mut dyn Write,
        err: &mut dyn Write,
    ) -> ExitCode;
}
```

The `ferry` binary is `fn main() -> ExitCode { Ferry::with_builtins().main() }`.
A third-party binary that adds a connector:

```rust
// acme-ferry/src/main.rs
use std::process::ExitCode;

use acme_ferry_clickhouse::ClickHouse;
use ferry_cli::Ferry;

fn main() -> ExitCode {
    Ferry::with_builtins()
        .register(ClickHouse)
        .name("acme-ferry")
        .main()
}
```

```rust
// acme-ferry-clickhouse/src/lib.rs (depends on ferry-core and ferry-registry only)
use std::sync::Arc;

use ferry_registry::{BuildContext, BuildError, Connector, Endpoints, Roles, Secret};

pub struct ClickHouse;

/// A ClickHouse database.
#[derive(serde::Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ClickHouseOptions {
    /// HTTP(S) endpoint, e.g. https://ch.example.com:8443
    url: String,
    user: String,
    password: Secret,
    database: String,
}

impl Connector for ClickHouse {
    const TYPE: &'static str = "clickhouse";
    const ROLES: Roles = Roles::SINK;
    type Options = ClickHouseOptions;

    fn build(&self, ctx: &BuildContext<'_>, o: ClickHouseOptions, _: Roles)
        -> Result<Endpoints, BuildError>
    {
        let sink = ClickHouseSink::new(&o.url, &o.user, o.password, &o.database)
            .map_err(BuildError::Other)?;
        Ok(Endpoints { sink: Some(Arc::new(sink)), ..Default::default() })
    }

    fn describe(&self, o: &ClickHouseOptions) -> String {
        format!("{} as {}, database {}", o.url, o.user, o.database)
    }
}
```

The third-party binary gets `validate`, `plan`, `run`, `resume`, `retry` and
`schema` unchanged. Its `schema` output includes `clickhouse`, so its users
get autocomplete for it.

Rust-level command functions (`validate(&Project)`, `plan(..)`, ...) stay
`pub(crate)` in v0.1. The subcommands are the reuse surface. Exposing the
functions would freeze planner and runner types before S3 has run them on
real data.

### 6. The CLI layer

Global flags: `-c, --config <DIR>` (default `./ferry`, also read from
`FERRY_CONFIG`), `--color auto|always|never` (honours `NO_COLOR`), `-v`/`-q`.
If `--config` points at a file, the error suggests its parent directory.
Command output (plan text, run id, summaries) goes to stdout. Diagnostics and
logs go to stderr, so `ferry plan > plan.txt` captures only the plan.

| Command | What it does | Connects? | Plugs into |
|---|---|---|---|
| `ferry validate` | Load the directory, deserialize every connection, `build` each with the roles its flows use, check every `from:`/`sink:` names a connection with the right role and that `runtime.state.connection` names a state-capable connection of the `backend` type (§8), run the static flow checks B1 offers. Reports **all** errors, not just the first. | No | New registry/CLI ticket. Uses B1's loader. |
| `ferry plan` | `validate`, then `discover` + `capabilities` on sources and `capabilities` on sinks, type check (B3), print the DAG, DDL and strategies. Writes nothing. | Read-only | B7 owns the handler and its snapshot. This ticket provides the subcommand, the built endpoints and rendering. |
| `ferry run` | `plan`, then create the run in the state store and start the runner. Prints the run id first. | Yes | S2 wires it end-to-end on A5's runner. |
| `ferry resume <run-id>` | Continue an unfinished run. The run id is required. Refused if the config hash or engine version changed (A4). | Yes | A5 entry point; refusal semantics from A4. |
| `ferry retry --failed <run-id>` | Re-run only the failed partitions of a finished run. The run id is required. | Yes | A5. |
| `ferry schema [--out <dir>]` | Print the JSON Schema for this binary's registry: one schema per file kind (`ferry.yaml`, `connections.yaml`, `runtime.yaml`, flow files). | No | New registry/CLI ticket. B1 generates the base, §3 adds connections. |

The skeleton ticket ships every subcommand. Until its owner lands, a
subcommand exits 1 with "not implemented yet (A5)" or similar, so S2
replaces a stub instead of adding a command.

Exit codes are part of the CLI's contract, for scripts and CI:

| Code | Meaning | What to do |
|---|---|---|
| 0 | Success. Warnings and dead-lettered rows under `max_errors` still exit 0; the summary prints the dead-letter count. | |
| 1 | Internal error or not implemented. | Report a bug. |
| 2 | Usage error (bad flags, `resume`/`retry` without a run id), clap's default. | Fix the command. |
| 3 | Config invalid: load, schema, options, missing `${ENV}`, wrong role, state connection not state-capable. | Fix the config. |
| 4 | Plan refused: type errors, no idempotency strategy, dependency cycle, unknown entity. | Fix the config or the data. |
| 5 | Could not connect or authenticate. | Retrying may help. |
| 6 | Run finished with failed partitions or exceeded `max_errors`. | `ferry retry --failed <run-id>`. |
| 7 | Verification failed (`expect:`, A8). Data was written. | Inspect the report. |
| 8 | Resume refused: config hash or engine version changed (A4). | Revert the config, or start a new run. |
| 130 | Interrupted (Ctrl-C). In-flight transactions aborted, leases released. | `ferry resume <run-id>`. |

Error rendering: every error the CLI prints is a diagnostic with

- a severity
- a stable code (e.g. `F0301`), so docs and CI can refer to it
- a message
- the original file and line after includes and merging (B1's spans)
- the field path
- an optional help line

```
error[F0301]: connections[1] (legacy): unknown field `shcema`
  --> ferry/connections.yaml:14:5
   |
14 |     shcema: public
   |     ^^^^^^ did you mean `schema`?
```

`--format json` prints the same diagnostics as JSON lines for CI and editor
integrations. The redaction pass (§4) runs last, on the final rendered text
of every format. `miette` fits this design (named sources, labels, help,
JSON); the ticket should confirm it or pick an equivalent.

### 7. Keeping a subprocess connector protocol possible, without building it

v0.1 registers connectors only at compile time. These choices keep a later
language-agnostic protocol (a child process that speaks JSON for control and
Arrow IPC for batches over stdio) an additive change:

- `DynConnector` is the real boundary, and it is data-only: options arrive
  as `serde_json::Value`, the schema is JSON Schema, and roles are two
  booleans. A `SubprocessConnector` would implement it by running
  `<cmd> spec` for its schema and returning proxy `Source`/`Sink`
  implementations. `register_dyn` and `type_name(&self) -> &str` (not
  `&'static`) exist so such a type can be registered from data, not only
  from a Rust const.
- `build` does no I/O and returns trait objects, so a proxy that starts its
  process lazily fits the same contract.
- `Source`/`Sink` already exchange Arrow `RecordBatch`es and serializable
  `Partition`s, which cross a process boundary as IPC without loss.
- `BuildContext` is `#[non_exhaustive]` and carries only data, never handles.
  Nothing lets a connector reach into the runner. The one state store handle
  a connector ever sees is the `Ledger` a state-capable connector is given
  (§8), and it does one thing: record a partition done.

The protocol itself, how such a connector is declared in config, and what
`ferry schema` does for it are all out of scope. One known limit: a
subprocess sink cannot share the state store's transaction, so it can only
offer `UpsertByKey` or `DeletePartitionThenInsert`, never `TxnWithState`.
`SinkCaps` already expresses that, and §8 explains why it stays true.

### 8. State-capable connectors: one transaction for the sink and the state store

`TxnWithState` is the only exactly-once strategy: a partition's rows and its
done-record (the checkpoint) commit in one transaction, and the commit is
refused if the lease epoch is no longer current (0001). Everything else the
state store does (create the run, store the plan, grant and renew leases,
mark failures) needs the same database but not the sink's transaction. The
sharing goes through `ferry-registry` as a typed extension, so the built-in
Postgres connector gets it the same way a third-party one would, and no
connector crate has a private path into the state store.

**Declaring it.** A connector that can host the state store implements a
second trait, and is registered with a backend that knows how to keep state
in its database:

```rust
// ferry-registry::state

/// The state store as the runner sees it: runs, plans, leases, done-records.
/// A4 owns the methods. It lives here because connectors (through
/// `StateBackend`) and the runner both name it. `ferry-state` implements it
/// for the file backend and for each database backend.
#[async_trait]
pub trait StateStore: Send + Sync { /* A4 */ }

/// The one piece of state store work that runs inside a sink's transaction.
#[async_trait]
pub trait Ledger<Conn: Send + 'static>: Send + Sync {
    /// Fails with `FerryError::StaleLease` if a newer lease exists for the
    /// partition, otherwise writes its done-record. Runs on `conn` inside the
    /// sink's open transaction, so the rows and the record commit together
    /// or not at all.
    async fn record_done(
        &self,
        conn: &mut Conn,
        lease: &Lease,
        summary: &WriteSummary,
    ) -> Result<()>;
}

/// A connector whose sink can carry the state store's done-record.
pub trait StateCapable: Connector {
    /// Driver-level handles the backend works with. They are types from the
    /// driver crate, not from this connector, so a backend can name them
    /// without depending on the connector. Postgres: the pool type A1/A3
    /// settle on, and `tokio_postgres::Client`.
    type Pool: Clone + Send + Sync + 'static;
    type Conn: Send + 'static;

    /// The connection's pool. Connects lazily: no I/O, like `build`.
    fn pool(
        &self,
        ctx: &BuildContext<'_>,
        options: &Self::Options,
    ) -> Result<Self::Pool, BuildError>;

    /// `build` on `pool`, with the sink bound to `ledger`. Only a sink built
    /// here lists `TxnWithState` in `capabilities()`. Under that strategy its
    /// commit calls `ledger.record_done` on the transaction's own connection,
    /// after the last write and before COMMIT.
    fn build_with_ledger(
        &self,
        ctx: &BuildContext<'_>,
        options: Self::Options,
        wants: Roles,
        pool: Self::Pool,
        ledger: Arc<dyn Ledger<Self::Conn>>,
    ) -> Result<Endpoints, BuildError>;
}

/// A state store kept in a connector's database (A4, in `ferry-state`).
pub trait StateBackend: Send + Sync + 'static {
    type Pool: Clone + Send + Sync + 'static;
    type Conn: Send + 'static;

    /// No I/O. The store creates its tables on first use.
    fn open(&self, pool: Self::Pool) -> (Arc<dyn StateStore>, Arc<dyn Ledger<Self::Conn>>);
}
```

`Ferry::with_builtins()` calls
`register_state_capable(Postgres, ferry_state::PgStateBackend)`. A
third-party database that wants exactly-once ships both halves and makes the
same call.

**Resolving `runtime.state.connection`.**

1. `ferry validate` looks up the named connection, checks its `type` equals
   `backend:` and that the registry says the type is state-capable. Any
   failure is a config error (exit 3) at the `runtime.yaml` line. Under
   `backend: file` there is no state connection, and nothing below happens.
2. The CLI builds every connection once, with the roles its flows use (§2).
   The state connection goes through `build_with_state` instead of `build`,
   with `Roles::NONE` if no flow uses it. Inside, the registry deserializes
   the options once and calls `pool`, then `backend.open(pool.clone())`, then
   `build_with_ledger(.., pool, ledger)`. The source half, the sink half and
   the state store share that one pool.
3. Each worker's `SinkTxn` holds one connection from the pool for the whole
   partition. Under `TxnWithState` the runner doesn't write the done-record
   itself: the sink's `commit` calls `ledger.record_done` on that
   connection, then commits. A crash anywhere before COMMIT leaves neither
   the rows nor the record; a crash after it leaves both. Under every other
   strategy the runner calls the store after `commit` returns, and a crash
   in between re-runs a partition the strategy makes safe to re-run.

**When the sink isn't the state connection.** This covers a flow whose sink
is a different connection (even one with the same type and the same URL:
connections are identified by name, and URLs are secrets the registry
doesn't compare), a sink whose connector isn't state-capable, and every flow
under `backend: file`. Such a sink was built with plain `build`, so it never
lists `TxnWithState`, and the planner takes the next strategy
`SinkCaps::strategy_for` returns: `DeletePartitionThenInsert`, `UpsertByKey`
or `StagingSwap`, depending on the sink and the mode. Only when that is
`None` is it a plan error (exit 4), as for any flow (0001, B7). `ferry plan`
names the strategy per flow and, for these flows, why:
`DeletePartitionThenInsert (state store is on "warehouse", not "reporting")`.

Falling back, rather than refusing the plan, because:

- Every strategy in `SinkCaps` already makes a re-run safe; that is what
  0001 requires of them. The difference is that a partition may be written
  twice and the second write repairs the first, instead of being written
  once. The data ends up the same.
- The config never names a strategy (0003 §5.5). Refusing would make the
  state store's location decide whether a run can start at all, and a run
  with flows into two databases (only one of which can hold the state
  store) could never start.
- It is what `strategy_for` means: `TxnWithState` is the first preference,
  not a requirement. The fallback stays visible in `ferry plan`, and moving
  `runtime.state.connection` to the flow's sink is a one-line fix.

**Type safety, without `unsafe`.** The connector's handle types and the
backend's must be equal, and the compiler checks it once, in the `where`
clause of `register_state_capable`. After that call, an internal
`StateTyped<C, B>` implements `DynConnector` and calls `pool`, `open` and
`build_with_ledger` with concrete types. The runner only ever sees
`Arc<dyn Sink>` and `Arc<dyn StateStore>`. There is no `Any`, no downcast
and no `TypeId`. If the connector and `ferry-state` end up on incompatible
driver versions, their `Pool` types differ and the binary fails to compile
rather than at run time. The contract suite checks that a sink lists
`TxnWithState` if and only if it was built through `build_with_ledger`, so a
connector can't claim exactly-once without being bound to the run's state
store.

**The subprocess protocol (§7).** `Pool` and `Conn` are in-process Rust
types, and a ledger call runs on a live driver connection, so neither can
cross a process boundary. A `SubprocessConnector` is therefore never
state-capable: it registers through `register_dyn`, keeps the default
`state_capable() == false`, its sinks never list `TxnWithState`, and its
flows fall back as above. Naming it in `runtime.state.connection` is a
config error. `DynConnector` gains a boolean and one method with a default,
so the boundary §7 keeps data-only is unchanged. If a later protocol wants
exactly-once, the state backend would have to run inside the subprocess too;
that is a new design, not an extension of this one.

## Consequences

- No change to frozen `ferry-core` contracts. One new workspace member,
  `ferry-registry`, and the README's crate table gains a row.
- `ferry-state` depends on `ferry-registry`, implements `StateStore` for the
  file backend, and provides `PgStateBackend` behind a `postgres` feature.
  The Postgres sink (A3) implements `StateCapable`; the state store (A4)
  implements the backend and `Ledger`. Both build on this record's crate, so
  A3 and A4 depend on its ticket.
- A run gets exactly-once only for flows whose sink is the state connection.
  Other flows re-run partitions safely through their sink's other strategy.
- Every connector crate (Track A) gains a dependency on `ferry-registry` and
  `schemars`, and a small `impl Connector` next to its `Source`/`Sink`.
  `PostgresSource::new` already has the no-I/O shape `build` requires.
- The JSON Schema is per-binary. Editor setup points at the output of
  `ferry schema` (via a `yaml-language-server` modeline or settings), not at
  a URL baked into the repo. The published schema is the stock binary's.
- Connection field names (`url`, `password`, `schema`, `database`) are now
  part of the user-facing config. They are owned here, and renaming them later
  is a breaking change.
- `ferry-registry` gets a `testkit` contract suite, `check_connector::<C>()`,
  in the same style as 0.5's source/sink suites. It checks that:
  - the schema generates
  - an unknown field is rejected with a path
  - a known secret never appears in `Debug`, `describe()` or build errors
  - `build` returns exactly the roles asked for
  - the `Options` schema uses no reserved key
  - for a state-capable connector: its sink lists `TxnWithState` only when
    built through `build_with_ledger`, and a crash between the data write
    and `record_done` commits neither

  Built-ins must pass it, and third parties can run the same suite.
- **New ticket** (replaces gap analysis items 3 and 6):

  Scope:
  - `ferry-registry`
  - the `Connector` impls for postgres and mongo
  - the `ferry-cli` lib/bin split with the `Ferry` builder
  - the state-capable extension (§8) and `register_state_capable`
  - argument parsing, `--config`, exit codes and diagnostic rendering
  - `ferry validate` and `ferry schema`
  - stub subcommands for `plan`, `run`, `resume <run-id>` and
    `retry --failed <run-id>`

  Done when:
  - validate reports every error in the bad-config set, each with file:line
  - a third-party example binary in `tests/` registers a fake connector and
    validates a config that uses it
  - the generated schema autocompletes `type:` and per-type fields in VS Code
    YAML (checked by hand once and recorded)
  - a planted secret never appears in any output
  - a state connection that isn't state-capable, or whose type differs from
    `backend:`, fails validate at the `runtime.yaml` line

  Blocks S2, A3 and A4. Depends on B1's loader and spans.

## Open questions

Decided 2026-10-16:

- Reserved connection keys: `name`, `type`, `description`, published in
  0003 §3. `register` (and `register_dyn`, `register_state_capable`) panics
  if an `Options` schema uses one, and the contract suite checks it.
- State store connection: the sink↔state store transaction goes through
  `ferry-registry` as the typed state-capable extension (§8), not privately
  inside the Postgres crates. A sink that isn't on the state connection
  falls back to its next strategy per `SinkCaps::strategy_for`.
- `ferry run` exits 0 when dead-lettered rows stay under `max_errors`, with
  the count in the summary.
- `resume` and `retry` require a run id.
- `${ENV}` only; no `${file:...}` in v0.1.
- `url` for both Postgres and Mongo.

Still open:

1. **Connector versions in the run record.** A4 pins runs to the engine
   version. In a third-party binary, a connector upgrade can change how
   partitions or keys are encoded. Proposed: record `type@version` per
   connection, and have `resume` warn (not refuse) on a mismatch.
2. **`--fail-on-dead-letters`.** Some CI users will want any dead-lettered
   row to fail the run. Not in v0.1 unless someone asks.
3. **Requiring exactly-once.** §8 falls back silently apart from the plan
   output. Should a flag (or a `ferry plan` warning that CI can fail on)
   refuse a run where any flow doesn't get `TxnWithState`?
4. **Per-connection throttles.** Whether they replace or add to the run-wide
   `source_max_connections` and `source_rows_per_sec` (0003 §6, §12).
