# dataferry

An open-source data migration engine written in Rust. A declarative config
in a `ferry/` directory describes how data moves, and how it is reshaped,
between data stores: 1:1 copies, splits into several tables, unnesting
arrays into child tables, and merges of several entities into one table.

v0.1 moves MongoDB → Postgres and Postgres → Postgres, as a single-process
CLI. Each partition commits on its own, so a run that stops halfway resumes
where it left off and writes nothing twice
([decision 0001](docs/decisions/0001-commit-per-partition.md)).

The [user guide](https://0xvasanth.github.io/ferry/) covers every config key,
the expression language and how runs resume. Each crate's API is on docs.rs
([`dataferry`](https://docs.rs/dataferry)).

## Install

**Prebuilt binaries** for Linux (x86_64, aarch64; glibc 2.35 or newer) and
macOS (x86_64, arm64) are on every
[GitHub release](https://github.com/0xvasanth/ferry/releases), each with a
SHA-256 checksum:

```sh
tar -xzf dataferry-<version>-<target>.tar.gz
./dataferry-<version>-<target>/dataferry --version
```

**From crates.io.** 0.1.0-beta.1 is a pre-release, so name the version:

```sh
cargo install --locked dataferry --version 0.1.0-beta.1
```

## Install from source

dataferry needs Rust 1.94 or newer.

```sh
git clone https://github.com/0xvasanth/ferry
cd ferry
cargo install --locked --path crates/dataferry
```

Each way installs the `dataferry` binary, with the built-in `postgres` and
`mongo` connectors. The project was called ferry before its first release:
the config directory is still `ferry/` (see [CHANGELOG.md](CHANGELOG.md)).

## The `ferry/` directory

```text
ferry/
  ferry.yaml          # version: 1, the migration's name, optional include:
  connections.yaml    # every database dataferry reads from or writes to
  runtime.yaml        # where run state lives, workers, memory, error policy
  flows/              # every *.yaml / *.yml file below here, at any depth
    orders.yaml
```

Every key is listed in the [config reference](docs/config-reference.md),
and the grammar is explained in
[decision 0003](docs/decisions/0003-config-shape.md). Credentials never go in
the config: a connection's `url` is a `${ENV}` reference. `dataferry schema`
prints JSON Schemas for editor completion ([schemas/](schemas/README.md)).

## Quickstart

The golden example, [`examples/golden/ferry`](examples/golden/ferry), moves
ten Mongo `orders` documents into Postgres `orders` and `order_items`
tables. Start a Mongo and an empty Postgres, and once Mongo is up, load the
documents into it:

```sh
docker run -d --name ferry-mongo -p 27017:27017 mongo:7
docker run -d --name ferry-pg -p 5432:5432 -e POSTGRES_PASSWORD=quickstart-pw postgres:16
docker exec -i ferry-mongo mongoimport --db shop --collection orders \
  --jsonArray < fixtures/golden/source/orders.json
```

Point the config at them. The config only names the variables:

```sh
export FERRY_GOLDEN_MONGO_URL=mongodb://localhost:27017
export FERRY_GOLDEN_PG_URL=postgres://postgres:quickstart-pw@localhost:5432/postgres
```

Check the config without connecting to anything, then print the JSON
Schemas this binary accepts:

```sh
dataferry validate --config examples/golden/ferry
dataferry schema --config examples/golden/ferry
```

See what a run would do. `plan` reads the sources and the sink's catalog,
and writes nothing:

```sh
dataferry plan --config examples/golden/ferry
```

Run it. The first line printed is the run id (`run <run-id>`); the report
is written to `examples/golden/.ferry/runs/<run-id>/`:

```sh
dataferry run --config examples/golden/ferry
```

A run that stopped (Ctrl-C, a crash, a lost connection) continues from its
committed partitions, and a run that finished with failed partitions reruns
only those:

```sh
dataferry resume --config examples/golden/ferry <run-id>
dataferry retry --config examples/golden/ferry --failed <run-id>
```

A test (`crates/dataferry/tests/readme.rs`) runs this section's
`mongoimport` and every `dataferry` command in it, in order, against real Mongo
and Postgres containers, so this quickstart can't silently stop working.

## A tour of the examples

Every config under [`examples/`](examples/README.md) passes
`dataferry validate`, and every case under `examples/bad/` must fail it. CI checks
both.

**1:1 copy.** `columns: "*"` copies every declared field of the entity
unchanged ([`pg-merge/ferry/legacy/customers.yaml`](examples/pg-merge/ferry/legacy/customers.yaml)):

```yaml
flows:
  - name: customers
    from: app.public.customers
    sink: reporting
    mode: upsert
    to:
      customers:
        key: [id]
        columns: "*"
```

**Split and unnest.** One Mongo document becomes a row in `orders` and one
row per element of its `items` array in `order_items`. `parent:` orders the
writes and adds the foreign key, and `$parent`, `$index` and `$key` reach the
mapped parent row, the element's position and a deterministic child key
([`golden/ferry/flows/orders.yaml`](examples/golden/ferry/flows/orders.yaml)):

```yaml
    to:
      orders:
        key: [id]
        columns:
          id: string(_id)
          status: lower(status)
          total_cents: int(round(total * 100))
      order_items:
        parent: orders
        unnest: items
        key: [order_id, line_no]
        columns:
          id: $key
          order_id: $parent.id
          line_no: $index
          qty: int(qty)
```

**Merge.** The first `from:` entry is the driver. Every other entry joins it
with a required `cardinality:`. A `one` join looks a row up, and a `many`
join can be nested into an ordered array column
([`pg-merge/ferry/flows/order_summaries.yaml`](examples/pg-merge/ferry/flows/order_summaries.yaml)):

```yaml
    from:
      - entity: app.public.orders
        as: o
      - entity: app.public.customers
        as: c
        join: {cardinality: one, keys: {id: o.customer_id}}
      - entity: app.public.order_items
        as: i
        join: {cardinality: many, keys: {order_id: o.id}}
    to:
      order_summaries:
        key: [order_id]
        columns:
          order_id: id
          customer_name: c.name
          items:
            nest: i
            order_by: [line_no]
            columns: {sku: sku, qty: qty}
```

The examples also show flow and target `where:` filters, dead-lettering
(`on_error: dead_letter`), `expect:` assertions checked after the run,
`depends_on:` and `include:`.

## Commands

| Command | What it does | Connects? |
| --- | --- | --- |
| `dataferry validate` | Loads the config, substitutes `${ENV}` and checks every connection's options and roles. Reports every error. | No |
| `dataferry schema [--out <dir>]` | The JSON Schema of each config file, including this binary's connection types. | No |
| `dataferry plan` | Discovers the sources, type-checks and compiles every flow, prints run order, strategies and DDL. | Read-only |
| `dataferry run` | Plans, then runs every flow, verifies the sinks and writes `report.json` and `report.md`. | Yes |
| `dataferry resume <run-id>` | Continues a run from its committed partitions. | Yes |
| `dataferry retry --failed <run-id>` | Puts a run's failed partitions back to pending, then resumes it. | Yes |

Global flags: `-c, --config <dir>` (default `./ferry`, or `FERRY_CONFIG`),
`--format text|json`, `--color auto|always|never`, `-v`, `-q`, and
`--work-dir <dir>` (or `FERRY_WORK_DIR`). The
[dataferry README](crates/dataferry/README.md) has the details and every
diagnostic code.

### Exit codes

Scripts and CI can rely on these: a code never changes meaning once
released.

| Code | Meaning |
| --- | --- |
| 0 | Success. Warnings, and dead-lettered rows under `max_errors`, still exit 0. |
| 1 | Internal error or a miswired binary. |
| 2 | Usage error: bad flags, or a run id the state store doesn't have. |
| 3 | Config invalid. |
| 4 | Plan refused: type errors, no idempotency strategy, dependency cycle. |
| 5 | Could not connect or authenticate. |
| 6 | Run finished with failed partitions, or exceeded `max_errors`. |
| 7 | Verification (`expect:`) failed. Data was written. |
| 8 | Resume refused: the config or the engine version changed. |
| 130 | Interrupted (Ctrl-C). Resume the run. |

## Adding a connector

A connector is a library crate that needs only `dataferry-core` and
`dataferry-registry` of dataferry's crates (plus `serde` and `schemars`). It implements `dataferry_registry::Connector`: a type
name, the roles it provides (source, sink or both), an options struct for
its fields in `connections.yaml` (serde with `deny_unknown_fields`, plus
`JsonSchema`, which `dataferry schema` and editors use), `build`, which returns
its `Source` and `Sink` endpoints, and `describe`
([decision 0004](docs/decisions/0004-connector-registry.md)).

```rust
use dataferry_registry::{BuildContext, BuildError, Connector, Endpoints, Roles, Secret};
use schemars::JsonSchema;
use serde::Deserialize;

pub struct ClickHouse;

/// A ClickHouse database. Doc comments become editor hover text.
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ClickHouseOptions {
    /// `clickhouse://` URL. A `Secret` is redacted from everything printed.
    url: Secret,
    /// The database written to.
    database: String,
}

impl Connector for ClickHouse {
    const TYPE: &'static str = "clickhouse";
    const ROLES: Roles = Roles::BOTH;
    type Options = ClickHouseOptions;

    fn build(
        &self,
        ctx: &BuildContext<'_>,
        options: ClickHouseOptions,
        wants: Roles,
    ) -> Result<Endpoints, BuildError> {
        // No I/O here: `dataferry validate` calls this. Keep the URL for the
        // endpoints, which connect on their first trait call.
        let ClickHouseOptions { url, database } = options;
        let _ = (ctx, url, database);
        todo!("the Source and/or Sink that {wants} asks for")
    }

    /// One line for `dataferry plan` and the run report. No secrets.
    fn describe(&self, options: &ClickHouseOptions) -> String {
        format!("clickhouse, database {}", options.database)
    }
}
```

Ship it in your own binary, which gets every `dataferry` subcommand, the
built-in connectors and yours:

```rust
fn main() -> std::process::ExitCode {
    dataferry::Ferry::with_builtins()
        .register(ClickHouse)
        .name("acme-dataferry")
        .main()
}
```

Check it with the contract suites in `dataferry-registry`'s `testkit` feature
(`check_connector`, and `check_state_capable` for a connector that can hold
the state store). [`crates/dataferry/tests/acme-dataferry`](crates/dataferry/tests/acme-dataferry)
is a complete third-party binary, used by the tests.

A connector counts as certified once it passes the conformance suite,
`dataferry_runtime::testkit::conformance` (`dataferry-runtime`'s `testkit`
feature), against real databases through your binary: a golden migration
with a green report (which takes at least one `expect:` assertion in the
pair's config), then 50 runs killed with SIGKILL mid-partition and
resumed, each ending with the expected rows and the uninterrupted run's
checksums. Its module docs show the `Pair` to implement;
[`crates/dataferry/tests/conformance.rs`](crates/dataferry/tests/conformance.rs)
certifies the built-ins (Mongo → Postgres, Postgres → Postgres) with it.

## Workspace

| Crate | Path | What it is |
| --- | --- | --- |
| `dataferry-core` | `crates/dataferry-core` | Shared contracts: `Source`, `Sink`, partitions, leases, errors |
| `dataferry-registry` | `crates/dataferry-registry` | The connector author's API and the connector registry |
| `dataferry-dsl` | `crates/dataferry-dsl` | The config model, expression language and planner |
| `dataferry-connector-tls` | `crates/dataferry-connectors/tls` | The TLS policy the connectors share |
| `dataferry-connector-postgres` | `crates/dataferry-connectors/postgres` | Postgres source and sink |
| `dataferry-connector-mongo` | `crates/dataferry-connectors/mongo` | MongoDB source |
| `dataferry-state` | `crates/dataferry-state` | Run, partition and lease state (file and Postgres) |
| `dataferry-runtime` | `crates/dataferry-runtime` | The runner |
| `dataferry-verify` | `crates/dataferry-verify` | The verifier and the run report |
| `dataferry` | `crates/dataferry` | The `dataferry` command line, as a library and a binary |

## Development

```sh
cargo build
cargo test --workspace
```

The minimum supported Rust version is 1.94, declared by every crate. CI
builds the workspace with it.

Connector tests start real Postgres and MongoDB containers with
[testcontainers](https://rust.testcontainers.org/), so a running Docker
daemon is required. If your Docker socket is not at `/var/run/docker.sock`
(OrbStack, Colima, rootless Docker), point testcontainers at the active
context:

```sh
export DOCKER_HOST=$(docker context inspect -f '{{.Endpoints.docker.Host}}')
```

`scripts/check-examples.sh` runs `dataferry validate` on every example and
checks every `examples/bad/` case fails, as CI does.
[`examples/golden/demo.sh`](examples/golden/README.md) runs the golden demo
on Docker, from `dataferry validate` to the report.

## Publishing

Every crate is published to crates.io by the release workflow when a
version tag is pushed: [RELEASING.md](RELEASING.md) has the steps.
`acme-dataferry` is test-only (`publish = false`). Every other crate
packages, and CI checks it:

```sh
cargo package --workspace --exclude acme-dataferry
```

Every dependency comes from crates.io. DataFusion is the released 55.2,
which is built on arrow 59, so the workspace's arrow crates stay on 59
until a DataFusion release moves to arrow 60; the two are upgraded
together.

## License

Apache-2.0
