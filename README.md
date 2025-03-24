# 0xferry

An open-source, Rust-based data migration engine where a declarative config
describes how data moves and reshapes between data stores: 1:1 copies,
splits, merges, nesting and unnesting.

> Status: pre-v0.1. Shared contracts (Phase 0) are being built.

## Workspace

| Crate | Path | Owner |
| --- | --- | --- |
| `ferry-core` | `crates/ferry-core` | Phase 0 (shared contracts). After Phase 0, changes need review from both tracks. |
| `ferry-registry` | `crates/ferry-registry` | Shared: connector author's API (decision 0004). Changes need review from both tracks. |
| `ferry-dsl` | `crates/ferry-dsl` | Track B: DSL & planning |
| `ferry-connector-postgres` | `crates/ferry-connectors/postgres` | Track A: data plane |
| `ferry-connector-mongo` | `crates/ferry-connectors/mongo` | Track A: data plane |
| `ferry-state` | `crates/ferry-state` | Track A: data plane |
| `ferry-cli` | `crates/ferry-cli` | Shared; wired at integration points |

## Development

```sh
cargo build
cargo test --workspace
```

Connector tests start real Postgres and MongoDB containers with
[testcontainers](https://rust.testcontainers.org/), so a running Docker
daemon is required. If your Docker socket is not at `/var/run/docker.sock`
(OrbStack, Colima, rootless Docker), point testcontainers at the active
context:

```sh
export DOCKER_HOST=$(docker context inspect -f '{{.Endpoints.docker.Host}}')
```

## License

Apache-2.0
