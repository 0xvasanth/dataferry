# Changelog

Every notable change to dataferry. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions
follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html). Exit
codes never change meaning once released.

## [Unreleased]

## [0.1.0-beta.1] - 2026-10-18

The first release: v0.1 as a beta. It is feature-complete for what it
covers and fully tested against real Postgres and MongoDB, but the config
format and the library APIs may still change before 0.1.0.

### Changed

- The project publishes as **dataferry**: `ferry`, `ferry-cli` and
  `ferry-core` were already taken on crates.io. The command is `dataferry`
  (`cargo install --locked dataferry --version 0.1.0-beta.1`), and the
  crates are `dataferry` (the CLI, as a library and a binary),
  `dataferry-core`, `dataferry-registry`, `dataferry-dsl`, `dataferry-state`,
  `dataferry-runtime`, `dataferry-verify` and
  `dataferry-connector-{postgres,mongo,tls}`.
- What a migration keeps on disk and in its databases keeps its name, so
  nothing in an existing setup changes: the `ferry/` config directory and
  `ferry.yaml`, the `.ferry/` work directory, the `_ferry_*` tables in
  Postgres, the `FERRY_*` environment variables and the `x-ferry-secret`
  schema keyword.

### Added

- **Sources:** MongoDB and Postgres. **Sink:** Postgres. Connections to
  remote hosts always use TLS: plaintext only when the connection string asks
  for it (`sslmode=disable`, `tls=false`), or as a fallback when every host
  is local. TLS always verifies the server's certificate and hostname.
- **A declarative `ferry/` directory:** `ferry.yaml`, `connections.yaml`,
  `runtime.yaml` and `flows/**/*.yaml`, with `${ENV}` substitution, `include:`
  and a JSON Schema for every file (`dataferry schema`).
- **Transforms:** 1:1 copies (`columns: "*"`), splits into several tables,
  unnesting arrays into child tables (`unnest:`, `parent:`, `$parent`,
  `$index`, `$key`), merges of several entities into one table (`one` and
  `many` joins, `nest:` into ordered array columns), and `where:` filters on
  flows and targets.
- **An expression language** (SQL-like, built on DataFusion) for columns and
  filters, with strict conversions, decimal rounding and null-versus-missing
  semantics that match the source.
- **Resume:** every partition commits on its own, with its state, in the
  sink's transaction, so a run that stops (Ctrl-C, a crash, `kill -9`) resumes
  where it left off and writes nothing twice. State lives in a file or in a
  Postgres connection. `dataferry resume <run-id>` continues a run;
  `dataferry retry --failed <run-id>` re-runs its failed partitions.
- **Verification:** after a run, read-time counts and multiset checksums are
  compared with what the sink holds, and the `expect:` assertions in flows are
  checked. Every run writes `report.json` and `report.md`.
- **Dead-letter queue:** with `on_error: dead_letter`, a rejected record goes
  to a `_ferry_dlq` table on the sink, with its error, and the run carries on,
  up to `max_errors`.
- **Commands:** `validate`, `schema`, `plan`, `run`, `resume` and `retry`,
  with text or JSON diagnostics and stable exit codes.
- **Connector registry:** a connector is a library crate built on
  `dataferry-core` and `dataferry-registry`, registered by name in a binary
  built on the `dataferry` library. The built-in `postgres` and `mongo`
  connectors are registered the same way, and a contract suite checks third
  party ones.
- Prebuilt binaries for Linux (x86_64, aarch64) and macOS (x86_64, arm64),
  with SHA-256 checksums, on every GitHub release.

### Known limitations

These are planned for v0.2.

- Redaction is too broad: a short secret, such as a one-word password, is
  also masked where it appears in unrelated text like paths.
- No foreign-key discovery: flows that must run in order need `depends_on:`
  (or `parent:` within a flow).
- `on_error` and `max_errors` are run-wide: per-flow settings are refused.
- A Postgres table with a composite primary key reads as one partition, with
  no parallelism and no resume within the table.
- Postgres is the only sink.
- `LIKE` and `ILIKE` only take a backslash as the `ESCAPE` character.

[Unreleased]: https://github.com/0xvasanth/ferry/compare/v0.1.0-beta.1...HEAD
[0.1.0-beta.1]: https://github.com/0xvasanth/ferry/releases/tag/v0.1.0-beta.1
