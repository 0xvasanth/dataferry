# Architecture

dataferry is a Cargo workspace of small crates with one-way dependencies.
This page maps them, follows a record through a run, and points to the
decision records behind the design.

## Crates

| Crate | Responsibility |
| --- | --- |
| `dataferry-core` | The contracts every other crate shares: `Source`, `Sink`, `SinkTxn`, `Entity`, partitions and co-partitions, leases and epochs, the canonical types, the transform contract, missing-value tracking, and `FerryError`. Depends on Arrow and a few small crates (serde, futures, async-trait, thiserror), nothing else of dataferry's. Its `testkit` feature has the source and sink contract suites and in-memory implementations. |
| `dataferry-registry` | The connector author's API: the `Connector` trait and the `ConnectorRegistry` that maps a `type:` to one, `Secret` and redaction, the JSON Schema composition of `connections.yaml`, the state-capable extension (`StateCapable`, `StateBackend`, `StateStore`, `Ledger`), finalizers and DDL rendering. Its `testkit` feature has the connector contract suites. |
| `dataferry-connector-tls` | The TLS policy both connectors follow. |
| `dataferry-connector-postgres` | The PostgreSQL source and sink, and its `Connector` implementation. |
| `dataferry-connector-mongo` | The MongoDB source, with schema inference, and its `Connector` implementation. |
| `dataferry-state` | The state store: runs, partitions, leases and epochs, kept in files or in Postgres (`PgStateBackend`), and the resume checks. |
| `dataferry-runtime` | The runner: orders flows, leases partitions, reads, transforms, writes and commits each one, dead-letters, throttles, sizes batches from the memory budget, finalizes. Its `testkit` feature has the chaos harness and the conformance suite. |
| `dataferry-verify` | The verifier: write-time checksums and counts, reading targets back, constraint checks, `expect:` evaluation, and the run report. |
| `dataferry-dsl` | The config: the loader (files, `include:`, `${ENV}`, spans for error locations), the model and its JSON Schemas, the config hash, the expression language (on DataFusion), the type checker, the compiler of flows into transform plans, `expect:` parsing, and the planner. |
| `dataferry` | The command line, as a library (`Ferry`) and the `dataferry` binary: argument parsing, diagnostics and their rendering, `validate`, `schema`, `plan`, `run`, `resume`, `retry`, and the wiring of the built-in connectors. |

How they depend on each other:

```text
dataferry                     ─► dataferry-dsl, dataferry-runtime, dataferry-state,
                                 dataferry-verify, dataferry-connector-postgres,
                                 dataferry-connector-mongo
dataferry-dsl                 ─► dataferry-runtime, dataferry-verify
dataferry-verify              ─► dataferry-runtime, dataferry-connector-postgres
dataferry-runtime             ─► dataferry-state
dataferry-connector-postgres,
dataferry-connector-mongo     ─► dataferry-connector-tls

every crate except
dataferry-connector-tls       ─► dataferry-registry ─► dataferry-core
```

The constraints that matter: a connector crate needs only
`dataferry-core` and `dataferry-registry` (plus the TLS policy), so a
third-party connector doesn't depend on the planner, the runner or
DataFusion; and the built-in connectors register through the same public
API a third party uses.

## A run, end to end

```text
 ferry/*.yaml
     │  dataferry-dsl: load ─► validate ─► hash ─► ${ENV} substitution
     ▼
 Config ──► dataferry-registry: build each connection (no I/O) ─► Source / Sink
     │
     │  dataferry (CLI): Source::discover, Sink::capabilities
     │  dataferry-dsl: type-check expressions ─► compile flows into
     ▼          transform plans ─► order flows ─► pick strategies
 Plan
     │  dataferry-state: create the run, pin hash + engine version
     ▼
 dataferry-runtime:
     every flow, up front: Sink::prepare ─► plan + store partitions
     workers, each flow once the flows it waits for are done:
        lease (epoch) ─► Source::read (Arrow batches)
                     ─► transform plan: rows per target, or rejected records
                     ─► SinkTxn::write per target ─► dead letters
                     ─► record counts + write-time checksums
                     ─► SinkTxn::commit (with the done-record, under TxnWithState)
     every flow done, after all have stopped: finalize (restore
        constraints and indexes, validate foreign keys)
     │
     ▼
 dataferry-verify: read targets back ─► compare checksums, counts,
     constraints, dead letters ─► evaluate expect: ─► report.md / report.json
```

Records travel as Arrow `RecordBatch`es from the source to the sink.
Expressions are compiled once per flow into DataFusion physical
expressions and evaluated over whole batches. A partition's rows for all
its targets go to the sink in one transaction.

## Decision records

The design decisions are written down, with their context and the
alternatives rejected. They are the authority when this guide and the code
seem to disagree on intent:

- [0001: Commit once per partition](reference/decisions/0001-commit-per-partition.md):
  one sink transaction per partition, no checkpoints inside one; every
  sink declares an idempotency strategy.
- [0002: Co-partitioning](reference/decisions/0002-co-partitioning.md):
  how a merge's joined entity is cut at the driver's partition boundaries,
  and the fallback to staging.
- [0003: Config shape](reference/decisions/0003-config-shape.md): the
  `ferry/` directory, the flow grammar, scopes and sigils, `where:`
  propagation, `${ENV}`, the config hash.
- [0004: Connector registry](reference/decisions/0004-connector-registry.md):
  the `Connector` trait, options and their schemas, secrets and
  redaction, the CLI layer and exit codes, state-capable connectors.

They were written while the project was called ferry, and use its crate
and command names (`ferry-core`, `ferry plan`): read them as
`dataferry-core` and `dataferry plan`.
