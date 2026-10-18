# dataferry

A data migration engine: a declarative `ferry/` directory says how data
moves from MongoDB and Postgres into Postgres, and how it is reshaped on the
way. Each partition commits on its own, so a stopped run resumes where it
left off. The [user guide](https://0xvasanth.github.io/dataferry/) and the
[repository](https://github.com/0xvasanth/dataferry) have the details.

```sh
cargo install --locked dataferry --version 0.1.0-beta.1
```

The `dataferry` command line, as a library and a binary (decision 0004 §5, §6).
The binary is `Ferry::with_builtins().main()`. A third party ships its own
binary with extra connectors the same way:

```rust
fn main() -> std::process::ExitCode {
    dataferry::Ferry::with_builtins()
        .register(ClickHouse)
        .name("acme-dataferry")
        .main()
}
```

`tests/acme-dataferry/` is such a binary, used by the tests.

## Built-in connectors

`with_builtins()` registers, through the same public calls:

| Type | Roles | Cargo feature (default on) |
| --- | --- | --- |
| `postgres` | source and sink; holds the state store (`backend: postgres`, with dataferry-state's `PgStateBackend`) | `postgres` |
| `mongo` | source | `mongo` |

Building with `--no-default-features` drops a driver; `with_builtins()` then
registers only what is left.

## Commands

| Command | What it does | Connects? |
| --- | --- | --- |
| `dataferry validate` | Loads the `ferry/` directory, substitutes `${ENV}`, and builds every connection through the registry with the roles its flows use. Checks connection options, that `from:` connections are sources and `sink:` connections are sinks, and that `runtime.state.connection` is a state-capable connection of the `backend:` type. Reports every error. | No |
| `dataferry schema [--out <dir>]` | The JSON Schema of each config file for this binary, with one `connections` branch per registered type. Prints one JSON object keyed by file name, or writes `<dir>/<file>.schema.json`. | No |
| `dataferry plan` | `validate`, then discovers every source the flows read and asks each sink for its capabilities, then type-checks and compiles every flow (`expect:` included). Prints the flows in run order (a flow waits for its `depends_on:` and for any flow writing a table it reads), each flow's idempotency strategy (and, when it isn't `TxnWithState`, why), partitioning, a merge's joins with each one's strategy (co-partitioned or staged) and why, each `nest:`'s full ordering, targets and the DDL its sink would run. Asks each co-partitioned join's source whether it can split on the driver's boundaries, and stages the join if it can't. Runs no DDL and writes nothing. | Read-only |
| `dataferry run` | Plans, prints the run id, then runs every flow on the runner with `runtime.yaml`'s settings and the `runtime.state` store. Verifies each sink after the run and writes `report.json` and `report.md` (one pair per sink, `report-<sink>.*`, when the run writes to several). | Yes |
| `dataferry resume <run-id>` | Plans again and continues the run from its stored partitions. Refused (exit 8) if the config hash or engine version changed. | Yes |
| `dataferry retry --failed <run-id>` | Puts the run's failed partitions back to pending, then resumes it. | Yes |

Global flags: `-c, --config <dir>` (default `./ferry`, or `FERRY_CONFIG`),
`--format text|json`, `--color auto|always|never` (`auto` honours
`NO_COLOR`), `-v`, `-q`, and `--work-dir <dir>` (or `FERRY_WORK_DIR`). Command
output goes to stdout, diagnostics to stderr.

A run's leases last 30 seconds without a heartbeat (every 10), so a resume
after a process was killed waits at most that long for the partitions it
held. `FERRY_LEASE_TTL_MS` sets the lease in milliseconds, from 1000 to
600000 (the heartbeat is a third of it); anything else is a usage error
(exit 2, F0601). It is for tests only: the chaos and conformance suites use
it to resume at once after a kill. It is not safe in production: a lease
shorter than a slow commit, a database pause or a network stall expires
under a live worker, which then loses its partition (fenced, so nothing is
written twice) and the run redoes or fails work it had done. `run`,
`resume` and `retry` warn (W0601) whenever it is set, even under `-q`.

The work directory (default: `.ferry` next to the config directory) holds
what a run keeps outside the state store: the write-time records the
verifier and `expect:` check against (so a resumed run is verified with
every attempt's records), `runs/<run-id>/report.{json,md}` (or
`report-<sink>.{json,md}` per sink; `report.md` escapes what the source and
target held and cuts each value to 200 characters, `report.json` keeps it
whole), the spill files of staged merge joins
while a run is going (`stage/`), and, under `backend: file` without a
`path:`, the state itself (`state/`).

## Exit codes

Part of the CLI's contract for scripts and CI. A test checks this table
against the code.

| Code | Meaning | What to do |
| --- | --- | --- |
| 0 | Success. Warnings, and dead-lettered rows under `max_errors`, still exit 0. | |
| 1 | Internal error, not implemented, or a miswired binary (a connector type registered twice). | Report a bug. |
| 2 | Usage error: bad flags, `resume`/`retry` without a run id, or with one the state store doesn't have. | Fix the command. |
| 3 | Config invalid: load, schema, options, missing `${ENV}`, wrong role, state connection not state-capable. | Fix the config. |
| 4 | Plan refused: type errors, no idempotency strategy, dependency cycle, unknown entity. | Fix the config or the data. |
| 5 | Could not connect or authenticate. | Retrying may help. |
| 6 | Run finished with failed partitions or exceeded `max_errors`. | `dataferry retry --failed <run-id>`. |
| 7 | Verification failed (`expect:`). Data was written. | Inspect the report. |
| 8 | Resume refused: config hash or engine version changed. | Revert the config, or start a new run. |
| 130 | Interrupted (Ctrl-C). In-flight partitions are abandoned uncommitted, and the leases the run held are released, so a resume takes them at once. | `dataferry resume <run-id>`. |

Each sink is verified on its own, against its flows' share of the run's
totals. A sink of a type this binary can't read back (a third-party type
without a verifier) is reported unverified (W0701), which doesn't affect the
exit code; what finalize couldn't restore on it still fails the run (7).

## Diagnostics

Every error and warning has a stable code, the file and line it was written
at (after includes), the field path and, where useful, a help line:

```text
error[F0302]: connections[warehouse].type: unknown connection type `postgresql`
  --> ferry/connections.yaml:10:11
   |
10 |     type: postgresql
   |           ^^^^^^^^^^
   = help: registered types: `mongo` (source), `postgres` (source and sink)
```

`--format json` prints the same diagnostics as one JSON object per line
(`severity`, `code`, `message`, `file`, `line`, `column`, `path`, `help`).
Every value substituted from the environment, and every value of a secret
connection field, is replaced with `***` in everything printed.

| Diagnostic | Meaning |
| --- | --- |
| F0001 | `--config` names nothing, or a file instead of the `ferry/` directory |
| F0002 | The binary registered its connectors wrongly |
| F0003 | Reserved: a command not implemented yet (none is now) |
| F0004 | Output could not be written |
| F0100–F0109 | Loader errors (dataferry-dsl): I/O, YAML, layout, include, model, duplicate, invalid reference, `${ENV}`, `depends_on` |
| F0110 | An expression doesn't parse or names nothing in scope (`plan`) |
| F0111 | An expression's type doesn't fit where it is used (`plan`) |
| F0112 | The planner refused the config: a cycle between flows, no idempotency strategy for a flow's mode, a per-flow setting the runner can't apply |
| F0301 | Bad connection options (unknown field, wrong type, bad value) |
| F0302 | Unknown connection type |
| F0303 | A flow uses a connection in a role its connector doesn't provide |
| F0304 | The state connection's type isn't `backend:` |
| F0305 | The state backend can't hold the state store |
| F0306 | A connector couldn't build a connection |
| W0301 | A secret field holds a literal value instead of `${ENV}` |
| W0401 | Something the plan points out that doesn't stop a run |
| F0501 | A source couldn't be discovered, a sink's capabilities read, or the state store reached |
| F0601 | The runner couldn't run the flows at all |
| F0602 | A partition failed, or a flow didn't start because one it waits for failed |
| F0603 | Interrupted (Ctrl-C) |
| W0601 | `FERRY_LEASE_TTL_MS` is set: leases are shortened for a test |
| F0701 | Verification failed, or couldn't run |
| W0701 | A sink this binary can't read back: its flows are not verified |
| F0801 | `resume`/`retry`: no such run, or it is pinned to another config or engine version |
