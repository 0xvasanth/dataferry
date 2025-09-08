# ferry-cli

The `ferry` command line, as a library and a binary (decision 0004 §5, §6).
The binary is `Ferry::with_builtins().main()`. A third party ships its own
binary with extra connectors the same way:

```rust
fn main() -> std::process::ExitCode {
    ferry_cli::Ferry::with_builtins()
        .register(ClickHouse)
        .name("acme-ferry")
        .main()
}
```

`tests/acme-ferry/` is such a binary, used by the tests.

## Built-in connectors

`with_builtins()` registers, through the same public calls:

| Type | Roles | Cargo feature (default on) |
| --- | --- | --- |
| `postgres` | source and sink; holds the state store (`backend: postgres`, with ferry-state's `PgStateBackend`) | `postgres` |
| `mongo` | source | `mongo` |

Building with `--no-default-features` drops a driver; `with_builtins()` then
registers only what is left.

## Commands

| Command | What it does | Connects? |
| --- | --- | --- |
| `ferry validate` | Loads the `ferry/` directory, substitutes `${ENV}`, and builds every connection through the registry with the roles its flows use. Checks connection options, that `from:` connections are sources and `sink:` connections are sinks, and that `runtime.state.connection` is a state-capable connection of the `backend:` type. Reports every error. | No |
| `ferry schema [--out <dir>]` | The JSON Schema of each config file for this binary, with one `connections` branch per registered type. Prints one JSON object keyed by file name, or writes `<dir>/<file>.schema.json`. | No |
| `ferry plan` | Not implemented yet (B7). | |
| `ferry run` | Not implemented yet (S2). | |
| `ferry resume <run-id>` | Not implemented yet (A5). | |
| `ferry retry --failed <run-id>` | Not implemented yet (A5). | |

Global flags: `-c, --config <dir>` (default `./ferry`, or `FERRY_CONFIG`),
`--format text|json`, `--color auto|always|never` (`auto` honours
`NO_COLOR`), `-v` and `-q`. Command output goes to stdout, diagnostics to
stderr.

## Exit codes

Part of the CLI's contract for scripts and CI. A test checks this table
against the code.

| Code | Meaning | What to do |
| --- | --- | --- |
| 0 | Success. Warnings, and dead-lettered rows under `max_errors`, still exit 0. | |
| 1 | Internal error, not implemented, or a miswired binary (a connector type registered twice). | Report a bug. |
| 2 | Usage error: bad flags, `resume`/`retry` without a run id. | Fix the command. |
| 3 | Config invalid: load, schema, options, missing `${ENV}`, wrong role, state connection not state-capable. | Fix the config. |
| 4 | Plan refused: type errors, no idempotency strategy, unknown entity. | Fix the config or the data. |
| 5 | Could not connect or authenticate. | Retrying may help. |
| 6 | Run finished with failed partitions or exceeded `max_errors`. | `ferry retry --failed <run-id>`. |
| 7 | Verification failed (`expect:`). Data was written. | Inspect the report. |
| 8 | Resume refused: config hash or engine version changed. | Revert the config, or start a new run. |
| 130 | Interrupted (Ctrl-C). | `ferry resume <run-id>`. |

Codes 4 to 8 and 130 belong to `plan`, `run`, `resume` and `retry`, which
their owning tickets wire.

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
| F0003 | The command is not implemented yet |
| F0004 | Output could not be written |
| F0100–F0109 | Loader errors (ferry-dsl): I/O, YAML, layout, include, model, duplicate, invalid reference, `${ENV}`, `depends_on` |
| F0301 | Bad connection options (unknown field, wrong type, bad value) |
| F0302 | Unknown connection type |
| F0303 | A flow uses a connection in a role its connector doesn't provide |
| F0304 | The state connection's type isn't `backend:` |
| F0305 | The state backend can't hold the state store |
| F0306 | A connector couldn't build a connection |
| W0301 | A secret field holds a literal value instead of `${ENV}` |
