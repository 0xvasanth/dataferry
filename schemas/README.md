# Config JSON Schemas

One JSON Schema (draft 2020-12) per kind of file in a `ferry/` directory,
generated from the `ferry-dsl` model (`crates/ferry-dsl/src/config/schema.rs`).
`v1/` is grammar `version: 1` (decision 0003).

| File | Schema |
| --- | --- |
| `ferry/ferry.yaml` | [`v1/ferry.schema.json`](v1/ferry.schema.json) |
| `ferry/connections.yaml` | [`v1/connections.schema.json`](v1/connections.schema.json) |
| `ferry/runtime.yaml` | [`v1/runtime.schema.json`](v1/runtime.schema.json) |
| `ferry/flows/**/*.yaml`, `*.yml` | [`v1/flows.schema.json`](v1/flows.schema.json) |

A file reached through `include:` holds `flows:`, `connections:` or both, and
matches the flows or connections schema for what it holds.

Connection entries are left open here: only `name` and `type` are checked,
and `description` is rejected. Each connector type's own fields come from the
connector registry, so the complete connections schema depends on the binary
and is printed by `ferry schema` (decision 0004 §3). Point editors at that
output when it exists.

## Editor setup

VS Code with the YAML extension (`redhat.vscode-yaml`), in
`.vscode/settings.json`:

```json
{
  "yaml.schemas": {
    "schemas/v1/ferry.schema.json": "ferry/ferry.{yaml,yml}",
    "schemas/v1/connections.schema.json": "ferry/connections.{yaml,yml}",
    "schemas/v1/runtime.schema.json": "ferry/runtime.{yaml,yml}",
    "schemas/v1/flows.schema.json": "ferry/flows/**/*.{yaml,yml}"
  }
}
```

Or per file, with a modeline on its first line:

```yaml
# yaml-language-server: $schema=../schemas/v1/flows.schema.json
```

## Keeping them current

`cargo test -p ferry-dsl --test schema` fails when these files no longer
match the model. Regenerate them with

```sh
FERRY_UPDATE_SCHEMAS=1 cargo test -p ferry-dsl --test schema
```

and commit the result. A new grammar version gets a new directory; `v1/`
stays as it is.
