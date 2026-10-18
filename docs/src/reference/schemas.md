# JSON Schemas

Every kind of config file has a JSON Schema (draft 2020-12), so a YAML
editor can complete keys, show their documentation, and flag unknown keys
and wrong values as you type.

## Getting them

The schemas depend on the binary: the `connections.yaml` schema has one
branch per connection type the binary registered, with that connector's
options. So take them from the binary you run:

```sh
dataferry schema --out schemas
```

That writes `schemas/ferry.schema.json`, `connections.schema.json`,
`runtime.schema.json` and `flows.schema.json`. Without `--out`,
`dataferry schema` prints all four in one JSON object, keyed by file name.

The repository also keeps the grammar's schemas in
[`schemas/v1/`](https://github.com/0xvasanth/dataferry/tree/main/schemas/v1),
generated from the config model and checked by its tests. They leave
connection entries open (only `name` and `type` are checked), because the
connector fields depend on the binary.

## Editor setup

VS Code with the YAML extension (`redhat.vscode-yaml`), in
`.vscode/settings.json`, with the schemas written to `schemas/` next to
`ferry/`:

```json
{
  "yaml.schemas": {
    "schemas/ferry.schema.json": "ferry/ferry.{yaml,yml}",
    "schemas/connections.schema.json": "ferry/connections.{yaml,yml}",
    "schemas/runtime.schema.json": "ferry/runtime.{yaml,yml}",
    "schemas/flows.schema.json": "ferry/flows/**/*.{yaml,yml}"
  }
}
```

Or per file, with a modeline on its first line (any editor using
yaml-language-server, such as Neovim, Helix or Zed):

```text
# yaml-language-server: $schema=../../schemas/flows.schema.json
```

JetBrains IDEs map schemas to files under *Settings → Languages &
Frameworks → Schemas and DTDs → JSON Schema Mappings*.

A file reached through `include:` holds `flows:`, `connections:` or both:
map it to the schema of what it holds.

## What they cover

- Every key of every file, with its description, default and allowed
  values, and unknown keys as errors.
- The `type:` of a connection, and each type's own fields, with secret
  fields marked (`writeOnly`, `x-ferry-secret`).
- `${NAME}` where a connection field expects a number or a boolean: those
  fields also accept a `${NAME}` string, since substitution happens before
  the value is read.

What they can't cover, and `dataferry validate` does: names that refer to
something defined elsewhere (the connection in `from:` and `sink:`, flows
in `depends_on:`), `${ENV}` rules, and expressions. Entity and field names
need the databases, so they are `dataferry plan`'s.

The [config reference](config.md) lists the same keys as these schemas,
and a test checks that the two agree.
