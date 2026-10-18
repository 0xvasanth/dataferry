# Connectors

A connector is the code that talks to one kind of database. Each
connection in `connections.yaml` names one by its `type:`. The `dataferry`
binary has two:

| Type | Source | Sink | State store | Page |
| --- | --- | --- | --- | --- |
| `postgres` | yes | yes | yes (`backend: postgres`) | [PostgreSQL](connectors/postgres.md) |
| `mongo` | yes | no | no | [MongoDB](connectors/mongo.md) |

Both verify TLS certificates whenever they use TLS, and use it by default
for anything that isn't on the local machine ([TLS](connectors/tls.md)).

`dataferry schema` prints each type's options as JSON Schema, and
`dataferry -v validate` shows how each connection was understood, without
secrets ([quickstart](quickstart.md#4-validate-offline)).

## Canonical types

Connectors don't convert to each other's types directly. A source maps
each of its types to one of dataferry's **canonical types**
([Expressions](expressions.md#types-and-conversions)); expressions compute
on those; a sink maps them to its own. Each connector page has its
mapping table. A source type with no mapping fails discovery with a
message naming the table and column, before anything is written.

## What every source does

- **Discovers** its entities: names, fields, types, nullability and
  primary key. `plan` and `run` do this on every invocation.
- **Partitions** an entity by ranges of its primary key, about
  `partition_rows` records each, and reads one partition at a time, in key
  order.
- **Co-partitions** a joined entity at a driver's boundaries, when a merge
  asks for it ([Merge and nest](transforms/merge-nest.md#join-strategies)).
- Reads with no consistent snapshot across partitions: rows changed while
  a run is going may or may not be seen. Migrate from a quiet source, or a
  snapshot of it.

## Your own connector

A connector is a Rust crate implementing a small trait, registered in your
own build of the command line. See [Writing a connector](extending.md).
