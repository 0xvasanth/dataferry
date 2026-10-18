# MongoDB

`type: mongo` is a source. It can't be a sink, or hold the state store,
in this release.

## Connection options

```yaml
{{#include ../../../examples/golden/ferry/connections.yaml:3:7}}
```

| Option | Default | Meaning |
| --- | --- | --- |
| `url` | required, secret | A `mongodb://` or `mongodb+srv://` connection string. TLS settings (`tls`, `tlsCAFile`) and driver options (`replicaSet`, `authSource`, …) go here ([TLS](tls.md#mongodb)). |
| `database` | required | The database whose collections are read. |

The whole `url` is secret: `plan` shows only the hosts and the database.
Every value substituted from the environment is also replaced with `***`
wherever it appears in output, so with `url: ${NAME}` the description is
just `***, database shop`, as in the quickstart.

## Collections and entities

Every collection of the database is an entity, named as the collection
is: `shop.orders` is the collection `orders` on connection `shop`.
`system.*` collections and views are skipped. A time-series collection
fails discovery for the whole database ("time-series collections are not
supported yet").

The primary key is always `_id`.

## Schema inference

A collection has no declared schema, so the connector **samples** it to
infer one, every time a run or plan starts:

- A collection of up to 1,000 documents (by MongoDB's estimated count,
  which can be stale) is read whole, in `_id` order. A larger one is
  sampled: 1,000 random documents (`$sample`).
- Each field's type is the union of what the sample holds, by these rules:
  - `int32` and `int64` together are `int64`; `int32` and `double` are
    `float64`;
  - `Decimal128` values become one `decimal(38, s)`, with the largest
    scale seen; together with integers, they stay decimal;
  - objects merge field by field, arrays element by element;
  - `null` adds nothing: a field that is always null, or always `{}`, is
    `json`, and one that is always `[]` is `list(json)`;
  - anything else that disagrees (`int64` and `double`, a number and a
    string, an `ObjectId` and a string, an object and a scalar) makes the
    field **`json`**: its values are read as relaxed Extended JSON.
- `_id` is never nullable; every other top-level field is nullable.
- A field name starting with `_ferry_` fails discovery: the prefix is the
  engine's.

Then, while reading, every document is checked against the inferred
schema. A document that doesn't fit (a field the sample never saw, at any
depth, or a value whose type the field can't hold) is **rejected**, with
its `_id` and the path, and `on_error` decides: fail the run, or
dead-letter the document ([Errors](../errors.md)). Nothing is cast or
dropped silently. With a large, irregular collection, a rare field the
sample missed shows up this way: run `plan`, look at the inferred types,
and expect such documents in the dead-letter table.

A document whose `_id` itself doesn't fit fails the whole read, not just
that document.

### Types

| BSON | Canonical |
| --- | --- |
| boolean | `bool` |
| int32, int64 | `int32`, `int64` |
| double | `float64` |
| Decimal128 | `decimal(38, s)`; `json` if the values need more than 38 digits; a `NaN` or infinity makes the field `json` |
| string | `utf8` |
| ObjectId | `utf8`: its 24-character lowercase hex |
| date | `timestamp(UTC)`, from milliseconds |
| binary, subtype 4 (UUID) | `uuid` |
| binary, subtype 0 | `binary` |
| embedded document | a struct of its fields |
| array | `list(<element type>)`; arrays of arrays are lists of lists |
| anything else, or conflicting types | `json` |

**ObjectId.** An `ObjectId` reads as its hex string, so `string(_id)` and
`_id` give the same text (`650000000000000000000001`), it sorts like the
ObjectId, and it can be compared with text. The connector still tells an
ObjectId field apart from a text field: a document holding a string where
the sample saw ObjectIds (or the reverse) doesn't fit.

**`json` fields.** A field that couldn't get one type holds JSON text.
`int(x)` of such a field parses that text, so the number `5` converts and
the string `"5"` (with its quotes) doesn't. An `unnest:` of a `json` field
unnests JSON arrays. Int64 values lose their width in JSON, and decimals
appear as `{"$numberDecimal": "1.5"}`.

**Missing and null.** The connector records, for each document, which
fields were absent, so `missing(x)` can tell a missing field from an
explicit `null` ([Expressions](../expressions.md#null-and-missing)).

## Partitions

A collection whose `_id` values are all integers, all strings, or all
ObjectIds is split into ranges of `_id`, about `partition_rows` documents
each, compared bytewise (`collation: simple`) whatever the collection's
default collation. Planning the ranges streams every `_id` of the
collection once. Any other `_id` type (or a mix) gives one partition.
Documents whose `_id` has another type, or none, land in the first range
and fail to read there, rather than being skipped.

Each partition is read with `find(...).sort({_id: 1})`. Filters are not
pushed down: `where:` is evaluated by dataferry. There is no snapshot
across partitions.

**Co-partitioning** (for a merge's `many` join) works on a field whose
values are integers, strings or ObjectIds.

## TLS and connections

The connector uses the MongoDB Rust driver, with one client per source
and the driver's default pool. Remote hosts get TLS by default
([TLS](tls.md#mongodb)).
