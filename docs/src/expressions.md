# Expressions

Every column, filter and assertion in a flow is an **expression**: one
SQL-like scalar expression, evaluated once per record (or per array
element, joined row or nested row, depending on the
[scope](transforms.md#scopes-what-a-bare-name-means)).

```text
lower(status)
int(round(total * 100))
coalesce(customer.email, 'unknown')
deleted_at == null
case when qty > 10 then 'bulk' else 'single' end
```

The language is deliberately small. It is parsed and type-checked against
the discovered source types by `dataferry plan`, before anything runs, and
every function in it is pure, so the same record always gives the same
row, however the run is partitioned and however often it is re-run.

Under the hood it is the SQL expression syntax of
[Apache DataFusion](https://datafusion.apache.org/), restricted to what is
listed on this page.

## Literals

| Literal | Example | Type |
| --- | --- | --- |
| text | `'PAID'`, `'it''s'` | `utf8` |
| integer | `42`, `-1` | `int64` |
| number with a fraction | `1.005`, `0.5` | a `decimal`, exact: never a float |
| boolean | `true`, `false` | `bool` |
| null | `null` | |

Text is in **single quotes**. Double quotes name a field: `"Order Id"`, or
`"index"` for a field called `index`.

In YAML, the whole expression is a string: write it plainly unless it
starts with a character YAML treats specially, or contains `: `
([Configuration](configuration.md#yaml-details-that-matter)).

## Names and paths

| Write | To read |
| --- | --- |
| `status` | the field `status` of the current scope |
| `customer.email` | the field `email` of the object `customer` |
| `c.name` | the field `name` of the joined `one` entry with alias `c` |
| `items[*].sku` | the list of the `sku` of every element of `items` |
| `"Order Id"` | a field whose name isn't a plain identifier |

- Names are **case-sensitive**: `Status` and `status` are different
  fields. Function names are not: `LOWER(x)` is `lower(x)`.
- A path that names nothing is a plan error, with a suggestion:
  `` no field `stauts` in entity `shop.orders` `` / ``did you mean `status`?``.
- A path can have one `[*]`. Indexing into an array (`items[0]`) isn't
  supported: `unnest:` gives one row per element instead.
- `parent`, `index`, `item` and `key` are **reserved** at the start of a
  path, because their sigil forms exist. `line_no: index` is an error that
  suggests `$index`; a field with one of those names is written quoted,
  `"index"`. The check runs in `validate`:

```text
{{#include examples/bad/reserved-word/validate.txt}}
```

- Names starting with `_ferry` are reserved for the engine.

## Sigils

| Sigil | Value | Valid in |
| --- | --- | --- |
| `$parent` | The parent target's row, after mapping: `$parent.id` is the parent's `id` column. | targets with `parent:` |
| `$index` | The 0-based position of the current element in the array being unnested (`int64`). In a nested unnest, the innermost position. | `unnest:` targets |
| `$item` | The current element itself, for arrays of scalars (`tag: $item`). For objects, `sku` is short for `$item.sku`. | targets that unnest a path |
| `$key` | A deterministic UUIDv5 over the driver record's key, the unnest path with every outer index, and `$index`. | `unnest:` targets |

Using a sigil where it isn't valid is a plan error, for example
`` `$index` is only available in `unnest:` targets ``.

## Operators

| Kind | Operators |
| --- | --- |
| arithmetic | `+`, `-`, `*`, `/`, `%` |
| comparison | `=` or `==`, `!=` or `<>`, `<`, `<=`, `>`, `>=` |
| logic | `and`, `or`, `not` |
| text | `\|\|` (concatenate), `like`, `ilike`, with `escape` |
| sets and ranges | `x in (a, b, c)`, `x between a and b` |
| null tests | `x is null`, `x is not null`, `x is distinct from y`, `x is true`, `x is false` |
| conditionals | `case when ... then ... else ... end` |
| SQL forms | `substring(x from a for b)`, `trim(...)`, `position(a in b)`, `cast(x as t)`, `x::t` |

An operation with a null operand gives null, except in the null tests and
in `coalesce`, `concat`, `concat_ws`, `greatest` and `least`, which skip
nulls (`concat('x', null)` is `'x'`). `and`/`or` use three-valued logic.

Three things that differ from what you may expect, and that the planner
doesn't catch:

- **Integer arithmetic isn't checked for overflow**: it wraps around, so
  the largest `int64` plus 1 is the smallest. Integer `/` truncates
  (`7 / 2` is `3`); write `7.0 / 2` for a decimal result.
- **`^` is bitwise XOR**, not a power (`7 ^ 2` is `5`): use `power()`.
  `&` and `|` are bitwise too.
- **`cast(x as t)` and `x::t`** take SQL type names and convert as Arrow
  does, silently: `1.7::int` is `1`. Prefer the strict conversions below,
  which never truncate.

Not available, each with an error that says so: subqueries and `exists`
(an expression sees one record; use a [merge](transforms/merge-nest.md) to
read another entity), aggregates (`sum`, `count`, … belong in
[`expect:`](verification.md#expect-assertions)), window functions, and
`*`.

## Null and missing

A source can say two different things about a field: it is **null**, or
it is **missing** (the document has no such field). MongoDB sources tell
them apart; relational sources have no missing fields. Most of the time
the difference doesn't matter, and the language treats them alike:

- `x == null` (or `x = null`, or `x is null`) is true when `x` is null
  **or** missing. `x != null` is the opposite.
- A missing value is null in every other expression, and lands in the
  target as `NULL`.
- `missing(x)` is true only when the field is absent. So
  `x == null and not missing(x)` means an explicit `null`. `missing()`
  takes a path into the current record (`missing(customer.email)`); a
  missing parent object doesn't make its fields missing, only null.
- Ordering comparisons with the `null` literal (`x > null`) are an error:
  they are never true.

In a `where:`, a predicate that is null counts as **false**: the record or
row is filtered. Use `coalesce()` to choose what a null means:
`where: not coalesce(discontinued, false)`.

## Functions

Only these functions exist. Any other name is an error, with a
suggestion when one is close (`lowr` → `lower`).

| Group | Functions |
| --- | --- |
| Strict conversions | `int(x)`, `float(x)`, `string(x)` |
| Nulls and choices | `coalesce(a, b, ...)`, `nullif(a, b)`, `greatest(...)`, `least(...)` |
| Numbers | `abs`, `ceil`, `floor`, `round(x)`, `round(x, digits)`, `trunc`, `power`, `signum` |
| Text | `lower`, `upper`, `trim`, `btrim`, `ltrim`, `rtrim`, `concat`, `concat_ws`, `replace`, `starts_with`, `ends_with`, `contains`, `split_part`, `repeat`, `octet_length` |
| Text, by character | `length` (also `char_length`, `character_length`), `substr`, `substring`, `left`, `right`, `lpad`, `rpad`, `strpos`, `reverse`, `translate` |
| Arrays | `cardinality(list)`: the number of elements (null for a null list) |
| Missing fields | `missing(path)` |

Besides the strict conversions and `missing()`, they behave as their
DataFusion namesakes.

There are no date, time, random or UUID functions: an expression can't
read the clock or generate randomness (see [Purity](#purity)). For a
generated key, use `$key`.

## Numbers and decimals

Numbers with a fraction are **decimals**, and arithmetic on decimals stays
exact. `round()` on a decimal rounds **half away from zero**:

| Expression | Result |
| --- | --- |
| `round(1.005 * 100)` | `101` |
| `round(-1.005 * 100)` | `-101` |
| `round(2.5)` | `3` |
| `round(1.005, 2)` | `1.01` |
| `round(float(1.005) * 100)` | `100.0`: as a float, 1.005 is 1.00499… |

Money should never go through `float()`. The golden example's
`int(round(total * 100))` turns a `Decimal128` total of `1.005` into 101
cents.

## Strict conversions

`int()`, `float()` and `string()` convert a value or reject the record,
never guess:

| Call | Accepts | Rejects |
| --- | --- | --- |
| `int(x)` → `int64` | integers, booleans, whole decimals and floats in range, text that is exactly an integer (`'42'`, `'-7'`) | `1.5` and `100.500` (`... is not a whole number (use round() first)`), `'3.0'`, `' 3'`, `''`, `'three'` |
| `float(x)` → `float64` | numbers, booleans, numeric text | `'NaN'`, `'inf'`, `'1,5'` |
| `string(x)` → `utf8` | numbers, booleans, text, UUIDs (hyphenated lowercase), dates and timestamps | binary |

A null converts to null. A value that can't be converted is an **error
row**: the record is rejected, with the reason, for example
``accounts.seats = `int(seats)`: int(): `ten` is not an integer``. That
happens at run time, even for a constant: `int('three')` passes `plan`
and rejects every record.

When a field holds mixed types in MongoDB (numbers in some documents,
text in others), it is read as `json`, and `int()` parses the value's JSON
text. That is how the golden example rejects `qty: "three"`: its error
reads ``int(): `"three"` is not an integer``, quotes included. A JSON
string holding digits (`"7"`) is rejected the same way.

## Types and conversions

dataferry types every value with one of its **canonical types**, which
connectors map to and from their own
([PostgreSQL](connectors/postgres.md#types),
[MongoDB](connectors/mongo.md#types)):

`int8`, `int16`, `int32`, `int64`, `uint8`, `uint16`, `uint32`, `uint64`,
`float32`, `float64`, `decimal(<precision>,<scale>)` (precision up to 76),
`bool`, `utf8`, `binary`, `date`, `timestamp` (wall-clock, no zone),
`timestamp(<zone>)` (an instant, e.g. `timestamp(UTC)`), `uuid`, `json`,
`list(<type>)`, and the structs of nested objects. Timestamps have
microsecond precision.

A typed column (`{expr: ..., type: ...}`) converts the expression's type
to the declared one. Conversions fall in three classes:

- **Safe**: every value converts exactly (integer widening,
  `decimal(10,2)` → `decimal(12,4)`, anything → `utf8`). Nothing to do.
- **Lossy**: some values change or fail. The plan refuses the column unless
  it says `allow_lossy: true`; then values change as the table says, and
  values that can't convert are error rows.
- **Impossible** (`bool` → `timestamp`, `uuid` → `int64`): an error even
  with `allow_lossy`.

| From | To | Once `allow_lossy: true` |
| --- | --- | --- |
| `int64` | `int32` | out-of-range values are error rows (any integer narrowing) |
| `int32` | `int16` | out-of-range values are error rows |
| `uint64` | `int64` | values above int64's maximum are error rows |
| `int64` | `uint64` | negative values are error rows |
| `int64` | `float64` | integers beyond ±2^53 are rounded |
| `float64` | `float32` | rounded to float32 |
| `float64` | `int64` | the fraction is truncated toward zero; out-of-range values are error rows |
| `decimal(38,10)` | `float64` | rounded to the nearest float64 |
| `decimal(12,4)` | `decimal(12,2)` | the scale shrinks: rounded half away from zero |
| `decimal(12,2)` | `decimal(8,2)` | values with more integer digits are error rows |
| `float64` | `decimal(12,2)` | rounded to the scale; out-of-range values are error rows |
| `decimal(12,2)` | `int64` | the fraction is dropped |
| `timestamp(UTC)` | `timestamp` | the zone is dropped: the UTC wall-clock time is kept |
| `timestamp` | `timestamp(UTC)` | the wall-clock time is read as UTC |
| `timestamp(UTC)` | `date` | the time of day is dropped |
| `utf8` | `int64` | text that isn't an integer is an error row (any parse from text) |
| `utf8` | `uuid` | text that isn't a UUID is an error row |
| `utf8` | `json` | text that isn't JSON is an error row |
| `binary` | `utf8` | bytes that aren't UTF-8 are error rows |

The opt-in is per column, and part of the config hash. A conversion you
write in the expression itself (`int(x)`) is explicit already.

## Purity

An expression's value depends only on the record. There is no clock
(`now()`), no randomness (`random()`, `uuid()`), no aggregates and no
reads of other rows. Every function is checked to be immutable, and a
property test checks that random expressions give the same result when
compiled again, evaluated again, evaluated one row at a time, and over
rows in reverse order. That is what makes a re-run partition, a resumed
run and a second run over the same data write the same rows.

## Errors

**At plan time**, every error points at the file, line and column of the
expression, with the field path and, where it can, a suggestion. `plan`
reports every error it finds, not just the first. Two mistakes in one
flow, from the guide's tests:

```yaml
{{#include examples/plan-errors/ferry/flows/accounts.yaml}}
```

```text
{{#include examples/plan-errors/expected.txt}}
```

`F0110` is an expression that doesn't parse or names nothing in scope;
`F0111` a type that doesn't fit where it is used. `plan` exits 4.

**At run time**, an expression that fails for one record (a conversion, a
null in a `NOT NULL` column, an array that isn't one) rejects that whole
record, and the run's `on_error` decides what happens next
([Errors and dead letters](errors.md)).
