# 0003: Config shape

Status: proposed. The points that were open were decided on 2026-10-16 (see
Open questions); the record still needs review from both tracks. Blocks B1
(0XT-254). Once accepted, B2, B4, B5, B6, B7 and B8 are edited to use its
words (see §11). The shape and validation of each connection type's own
fields belong to 0004.

## Context

The config layout is already agreed: a `ferry/` directory holding `ferry.yaml`
(version and name), `connections.yaml`, `runtime.yaml`, and every YAML file
under `flows/`, with `include:` as an escape hatch. `from:` is one entity or a
list of joined entities (merge), and `to:` can name several target tables
(split).

What the agreement leaves open is the grammar inside a flow, and the tickets
describe the same ideas in a second vocabulary: B4 has `columns:`, `split:`,
`where:` and `key:`, B5 has `unnest: path:`, B6 has `merge:` with a driver
entity, and B8 has `expect:`. Our draft `from:` list also lacks two things B6
needs: which entity is the driver, and the cardinality of each join. B1 can't
freeze a JSON Schema, and the golden config (B1, B7, S2 and S3 test against it)
can't be written, until there is one vocabulary.

The grammar has to fit the ferry-core contracts as they stand:

- `TransformPlan::inputs()` lists the driver first. Every output row is
  attributed to one driver record, and a record is written in full or
  rejected in full.
- `TransformPlan::write_order()` puts parents before children, and
  `TransformOutput::validate` enforces it.
- One `SinkTxn` per partition covers every target of a flow (0001), so a flow
  writes to exactly one sink.
- `Sink::prepare` and `SinkCaps::strategy_for` take one `WriteMode` per flow.
- Co-partitioning (0002) needs the driver's partition column and the child's
  join column.

## Decision

### 1. Grammar at a glance

Each line names a key, its type, and its default. `?` means optional, and
`|` separates alternatives. This is a summary, not literal YAML.

```text
# ferry.yaml
version: 1                        # int, required. Version of this grammar
name: <ident>                     # required. Names the migration in state and reports
include: [<path-or-glob>, ...]    # ? escape hatch, see §8

# connections.yaml
connections:
  - name: <ident>                 # required, unique, no dots
    type: <connector type>        # required, a key in the ConnectorRegistry
    # ...type-specific fields: owned by 0004. Reserved keys: see §3

# runtime.yaml (every key optional)
runtime:
  state: {backend: file, path: <dir>}    # default backend: file
       | {backend: postgres, connection: <conn>}   # a state-capable type, see §4
  workers: auto | <int>           # default auto (one per core)
  memory_budget: <size>           # e.g. 512MiB, default 1GiB
  partition_rows: <int>           # PartitionHint::target_rows, default 100000
  on_error: fail | dead_letter    # default fail
  max_errors: <int>               # ? default unbounded (plan warns)
  source_max_connections: <int>   # ? A5 throttle
  source_rows_per_sec: <int>      # ? A5 throttle

# flows/*.yaml (any file name, any depth)
flows:
  - name: <ident>                 # required, unique across every file
    from: <conn>.<entity>         # one entity
       |  [<from entry>, ...]     # or a merge, see §5.2
    sink: <conn>                  # required, the one connection this flow writes to
    mode: insert | upsert         # default insert
    where: <expr> | {<name>: <expr>, ...}   # ? filter on the driver record
    depends_on: [<flow name>, ...]          # ? see §7
    runtime: {<overridable key>: ...}       # ? see §6
    to:                           # required, one or more targets (split)
      <table>:
        parent: <table>           # ? another target of this flow
        unnest: <path> | <many alias>       # ? one row per element
        key: [<column>, ...]      # required if mode is upsert
        where: <expr> | {<name>: <expr>}    # ? filter on this target's rows
        columns: "*" | {<column>: <column spec>, ...}   # required
    expect: {<name>: <assertion>, ...}      # ? see §5.9
```

Unknown keys are errors at every level. A typo must fail at load time, not be
ignored.

### 2. The `ferry/` directory and loading

- **Finding it.** `--config <dir>` if given, otherwise `./ferry`. The
  directory must contain `ferry.yaml`. That file is what marks it as a ferry
  config.
- **Well-known files.** `ferry.yaml` is required. `connections.yaml` is
  required once any flow exists. `runtime.yaml` is optional, and every
  setting has a default. Each file may contain only its own top-level key:
  `connections.yaml` only `connections:`, `runtime.yaml` only `runtime:`, a
  flow file only `flows:`. Both `.yaml` and `.yml` are accepted. Having
  `connections.yaml` and `connections.yml` side by side is an error.
- **Flow files.** Every `*.yaml` or `*.yml` file under `flows/`, at any depth,
  is loaded. Other files (a `README.md`, say) are ignored, and so are files
  and directories whose names start with `.` (editor swap files). An empty
  `flows:` list is allowed. A config with no flows at all is an error.
- **Order.** Files are loaded sorted by their path relative to `ferry/`,
  compared bytewise as UTF-8. The order only affects which file an error
  points at first. The model itself does not depend on it (§10).
- **Single document.** A file with several YAML documents (`---`) is an
  error, which keeps every error's file and line exact.
- **Names.** Connection and flow names match `[a-z][a-z0-9_]*`. A flow
  `name:` that appears twice, in one file or in two, is an error naming both
  files and lines.
- **Error locations.** Every error after loading, merging, including and
  type-checking carries the original file, line and field path
  (`flows/orders.yaml:14 flows[orders].to.order_items.key`).
- **YAML 1.2.** The loader uses the YAML 1.2 core schema, so `on`, `yes` and
  `no` are strings. Editors and other tools often still resolve YAML 1.1,
  where those words are booleans, so the grammar never uses them as keys.
  That is why joins say `keys:` rather than `on:`.

### 3. Connections and how flows reference them

`connections.yaml` holds a list whose entries are `{name, type, ...}`. `name`
and `type` are owned here. Everything else in an entry is type-specific and
belongs to 0004. `type` is looked up in the `ConnectorRegistry`. The built-in
`postgres` and `mongo` types register the same way a third-party connector
would, and an unknown type is an error that lists the registered ones.

**Reserved keys.** `name`, `type` and `description` are reserved for this
record in every connection entry, so a key 0003 adds later can't collide with
a connector's own field. `ConnectorRegistry::register` rejects a connector
whose `Options` schema uses one (0004 §2). `description` has no meaning in
v0.1 and is rejected as an unknown key until this record defines it.

Flows reference connections in exactly two ways:

- **An entity**, as `<conn>.<entity>`, in `from:` and in merge entries. The
  string is split at the **first** dot: what comes before it is the
  connection name (which can't contain a dot), and the rest is the entity
  name exactly as the connector's `discover()` reports it (`Entity::name`).
  So `app.public.orders` is entity `public.orders` on connection `app`.
  *Amended in S3 (2026-10-17):* a connector type may also accept the
  entity qualified by the namespace its connection reads. For `postgres`
  that is the connection's `schema:`, so on a connection with `schema:
  public`, `app.public.orders` names the table `orders` that `discover()`
  reports, as does `app.orders`. An entity discovered under the full dotted
  name still wins. Which types have a namespace is set where the binary
  registers its connectors, next to how their sinks are verified.
- **A connection**, by bare name: `sink:`, and `runtime.state.connection`.

Target tables (the keys under `to:`) are plain identifiers with no dots. Which
schema or database they land in is a property of the sink connection (0004).
A given `(sink, table)` pair may be written by only one flow. Two flows
inserting into one table would undo each other's idempotency strategy.

### 4. `runtime.yaml`

`runtime:` holds process-wide settings, listed in §1. `state` picks the A4
backend. `backend: file` keeps state in a local directory. Any other backend
is the `type` of a state-capable connector (`postgres` in the stock binary),
and `connection:` must name a connection of that type. Both are checked by
`ferry validate` through the registry (0004 §8).

A flow gets `TxnWithState` (data and the partition's done-record in one
transaction) only when `runtime.state.connection` is the flow's `sink:`.
Every other flow, including every flow under `backend: file`, gets the next
strategy `SinkCaps::strategy_for` offers, and `ferry plan` says why (0004
§8). Batch size is not a setting: the runner derives it from `memory_budget`
(A5).

`on_error: fail` stops the run at the first error row. `on_error:
dead_letter` writes the rejected source record to `_ferry_dlq` on the flow's
sink (A7) and continues until more than `max_errors` records have been
rejected. There is no `skip`: a record is never dropped without a trace.

### 5. Flows

#### 5.1 Shapes

There are no `split:` or `merge:` keywords. They are shapes the grammar
already expresses:

| Shape | How it's written |
| --- | --- |
| 1:1 copy | one `from:` entity, one target, often `columns: "*"` |
| split | one `from:` entity, several keys under `to:` |
| merge | `from:` as a list: one driver plus joined entities |
| unnest | a target with `unnest:` (one row per array element) |
| nest | a column built from a `many` join, ordered |

#### 5.2 `from:` and merges

The single form is a string: `from: shop.orders`.

The merge form is a list of entries:

```yaml
from:
  - entity: app.public.orders          # the driver: first, and no join
    as: o
  - entity: app.public.customers
    as: c
    join:
      cardinality: one
      keys: {id: o.customer_id}        # c.id = o.customer_id
  - entity: app.public.order_items
    as: i
    join:
      cardinality: many
      keys: {order_id: o.id}
      strategy: co_partition           # ? auto (default) | co_partition | stage
```

- **The driver** is the first entry, and it must not have `join:`. Every
  other entry must have one. Position and structure agree, so the driver
  can't be ambiguous, and it maps directly onto `TransformPlan::inputs()[0]`.
  Output rows are attributed to driver records, and `ferry plan` prints
  `driver: app.public.orders`.
- **`as:`** is the alias that expressions use. It defaults to the last
  dot-separated part of the entity name, and aliases must be unique within a
  flow. In the flow's columns, a bare name refers to a driver field, so
  `status` and `o.status` mean the same thing there.
- **`join.keys:`** is required. It maps a column of *this* entry to a
  qualified column of an earlier entry, each pair being an equality, and
  several pairs make a composite key. Only equi-joins on columns are allowed,
  which is exactly what co-partitioning (0002) and hash joins need, and it
  lets the planner refuse a join with no keys. A join may reference the
  driver or an earlier `one` entry (lookup chains such as orders → customers
  → regions). It may not reference a `many` entry.
- **`join.cardinality:`** is required and has no default:
  - `one`: at most one match per driver record. No match gives null joined
    columns. A second match found **at run time** rejects the affected
    driver record (error row, then `on_error`), and never fans out. Sampling
    at plan time is advisory only.
  - `many`: zero or more matches. A `many` alias can be used only as the
    source of `nest:` (§5.8) or of `unnest:` (§5.7). Anywhere else it would
    fan out the driver, and the plan fails.
- Every join is a left join from the driver. To drop drivers with no match,
  use a `where:` on the joined column.
- **`join.strategy:`** defaults to `auto`, where the planner picks between
  co-partitioned streaming and staging (0002 step 5) and shows the choice in
  `ferry plan`. `co_partition` pins streaming and makes the plan fail, instead
  of falling back, if co-partitioning is impossible. `stage` forces staging.
- Merge entries may come from different connections. The planner names
  inputs `<conn>.<entity>`, so two entities with the same name don't collide
  in `PartitionInputs`. A cross-connection join can only be staged.

#### 5.3 `sink:`, `to:`, and write order

`sink:` names the single connection the flow writes to, which follows from
0001 (one `SinkTxn` per partition). Each key under `to:` is a target table.
`parent: <table>` makes one target the child of another in the same flow.
That one declaration drives three things:

- **Write order.** Parents come before children, ordered topologically on
  `parent:` with ties broken by table name. Nothing else affects it, so the
  order doesn't depend on the order of keys in the YAML file. This becomes
  `TransformPlan::write_order()`.
- **Filter propagation** (§5.6).
- **Foreign keys.** If a child maps every one of its parent's `key:` columns
  from `$parent.<col>`, the planner emits a `ForeignKey` from those child
  columns to the parent's key. A6 decides whether that FK is enforced during
  the load.

A `parent:` cycle, or a parent that isn't a target of this flow, is an error.

#### 5.4 `columns:`, scopes and sigils

`columns:` is an ordered map from target column to column spec, and its order
is the column order in the target DDL. A spec is one of:

- a string, which is an expression (B2):
  `status: lower(status)`
- an object, for when the type matters:
  `{expr: <expr>, type: <canonical type>, nullable: <bool>, allow_lossy: <bool>}`.
  Type names follow `CanonicalType` (`int64`, `decimal(12,2)`, `utf8`,
  `timestamp(UTC)`, `uuid`, `json`, …). B3 owns the exact list.
  `allow_lossy: true` is the explicit opt-in B3 requires for a lossy
  conversion to `type:`. A cast written in the expression itself
  (`int(qty)`) also counts as explicit. A value that doesn't fit at run time
  is an error row. See the amendment below for the exact rule and table.
- a nest object (§5.8).

`columns: "*"` copies every declared field of the entity unchanged
(`IdentityPlan`). It is allowed only on a target with no `parent:` and no
`unnest:` in a single-entity flow, and can't be combined with other columns
in v0.1. It keeps the ~190 plain copies in a 200-table migration to about six
lines each. Generating whole flows (wildcards, `ferry infer`) is out of scope
for v0.1.

Values must be YAML strings. A bare `null`, `true` or `5` is an error telling
you to quote it (`"null"`), so YAML's type guessing never silently changes an
expression. Quote any expression that starts with `*`, `[`, `{`, `&`, `!`,
`|`, `>`, `'`, `"`, `%` or `@`, or that contains `: `.

**Scope.** Every target evaluates its expressions in one scope:

| Target | Bare names resolve to |
| --- | --- |
| no `unnest:` | the driver record (merge: driver fields, plus `alias.field` for joins) |
| `unnest: <path>` | the current array element |
| `unnest: <many alias>` | the current joined row |
| inside a nest object | the current nested row |

**Sigils** (B2 reserves the `$` prefix):

| Sigil | Meaning | Valid in |
| --- | --- | --- |
| `$parent` | the **mapped** row of the `parent:` target for this record or element: `$parent.id` is the parent's `id` column after mapping | targets with `parent:` |
| `$index` | 0-based position of the current element in the array being unnested (`int64`) | `unnest:` targets |
| `$item` | the current element itself, for arrays of scalars (`tags: [a, b]`). `sku` is short for `$item.sku` | `unnest:` targets |
| `$key` | a deterministic UUIDv5 over (driver record id, full unnest path including outer indexes, `$index`). B5 owns the namespace | `unnest:` targets |

A child doesn't see the driver record's raw fields directly. It sees them
through `$parent`, so values it shares with the parent are mapped once, in
one place. Missing and null keep the meaning B2 gives them (read through
`_ferry_missing`).

#### 5.5 `mode:` and `key:`

`mode:` is set per flow, because `Sink::prepare` and `strategy_for` take one
`WriteMode` per flow:

- `insert` (default). This run creates the target rows. Re-runs are made
  safe by the sink's strategy (`TxnWithState` when the state store is on the
  flow's sink connection, see §4, otherwise `DeletePartitionThenInsert` or
  `StagingSwap`).
- `upsert`. Rows may already exist and are replaced by key. **Every** target
  must declare `key:`, and the plan error names any that doesn't (B4).
  Children with a `parent:` get replace-children semantics (A3): the
  children of each parent in the batch are deleted and then rewritten. Rows
  for records that are filtered out are never deleted from the target.
  Upsert doesn't propagate deletes.

`key:` lists target columns, and they become the target's primary key in
`TargetTable`. Key columns are implicitly `nullable: false`. In insert mode
`key:` is optional but recommended. `ferry plan` warns about any child target
without one. There is no implicit generated key. A child that needs a
surrogate key maps it explicitly: `id: $key`.

The config never names an idempotency strategy. The planner derives it from
`mode` and `SinkCaps::strategy_for`, shows it in `ferry plan`, and refuses
the flow when the result is `None` (B7).

#### 5.6 `where:` and the propagation rule

`where:` is either one expression or an ordered map of named predicates that
must all hold:

```yaml
where: deleted_at == null
# or
where:
  not_deleted: deleted_at == null
  not_test: customer.id != 'test'
```

The name of the first predicate that fails is what reports and `filtered`
counts show. An unnamed `where:` is reported by its expression text. A
predicate that evaluates to null counts as false. A predicate that **errors**
doesn't filter: it is an error row, and the record is rejected.

There are two levels, and one rule connects them.

- **Flow `where:`** applies to the driver record, in driver scope (joined
  `one` columns included). When it fails, the record is *filtered*: it has
  no rows in any target and is counted once. A flow `where:` may be pushed
  down to the source (`predicate_pushdown`) only if the pushed form keeps the
  missing-versus-null meaning exactly. Otherwise it is evaluated in the
  transform.
- **Target `where:`** applies to that target's rows, in that target's scope
  (§5.4). It is evaluated against source values, not against mapped output.

**Propagation rule.** When a row is filtered out of a target, every row that
reaches it through `parent:` (children, grandchildren, and so on) is dropped
with it. Filters never travel upward or sideways: a sibling target with no
`parent:` link keeps its rows. Filtering is not an error, so the record
isn't rejected and the rows that remain are written. Errors keep their own
rule from 0.4: any error in any row rejects the whole driver record.

In the golden dataset this is why document `…0004` writes neither the order
nor its item (a flow `where:`), and why a target `where:` on `orders` would
also drop that order's `order_items`.

#### 5.7 `unnest:`

`unnest: <path>` writes one row per element of an array. The path is
resolved in the scope of the target's parent: the parent's element if the
parent is itself unnested, otherwise the driver record. A target without
`parent:` unnests from the driver record and has no `$parent`.
`unnest: <many alias>` writes one row per joined row of a `many` join.

- An empty array, a missing field and an explicit `null` all produce zero
  children, and the parent is still written. The golden dataset pins the
  first two, and treating null the same way matches how B2 treats null and
  missing elsewhere. It is not configurable.
- A value that isn't an array (`items: "x"`) is an error row, so the record
  is rejected.
- Unnests can nest (`order_items` → `item_options` with
  `parent: order_items, unnest: options`). `$index` is always the innermost
  index.

#### 5.8 `nest:`

A nest builds an array column from a `many` join:

```yaml
items:
  nest: i                 # a many alias
  order_by: [line_no]     # ? in the nested row's scope; "<expr> desc" sorts descending
  where: qty > 0          # ? drops nested rows from the array
  columns:                # same column specs as a target
    sku: sku
    qty: qty
```

- **Deterministic order.** The planner always appends the nested entity's
  primary key (the columns not already listed) to `order_by` as the final
  tie-breaker. Without `order_by`, the order is that primary key. `ferry
  plan` shows the full effective ordering. The same input under any
  partitioning gives the same array (B6).
- A driver with no matches gets `[]`, not null.
- The column's canonical type is `List(Struct(...))`. How a sink stores it
  (Postgres `jsonb`, say) is the sink's decision.

#### 5.9 `expect:`

`expect:` is an ordered map of named assertions. The names appear in the run
report.

```yaml
expect:
  order_count: count(orders) == source.count()
  order_total: sum(orders.total_cents) == source.sum(round(total * 100))
```

- An assertion compares two aggregates with `==`, `!=`, `<`, `<=`, `>` or
  `>=`. Arithmetic is allowed on either side (`abs(a - b) <= 1`).
- **Target side.** The aggregates `count(<table>)`, `count`, `sum`, `min`,
  `max` and `count_distinct`, over `<table>.<column>`. The table qualifier
  may be omitted when the flow has one target. They are computed by querying
  the target after the run.
- **Source side.** `source.<agg>(<expr>)` is evaluated in driver scope and
  accumulated at read time (B8, A8) over the driver records the run
  **wrote**: not filtered and not rejected. That makes an assertion a check
  on the mapping, while A8's counts account for filtered and dead-lettered
  records separately.
- A column or path an assertion references that doesn't exist fails at
  `plan` time (B8).
- `expect:` is meant for full loads into empty targets. Target-side
  aggregates count every row in the table, so under `mode: upsert` into a
  table that already has rows they include rows this run didn't write. The
  user docs say so. Scoping assertions to the keys a run wrote is deferred.

#### 5.10 The flow vocabulary is closed

`from`, `sink`, `to`, `columns`, `key`, `mode`, `where`, `parent`, `unnest`,
`nest`, `order_by`, `join`, `keys`, `cardinality`, `strategy`, `expect`,
`depends_on` and `runtime` are the whole flow vocabulary. A target or column
named after one of them is fine, because they appear only as keys at fixed
places in the grammar.

### 6. Per-flow overrides of runtime settings

**Rule:** a flow may override a runtime setting only if the setting's effect
is limited to that flow's own work. Settings that size or share a
process-wide resource can't be overridden per flow.

| Setting | Per-flow override | Why |
| --- | --- | --- |
| `on_error` | yes | decides the fate of this flow's records |
| `max_errors` | yes | a flow value adds a per-flow budget; the run-wide value still applies |
| `partition_rows` | yes | sizes this flow's partitions and transactions (0001) |
| `state` | no | one state store per run |
| `workers`, `memory_budget` | no | shared by every flow in the process |
| `source_max_connections`, `source_rows_per_sec` | no | a pool shared across flows. Per-connection limits, if wanted, belong in 0004 |

Overrides use the same key names under the flow's `runtime:`. Precedence is
the flow value, then `runtime.yaml`, then the built-in default. There are no
per-file defaults. Setting a non-overridable key on a flow is an error that
names the key and gives the reason above.

**v0.1 limit, v0.2 item (S3, 2026-10-17).** The v0.1 runner has run-wide
`on_error` and `max_errors` only, so `ferry plan` refuses a flow whose
`on_error` or `max_errors` differs from `runtime.yaml`'s (exit 4, naming the
key), rather than silently applying the run-wide value. A flow value equal
to the run-wide one is accepted. Per-flow budgets (a flow's own error
count, checked alongside the run-wide one, and per-flow `fail` vs
`dead_letter`) are planned for v0.2, together with the runner support they
need. The grammar above doesn't change, so configs written for v0.2 keep
their meaning; `partition_rows` is honoured per flow already.

### 7. `depends_on:` for ordering flows

In v0.1, flows are ordered across files only by an explicit `depends_on:`
list per flow. FK discovery, which needs a `ferry-core` change, is deferred
until after S2.

`depends_on: [<flow>, ...]` makes a flow start only after each named flow has
finished all of its partitions. Every name must exist, a name may appear only
once in the list, a flow may not name itself, and a cycle is an error that
names the cycle. All of these are checked at load time, so `ferry validate`
catches them without connecting (B1d). Flows with no path between them run
concurrently. Within a flow, order comes from `parent:` (§5.3).

Why this over FK discovery (A1, B7):

- **For `depends_on:`.** It needs no ferry-core change, so S2 is unblocked
  now. It is visible in the config and in the hash. It works for Mongo
  sources, which have no foreign keys to discover. And it can express
  dependencies that exist only in the target, created by the mapping, which
  source FK discovery could never find.
- **Against.** It is manual and can drift from the real schema, and a
  missing edge shows up only as an FK violation, or as a slower run. It is
  also a flow-level barrier, which costs parallelism compared with finer
  ordering.
- **Why the drift is tolerable in v0.1.** A6 drops or defers FKs during the
  load and validates them at finalize ("write order is an optimization, not
  the integrity mechanism"). A missing `depends_on:` therefore produces a
  finalize report, never silent corruption.
- **After S2.** Once `Source` can return FKs, discovered edges are added to
  the explicit ones (union), and `ferry plan` labels each edge `declared` or
  `discovered`. `depends_on:` stays the way to state edges discovery can't
  see.

Cross-flow foreign keys in the target DDL are not part of v0.1.
`TargetSchema` FKs stay within one flow.

### 8. `include:`

`include:` lives only in `ferry.yaml`. It is a list of paths or globs (`*`,
`**`), resolved relative to the `ferry/` directory and allowed to point
outside it (`../shared/connections.yaml`).

- An included file has the same form as a well-known file: it contains
  `flows:`, or `connections:`, or both. `runtime:` may only come from
  `runtime.yaml`, so that one file holds every runtime setting.
- Entries from included files merge with the conventional ones under the
  same rules: duplicate flow or connection names are errors naming both
  files.
- A literal path that doesn't exist, or a glob that matches nothing, is an
  error. That catches typos.
- A file reached twice (by convention and by `include:`, or by two
  overlapping globs) is loaded once. Files are identified by canonical path.
- An `include:` key anywhere except `ferry.yaml` is an error. With no
  nesting there can be no cycles. If nested includes are ever allowed, they
  will need cycle detection.

### 9. `${ENV}`

`${NAME}` is substituted in string values **only** in `connections.yaml`
entries (including included `connections:`) and in `runtime.state`. A `${`
anywhere in a flow is a config error naming the file and line. `NAME` matches
`[A-Za-z_][A-Za-z0-9_]*` and is read from the environment. There is no other
form in v0.1: `${file:/run/secrets/pg}` and the like are errors, and secrets
mounted as files are exported to the environment by the caller. A missing
variable is an error that names it without printing any other value, and
substituted values never appear in errors or in `ferry plan` (B1).

Keeping `${ENV}` out of flows follows from §10. The hash is computed before
substitution, so an environment-driven `where: created_at < '${CUTOFF}'` could
change between a run and its resume without changing the hash, and a resumed
run would then write different rows than the attempt it is resuming.
Parameterized flows are deferred.

### 10. The config hash

The run pins this hash (A4), and `resume` refuses to continue if it changes.

1. It is computed on the **parsed, merged model**, after defaults are filled
   in and **before `${ENV}` substitution**. `url: ${PG_URL}` is hashed as that
   literal text. Rotating a password doesn't block resume, and secret values
   never reach the hash.
2. File layout doesn't count. Moving a flow to another file, renaming a flow
   file, editing comments or whitespace, or changing `include:` leaves the
   hash unchanged as long as the model is the same.
3. Writing a default value out explicitly (`mode: insert`) doesn't change the
   hash, because defaults are filled in before hashing.
4. Canonical form: connections sorted by name, flows sorted by name, targets
   under `to:` sorted by name (write order is derived from `parent:`, not
   from key order). Everything whose order means something stays as written
   and is serialized as a list, not an object: `columns` (DDL column order),
   `from` entries (the driver comes first), named `where` predicates (which
   one is reported first) and `expect`. The hash is SHA-256 over canonical
   JSON with sorted object keys, written `sha256:<hex>`. `depends_on` has
   no meaningful order, so it is sorted before hashing.
5. Settings that change only how fast or how much at a time, not what gets
   written, are left out of the hash: `workers`, `memory_budget`,
   `partition_rows`, `source_max_connections` and `source_rows_per_sec`,
   both in `runtime.yaml` and in a flow's `runtime:`. Partitions are stored
   at plan time, so changing `partition_rows` has no effect on a resumed run
   anyway. This lets you resume a run that ran out of memory with a bigger
   `memory_budget`. Behavioural settings are hashed: `on_error` and
   `max_errors` (in `runtime.yaml` and per flow), and `state`, which decides
   whether a flow gets `TxnWithState` (§4).

The engine version is pinned separately (A4).

### 11. Vocabulary: what changes in the tickets

| Ticket | Ticket wording | 0003 wording |
| --- | --- | --- |
| Draft | `map:` | `columns:`. It names what the keys are (target columns) and avoids confusion with the canonical `Map` type and a `map()` function |
| Draft | `from:` list with `join:` | kept. Adds driver = first entry with no `join:`, and `join.cardinality` + `join.keys` on every join |
| B1 | `runtime` incl. `on_error`, `state` | `runtime.yaml` keys in §1. Per-flow overrides per §6 |
| B1 | `include:` for flow-per-file | flow-per-file is the `flows/` convention. `include:` is the escape hatch (§8) |
| B1 | `${ENV}` substitution | connections and `runtime.state` only (§9). Hash before substitution (§10) |
| B2 | reserved sigils `$parent`, `$index`, `$key` | kept and defined in §5.4, plus `$item`. `$key` is the UUIDv5 child key |
| B2 | path syntax `customer.id`, `items[*].sku` | unchanged. Owned by B2 |
| B4 | `columns:` | `columns:` per target, an ordered map. `"*"` for identity |
| B4 | `split:` | no keyword: several keys under `to:` |
| B4 | `where:` with propagation | `where:` on the flow (driver) and on each target. Propagation follows `parent:` (§5.6) |
| B4 | `key:` (required for upsert) | `key:` per target, a list of target columns. Required on every target when `mode: upsert` |
| B4 | declared per-partition write order | derived from `parent:`, not declared |
| B5 | `unnest: path:` | `unnest: <path>` (scalar) on a target, usually together with `parent:`. Also accepts a `many` alias. B5 moves to S2 (see Consequences) |
| B5 | `parent_ref` vs `_parent.id` | only `$parent.<column>`, the **mapped** parent row |
| B5 | UUIDv5 child keys "by default" | opt-in: `id: $key`. `key:` is always explicit |
| B5 | null array "per policy" | zero children, not configurable |
| B6 | `merge:` with a driver entity | no keyword: `from:` as a list. The first entry is the driver |
| B6 | `cardinality: one \| many` | `join.cardinality`, required, no default |
| B6 | join keys | `join.keys: {<this column>: <alias.column>}`, equi-joins only |
| B6 | strategy chosen at plan time | `join.strategy: auto \| co_partition \| stage`, default `auto` |
| B6 | `nest:` with `order_by` + tie-breaker | a column spec `{nest: <many alias>, order_by, where, columns}`. The primary key is always appended as the tie-breaker |
| B7 | DAG from config + schema graph | `depends_on:` across flows, `parent:` within a flow. FK discovery deferred until after S2 (§7) |
| B7 | per-flow idempotency strategy | not configurable: derived from `mode` + `SinkCaps`, shown in plan. `TxnWithState` only when the state store is on the flow's sink (§4) |
| B8 | `expect:` blocks, `sum(total_cents) == source.sum(total * 100)` | `expect:` is a map of name → assertion. Target columns are table-qualified in split flows. `source.` covers written driver records |
| A5 | `source_max_connections`, `source_rows_per_sec`, `memory_budget` | `runtime.yaml` keys, not overridable per flow |
| A7 | `on_error: dead_letter`, `max_errors` | same names. `on_error: fail \| dead_letter`, default `fail` |
| WriteMode | `Insert`, `Upsert` | `mode: insert \| upsert` per flow |

### 12. Interface with 0004

0003 owns `name` and `type` in each connection entry, the reserved keys and
the reference syntax (§3), and the hash and `${ENV}` rules (§9, §10). 0004
owns the other fields of each connector type, how they are validated and
described in the JSON Schema, and how `runtime.state.connection` is resolved
and shares the sink's transaction (0004 §8). What 0003 needs from 0004:

- Which fields are secret, so `ferry plan` and `validate` can warn when a
  secret is written literally instead of through `${ENV}`. Answered: fields
  of type `Secret`, marked `x-ferry-secret` (0004 §4).
- Where a Postgres sink's default schema is set, since target keys under
  `to:` carry no schema (§3). Answered: the connection's `schema:` field,
  default `public` (0004 §2).
- Whether per-connection throttles replace or add to the run-wide
  `source_max_connections` and `source_rows_per_sec` (§6). Still open.

## Consequences

- B1 can freeze serde structs and the JSON Schema from §1. The bad-config set
  follows from the error rules in §2, §8 and §9.
- B4, B5, B6 and B8 are edited to the wording in §11 before work starts.
  The golden config at `examples/golden/ferry/` and the merge example at
  `examples/pg-merge/ferry/` are written in this grammar and are what B1, B7,
  S2 and S3 test against.
- Every flow has exactly one sink, which rules out fan-out to two databases in
  one flow. Two flows reading the same source do it instead.
- Upsert never deletes target rows for records that are later filtered out.
  Anyone who needs that needs a sync tool, not v0.1.
- `${ENV}` can't parameterize flows. A cutoff date or tenant id has to be
  written into the flow, which also puts it in the hash.
- B5 (unnest) is part of S2, not S3: the first end-to-end run produces both
  `orders` and `order_items` from the golden data, so `unnest:`, `$parent`
  and `$index` (§5.4, §5.7) have to work by S2. `nest:` (B6) stays in S3.
- Exactly-once (`TxnWithState`) needs the state store on the flow's sink
  connection. A run whose state store is elsewhere still re-runs partitions
  safely, through the sink's other strategy (§4, 0004 §8).
- A connection's real location can change behind an unchanged
  `url: ${PG_URL}` without changing the hash. A4 should also record the
  identity of the connected server (0004 decides what that is) if pointing a
  resume at a different database has to be refused.
- `golden/expected/filtered.json` gives the reason as prose ("deleted_at is
  set"). Under §5.6 the reason is the predicate's name or expression text,
  so the fixture and its README need to change together (Track B owns them).

## Amendment 2026-10-17: lossy conversions and expression names (B2, B3)

**Opt-in syntax (decided).** A lossy conversion is allowed per column, by
`allow_lossy: true` on a typed column object:

```yaml
total_cents: {expr: "int(round(total * 100))", type: int32, allow_lossy: true}
```

- `allow_lossy` needs `type:` in the same object; on its own it is a model
  error (the JSON Schema says so with `dependentRequired`). There is no
  flow-wide or global switch: each lossy column says so where it is
  written, and the opt-in is part of the config hash.
- Only the conversion from the expression's inferred type to `type:` is
  checked. Conversions written in the expression (`int(x)`, `CAST`) are
  explicit already.
- Conversions fall in three classes. *Safe* (every value converts exactly:
  integer widening, `decimal(10,2)` → `decimal(12,4)`, anything → `utf8`)
  needs nothing. *Lossy* needs `allow_lossy: true`; values that change are
  changed as the table says, values that can't convert are error rows.
  *Impossible* (`bool` → `timestamp`) is an error even with the opt-in.

The canonical lossy table (`ferry_dsl::typecheck::cast::LOSSY`, each row
tested both ways):

| From | To | Effect once opted in |
| --- | --- | --- |
| `int64` | `int32` | out-of-range values are error rows (any integer narrowing) |
| `int32` | `int16` | out-of-range values are error rows |
| `uint64` | `int64` | values above int64's maximum are error rows |
| `int64` | `uint64` | negative values are error rows |
| `int64` | `float64` | integers beyond ±2^53 are rounded |
| `float64` | `float32` | rounded to float32 |
| `float64` | `int64` | the fraction is truncated toward zero; out-of-range values are error rows |
| `decimal(38,10)` | `float64` | rounded to the nearest float64 (any decimal over 15 digits) |
| `decimal(12,4)` | `decimal(12,2)` | scale shrinks: rounded half away from zero |
| `decimal(12,2)` | `decimal(8,2)` | values with more integer digits are error rows |
| `float64` | `decimal(12,2)` | rounded to the scale |
| `decimal(12,2)` | `int64` | the fraction is dropped |
| `timestamp(UTC)` | `timestamp` | the zone is dropped |
| `timestamp` | `timestamp(UTC)` | the wall-clock time is read as UTC |
| `timestamp(UTC)` | `date` | the time of day is dropped |
| `utf8` | `int64` | text that isn't an integer is an error row (any parse from text) |
| `utf8` | `uuid` | text that isn't a UUID is an error row |
| `utf8` | `json` | text that isn't JSON is an error row |
| `binary` | `utf8` | bytes that aren't UTF-8 are error rows |

**Type names** for `type:` are `int8` … `int64`, `uint8` … `uint64`,
`float32`, `float64`, `decimal(p,s)`, `bool`, `utf8`, `binary`, `date`,
`timestamp`, `timestamp(<zone>)`, `uuid`, `json` and `list(<type>)`.

**Expression names (B2, confirms §5.4).** The sigils are `$parent`,
`$index`, `$item` and `$key`. Their bare words `parent`, `index`, `item` and
`key` are reserved at the start of a path, so `line_no: index` is a load
error that suggests `$index`; a field with one of those names is written
quoted (`"index"`). Names are case-sensitive. `x == null` (and SQL's
`x = null`) is true for null and missing; `missing(x)` is true only for
missing.

## Open questions

Decided 2026-10-16:

- §7: flows are ordered only by explicit `depends_on:` in v0.1. The
  `ferry-core` FK-discovery change is deferred until after S2.
- §10.5: the hash leaves out runtime tuning settings (`workers`,
  `memory_budget`, `partition_rows`, the two throttles). `on_error`,
  `max_errors` and `state` stay in it.
- §9: no `${ENV}` in flows, only in connections and `runtime.state`. Only
  `${NAME}` from the environment; no `${file:...}`.
- §5.3: each flow names its sink once with `sink: <conn>`, and keys under
  `to:` are plain table names.
- §5.9: `expect:` is documented as meant for full loads.
- §3: `name`, `type` and `description` are reserved connection keys.
- §4: `TxnWithState` goes through the registry's state-capable extension
  (0004 §8). A flow whose sink isn't the state connection falls back to the
  sink's next strategy.
- B5 (unnest) moves into S2.

Still open:

1. §5.4: a child sees the driver record only through `$parent`. A `$record`
   sigil may be needed later.
2. §9: how to write a literal `${` in a connection value, and whether a
   variable set to the empty string counts as missing. B1c needs both.
