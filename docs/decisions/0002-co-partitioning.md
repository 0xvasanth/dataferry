# 0002: Co-partitioning handshake for streaming merges

Status: accepted (Phase 0, ITS-112). Needs review from both tracks.

## Context

A merge like `orders` + `order_items` → documents can stream partition by
partition only if every child row is read in the same unit of work as its
parent. That needs the planner (Track B) to tell the source partitioner
(Track A) where to cut. Agreeing on this late would force both sides to be
rewritten.

## Decision

1. The planner plans the **driver** entity normally (e.g. `orders` on `id`).
2. It derives `CoPartition::from_partitions("order_id", &driver)`: the
   driver's range boundaries, applied to the child's join column. This only
   works if the driver's partitions are contiguous key ranges on one column
   that are unbounded at both ends; otherwise it returns
   `CoPartitionUnsupported`.
3. It plans the child with `PartitionHint { co_partition: Some(..), .. }`.
   A source **must** then produce exactly `CoPartition::ranges()`, in order,
   on that column, ignoring its own sizing. A source that can't (no such
   column, no efficient range scan on it) returns `CoPartitionUnsupported`.
4. `align(&driver, &child)` pairs partition *i* with partition *i* and
   refuses lists whose bounds differ. The runner leases a pair as one unit of
   work and feeds both streams to the merge's `TransformPlan`.
5. On `CoPartitionUnsupported`, the planner falls back to staging the
   smaller side and hash-joining. It shows the chosen strategy in
   `ferry plan`.

## Consequences

- Child partitions can be very uneven (one order with 100k items). That's
  accepted for v0.1. The child's `target_rows` is ignored when co-partitioning.
- Boundaries are `KeyValue`s, so driver key and child join column must
  compare as the same kind (both integers, both strings, both bytes).
- Track A (A1, A2) implements step 3 for Postgres and Mongo. Track B (B6)
  implements steps 1, 2, 4 and 5.
