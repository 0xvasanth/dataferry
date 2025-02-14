# 0001: Commit once per partition in v0.1

Status: accepted (Phase 0, ITS-109)

## Context

The spec's `Source::read(partition, from: Option<Checkpoint>)` implies
checkpoints inside a partition, while `Sink::commit` happens per partition.
Mixing the two means either long sink transactions (a partition of several
minutes held open in Postgres causes bloat and lock pressure) or commits
inside a partition, which breaks the "delete the partition's rows, then
insert" idempotency strategy because each chunk would delete the previous
chunks' rows.

## Decision

- The runner opens one `SinkTxn` per leased partition, writes every target
  batch for it, and commits once.
- Transaction length is bounded by partition size: the planner passes
  `PartitionHint::target_rows`, and sources split entities accordingly.
- `Source::read` takes no resume checkpoint. A partition that didn't commit
  is re-read from the start.
- `Checkpoint` stays in `ferry-core` but is unused in v0.1. It is reserved for
  chunked commits, which v0.2 can add for sinks whose strategy tolerates them
  (`UpsertByKey`, `TxnWithState`).

## Consequences

- Resume granularity is one partition: a crash loses at most one
  partition's work per worker.
- Every sink must declare an `IdempotencyStrategy` compatible with the write
  mode; `SinkCaps::strategy_for` returns `None` otherwise and the planner
  refuses the flow.
- Under `TxnWithState`, the sink checks the lease epoch inside the commit,
  so a worker whose lease was superseded cannot commit.
