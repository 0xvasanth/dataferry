use ferry_core::{Epoch, IdempotencyStrategy as S, Sink, SinkCaps, Source, WriteMode};

fn caps(strategies: &[S]) -> SinkCaps {
    SinkCaps {
        strategies: strategies.to_vec(),
        bulk_copy: false,
    }
}

#[test]
fn insert_into_an_upsert_only_sink_is_refused() {
    // Re-running an inserted partition on a sink that can only upsert by key
    // would duplicate rows that have no key conflict, so there is no safe
    // strategy and the planner must refuse.
    assert_eq!(
        caps(&[S::UpsertByKey]).strategy_for(WriteMode::Insert),
        None
    );
}

#[test]
fn upsert_into_an_upsert_only_sink_uses_upsert_by_key() {
    assert_eq!(
        caps(&[S::UpsertByKey]).strategy_for(WriteMode::Upsert),
        Some(S::UpsertByKey)
    );
}

#[test]
fn sink_with_no_strategies_supports_no_write_mode() {
    assert_eq!(caps(&[]).strategy_for(WriteMode::Insert), None);
    assert_eq!(caps(&[]).strategy_for(WriteMode::Upsert), None);
}

#[test]
fn txn_with_state_is_preferred_whenever_available() {
    let all = caps(&[
        S::StagingSwap,
        S::UpsertByKey,
        S::DeletePartitionThenInsert,
        S::TxnWithState,
    ]);
    assert_eq!(all.strategy_for(WriteMode::Insert), Some(S::TxnWithState));
    assert_eq!(all.strategy_for(WriteMode::Upsert), Some(S::TxnWithState));
}

#[test]
fn insert_without_txn_with_state_falls_back_to_delete_then_insert() {
    let c = caps(&[S::UpsertByKey, S::StagingSwap, S::DeletePartitionThenInsert]);
    assert_eq!(
        c.strategy_for(WriteMode::Insert),
        Some(S::DeletePartitionThenInsert)
    );
}

#[test]
fn staging_swap_is_insert_only() {
    // Swapping a staging table replaces the whole target, which would drop
    // rows an upsert is meant to keep.
    assert!(S::StagingSwap.is_compatible_with(WriteMode::Insert));
    assert!(!S::StagingSwap.is_compatible_with(WriteMode::Upsert));
    assert_eq!(
        caps(&[S::StagingSwap]).strategy_for(WriteMode::Upsert),
        None
    );
}

#[test]
fn a_newer_lease_has_a_higher_epoch() {
    assert!(Epoch(8) > Epoch(7));
}

#[test]
fn source_and_sink_are_usable_as_trait_objects() {
    // The runner picks connectors from config at runtime, so both traits must
    // stay dyn-compatible. This fails to compile if that ever regresses.
    fn _accepts(_: &dyn Source, _: &dyn Sink) {}
}
