use std::collections::BTreeMap;

use futures::TryStreamExt;

use crate::entity::Entity;
use crate::partition::{Partition, PartitionHint, PartitionId, PartitionScope};
use crate::source::Source;
use crate::transform::SourceRecordId;

/// Checks the guarantees every [`Source`] makes, against whatever data the
/// source currently holds (which must not change while the check runs):
///
/// - `discover` returns at least one entity;
/// - batches match [`Entity::arrow_schema`] exactly;
/// - planned partitions have unique ids, belong to their entity, and together
///   contain every row of the entity exactly once (no gaps, no overlaps);
/// - reading the same partition twice returns the same rows.
pub async fn check_source(source: &dyn Source, hint: &PartitionHint) -> Vec<String> {
    let mut violations = Vec::new();
    let entities = match source.discover().await {
        Ok(e) if e.is_empty() => return vec!["discover returned no entities".into()],
        Ok(e) => e,
        Err(e) => return vec![format!("discover failed: {e}")],
    };
    for entity in &entities {
        check_entity(source, entity, hint, &mut violations).await;
    }
    violations
}

async fn check_entity(
    source: &dyn Source,
    entity: &Entity,
    hint: &PartitionHint,
    violations: &mut Vec<String>,
) {
    let name = entity.name();
    let whole = Partition {
        id: PartitionId(format!("{name}-contract-whole")),
        entity: name.to_owned(),
        scope: PartitionScope::Whole,
    };
    let Some(all_rows) = read_keys(source, entity, &whole, violations).await else {
        return;
    };

    let partitions = match source.plan_partitions(entity, hint).await {
        Ok(p) => p,
        Err(e) => {
            violations.push(format!("{name}: plan_partitions failed: {e}"));
            return;
        }
    };

    let mut seen_ids = Vec::new();
    let mut found: BTreeMap<SourceRecordId, Vec<PartitionId>> = BTreeMap::new();
    for p in &partitions {
        if p.entity != name {
            violations.push(format!(
                "{name}: partition {:?} names entity `{}`",
                p.id, p.entity
            ));
        }
        if seen_ids.contains(&p.id) {
            violations.push(format!("{name}: partition id {:?} is used twice", p.id));
        }
        seen_ids.push(p.id.clone());

        let Some(keys) = read_keys(source, entity, p, violations).await else {
            continue;
        };
        if let Some(again) = read_keys(source, entity, p, violations).await
            && again != keys
        {
            violations.push(format!(
                "{name}: partition {:?} returned different rows on a second read",
                p.id
            ));
        }
        for k in keys {
            found.entry(k).or_default().push(p.id.clone());
        }
    }

    for (key, ids) in &found {
        if ids.len() > 1 {
            violations.push(format!(
                "{name}: row {key:?} is in more than one partition: {ids:?}"
            ));
        }
    }
    for key in &all_rows {
        if !found.contains_key(key) {
            violations.push(format!("{name}: row {key:?} is in no partition"));
        }
    }
    for key in found.keys() {
        if !all_rows.contains(key) {
            violations.push(format!(
                "{name}: partitions returned row {key:?} that a whole read does not"
            ));
        }
    }
}

/// Keys of every row in a partition, sorted, or `None` if reading failed.
async fn read_keys(
    source: &dyn Source,
    entity: &Entity,
    partition: &Partition,
    violations: &mut Vec<String>,
) -> Option<Vec<SourceRecordId>> {
    let name = entity.name();
    let batches = match source.read(entity, partition).await {
        Ok(stream) => stream.try_collect::<Vec<_>>().await,
        Err(e) => Err(e),
    };
    let batches = match batches {
        Ok(b) => b,
        Err(e) => {
            violations.push(format!(
                "{name}: reading partition {:?} failed: {e}",
                partition.id
            ));
            return None;
        }
    };
    let expected = entity.arrow_schema();
    let mut keys = Vec::new();
    for batch in &batches {
        if batch.schema().fields() != expected.fields() {
            violations.push(format!(
                "{name}: partition {:?} produced a batch whose schema differs from the entity's",
                partition.id
            ));
            return None;
        }
        match entity.record_ids(batch) {
            Ok(k) => keys.extend(k),
            Err(e) => {
                violations.push(format!("{name}: {e}"));
                return None;
            }
        }
    }
    keys.sort();
    Some(keys)
}
