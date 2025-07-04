//! What the runner is given for each flow: its inputs, its compiled plan
//! and its sink.

use std::sync::Arc;

use ferry_core::{
    CoPartition, Entity, Partition, PartitionId, PartitionScope, Result, Sink, Source,
    TargetSchema, TransformPlan, WriteMode,
};

/// One flow, ready to run: the planner (B7) builds these from the config.
#[derive(Clone)]
pub struct Flow {
    /// The flow's name in the config. Unique within a run.
    pub name: String,
    /// One per entry of [`TransformPlan::inputs`], in the same order: the
    /// driver first.
    pub inputs: Vec<Input>,
    pub plan: Arc<dyn TransformPlan>,
    /// The one sink every target of the flow is written to (0003 §5).
    pub sink: Arc<dyn Sink>,
    /// The target tables, for [`Sink::prepare`].
    pub target: TargetSchema,
    pub mode: WriteMode,
    /// Flows that must be done, every partition, before this one starts
    /// (0003 §7).
    pub depends_on: Vec<String>,
}

/// One entity a flow reads.
#[derive(Clone)]
pub struct Input {
    pub source: Arc<dyn Source>,
    pub entity: Entity,
    /// `None` for the driver. For every other input, its join column: the
    /// input is split on the driver's partition boundaries applied to this
    /// column (0002), so partition *i* of each input covers the same keys
    /// and is read in the same unit of work.
    pub co_partition: Option<String>,
}

/// Partition ids are stored as `<flow>/<source id>`, so two flows reading
/// the same entity never plan the same id: the state store needs ids unique
/// within a run (A4), and sources only make them unique per entity. Flow
/// names are identifiers (0003 §3) and can't contain the `/`.
pub(crate) fn namespaced(flow: &str, partition: &Partition) -> Partition {
    Partition {
        id: PartitionId(format!("{flow}/{}", partition.id.0)),
        ..partition.clone()
    }
}

/// The partition as its source planned it, for [`Source::read`].
pub(crate) fn unnamespaced(flow: &str, partition: &Partition) -> Partition {
    let id = partition
        .id
        .0
        .strip_prefix(flow)
        .and_then(|rest| rest.strip_prefix('/'))
        .unwrap_or(&partition.id.0);
    Partition {
        id: PartitionId(id.to_owned()),
        ..partition.clone()
    }
}

/// What each input reads, per driver partition: the driver partition
/// itself, then one partition per co-partitioned input. Those are not
/// stored: their ranges follow from the stored driver partitions, exactly
/// as the source had to produce them when the flow was planned (0002 step
/// 3), so resume reads the same keys without asking the source again.
pub(crate) fn unit_reads(flow: &Flow, drivers: &[Partition]) -> Result<Vec<Vec<Partition>>> {
    let mut units: Vec<Vec<Partition>> = drivers.iter().map(|d| vec![d.clone()]).collect();
    for input in &flow.inputs[1..] {
        let column = input
            .co_partition
            .as_deref()
            .expect("checked when the runner was built");
        let ranges = CoPartition::from_partitions(column, drivers)?.ranges();
        for (i, (unit, range)) in units.iter_mut().zip(ranges).enumerate() {
            unit.push(Partition {
                id: PartitionId(format!("{}-{i}", input.entity.name())),
                entity: input.entity.name().to_owned(),
                scope: PartitionScope::KeyRange(range),
            });
        }
    }
    Ok(units)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn namespacing_round_trips_and_keeps_the_scope() {
        let p = Partition {
            id: PartitionId("orders-3".into()),
            entity: "orders".into(),
            scope: PartitionScope::Whole,
        };
        let stored = namespaced("orders_copy", &p);
        assert_eq!(stored.id.0, "orders_copy/orders-3");
        assert_eq!(stored.scope, p.scope);
        assert_eq!(unnamespaced("orders_copy", &stored), p);
        // A flow whose name prefixes another's doesn't strip the wrong part.
        assert_eq!(unnamespaced("orders", &stored).id.0, "orders_copy/orders-3");
    }
}
