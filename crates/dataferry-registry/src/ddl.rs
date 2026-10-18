//! Showing what a sink's `prepare` would create, without creating it (B7).
//!
//! `dataferry plan` prints each flow's target DDL and must not touch the
//! target. [`Sink`] only has `prepare`, which runs it, so a connector that
//! can say what it would run returns a [`RenderDdl`] next to its sink in
//! [`Endpoints`](crate::Endpoints). The planner emits the dialect-neutral
//! [`TargetSchema`]; only the sink knows its dialect.
//!
//! [`Sink`]: dataferry_core::Sink

use dataferry_core::{TargetSchema, WriteMode};

/// Renders a sink's DDL for a target schema. Pure: no I/O.
pub trait RenderDdl: Send + Sync {
    /// The statements `prepare(target, mode)` followed by the flow's
    /// finalize would run on a database where none of `target`'s tables
    /// exist yet, in order. A table that already exists is validated, not
    /// created, so a real run may run fewer of them.
    fn render_ddl(&self, target: &TargetSchema, mode: WriteMode) -> Vec<String>;
}
