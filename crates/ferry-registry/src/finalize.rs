//! Finishing a sink's targets once a flow is loaded (A6).
//!
//! A sink may loosen its targets for a bulk load: drop secondary indexes,
//! drop foreign keys so rows can land in any order. [`Sink`] has no hook for
//! after the load, so a connector that needs one returns a [`Finalizer`]
//! next to its sink in [`Endpoints`](crate::Endpoints). The runner calls it
//! once every partition of a flow is done, and again on every later
//! execution of the run (resume, retry), so it must be idempotent.
//!
//! "Write order is an optimization, not the integrity mechanism": a
//! constraint the loaded rows break is reported with the rows that break
//! it, never silently left off.
//!
//! [`Sink`]: ferry_core::Sink

use std::fmt;

use async_trait::async_trait;
use ferry_core::{Result, TargetSchema};

/// At most this many orphaned rows are listed per broken foreign key.
pub const MAX_ORPHANS: usize = 20;

/// Restores what a sink's `prepare` loosened on a flow's targets.
#[async_trait]
pub trait Finalizer: Send + Sync {
    /// Recreates what was dropped for the load of `target`'s tables, and
    /// creates the foreign keys `target` declares. Each object is restored
    /// on its own: one that fails doesn't keep the others off. Returns
    /// `Err` only when the sink can't be worked on at all; a constraint the
    /// data breaks is a [`FinalizeProblem`].
    async fn finalize(&self, target: &TargetSchema) -> Result<Finalized>;
}

/// What one flow's finalize did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Finalized {
    /// Indexes and constraints created or recreated, as `table.name`.
    pub restored: Vec<String>,
    pub problems: Vec<FinalizeProblem>,
}

impl Finalized {
    pub fn is_clean(&self) -> bool {
        self.problems.is_empty()
    }
}

/// An index or constraint that couldn't be restored as it was.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalizeProblem {
    pub table: String,
    /// The index or constraint's name.
    pub object: String,
    pub message: String,
    /// For a foreign key the loaded rows break: how many rows reference no
    /// parent, and the keys of up to [`MAX_ORPHANS`] of them, each as
    /// `column=value, ...` (the table's primary key, or the foreign key's
    /// own columns when it has none).
    pub orphans: Option<Orphans>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Orphans {
    pub count: u64,
    pub keys: Vec<String>,
}

impl fmt::Display for FinalizeProblem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "`{}`: `{}`: {}", self.table, self.object, self.message)?;
        let Some(orphans) = &self.orphans else {
            return Ok(());
        };
        let mut parts = orphans.keys.clone();
        let unlisted = orphans.count.saturating_sub(orphans.keys.len() as u64);
        if unlisted > 0 {
            parts.push(match parts.is_empty() {
                true => format!("{unlisted} rows"),
                false => format!("{unlisted} more"),
            });
        }
        if !parts.is_empty() {
            write!(f, " ({})", parts.join("; "))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn problem(orphans: Option<Orphans>) -> String {
        FinalizeProblem {
            table: "order_items".into(),
            object: "order_items_order_fk".into(),
            message: "rows reference no parent".into(),
            orphans,
        }
        .to_string()
    }

    #[test]
    fn a_problem_lists_the_orphans_it_has_keys_for_and_counts_the_rest() {
        let head = "`order_items`: `order_items_order_fk`: rows reference no parent";
        assert_eq!(problem(None), head);
        let orphans = |count, keys: &[&str]| {
            Some(Orphans {
                count,
                keys: keys.iter().map(|k| k.to_string()).collect(),
            })
        };
        assert_eq!(
            problem(orphans(2, &["id=1", "id=2"])),
            format!("{head} (id=1; id=2)")
        );
        assert_eq!(
            problem(orphans(25, &["id=1", "id=2"])),
            format!("{head} (id=1; id=2; 23 more)")
        );
        // A count without keys reads as a count, with no stray separator.
        assert_eq!(problem(orphans(5, &[])), format!("{head} (5 rows)"));
    }
}
