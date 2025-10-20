//! The interface `expect:` assertions plug into (0003 §5.9).
//!
//! B8 compiles each named assertion of a flow into a [`CompiledAssertion`];
//! the verifier evaluates it after the load with an [`AssertionContext`]
//! and puts the outcome in the run report. An assertion compares two
//! sides: target-side aggregates, queried from the target through
//! [`AssertionContext::target`], and source-side values accumulated at
//! read time over the driver records the run wrote.
//!
//! The context exposes one source-side value itself,
//! [`AssertionContext::source_count`] (`source.count()`). Source
//! aggregates over driver-scope expressions (`source.sum(total * 100)`)
//! are accumulated at read time by B8's own observer, recorded per
//! partition attempt like a [`PartitionRecord`]; an assertion sums the
//! records of [`AssertionContext::partitions`], the attempts that
//! committed.

use std::cmp::Ordering;
use std::fmt;

use async_trait::async_trait;
use ferry_core::Result;
use serde::Serialize;

use ferry_registry::state::RunId;

use crate::log::{PartitionRecord, RecordCounts};
use crate::target::{Aggregate, Scalar, Target};

/// One compiled `expect:` assertion of one flow.
#[async_trait]
pub trait CompiledAssertion: Send + Sync {
    /// Its name in the flow's `expect:` map.
    fn name(&self) -> &str;

    /// The assertion as written, for the report.
    fn expression(&self) -> &str;

    /// Computes both sides and compares them. An `Err` is a failure to
    /// evaluate (the target refused a query), reported as such: it is not
    /// a passing or failing comparison.
    async fn evaluate(&self, ctx: &AssertionContext<'_>) -> Result<AssertionOutcome>;
}

/// What an assertion can see.
pub struct AssertionContext<'a> {
    pub(crate) run: &'a RunId,
    pub(crate) flow: &'a str,
    pub(crate) partitions: &'a [PartitionRecord],
    pub(crate) target: &'a dyn Target,
    pub(crate) records: RecordCounts,
}

impl AssertionContext<'_> {
    /// The flow the assertion belongs to.
    pub fn flow(&self) -> &str {
        self.flow
    }

    /// The run being verified.
    pub fn run(&self) -> &RunId {
        self.run
    }

    /// The write-time record of each of the flow's done partitions: the
    /// attempt that committed it, with its lease epoch. A done partition
    /// with no record is a problem the verifier reports itself, and is
    /// not in this list.
    pub fn partitions(&self) -> &[PartitionRecord] {
        self.partitions
    }

    /// A target-side aggregate, queried now.
    pub async fn target(
        &self,
        table: &str,
        aggregate: Aggregate,
        column: Option<&str>,
    ) -> Result<Scalar> {
        self.target.aggregate(table, aggregate, column).await
    }

    /// `source.count()`: the flow's driver records the run wrote, neither
    /// filtered nor rejected, counted at read time over its done
    /// partitions.
    pub fn source_count(&self) -> u64 {
        self.records.written
    }

    /// Every driver record of the flow, by what became of it.
    pub fn records(&self) -> RecordCounts {
        self.records
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum Comparison {
    #[serde(rename = "==")]
    Eq,
    #[serde(rename = "!=")]
    Ne,
    #[serde(rename = "<")]
    Lt,
    #[serde(rename = "<=")]
    Le,
    #[serde(rename = ">")]
    Gt,
    #[serde(rename = ">=")]
    Ge,
}

impl Comparison {
    /// Whether `left <op> right` holds. Null, or a number against text,
    /// compares as nothing: only `!=` holds.
    pub fn holds(self, left: &Scalar, right: &Scalar) -> bool {
        match left.compare(right) {
            None => self == Comparison::Ne,
            Some(o) => match self {
                Comparison::Eq => o == Ordering::Equal,
                Comparison::Ne => o != Ordering::Equal,
                Comparison::Lt => o == Ordering::Less,
                Comparison::Le => o != Ordering::Greater,
                Comparison::Gt => o == Ordering::Greater,
                Comparison::Ge => o != Ordering::Less,
            },
        }
    }
}

impl fmt::Display for Comparison {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Comparison::Eq => "==",
            Comparison::Ne => "!=",
            Comparison::Lt => "<",
            Comparison::Le => "<=",
            Comparison::Gt => ">",
            Comparison::Ge => ">=",
        })
    }
}

/// Both sides of an evaluated assertion and whether it held.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AssertionOutcome {
    pub left: Scalar,
    pub op: Comparison,
    pub right: Scalar,
    pub passed: bool,
}

impl AssertionOutcome {
    pub fn compare(left: Scalar, op: Comparison, right: Scalar) -> Self {
        let passed = op.holds(&left, &right);
        Self {
            left,
            op,
            right,
            passed,
        }
    }
}
