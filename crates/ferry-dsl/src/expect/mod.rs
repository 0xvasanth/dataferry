//! `expect:` assertions (B8, 0003 §5.9), compiled for the verifier (A8).
//!
//! An assertion is checked three times, each as early as it can be:
//!
//! 1. **When the config is validated** ([`Assertion::parse_at`]): the
//!    comparison, the aggregates and the B2 syntax of every source-side
//!    expression. No schema is needed.
//! 2. **At `plan` time**, with the flow's type check (B3): every table and
//!    column it names exists among the flow's targets, every source-side
//!    expression compiles in driver scope, and each aggregate gets values
//!    it can aggregate. An unknown column fails here, never after the run.
//!    The result is the flow's [`FlowExpect`] in
//!    [`CheckedFlow::expect`](crate::typecheck::CheckedFlow::expect).
//! 3. **After the run**, by the verifier: [`Expectations`] gives the
//!    runner an observer that accumulates source-side aggregates at read
//!    time, and the verifier one [`CompiledAssertion`] per entry.
//!
//! # Grammar
//!
//! ```text
//! assertion  := side ("==" | "!=" | "<" | "<=" | ">" | ">=") side
//! side       := number | 'text' | aggregate | "-" side | side ("+" | "-" | "*" | "/") side
//!             | "abs(" side ")" | "round(" side ["," digits] ")" | "(" side ")"
//! aggregate  := target | source
//! target     := "count(" table ")" | "count(*)"
//!             | ("count" | "sum" | "min" | "max" | "count_distinct") "(" [table "."] column ")"
//! source     := "source.count()"
//!             | "source." ("count" | "sum" | "min" | "max") "(" B2 expression ")"
//! ```
//!
//! - **Target side.** Computed by querying the target after the run.
//!   `count(<table>)` counts rows, `count(<column>)` the column's non-null
//!   values. The table may be left out when the flow has one target; with
//!   several, a bare name must be a table, and a column must be
//!   qualified. A name that is both a target and a column is the table.
//! - **Source side.** Evaluated in driver scope, on the driver record as
//!   the source returned it (a merge's joined entries aren't there), and
//!   accumulated at read time over the records the run **wrote**: neither
//!   filtered nor rejected. The source is never queried again, so a
//!   source that changes during or after the run doesn't change the
//!   result. `source.count()` is the verifier's count of written records.
//!   `source.count_distinct` isn't supported: distinct values can't be
//!   added up partition by partition.
//! - **Values.** Integers and decimals are exact, so money compares
//!   without float rounding; division gives a float. `round` rounds half
//!   away from zero. Null follows SQL: arithmetic with null is null, an
//!   aggregate skips nulls (`sum` of nothing is null, `count` 0), and a
//!   comparison with null holds only for `!=`. `sum`, `min` and `max`
//!   take numbers, and `min` and `max` also text.
//!
//! # Retries
//!
//! Source-side aggregates are recorded per partition attempt, before the
//! commit, in a [`SourceLog`], with the same rules as the verifier's write
//! log: a later attempt replaces an earlier one, a stale one is refused,
//! and an assertion only adds up the attempts that committed. A partition
//! that is retried counts once.
//!
//! `expect:` is meant for full loads into empty targets: target-side
//! aggregates count every row in the table (0003 §5.9).

mod assertion;
mod check;
mod log;
mod observer;
mod parse;
pub mod value;

use std::collections::HashMap;
use std::sync::Arc;

use ferry_runtime::WriteObserver;
use ferry_verify::{CompiledAssertion, Verifier};

pub use assertion::ExpectAssertion;
pub(crate) use check::check;
pub use check::{CheckedAssertion, FlowExpect, SourceTerm};
pub use log::{FileSourceLog, MemorySourceLog, SourceLog, SourceRecord};
pub use observer::SourceAggregates;
pub use parse::{Agg, Assertion};

use crate::typecheck::CheckedFlow;

/// The `expect:` of every checked flow, wired to one [`SourceLog`]: the
/// runner's observer and the verifier's assertions share it.
pub struct Expectations {
    log: Arc<dyn SourceLog>,
    flows: Vec<(String, FlowExpect, Arc<[SourceTerm]>)>,
}

impl Expectations {
    pub fn new(flows: &[CheckedFlow], log: Arc<dyn SourceLog>) -> Self {
        Self {
            log,
            flows: flows
                .iter()
                .filter(|f| !f.expect.is_empty())
                .map(|f| {
                    let terms: Arc<[SourceTerm]> = f.expect.sources.clone().into();
                    (f.name.clone(), f.expect.clone(), terms)
                })
                .collect(),
        }
    }

    /// For `Runner::with_observer`: accumulates every flow's source-side
    /// aggregates at read time.
    pub fn observer(&self) -> Arc<dyn WriteObserver> {
        let terms: HashMap<String, Arc<[SourceTerm]>> = self
            .flows
            .iter()
            .filter(|(_, _, terms)| !terms.is_empty())
            .map(|(name, _, terms)| (name.clone(), Arc::clone(terms)))
            .collect();
        Arc::new(SourceAggregates::new(Arc::clone(&self.log), terms))
    }

    /// Every assertion, with the flow it belongs to, in config order.
    pub fn assertions(&self) -> Vec<(String, Arc<dyn CompiledAssertion>)> {
        self.flows
            .iter()
            .flat_map(|(name, expect, terms)| {
                expect.assertions.iter().map(|a| {
                    let compiled: Arc<dyn CompiledAssertion> = Arc::new(ExpectAssertion::new(
                        a.clone(),
                        Arc::clone(terms),
                        Arc::clone(&self.log),
                    ));
                    (name.clone(), compiled)
                })
            })
            .collect()
    }

    /// `verifier` with every assertion added.
    pub fn verifier(&self, verifier: Verifier) -> Verifier {
        self.assertions()
            .into_iter()
            .fold(verifier, |v, (flow, a)| v.with_assertion(&flow, a))
    }
}
