//! A checked assertion as the verifier (A8) evaluates it.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use ferry_core::Result;
use ferry_verify::{AssertionContext, AssertionOutcome, CompiledAssertion};

use super::check::{Calc, CheckedAssertion, SourceTerm};
use super::log::{SourceLog, expect_error};
use super::parse::Arith;
use super::value::{Accumulated, Value};

/// One `expect:` entry, ready for `Verifier::with_assertion`.
pub struct ExpectAssertion {
    assertion: CheckedAssertion,
    terms: Arc<[SourceTerm]>,
    log: Arc<dyn SourceLog>,
}

impl ExpectAssertion {
    pub(crate) fn new(
        assertion: CheckedAssertion,
        terms: Arc<[SourceTerm]>,
        log: Arc<dyn SourceLog>,
    ) -> Self {
        Self {
            assertion,
            terms,
            log,
        }
    }

    /// Each source term's aggregate over the attempts that committed.
    async fn sources(&self, ctx: &AssertionContext<'_>) -> Result<Vec<Accumulated>> {
        let mut used = Vec::new();
        uses(&self.assertion.left, &mut used);
        uses(&self.assertion.right, &mut used);
        if used.is_empty() {
            return Ok(Vec::new());
        }
        let records: BTreeMap<String, _> = self
            .log
            .load(ctx.run())
            .await?
            .into_iter()
            .filter(|r| r.flow == ctx.flow())
            .map(|r| (r.partition.clone(), r))
            .collect();
        let mut totals: Vec<Accumulated> = self
            .terms
            .iter()
            .map(|t| Accumulated::empty(t.agg))
            .collect();
        for done in ctx.partitions() {
            let record = records.get(&done.partition).ok_or_else(|| {
                expect_error(format!(
                    "partition `{}` has no source-side record, so its rows' source values are \
                     unknown",
                    done.partition
                ))
            })?;
            if record.epoch != done.epoch {
                return Err(expect_error(format!(
                    "partition `{}` committed at epoch {}, but its source-side record is from \
                     epoch {}",
                    done.partition, done.epoch, record.epoch
                )));
            }
            for (total, term) in totals.iter_mut().zip(self.terms.iter()) {
                let Some(aggregate) = record.aggregates.get(&term.key) else {
                    return Err(expect_error(format!(
                        "partition `{}` has no `source.{}` record",
                        done.partition, term.key
                    )));
                };
                total.merge(aggregate);
            }
        }
        Ok(totals)
    }
}

/// The source terms `calc` reads.
fn uses(calc: &Calc, out: &mut Vec<usize>) {
    match calc {
        Calc::Source(i) => out.push(*i),
        Calc::Neg(c) | Calc::Abs(c) | Calc::Round(c, _) => uses(c, out),
        Calc::Binary(_, l, r) => {
            uses(l, out);
            uses(r, out);
        }
        Calc::Literal(_) | Calc::Target { .. } | Calc::SourceCount => {}
    }
}

/// Every target aggregate `calc` reads, queried in order.
fn targets<'c>(calc: &'c Calc, out: &mut Vec<&'c Calc>) {
    match calc {
        Calc::Target { .. } => out.push(calc),
        Calc::Neg(c) | Calc::Abs(c) | Calc::Round(c, _) => targets(c, out),
        Calc::Binary(_, l, r) => {
            targets(l, out);
            targets(r, out);
        }
        Calc::Literal(_) | Calc::SourceCount | Calc::Source(_) => {}
    }
}

/// What one side reads, gathered before it is computed.
struct Inputs<'c> {
    targets: Vec<(&'c Calc, Value)>,
    sources: Vec<Accumulated>,
    source_count: u64,
}

fn compute(calc: &Calc, inputs: &Inputs<'_>) -> std::result::Result<Value, String> {
    Ok(match calc {
        Calc::Literal(v) => v.clone(),
        Calc::Target { .. } => inputs
            .targets
            .iter()
            .find(|(c, _)| std::ptr::eq(*c, calc))
            .map(|(_, v)| v.clone())
            .expect("every target aggregate was queried"),
        Calc::SourceCount => Value::int(i128::from(inputs.source_count)),
        Calc::Source(i) => inputs.sources[*i].value()?,
        Calc::Neg(c) => compute(c, inputs)?.neg()?,
        Calc::Abs(c) => compute(c, inputs)?.abs()?,
        Calc::Round(c, digits) => compute(c, inputs)?.round(*digits)?,
        Calc::Binary(op, l, r) => {
            let (l, r) = (compute(l, inputs)?, compute(r, inputs)?);
            match op {
                Arith::Add => l.add(&r)?,
                Arith::Sub => l.sub(&r)?,
                Arith::Mul => l.mul(&r)?,
                Arith::Div => l.div(&r)?,
            }
        }
    })
}

#[async_trait]
impl CompiledAssertion for ExpectAssertion {
    fn name(&self) -> &str {
        &self.assertion.name
    }

    fn expression(&self) -> &str {
        &self.assertion.text
    }

    async fn evaluate(&self, ctx: &AssertionContext<'_>) -> Result<AssertionOutcome> {
        let mut wanted = Vec::new();
        targets(&self.assertion.left, &mut wanted);
        targets(&self.assertion.right, &mut wanted);
        let mut queried = Vec::with_capacity(wanted.len());
        for calc in wanted {
            let Calc::Target {
                table,
                aggregate,
                column,
            } = calc
            else {
                unreachable!("only target aggregates are gathered")
            };
            let scalar = ctx.target(table, *aggregate, column.as_deref()).await?;
            queried.push((calc, Value::from_scalar(scalar)));
        }
        let inputs = Inputs {
            targets: queried,
            sources: self.sources(ctx).await?,
            source_count: ctx.source_count(),
        };
        let side = |calc| compute(calc, &inputs).map_err(expect_error);
        let left = side(&self.assertion.left)?;
        let right = side(&self.assertion.right)?;
        Ok(AssertionOutcome::compare(
            left.to_scalar(),
            self.assertion.op,
            right.to_scalar(),
        ))
    }
}
