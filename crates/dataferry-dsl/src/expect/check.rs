//! Type-checking an assertion at `plan` time: every table and column it
//! names exists in the flow's checked targets, every source-side
//! expression compiles in driver scope (B2), and each aggregate gets
//! values it can aggregate.

use dataferry_core::types::CanonicalType;
use dataferry_verify::{Aggregate, Comparison};

use super::parse::{Agg, Arith, Assertion, CharSpan, Name, SourceArg, TargetArg, Term};
use super::value::Value;
use crate::config::model::{Expr, OrderedMap};
use crate::config::{ConfigError, ErrorKind, FieldPath, SourceFile, suggest};
use crate::expr::{Compiled, ExprError, Scope};
use crate::typecheck::{CheckedColumn, CheckedTarget};
use crate::types;

/// A flow's `expect:`, checked.
#[derive(Clone, Debug, Default)]
pub struct FlowExpect {
    /// In the order written.
    pub assertions: Vec<CheckedAssertion>,
    /// Every distinct source-side aggregate the assertions use, which the
    /// run accumulates at read time.
    pub sources: Vec<SourceTerm>,
}

impl FlowExpect {
    pub fn is_empty(&self) -> bool {
        self.assertions.is_empty()
    }
}

/// One source-side aggregate over a driver-scope expression.
#[derive(Clone, Debug)]
pub struct SourceTerm {
    /// `sum(total * 100)`, as written: names the accumulator in the
    /// source log.
    pub key: String,
    pub agg: Agg,
    /// In driver scope: the driver record as it was read, before any join.
    pub expr: Compiled,
}

#[derive(Clone, Debug)]
pub struct CheckedAssertion {
    pub name: String,
    /// As written.
    pub text: String,
    pub op: Comparison,
    pub(crate) left: Calc,
    pub(crate) right: Calc,
}

/// How to compute one side once the run is over.
#[derive(Clone, Debug)]
pub(crate) enum Calc {
    Literal(Value),
    Target {
        table: String,
        aggregate: Aggregate,
        column: Option<String>,
    },
    /// `source.count()`: the written driver records, as the verifier
    /// counted them.
    SourceCount,
    /// An index into [`FlowExpect::sources`].
    Source(usize),
    Neg(Box<Calc>),
    Binary(Arith, Box<Calc>, Box<Calc>),
    Abs(Box<Calc>),
    Round(Box<Calc>, i32),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Number,
    Text,
}

impl Kind {
    fn name(self) -> &'static str {
        match self {
            Kind::Number => "a number",
            Kind::Text => "text",
        }
    }
}

/// Checks every assertion of `expect` against the flow's `targets` and its
/// driver. `driver` is the driver record alone (what the source returns);
/// `scope` is the full driver scope with joined entries, used only to
/// explain why a joined alias isn't available on the source side.
pub(crate) fn check(
    expect: &OrderedMap<Expr>,
    targets: &[CheckedTarget],
    driver: &Scope,
    scope: &Scope,
    path: &FieldPath,
    files: &[SourceFile],
) -> (FlowExpect, Vec<ConfigError>) {
    let mut checker = Checker {
        targets,
        driver,
        scope,
        expect: FlowExpect::default(),
    };
    let mut errors = Vec::new();
    for (name, text) in expect.iter() {
        let path = path.key(name.as_str());
        let checked = Assertion::parse_at(name, text, text.location())
            .and_then(|assertion| checker.assertion(&assertion));
        match checked {
            Ok(checked) => checker.expect.assertions.push(checked),
            Err(e) => errors.push(e.to_config_error(&path, files)),
        }
    }
    (checker.expect, errors)
}

struct Checker<'a> {
    targets: &'a [CheckedTarget],
    driver: &'a Scope,
    scope: &'a Scope,
    expect: FlowExpect,
}

impl Checker<'_> {
    fn assertion(&mut self, a: &Assertion) -> Result<CheckedAssertion, ExprError> {
        let (left, left_kind) = self.term(a, &a.left)?;
        let (right, right_kind) = self.term(a, &a.right)?;
        if left_kind != right_kind {
            return Err(ExprError::new(
                ErrorKind::Type,
                &a.text,
                &a.location,
                format!(
                    "the assertion compares {} with {}",
                    left_kind.name(),
                    right_kind.name()
                ),
            ));
        }
        Ok(CheckedAssertion {
            name: a.name.clone(),
            text: a.text.clone(),
            op: a.op,
            left,
            right,
        })
    }

    fn term(&mut self, a: &Assertion, term: &Term) -> Result<(Calc, Kind), ExprError> {
        let number = |this: &mut Self, t: &Term, what: &str, span: CharSpan| {
            let (calc, kind) = this.term(a, t)?;
            if kind != Kind::Number {
                return Err(type_error(
                    a,
                    span,
                    format!("{what} needs a number, not text"),
                ));
            }
            Ok(calc)
        };
        Ok(match term {
            Term::Number(text, span) => match Value::number(text) {
                Some(v) => (Calc::Literal(v), Kind::Number),
                None => return Err(type_error(a, *span, format!("`{text}` is not a number"))),
            },
            Term::Text(text) => (
                Calc::Literal(Value::Text {
                    value: text.clone(),
                }),
                Kind::Text,
            ),
            Term::Neg(t, span) => (
                Calc::Neg(Box::new(number(self, t, "`-`", *span)?)),
                Kind::Number,
            ),
            Term::Abs(t, span) => (
                Calc::Abs(Box::new(number(self, t, "`abs`", *span)?)),
                Kind::Number,
            ),
            Term::Round(t, digits, span) => (
                Calc::Round(Box::new(number(self, t, "`round`", *span)?), *digits),
                Kind::Number,
            ),
            Term::Binary(op, l, r, span) => {
                let what = match op {
                    Arith::Add => "`+`",
                    Arith::Sub => "`-`",
                    Arith::Mul => "`*`",
                    Arith::Div => "`/`",
                };
                let l = number(self, l, what, *span)?;
                let r = number(self, r, what, *span)?;
                (Calc::Binary(*op, Box::new(l), Box::new(r)), Kind::Number)
            }
            Term::Target { agg, arg, span } => self.target(a, *agg, arg, *span)?,
            Term::Source { agg, arg, span } => match arg {
                None => (Calc::SourceCount, Kind::Number),
                Some(arg) => self.source(a, *agg, arg, *span)?,
            },
        })
    }

    fn target(
        &self,
        a: &Assertion,
        agg: Agg,
        arg: &TargetArg,
        span: CharSpan,
    ) -> Result<(Calc, Kind), ExprError> {
        let tables = || self.targets.iter().map(|t| t.table.as_str());
        let (table, column): (&CheckedTarget, Option<&CheckedColumn>) = match arg {
            TargetArg::Rows => match self.targets {
                [one] => (one, None),
                _ => {
                    return Err(name_error(
                        a,
                        span,
                        "`count(*)` is ambiguous in a flow with several targets".into(),
                    )
                    .help(format!(
                        "count one table's rows: {}",
                        tables()
                            .map(|t| format!("`count({t})`"))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )));
                }
            },
            TargetArg::Path(path) => match path.as_slice() {
                [name] => match self.targets.iter().find(|t| t.table == name.value) {
                    Some(table) if agg == Agg::Count => (table, None),
                    Some(table) => {
                        return Err(type_error(
                            a,
                            name.span,
                            format!(
                                "`{}` is a table: `{}` aggregates one of its columns",
                                table.table,
                                agg.name()
                            ),
                        )
                        .help(format!(
                            "`{}({}.<column>)`",
                            agg.name(),
                            table.table
                        )));
                    }
                    None => match self.targets {
                        [one] => (one, Some(column(a, one, name)?)),
                        _ => return Err(self.unqualified(a, name)),
                    },
                },
                [table, name] => {
                    let Some(target) = self.targets.iter().find(|t| t.table == table.value) else {
                        let help = suggest(&table.value, tables())
                            .map(|s| format!("did you mean `{s}`?"))
                            .unwrap_or_else(|| {
                                format!(
                                    "this flow's targets: {}",
                                    tables().collect::<Vec<_>>().join(", ")
                                )
                            });
                        return Err(name_error(
                            a,
                            table.span,
                            format!("`{}` is not a target of this flow", table.value),
                        )
                        .help(help));
                    };
                    (target, Some(column(a, target, name)?))
                }
                _ => unreachable!("parsed as one or two names"),
            },
        };
        let aggregate = match agg {
            Agg::Count => Aggregate::Count,
            Agg::Sum => Aggregate::Sum,
            Agg::Min => Aggregate::Min,
            Agg::Max => Aggregate::Max,
            Agg::CountDistinct => Aggregate::CountDistinct,
        };
        let kind = match column {
            Some(c) => {
                let what = format!("`{}.{}`", table.table, c.field.name);
                kind_of(a, agg, Some(&c.field.ty), &what, span)?
            }
            None => Kind::Number,
        };
        Ok((
            Calc::Target {
                table: table.table.clone(),
                aggregate,
                column: column.map(|c| c.field.name.clone()),
            },
            kind,
        ))
    }

    /// A bare column in a flow with several targets.
    fn unqualified(&self, a: &Assertion, name: &Name) -> ExprError {
        let owners: Vec<&str> = self
            .targets
            .iter()
            .filter(|t| t.column(&name.value).is_some())
            .map(|t| t.table.as_str())
            .collect();
        if let Some(first) = owners.first() {
            return name_error(
                a,
                name.span,
                format!(
                    "`{}` is ambiguous in a flow with several targets",
                    name.value
                ),
            )
            .help(format!(
                "qualify it with its table: `{first}.{}`",
                name.value
            ));
        }
        let candidates: Vec<String> = self
            .targets
            .iter()
            .flat_map(|t| {
                std::iter::once(t.table.clone()).chain(
                    t.columns
                        .iter()
                        .map(move |c| format!("{}.{}", t.table, c.field.name)),
                )
            })
            .collect();
        let bare: Vec<&str> = candidates
            .iter()
            .map(|c| c.rsplit('.').next().unwrap_or(c))
            .collect();
        let help = suggest(&name.value, bare.iter().copied())
            .and_then(|s| bare.iter().position(|b| *b == s))
            .map(|i| format!("did you mean `{}`?", candidates[i]));
        let error = name_error(
            a,
            name.span,
            format!(
                "`{}` is not a target or a column of a target of this flow",
                name.value
            ),
        );
        match help {
            Some(help) => error.help(help),
            None => error,
        }
    }

    fn source(
        &mut self,
        a: &Assertion,
        agg: Agg,
        arg: &SourceArg,
        span: CharSpan,
    ) -> Result<(Calc, Kind), ExprError> {
        let compiled = match arg.expression.compile(self.driver) {
            Ok(compiled) => compiled,
            Err(e) => {
                let joined = arg.expression.compile(self.scope).is_ok();
                let mut e = arg.relocate(e, &a.text, &a.location);
                if joined {
                    e.help = Some(
                        "source-side aggregates see the driver record as it was read, before \
                         any join; aggregate a target column instead"
                            .into(),
                    );
                }
                return Err(e);
            }
        };
        let what = format!("`{}`", arg.text);
        let kind = kind_of(a, agg, compiled.output_type(), &what, span)?;
        let key = format!("{}({})", agg.name(), arg.text);
        let index = match self.expect.sources.iter().position(|s| s.key == key) {
            Some(i) => i,
            None => {
                self.expect.sources.push(SourceTerm {
                    key,
                    agg,
                    expr: compiled,
                });
                self.expect.sources.len() - 1
            }
        };
        Ok((Calc::Source(index), kind))
    }
}

fn column<'t>(
    a: &Assertion,
    table: &'t CheckedTarget,
    name: &Name,
) -> Result<&'t CheckedColumn, ExprError> {
    if let Some(c) = table.column(&name.value) {
        return Ok(c);
    }
    let names = table.columns.iter().map(|c| c.field.name.as_str());
    let help = suggest(&name.value, names.clone())
        .map(|s| format!("did you mean `{s}`?"))
        .unwrap_or_else(|| {
            format!(
                "`{}` has: {}",
                table.table,
                names.collect::<Vec<_>>().join(", ")
            )
        });
    Err(name_error(
        a,
        name.span,
        format!("`{}` has no column `{}`", table.table, name.value),
    )
    .help(help))
}

/// What `agg` over values of `ty` gives, or why it can't. `ty` is `None`
/// for a bare `null`.
fn kind_of(
    a: &Assertion,
    agg: Agg,
    ty: Option<&CanonicalType>,
    what: &str,
    span: CharSpan,
) -> Result<Kind, ExprError> {
    let numeric = ty.is_some_and(is_numeric);
    let shown = ty.map_or("null".to_owned(), types::name);
    match agg {
        Agg::Count | Agg::CountDistinct => Ok(Kind::Number),
        Agg::Sum if numeric => Ok(Kind::Number),
        Agg::Sum => Err(type_error(
            a,
            span,
            format!("`sum` adds numbers, but {what} is {shown}"),
        )),
        Agg::Min | Agg::Max if numeric => Ok(Kind::Number),
        Agg::Min | Agg::Max if ty == Some(&CanonicalType::Utf8) => Ok(Kind::Text),
        Agg::Min | Agg::Max => Err(type_error(
            a,
            span,
            format!(
                "`{}` compares numbers or text, but {what} is {shown}",
                agg.name()
            ),
        )),
    }
}

fn is_numeric(ty: &CanonicalType) -> bool {
    use CanonicalType::*;
    matches!(
        ty,
        Int8 | Int16
            | Int32
            | Int64
            | UInt8
            | UInt16
            | UInt32
            | UInt64
            | Float32
            | Float64
            | Decimal(_)
    )
}

fn name_error(a: &Assertion, span: CharSpan, message: String) -> ExprError {
    ExprError::new(ErrorKind::Expr, &a.text, &a.location, message).span(Some(span))
}

fn type_error(a: &Assertion, span: CharSpan, message: String) -> ExprError {
    ExprError::new(ErrorKind::Type, &a.text, &a.location, message).span(Some(span))
}
