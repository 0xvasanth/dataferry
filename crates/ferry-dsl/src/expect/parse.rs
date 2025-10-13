//! Parsing an assertion: the comparison, the arithmetic around it, and the
//! aggregates on each side. Source-side arguments are B2 expressions,
//! parsed (not yet compiled) here.

use datafusion_sql::sqlparser::ast::{
    BinaryOperator, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments, Ident,
    Spanned as _, UnaryOperator, Value as SqlValue,
};
use datafusion_sql::sqlparser::dialect::GenericDialect;
use datafusion_sql::sqlparser::parser::Parser;
use datafusion_sql::sqlparser::tokenizer::{Location as SqlLocation, Span, Token};
use ferry_verify::Comparison;

use crate::config::{ErrorKind, Location, suggest};
use crate::expr::{ExprError, Expression, lex, line_starts, syntax_error};

/// An aggregate function, on either side.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Agg {
    Count,
    Sum,
    Min,
    Max,
    CountDistinct,
}

impl Agg {
    pub const ALL: [Agg; 5] = [Agg::Count, Agg::Sum, Agg::Min, Agg::Max, Agg::CountDistinct];

    pub fn name(self) -> &'static str {
        match self {
            Agg::Count => "count",
            Agg::Sum => "sum",
            Agg::Min => "min",
            Agg::Max => "max",
            Agg::CountDistinct => "count_distinct",
        }
    }

    fn from_name(name: &str) -> Option<Agg> {
        Agg::ALL.into_iter().find(|a| a.name() == name)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Arith {
    Add,
    Sub,
    Mul,
    Div,
}

/// Char offsets into the assertion's text.
pub(crate) type CharSpan = (usize, usize);

/// One side of the comparison, or a part of one.
#[derive(Clone, Debug)]
pub(crate) enum Term {
    /// A number literal as written.
    Number(String, CharSpan),
    Text(String),
    /// `count(orders)`, `sum(orders.total_cents)`, `count(*)`.
    Target {
        agg: Agg,
        arg: TargetArg,
        span: CharSpan,
    },
    /// `source.count()`, `source.sum(total * 100)`.
    Source {
        agg: Agg,
        arg: Option<Box<SourceArg>>,
        span: CharSpan,
    },
    Neg(Box<Term>, CharSpan),
    Binary(Arith, Box<Term>, Box<Term>, CharSpan),
    Abs(Box<Term>, CharSpan),
    /// `round(x)` or `round(x, n)`.
    Round(Box<Term>, i32, CharSpan),
}

#[derive(Clone, Debug)]
pub(crate) enum TargetArg {
    /// `count(*)`: rows of the flow's one target.
    Rows,
    /// `orders`, `total_cents` or `orders.total_cents`.
    Path(Vec<Name>),
}

#[derive(Clone, Debug)]
pub(crate) struct Name {
    pub value: String,
    pub span: CharSpan,
}

/// A source-side argument: a B2 expression written inside the assertion.
#[derive(Clone, Debug)]
pub(crate) struct SourceArg {
    /// As written, trimmed.
    pub text: String,
    /// Where `text` starts in the assertion, in chars.
    pub offset: usize,
    pub expression: Expression,
}

impl SourceArg {
    /// An error of the argument's own expression, pointing into the
    /// whole assertion instead.
    pub fn relocate(&self, error: ExprError, assertion: &str, at: &Location) -> ExprError {
        relocate(error, self.offset, assertion, at)
    }
}

fn relocate(mut error: ExprError, offset: usize, assertion: &str, at: &Location) -> ExprError {
    error.text = assertion.into();
    error.location = at.clone();
    error.span = error
        .span
        .map(|(start, end)| (start + offset, end + offset));
    error
}

/// A parsed `expect:` entry. Parsing checks everything that needs no
/// schema; [`check`](super::check) resolves its names at `plan` time.
#[derive(Clone, Debug)]
pub struct Assertion {
    pub(crate) name: String,
    pub(crate) text: String,
    pub(crate) location: Location,
    pub(crate) left: Term,
    pub(crate) op: Comparison,
    pub(crate) right: Term,
}

impl Assertion {
    /// Parses the assertion named `name`, written at `location`.
    pub fn parse_at(name: &str, text: &str, location: &Location) -> Result<Self, ExprError> {
        let (sql, _) = lex(text);
        let p = Parse {
            text,
            chars: text.chars().collect(),
            location,
            line_starts: line_starts(text),
        };
        let fail = |message: String, span: Option<(usize, usize)>| {
            ExprError::new(ErrorKind::Expr, text, location, message).span(span)
        };
        let len = p.chars.len();
        let dialect = GenericDialect {};
        // A source argument that doesn't parse can make sqlparser read
        // `source.sum` as a column and fail later, at the `(`: report the
        // argument's own error instead.
        let syntax = |e| {
            p.source_arg_error()
                .unwrap_or_else(|| syntax_error(e, len, &p.line_starts, &fail))
        };
        let mut parser = Parser::new(&dialect).try_with_sql(&sql).map_err(syntax)?;
        let expr = parser.parse_expr().map_err(syntax)?;
        let next = parser.peek_token();
        if next.token != Token::EOF {
            return Err(p.source_arg_error().unwrap_or_else(|| {
                p.error(
                    next.span,
                    format!("unexpected `{}` after the assertion", next.token),
                )
                .help("an assertion is one comparison, such as `count(orders) == source.count()`")
            }));
        }
        let (left, op, right) = match &expr {
            Expr::BinaryOp { left, op, right } => match comparison(op) {
                Some(op) => (left, op, right),
                None => return Err(p.not_a_comparison(&expr)),
            },
            Expr::Nested(_) => {
                return Err(p
                    .error(expr.span(), "an assertion can't be in parentheses".into())
                    .help("write the comparison itself: `count(orders) == source.count()`"));
            }
            _ => return Err(p.not_a_comparison(&expr)),
        };
        Ok(Self {
            name: name.to_owned(),
            text: text.to_owned(),
            location: location.clone(),
            left: p.term(left)?,
            op,
            right: p.term(right)?,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// The assertion as written.
    pub fn text(&self) -> &str {
        &self.text
    }

    pub fn location(&self) -> &Location {
        &self.location
    }
}

fn comparison(op: &BinaryOperator) -> Option<Comparison> {
    Some(match op {
        BinaryOperator::Eq => Comparison::Eq,
        BinaryOperator::NotEq => Comparison::Ne,
        BinaryOperator::Lt => Comparison::Lt,
        BinaryOperator::LtEq => Comparison::Le,
        BinaryOperator::Gt => Comparison::Gt,
        BinaryOperator::GtEq => Comparison::Ge,
        _ => return None,
    })
}

/// The functions an assertion can call, for suggestions.
const FUNCTIONS: [&str; 11] = [
    "count",
    "sum",
    "min",
    "max",
    "count_distinct",
    "abs",
    "round",
    "source.count",
    "source.sum",
    "source.min",
    "source.max",
];

struct Parse<'a> {
    text: &'a str,
    chars: Vec<char>,
    location: &'a Location,
    line_starts: Vec<usize>,
}

impl Parse<'_> {
    fn offset(&self, location: SqlLocation) -> Option<usize> {
        let line = usize::try_from(location.line).ok()?.checked_sub(1)?;
        let column = usize::try_from(location.column).ok()?.checked_sub(1)?;
        Some(self.line_starts.get(line)? + column)
    }

    fn span(&self, span: Span) -> Option<CharSpan> {
        if span == Span::empty() {
            return None;
        }
        Some((self.offset(span.start)?, self.offset(span.end)?))
    }

    /// The span of `e`, or the whole assertion when sqlparser has none.
    fn span_of(&self, e: &Expr) -> CharSpan {
        self.span(e.span()).unwrap_or((0, self.chars.len()))
    }

    fn error(&self, span: Span, message: String) -> ExprError {
        ExprError::new(ErrorKind::Expr, self.text, self.location, message).span(self.span(span))
    }

    fn error_at(&self, span: CharSpan, message: String) -> ExprError {
        ExprError::new(ErrorKind::Expr, self.text, self.location, message).span(Some(span))
    }

    fn not_a_comparison(&self, e: &Expr) -> ExprError {
        if let Expr::BinaryOp {
            op: BinaryOperator::And | BinaryOperator::Or,
            ..
        } = e
        {
            return self
                .error(
                    e.span(),
                    "an assertion is one comparison, without `and` or `or`".into(),
                )
                .help("write each comparison as an assertion of its own");
        }
        self.error(
            e.span(),
            "an assertion compares two values with ==, !=, <, <=, > or >=".into(),
        )
        .help("for example `sum(orders.total_cents) == source.sum(total * 100)`")
    }

    fn term(&self, e: &Expr) -> Result<Term, ExprError> {
        let span = self.span_of(e);
        match e {
            Expr::Nested(inner) => self.term(inner),
            Expr::Value(v) => match &v.value {
                SqlValue::Number(n, _) => Ok(Term::Number(n.clone(), span)),
                SqlValue::SingleQuotedString(s) => Ok(Term::Text(s.clone())),
                SqlValue::Null => Err(self
                    .error_at(span, "an assertion can't compare with null".into())
                    .help(
                        "every comparison with null is false; an aggregate over no rows is null, \
                         so compare `count(...)` instead",
                    )),
                _ => Err(self.error_at(span, format!("`{v}` can't be used in an assertion"))),
            },
            Expr::UnaryOp {
                op: UnaryOperator::Minus,
                expr,
            } => Ok(Term::Neg(Box::new(self.term(expr)?), span)),
            Expr::UnaryOp {
                op: UnaryOperator::Plus,
                expr,
            } => self.term(expr),
            Expr::BinaryOp { left, op, right } => {
                let arith = match op {
                    BinaryOperator::Plus => Arith::Add,
                    BinaryOperator::Minus => Arith::Sub,
                    BinaryOperator::Multiply => Arith::Mul,
                    BinaryOperator::Divide => Arith::Div,
                    op if comparison(op).is_some() => {
                        return Err(self
                            .error_at(span, "an assertion has exactly one comparison".into())
                            .help("write a second assertion for the second comparison"));
                    }
                    op => {
                        return Err(self.error_at(
                            span,
                            format!("`{op}` can't be used in an assertion: only + - * /"),
                        ));
                    }
                };
                Ok(Term::Binary(
                    arith,
                    Box::new(self.term(left)?),
                    Box::new(self.term(right)?),
                    span,
                ))
            }
            Expr::Function(f) => self.function(f, span),
            Expr::Identifier(_) | Expr::CompoundIdentifier(_) => Err(self
                .error_at(span, format!("`{e}` is a column, not an aggregate"))
                .help(format!(
                    "aggregate it: `sum({e})` over the target, or `source.sum(...)` over the \
                     records read"
                ))),
            _ => Err(self.error_at(span, format!("`{e}` can't be used in an assertion"))),
        }
    }

    fn function(&self, f: &Function, span: CharSpan) -> Result<Term, ExprError> {
        let parts: Option<Vec<String>> = f
            .name
            .0
            .iter()
            .map(|p| p.as_ident().map(|i| i.value.to_lowercase()))
            .collect();
        let shown = f.name.to_string();
        let parts = parts.unwrap_or_default();
        if f.over.is_some()
            || f.filter.is_some()
            || !f.within_group.is_empty()
            || f.null_treatment.is_some()
            || !matches!(f.parameters, FunctionArguments::None)
        {
            return Err(self.error_at(
                span,
                format!("`{shown}` can't take OVER, FILTER or WITHIN GROUP"),
            ));
        }
        if let FunctionArguments::List(list) = &f.args
            && (list.duplicate_treatment.is_some() || !list.clauses.is_empty())
        {
            return Err(self
                .error_at(span, format!("`{shown}` takes plain arguments"))
                .help("count distinct values with `count_distinct(<table>.<column>)`"));
        }
        let args: Vec<&FunctionArg> = match &f.args {
            FunctionArguments::List(list) => list.args.iter().collect(),
            FunctionArguments::None => Vec::new(),
            FunctionArguments::Subquery(_) => {
                return Err(self.error_at(span, "a subquery can't be an argument".into()));
            }
        };
        match parts
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
            .as_slice()
        {
            ["source", agg] => self.source(f, agg, &args, span),
            ["abs"] => {
                let [arg] = self.exprs(&args, "abs", 1, span)?[..] else {
                    unreachable!("checked")
                };
                Ok(Term::Abs(Box::new(self.term(arg)?), span))
            }
            ["round"] => {
                let exprs = match args.len() {
                    1 => self.exprs(&args, "round", 1, span)?,
                    _ => self.exprs(&args, "round", 2, span)?,
                };
                let digits = match exprs.get(1) {
                    None => 0,
                    Some(d) => self.digits(d)?,
                };
                Ok(Term::Round(Box::new(self.term(exprs[0])?), digits, span))
            }
            [name] => match Agg::from_name(name) {
                Some(agg) => self.target(agg, &shown, &args, span),
                None => Err(self.unknown_function(name, span)),
            },
            _ => Err(self.unknown_function(&shown, span)),
        }
    }

    fn unknown_function(&self, name: &str, span: CharSpan) -> ExprError {
        let why = match name {
            "avg" | "source.avg" => " (divide a `sum` by a `count`)",
            _ => "",
        };
        let help = suggest(name, FUNCTIONS)
            .map(|s| format!("did you mean `{s}`?"))
            .unwrap_or_else(|| {
                "assertions use count, sum, min, max and count_distinct over the target, \
                 source.count, source.sum, source.min and source.max over the records read, \
                 and abs and round"
                    .into()
            });
        self.error_at(span, format!("`{name}` is not an assertion function{why}"))
            .help(help)
    }

    /// Exactly `n` plain expression arguments.
    fn exprs<'b>(
        &self,
        args: &[&'b FunctionArg],
        name: &str,
        n: usize,
        span: CharSpan,
    ) -> Result<Vec<&'b Expr>, ExprError> {
        let exprs: Option<Vec<&Expr>> = args
            .iter()
            .map(|a| match a {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(e),
                _ => None,
            })
            .collect();
        match exprs {
            Some(exprs) if exprs.len() == n => Ok(exprs),
            _ => Err(self.error_at(
                span,
                format!(
                    "`{name}` takes {n} argument{}",
                    if n == 1 { "" } else { "s" }
                ),
            )),
        }
    }

    /// `round`'s digits: a whole number literal.
    fn digits(&self, e: &Expr) -> Result<i32, ExprError> {
        let (negative, inner) = match e {
            Expr::UnaryOp {
                op: UnaryOperator::Minus,
                expr,
            } => (true, expr.as_ref()),
            other => (false, other),
        };
        let n = match inner {
            Expr::Value(v) => match &v.value {
                SqlValue::Number(n, _) => n.parse::<i32>().ok(),
                _ => None,
            },
            _ => None,
        };
        match n {
            Some(n) if n <= 38 => Ok(if negative { -n } else { n }),
            _ => Err(self.error_at(
                self.span_of(e),
                "`round`'s digits are a whole number from -38 to 38".into(),
            )),
        }
    }

    fn target(
        &self,
        agg: Agg,
        shown: &str,
        args: &[&FunctionArg],
        span: CharSpan,
    ) -> Result<Term, ExprError> {
        let usage = || {
            format!(
                "`{shown}` takes one target column, `{shown}(<table>.<column>)`{}",
                if agg == Agg::Count {
                    ", or a table to count its rows, `count(<table>)`"
                } else {
                    ""
                }
            )
        };
        let [arg] = args else {
            return Err(self.error_at(span, usage()));
        };
        let name = |i: &Ident| Name {
            value: i.value.clone(),
            span: self.span(i.span).unwrap_or(span),
        };
        let arg = match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Wildcard) if agg == Agg::Count => TargetArg::Rows,
            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(i))) => {
                TargetArg::Path(vec![name(i)])
            }
            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::CompoundIdentifier(parts)))
                if parts.len() == 2 =>
            {
                TargetArg::Path(parts.iter().map(name).collect())
            }
            _ => {
                return Err(self.error_at(span, usage()).help(
                    "target-side aggregates read columns as they landed; to aggregate an \
                     expression over the records read, use `source.<agg>(...)`",
                ));
            }
        };
        Ok(Term::Target { agg, arg, span })
    }

    fn source(
        &self,
        f: &Function,
        agg: &str,
        args: &[&FunctionArg],
        span: CharSpan,
    ) -> Result<Term, ExprError> {
        let agg = match Agg::from_name(agg) {
            Some(Agg::CountDistinct) => {
                return Err(self
                    .error_at(span, "`source.count_distinct` isn't supported".into())
                    .help(
                        "distinct values can't be added up partition by partition; when the \
                         values are unique, compare `count_distinct(<table>.<column>)` with \
                         `source.count()`",
                    ));
            }
            Some(agg) => agg,
            None => return Err(self.unknown_function(&format!("source.{agg}"), span)),
        };
        if args.len() > 1 {
            return Err(self.error_at(
                span,
                format!("`source.{}` takes one expression", agg.name()),
            ));
        }
        let arg = self.source_arg(f, span)?;
        let usage = match (agg, &arg) {
            (Agg::Count, _) | (_, Some(_)) => None,
            (_, None) => Some(format!(
                "`source.{}` takes one expression over the driver record",
                agg.name()
            )),
        };
        if let Some(usage) = usage {
            return Err(self.error_at(span, usage));
        }
        Ok(Term::Source { agg, arg, span })
    }

    /// The text between the parentheses of a `source.` call, as a B2
    /// expression. `None` for `()` and `(*)`.
    fn source_arg(
        &self,
        f: &Function,
        span: CharSpan,
    ) -> Result<Option<Box<SourceArg>>, ExprError> {
        let after_name = self.span(f.name.span()).map_or(span.0, |(_, end)| end);
        let Some((start, end)) = self.parenthesized(after_name) else {
            return Err(self.error_at(span, "`source.` aggregates need parentheses".into()));
        };
        let inner: String = self.chars[start..end].iter().collect();
        let leading = inner.len() - inner.trim_start().len();
        let text = inner.trim();
        if text.is_empty() || text == "*" {
            return Ok(None);
        }
        let offset = start + inner[..leading].chars().count();
        match Expression::parse_at(text, self.location) {
            Ok(expression) => Ok(Some(Box::new(SourceArg {
                text: text.to_owned(),
                offset,
                expression,
            }))),
            Err(e) => Err(relocate(e, offset, self.text, self.location)),
        }
    }

    /// The first `source.<agg>(...)` whose argument isn't a valid B2
    /// expression, as that expression's error.
    fn source_arg_error(&self) -> Option<ExprError> {
        let (sql, _) = lex(self.text);
        let lexed: Vec<char> = sql.chars().collect();
        let mut quote = None::<char>;
        for (i, &c) in lexed.iter().enumerate() {
            match quote {
                Some(q) if c == q => quote = None,
                Some(_) => continue,
                None if c == '\'' || c == '"' => quote = Some(c),
                None => {}
            }
            let starts_word = i == 0 || !(lexed[i - 1].is_alphanumeric() || lexed[i - 1] == '_');
            if quote.is_some() || !starts_word || !sql_starts_with(&lexed[i..], "source") {
                continue;
            }
            // `source`, optional spaces, `.`, a name, then the parentheses.
            let mut j = i + "source".len();
            let skip = |j: &mut usize| {
                while lexed.get(*j).is_some_and(|c| c.is_whitespace()) {
                    *j += 1;
                }
            };
            skip(&mut j);
            if lexed.get(j) != Some(&'.') {
                continue;
            }
            j += 1;
            skip(&mut j);
            let name_start = j;
            while lexed
                .get(j)
                .is_some_and(|c| c.is_alphanumeric() || *c == '_')
            {
                j += 1;
            }
            if j == name_start {
                continue;
            }
            let Some((start, end)) = self.parenthesized(j) else {
                continue;
            };
            let inner: String = self.chars[start..end].iter().collect();
            let text = inner.trim();
            if text.is_empty() || text == "*" {
                continue;
            }
            let leading = inner.len() - inner.trim_start().len();
            let offset = start + inner[..leading].chars().count();
            if let Err(e) = Expression::parse_at(text, self.location) {
                return Some(relocate(e, offset, self.text, self.location));
            }
        }
        None
    }

    /// The char range inside the parentheses that open at or after
    /// `from` (skipping whitespace), quotes respected.
    fn parenthesized(&self, from: usize) -> Option<(usize, usize)> {
        let mut i = from;
        while self.chars.get(i)?.is_whitespace() {
            i += 1;
        }
        if self.chars[i] != '(' {
            return None;
        }
        let start = i + 1;
        let (mut depth, mut quote) = (0_usize, None::<char>);
        for (j, &c) in self.chars.iter().enumerate().skip(start) {
            match quote {
                Some(q) if c == q => quote = None,
                Some(_) => {}
                None if c == '\'' || c == '"' => quote = Some(c),
                None if c == '(' => depth += 1,
                None if c == ')' && depth == 0 => return Some((start, j)),
                None if c == ')' => depth -= 1,
                None => {}
            }
        }
        None
    }
}

fn sql_starts_with(chars: &[char], word: &str) -> bool {
    let n = word.chars().count();
    chars.len() >= n
        && chars[..n]
            .iter()
            .collect::<String>()
            .eq_ignore_ascii_case(word)
        && !chars
            .get(n)
            .is_some_and(|c| c.is_alphanumeric() || *c == '_')
}
