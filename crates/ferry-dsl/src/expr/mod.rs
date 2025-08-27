//! The expression language (B2): DataFusion SQL expressions over one record,
//! plus a thin path syntax and the engine's sigils.
//!
//! # Syntax
//!
//! An expression is one SQL scalar expression as DataFusion parses it
//! (generic dialect): literals, arithmetic, comparisons, `AND`/`OR`/`NOT`,
//! `CASE`, `IN (...)`, `BETWEEN`, `LIKE`/`ILIKE`, `||`, `CAST(x AS t)` and
//! `x::t`, and calls to the functions in [`functions::names`]. `==` is
//! accepted as `=`. Subqueries, aggregates, window functions and any
//! function not on the list are errors.
//!
//! **Paths.** A bare name is a field of the current record (0003 §5.4 says
//! which record that is per target). `customer.id` reads a field of a
//! struct, `c.name` a field of the joined `one` entry aliased `c`, and
//! `items[*].sku` the `sku` of every element of an array, as an array.
//! Names are case-sensitive and never normalized; quote a name that isn't a
//! plain identifier (`"Order Id"`). Indexing (`items[0]`) isn't supported:
//! use `unnest:`.
//!
//! **Sigils** are the values the engine provides: `$parent` (the mapped
//! parent row), `$index` (0-based element position), `$item` (the current
//! element) and `$key` (the deterministic UUIDv5 child key), each valid only
//! where 0003 §5.4 says. Their bare words `parent`, `index`, `item` and
//! `key` are reserved at the start of a path, so a typo can't silently read
//! a field: write the sigil, or quote the word (`"index"`) to mean a field
//! with that name.
//!
//! # Null and missing
//!
//! A document source can omit a field or set it to null. Both read as null,
//! and ferry-core's `_ferry_missing` column records which paths were absent.
//!
//! - `x == null` is true when `x` is null **or missing**, and `x != null`
//!   when it is present and not null. They are rewritten to `IS NULL` and
//!   `IS NOT NULL`, so the SQL trap of `x = NULL` (always unknown, which
//!   drops every row in a filter) doesn't exist here.
//! - `missing(path)` is true only when the path was absent from the source
//!   record, so `x == null and not missing(x)` is an explicit null. A source
//!   that can't omit fields (Postgres) never has missing ones.
//! - Every other operator follows SQL: a comparison with a null or missing
//!   value is null, and a `where:` predicate that is null counts as false
//!   (0003 §5.6). `<`, `<=`, `>` and `>=` against the `null` literal are
//!   errors, since they can never be true.
//!
//! # Numbers and money
//!
//! Number literals with a fraction are decimals, not floats: `1.005` is
//! `decimal(4,3)`, and arithmetic on decimals stays decimal. `round(x)` and
//! `round(x, n)` round **half away from zero** on decimals (`1.005 * 100`
//! rounds to `101`, `-1.005 * 100` to `-101`), which is the explicit rounding
//! mode for money. On floats `round` rounds the float value, so money must
//! not go through `float()`.
//!
//! `int(x)`, `float(x)` and `string(x)` are strict conversions: `int` of
//! `"three"` or of `100.5` is an error (the record becomes an error row),
//! never a null or a truncation. `CAST` follows SQL and truncates.
//!
//! # Purity
//!
//! The same expression on the same input gives the same output, on any
//! machine, in any partitioning. Only the listed functions can be called and
//! each is `Immutable` (no clock, no randomness, no session state). The
//! planner's options are built in, never read from the environment, so the
//! process time zone or locale can't change a result: a timestamp's zone is
//! part of its type.

pub mod convert;
mod error;
pub mod functions;
mod resolve;
mod scope;

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, new_null_array};
use arrow_schema::SchemaRef;
use datafusion_common::DFSchema;
use datafusion_common::tree_node::TreeNode;
use datafusion_expr::ExprSchemable;
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::simplify::SimplifyContext;
use datafusion_optimizer::analyzer::type_coercion::TypeCoercionRewriter;
use datafusion_optimizer::simplify_expressions::ExprSimplifier;
use datafusion_physical_expr::{PhysicalExpr, create_physical_expr};
use datafusion_sql::planner::{ParserOptions, PlannerContext, SqlToRel};
use datafusion_sql::sqlparser::ast::Expr as SqlExpr;
use datafusion_sql::sqlparser::dialect::GenericDialect;
use datafusion_sql::sqlparser::parser::{Parser, ParserError};
use datafusion_sql::sqlparser::tokenizer::Token;
use ferry_core::types::CanonicalType;

pub use error::ExprError;
pub use scope::{INDEX_COLUMN, ITEM_COLUMN, JOIN_PREFIX, KEY_COLUMN, PARENT_COLUMN, Scope, Sigil};

use crate::config::{ErrorKind, Location, Spanned};
use resolve::{Resolver, STAR};

/// A parsed expression, not yet bound to a scope. Parsing checks everything
/// that doesn't need a schema: syntax, reserved names, sigil names, which
/// functions exist.
#[derive(Clone, Debug)]
pub struct Expression {
    text: String,
    location: Location,
    ast: SqlExpr,
    line_starts: Vec<usize>,
    stars: Vec<usize>,
}

impl Expression {
    /// Parses an expression from the config. Errors point at the file,
    /// line and column it was written at.
    pub fn parse(source: &Spanned<String>) -> Result<Self, ExprError> {
        Self::parse_at(source.value(), source.location())
    }

    pub fn parse_at(text: &str, location: &Location) -> Result<Self, ExprError> {
        let (sql, stars) = lex(text);
        let line_starts = line_starts(text);
        let fail = |message: String, span: Option<(usize, usize)>| {
            ExprError::new(ErrorKind::Expr, text, location, message).span(span)
        };
        let dialect = GenericDialect {};
        let mut parser = Parser::new(&dialect)
            .try_with_sql(&sql)
            .map_err(|e| syntax_error(e, text.chars().count(), &line_starts, &fail))?;
        let ast = parser
            .parse_expr()
            .map_err(|e| syntax_error(e, text.chars().count(), &line_starts, &fail))?;
        let next = parser.peek_token();
        if next.token != Token::EOF {
            let resolver = Resolver {
                text,
                location,
                line_starts: line_starts.clone(),
                stars: Vec::new(),
                scope: None,
            };
            return Err(fail(
                format!("unexpected `{}` after the expression", next.token),
                resolver.span_of(next.span),
            )
            .help("an expression is one value; join conditions with `and` / `or`"));
        }
        let expression = Self {
            text: text.to_owned(),
            location: location.clone(),
            ast,
            line_starts,
            stars,
        };
        // The checks that need no schema.
        expression
            .resolver(None)
            .expr(&mut expression.ast.clone())?;
        Ok(expression)
    }

    pub fn text(&self) -> &str {
        &self.text
    }

    pub fn location(&self) -> &Location {
        &self.location
    }

    fn resolver<'a>(&'a self, scope: Option<&'a Scope>) -> Resolver<'a> {
        Resolver {
            text: &self.text,
            location: &self.location,
            line_starts: self.line_starts.clone(),
            stars: self.stars.clone(),
            scope,
        }
    }

    /// Binds the expression to `scope`: resolves every path, plans it with
    /// DataFusion and infers its type.
    pub fn compile(&self, scope: &Scope) -> Result<Compiled, ExprError> {
        let mut ast = self.ast.clone();
        self.resolver(Some(scope)).expr(&mut ast)?;
        let type_error = |e: datafusion_common::DataFusionError| {
            ExprError::new(ErrorKind::Type, &self.text, &self.location, clean(e))
        };
        let input = scope.input_schema();
        let schema = DFSchema::try_from(input.as_ref().clone()).map_err(type_error)?;
        let functions = functions::Functions::new();
        let options = ParserOptions::new()
            .with_parse_float_as_decimal(true)
            .with_enable_ident_normalization(false)
            .with_map_string_types_to_utf8view(false);
        let planner = SqlToRel::new_with_options(&functions, options);
        let logical = planner
            .sql_to_expr(ast, &schema, &mut PlannerContext::new())
            .map_err(type_error)?;
        let logical = logical
            .rewrite(&mut TypeCoercionRewriter::new(&schema))
            .map_err(type_error)?
            .data;
        // Some functions (`coalesce`) only run in the form the simplifier
        // rewrites them to. It also folds constants, so a constant that
        // can't be computed (`int('three')`) fails here, at plan time.
        let schema = Arc::new(schema);
        let simplifier = ExprSimplifier::new(
            SimplifyContext::builder()
                .with_schema(Arc::clone(&schema))
                .build(),
        );
        let logical = simplifier.simplify(logical).map_err(type_error)?;
        let (_, field) = logical.to_field(&schema).map_err(type_error)?;
        let physical = create_physical_expr(
            &logical,
            &schema,
            &ExecutionProps::new(),
            &Default::default(),
        )
        .map_err(type_error)?;
        let output = crate::types::from_arrow(&field).map_err(|reason| {
            ExprError::new(
                ErrorKind::Type,
                &self.text,
                &self.location,
                format!("the result has no canonical type: {reason}"),
            )
        })?;
        Ok(Compiled {
            text: self.text.as_str().into(),
            location: self.location.clone(),
            physical,
            input,
            output,
            nullable: field.is_nullable(),
            target: None,
        })
    }
}

/// `[*]` isn't SQL. Replace its `*` with an identifier of the same length,
/// outside string literals and quoted names, and remember where.
fn lex(text: &str) -> (String, Vec<usize>) {
    let chars: Vec<char> = text.chars().collect();
    let mut out = String::with_capacity(text.len());
    let mut stars = Vec::new();
    let mut quote: Option<char> = None;
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        match quote {
            Some(q) if c == q => quote = None,
            Some(_) => {}
            None if c == '\'' || c == '"' => quote = Some(c),
            None if c == '['
                && chars.get(i + 1) == Some(&'*')
                && chars.get(i + 2) == Some(&']') =>
            {
                out.push('[');
                out.push_str(STAR);
                out.push(']');
                stars.push(i + 1);
                i += 3;
                continue;
            }
            None => {}
        }
        out.push(c);
        i += 1;
    }
    (out, stars)
}

fn line_starts(text: &str) -> Vec<usize> {
    std::iter::once(0)
        .chain(
            text.chars()
                .enumerate()
                .filter(|(_, c)| *c == '\n')
                .map(|(i, _)| i + 1),
        )
        .collect()
}

/// sqlparser reports `... at Line: 1, Column: 7`; turn that into a span.
fn syntax_error(
    error: ParserError,
    text_len: usize,
    line_starts: &[usize],
    fail: &dyn Fn(String, Option<(usize, usize)>) -> ExprError,
) -> ExprError {
    let text = match &error {
        ParserError::TokenizerError(m) | ParserError::ParserError(m) => m.clone(),
        ParserError::RecursionLimitExceeded => "the expression is nested too deeply".into(),
    };
    let (message, position) = match text.rfind(" at Line: ") {
        Some(at) => (
            text[..at].to_owned(),
            Some(&text[at + " at Line: ".len()..]),
        ),
        None => (text, None),
    };
    let span = position.and_then(|p| {
        let (line, column) = p.split_once(", Column: ")?;
        let line: usize = line.trim().parse().ok()?;
        let column: usize = column.trim().parse().ok()?;
        let offset = line_starts.get(line.checked_sub(1)?)? + column.checked_sub(1)?;
        Some((offset, offset + 1))
    });
    // "found: EOF" comes without a position: it is the end of the text.
    let span = span.or_else(|| {
        message
            .ends_with("found: EOF")
            .then(|| (text_len, text_len + 1))
    });
    let message = message
        .replace("Expected: ", "expected ")
        .replace(", found: EOF", ", found the end of the expression")
        .replace(", found: ", ", found ");
    fail(format!("syntax error: {message}"), span)
}

/// A DataFusion error without its "Error during planning:" wrapping.
fn clean(error: datafusion_common::DataFusionError) -> String {
    let text = error.strip_backtrace();
    let mut text = text.as_str();
    for prefix in [
        "Error during planning: ",
        "Schema error: ",
        "This feature is not implemented: ",
        "Internal error: ",
    ] {
        text = text.strip_prefix(prefix).unwrap_or(text);
    }
    text.lines().next().unwrap_or(text).to_owned()
}

/// An expression bound to a scope, ready to evaluate on batches laid out as
/// [`Scope::input_schema`].
#[derive(Clone, Debug)]
pub struct Compiled {
    text: Arc<str>,
    location: Location,
    physical: Arc<dyn PhysicalExpr>,
    input: SchemaRef,
    output: Option<CanonicalType>,
    nullable: bool,
    target: Option<CanonicalType>,
}

/// Why an expression failed on some input.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EvalError(pub String);

impl std::fmt::Display for EvalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for EvalError {}

/// A row of a batch the expression failed on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RowError {
    pub row: usize,
    pub message: String,
}

impl Compiled {
    pub fn text(&self) -> &str {
        &self.text
    }

    pub fn location(&self) -> &Location {
        &self.location
    }

    /// The inferred type; `None` for a bare `null`.
    pub fn output_type(&self) -> Option<&CanonicalType> {
        self.output.as_ref()
    }

    pub fn nullable(&self) -> bool {
        self.nullable
    }

    /// The type results are converted to: the target, or else the inferred
    /// type.
    pub fn result_type(&self) -> Option<&CanonicalType> {
        self.target.as_ref().or(self.output.as_ref())
    }

    pub fn input_schema(&self) -> &SchemaRef {
        &self.input
    }

    /// Converts every result to `ty` (a column's `type:`). Whether that
    /// conversion is allowed is the type checker's call.
    pub fn converting_to(mut self, ty: CanonicalType) -> Self {
        self.target = Some(ty);
        self
    }

    /// Evaluates on a whole batch. The first value that fails fails the
    /// batch; see [`Compiled::evaluate_rows`] to isolate it.
    pub fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef, EvalError> {
        let schema = batch.schema();
        let matches = schema.fields().len() == self.input.fields().len()
            && schema
                .fields()
                .iter()
                .zip(self.input.fields())
                .all(|(a, b)| a.name() == b.name() && a.data_type() == b.data_type());
        if !matches {
            return Err(EvalError(format!(
                "`{}`: the batch doesn't have the scope's columns (expected {:?})",
                self.text,
                self.input
                    .fields()
                    .iter()
                    .map(|f| f.name().as_str())
                    .collect::<Vec<_>>()
            )));
        }
        let values = self
            .physical
            .evaluate(batch)
            .and_then(|v| v.into_array(batch.num_rows()))
            .map_err(|e| EvalError(clean(e)))?;
        match self.result_type() {
            Some(ty) => convert::to_canonical(&values, ty).map_err(|e| EvalError(e.to_string())),
            None => Ok(values),
        }
    }

    /// Evaluates row by row where the batch as a whole fails, so one bad
    /// value fails its own row only. Failed rows are null in the result.
    pub fn evaluate_rows(&self, batch: &RecordBatch) -> (ArrayRef, Vec<RowError>) {
        if let Ok(values) = self.evaluate(batch) {
            return (values, Vec::new());
        }
        let data_type = self
            .result_type()
            .map_or(arrow_schema::DataType::Null, CanonicalType::to_arrow);
        let mut errors = Vec::new();
        let mut parts: Vec<ArrayRef> = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            match self.evaluate(&batch.slice(row, 1)) {
                Ok(value) => parts.push(value),
                Err(e) => {
                    errors.push(RowError { row, message: e.0 });
                    parts.push(new_null_array(&data_type, 1));
                }
            }
        }
        let refs: Vec<&dyn arrow_array::Array> = parts.iter().map(|a| a.as_ref()).collect();
        let values =
            arrow_select::concat::concat(&refs).expect("every row's value has the result type");
        (values, errors)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lex_replaces_only_unquoted_stars() {
        let (sql, stars) = lex("items[*].sku || '[*]' || \"a[*]\"");
        assert_eq!(sql, "items[_].sku || '[*]' || \"a[*]\"");
        assert_eq!(stars, [6]);
    }
}
