//! The dataferry layer over SQL: one walk over the parsed expression that
//!
//! - rejects SQL that isn't part of the language (subqueries, aggregates,
//!   window functions, unknown functions),
//! - rejects reserved names written without their sigil,
//! - rewrites `x == null` / `x != null` to `IS [NOT] NULL` and
//!   `missing(path)` to a lookup in `_ferry_missing`,
//! - and, given a [`Scope`], resolves every path (`customer.id`,
//!   `c.name`, `$parent.id`, `items[*].sku`) to the batch column it reads.
//!
//! Without a scope only the first three happen, so a config can be checked
//! before any schema is known.

use arrow_schema::DataType;
use datafusion_sql::sqlparser::ast::{
    AccessExpr, BinaryOperator, CastKind, Expr, Function, FunctionArg, FunctionArgExpr,
    FunctionArgumentList, FunctionArguments, Ident, ObjectName, Spanned as _, Subscript, Value,
};
use datafusion_sql::sqlparser::tokenizer::Span;

use super::error::ExprError;
use super::functions::{self, ARRAY_HAS, GET_FIELD, LIST_FIELD};
use super::scope::{JOIN_PREFIX, Scope, Sigil};
use crate::config::{ErrorKind, Location, suggest};

/// Stands in for `*` in `[*]`, which SQL can't parse. Same length, so
/// spans don't move; only positions the lexer replaced count.
pub(crate) const STAR: &str = "_";

pub(crate) struct Resolver<'a> {
    pub(crate) text: &'a str,
    pub(crate) location: &'a Location,
    /// Char offset where each line of `text` starts.
    pub(crate) line_starts: Vec<usize>,
    /// Char offsets of the `*`s of `[*]`.
    pub(crate) stars: Vec<usize>,
    pub(crate) scope: Option<&'a Scope>,
}

/// One step of a path after its head.
enum Step {
    Field(Ident),
    All(Span),
}

/// What a path starts with.
enum Head {
    Name(Ident),
    Sigil(Sigil, Span),
    Placeholder(String, Span),
}

impl<'a> Resolver<'a> {
    fn offset(&self, location: datafusion_sql::sqlparser::tokenizer::Location) -> Option<usize> {
        let line = usize::try_from(location.line).ok()?.checked_sub(1)?;
        let column = usize::try_from(location.column).ok()?.checked_sub(1)?;
        Some(self.line_starts.get(line)? + column)
    }

    pub(crate) fn span_of(&self, span: Span) -> Option<(usize, usize)> {
        if span == Span::empty() {
            return None;
        }
        Some((self.offset(span.start)?, self.offset(span.end)?))
    }

    fn error(&self, span: Span, message: String) -> ExprError {
        ExprError::new(ErrorKind::Expr, self.text, self.location, message).span(self.span_of(span))
    }

    fn unsupported(&self, e: &Expr, what: &str) -> ExprError {
        self.error(
            e.span(),
            format!("{what} can't be used in a dataferry expression"),
        )
    }

    pub(crate) fn expr(&self, e: &mut Expr) -> Result<(), ExprError> {
        if let Some(path) = self.path(e)? {
            *e = self.resolve(path)?;
            return Ok(());
        }
        match e {
            Expr::Value(_) | Expr::TypedString(_) => Ok(()),
            Expr::BinaryOp { left, op, right } => {
                if let Some(rewritten) = self.null_comparison(left, op, right)? {
                    *e = rewritten;
                    return self.expr(e);
                }
                self.expr(left)?;
                self.expr(right)
            }
            Expr::UnaryOp { expr, .. }
            | Expr::Nested(expr)
            | Expr::IsNull(expr)
            | Expr::IsNotNull(expr)
            | Expr::IsTrue(expr)
            | Expr::IsNotTrue(expr)
            | Expr::IsFalse(expr)
            | Expr::IsNotFalse(expr)
            | Expr::IsUnknown(expr)
            | Expr::IsNotUnknown(expr)
            | Expr::Ceil { expr, .. }
            | Expr::Floor { expr, .. } => self.expr(expr),
            Expr::IsDistinctFrom(a, b) | Expr::IsNotDistinctFrom(a, b) => {
                self.expr(a)?;
                self.expr(b)
            }
            Expr::Position { expr, r#in } => {
                self.expr(expr)?;
                self.expr(r#in)
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                self.expr(expr)?;
                self.expr(low)?;
                self.expr(high)
            }
            Expr::InList { expr, list, .. } => {
                self.expr(expr)?;
                list.iter_mut().try_for_each(|e| self.expr(e))
            }
            // The escape character (`ESCAPE '!'`) is a literal value, never
            // a path, so only the operands resolve.
            Expr::Like {
                expr,
                pattern,
                any: false,
                ..
            }
            | Expr::ILike {
                expr,
                pattern,
                any: false,
                ..
            } => {
                self.expr(expr)?;
                self.expr(pattern)
            }
            Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                operand.iter_mut().try_for_each(|e| self.expr(e))?;
                for when in conditions {
                    self.expr(&mut when.condition)?;
                    self.expr(&mut when.result)?;
                }
                else_result.iter_mut().try_for_each(|e| self.expr(e))
            }
            Expr::Cast {
                kind: CastKind::Cast | CastKind::DoubleColon,
                expr,
                format: None,
                ..
            } => self.expr(expr),
            Expr::Substring {
                expr,
                substring_from,
                substring_for,
                ..
            } => {
                self.expr(expr)?;
                substring_from.iter_mut().try_for_each(|e| self.expr(e))?;
                substring_for.iter_mut().try_for_each(|e| self.expr(e))
            }
            Expr::Trim {
                expr,
                trim_what,
                trim_characters,
                ..
            } => {
                self.expr(expr)?;
                trim_what.iter_mut().try_for_each(|e| self.expr(e))?;
                trim_characters
                    .iter_mut()
                    .flatten()
                    .try_for_each(|e| self.expr(e))
            }
            Expr::Function(function) => {
                if let Some(rewritten) = self.missing(function)? {
                    *e = rewritten;
                    return Ok(());
                }
                self.function(function)
            }
            Expr::Subquery(_) | Expr::Exists { .. } | Expr::InSubquery { .. } => {
                Err(self.unsupported(e, "a subquery").help(
                    "an expression sees one record; use a merge (`from:` list) to read another entity",
                ))
            }
            Expr::Wildcard(_) | Expr::QualifiedWildcard(..) => {
                Err(self.unsupported(e, "`*`").help("a path element list is written `items[*]`"))
            }
            Expr::CompoundFieldAccess { .. } => {
                Err(self.unsupported(e, "this field access").help(
                    "paths start with a field, an alias or a sigil: `customer.id`, `c.name`, `$parent.id`",
                ))
            }
            Expr::Cast { .. } => Err(self.unsupported(e, "this cast form")),
            _ => Err(self.unsupported(e, "this SQL syntax")),
        }
    }

    /// `x == null` and `x != null` mean "is (not) null or missing", never
    /// SQL's always-unknown `= NULL`.
    fn null_comparison(
        &self,
        left: &Expr,
        op: &BinaryOperator,
        right: &Expr,
    ) -> Result<Option<Expr>, ExprError> {
        let is_null = |e: &Expr| matches!(e, Expr::Value(v) if v.value == Value::Null);
        let other = match (is_null(left), is_null(right)) {
            (false, false) => return Ok(None),
            (true, _) => right,
            (false, true) => left,
        };
        match op {
            BinaryOperator::Eq => Ok(Some(Expr::IsNull(Box::new(other.clone())))),
            BinaryOperator::NotEq => Ok(Some(Expr::IsNotNull(Box::new(other.clone())))),
            BinaryOperator::Lt
            | BinaryOperator::LtEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq => Err(self
                .error(
                    other.span(),
                    format!("comparing with null using `{op}` is never true"),
                )
                .help("test for null with `== null` or `!= null`")),
            // Arithmetic and string operators with null are just null.
            _ => Ok(None),
        }
    }

    /// `missing(path)`: true when the path was absent from the source
    /// record (not when it was an explicit null). Reads `_ferry_missing`.
    fn missing(&self, function: &mut Function) -> Result<Option<Expr>, ExprError> {
        if function_name(function).as_deref() != Some("missing") {
            return Ok(None);
        }
        let span = function.name.span();
        let args = plain_args(function)
            .ok_or_else(|| self.error(span, "missing() takes one path".into()))?;
        let [arg] = args.as_slice() else {
            return Err(self.error(span, "missing() takes one path".into()));
        };
        let Some(path) = self.path(arg)? else {
            return Err(self
                .error(
                    arg.span(),
                    "missing() takes a path, not an expression".into(),
                )
                .help("e.g. `missing(customer.email)`"));
        };
        let (head, steps) = match path {
            (Head::Name(head), steps) => (head, steps),
            (_, _) => {
                return Err(self.error(
                    arg.span(),
                    "missing() takes a path into the current record, not a sigil".into(),
                ));
            }
        };
        let mut dotted = vec![head.value.clone()];
        for step in &steps {
            match step {
                Step::Field(ident) => dotted.push(ident.value.clone()),
                Step::All(span) => {
                    return Err(self.error(*span, "missing() can't take `[*]`".into()));
                }
            }
        }
        let Some(scope) = self.scope else {
            return Ok(Some(Expr::value(Value::Boolean(false))));
        };
        let is_alias = scope.aliases().any(|a| a == head.value) && !steps.is_empty();
        if is_alias {
            return Err(self.error(
                arg.span(),
                "missing() reads the current record's fields only, not a joined entry's".into(),
            ));
        }
        // Check the path exists, so a typo isn't silently "missing".
        self.resolve((Head::Name(head), steps))?;
        Ok(Some(match scope.missing_column() {
            Some(column) => call(
                ARRAY_HAS,
                vec![column_ref(column), string(&dotted.join("."))],
            ),
            // A source that can't omit fields never has missing ones.
            None => Expr::value(Value::Boolean(false)),
        }))
    }

    fn function(&self, function: &mut Function) -> Result<(), ExprError> {
        let span = function.name.span();
        let Some(name) = function_name(function) else {
            return Err(self.error(span, format!("`{}` is not a function", function.name)));
        };
        if !functions::is_user_function(&name) {
            let help = suggest(&name, functions::names())
                .map(|s| format!("did you mean `{s}`?"))
                .unwrap_or_else(|| {
                    "see the function list in the dataferry-dsl `expr` docs".to_owned()
                });
            let why = match name.as_str() {
                "now" | "current_date" | "current_time" | "current_timestamp" | "random"
                | "uuid" => " (expressions are pure: no clock, no randomness)",
                "count" | "sum" | "avg" | "min" | "max" => {
                    " (aggregates belong in `expect:`, not in a row expression)"
                }
                _ => "",
            };
            return Err(self
                .error(span, format!("unknown function `{name}`{why}"))
                .help(help));
        }
        // Keep the name as the planner looks it up.
        function.name = ObjectName::from(vec![Ident::new(&name)]);
        if function.over.is_some()
            || function.filter.is_some()
            || !function.within_group.is_empty()
            || function.null_treatment.is_some()
            || !matches!(function.parameters, FunctionArguments::None)
        {
            return Err(self.error(
                span,
                format!("`{name}` can't take OVER, FILTER or WITHIN GROUP"),
            ));
        }
        match &mut function.args {
            FunctionArguments::None => Ok(()),
            FunctionArguments::Subquery(_) => {
                Err(self.error(span, "a subquery can't be a function argument".into()))
            }
            FunctionArguments::List(list) => {
                if list.duplicate_treatment.is_some() || !list.clauses.is_empty() {
                    return Err(self.error(span, format!("`{name}` takes plain arguments")));
                }
                for arg in &mut list.args {
                    match arg {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => self.expr(e)?,
                        _ => {
                            return Err(self.error(
                                span,
                                format!("`{name}` takes plain arguments, not named or `*`"),
                            ));
                        }
                    }
                }
                Ok(())
            }
        }
    }

    /// The expression as a path, if it is one.
    fn path(&self, e: &Expr) -> Result<Option<(Head, Vec<Step>)>, ExprError> {
        let (head, mut steps) = match e {
            Expr::Identifier(ident) => (Head::Name(ident.clone()), Vec::new()),
            Expr::CompoundIdentifier(idents) => (
                Head::Name(idents[0].clone()),
                idents[1..].iter().cloned().map(Step::Field).collect(),
            ),
            Expr::Value(v) => match &v.value {
                Value::Placeholder(p) => (self.sigil(p, v.span), Vec::new()),
                _ => return Ok(None),
            },
            Expr::CompoundFieldAccess { root, access_chain } => {
                let Some((head, mut steps)) = self.path(root)? else {
                    return Ok(None);
                };
                for access in access_chain {
                    match access {
                        AccessExpr::Dot(Expr::Identifier(ident)) => {
                            steps.push(Step::Field(ident.clone()))
                        }
                        AccessExpr::Dot(Expr::CompoundIdentifier(idents)) => {
                            steps.extend(idents.iter().cloned().map(Step::Field))
                        }
                        AccessExpr::Subscript(Subscript::Index {
                            index: Expr::Identifier(ident),
                        }) if self.is_star(ident) => steps.push(Step::All(ident.span)),
                        AccessExpr::Subscript(subscript) => {
                            let span = match subscript {
                                Subscript::Index { index } => index.span(),
                                Subscript::Slice { .. } => e.span(),
                            };
                            return Err(self
                                .error(span, "indexing into an array isn't supported".into())
                                .help(
                                    "`items[*].sku` takes a field of every element; `unnest:` gives one row per element",
                                ));
                        }
                        AccessExpr::Dot(other) => {
                            return Err(
                                self.error(other.span(), "after `.` comes a field name".into())
                            );
                        }
                    }
                }
                return Ok(Some((head, steps)));
            }
            _ => return Ok(None),
        };
        steps.shrink_to_fit();
        Ok(Some((head, steps)))
    }

    fn is_star(&self, ident: &Ident) -> bool {
        ident.value == STAR
            && ident.quote_style.is_none()
            && self
                .span_of(ident.span)
                .is_some_and(|(start, _)| self.stars.contains(&start))
    }

    fn sigil(&self, placeholder: &str, span: Span) -> Head {
        match placeholder.strip_prefix('$').and_then(Sigil::from_word) {
            Some(sigil) => Head::Sigil(sigil, span),
            None => Head::Placeholder(placeholder.to_owned(), span),
        }
    }

    /// A path as the expression that reads it: a column, then one
    /// `get_field` per step (`list_field` after `[*]`).
    fn resolve(&self, (head, steps): (Head, Vec<Step>)) -> Result<Expr, ExprError> {
        let sigils = || {
            Sigil::ALL
                .iter()
                .map(|s| format!("${}", s.word()))
                .collect::<Vec<_>>()
                .join(", ")
        };
        let (head_ident, mut steps) = match head {
            Head::Placeholder(p, span) => {
                return Err(self
                    .error(span, format!("`{p}` is not a sigil"))
                    .help(format!("the sigils are {}", sigils())));
            }
            Head::Sigil(sigil, span) => {
                let Some(scope) = self.scope else {
                    return Ok(Expr::value(Value::Null));
                };
                let Some(field) = scope.sigil_field(sigil) else {
                    return Err(self.error(
                        span,
                        format!(
                            "`${}` is only available in {}",
                            sigil.word(),
                            sigil.where_valid()
                        ),
                    ));
                };
                return self.steps(
                    column_ref(field.name()),
                    field.data_type().clone(),
                    &format!("${}", sigil.word()),
                    steps,
                );
            }
            Head::Name(ident) => (ident, steps),
        };
        let name = head_ident.value.as_str();
        if head_ident.quote_style.is_none()
            && let Some(sigil) = Sigil::from_word(name)
        {
            return Err(self
                .error(head_ident.span, format!("`{name}` is a reserved name"))
                .help(format!(
                    "write `${}` for the engine's value, or `\"{name}\"` for a field named {name}",
                    sigil.word()
                )));
        }
        if name.starts_with("_ferry") || name.starts_with("__ferry") {
            return Err(self
                .error(
                    head_ident.span,
                    format!("`{name}`: names starting with `_ferry` are reserved for the engine"),
                )
                .help("to test whether a field was absent, use `missing(<path>)`"));
        }
        if name == STAR && self.is_star(&head_ident) {
            return Err(self.error(head_ident.span, "`[*]` follows an array field".into()));
        }
        let Some(scope) = self.scope else {
            return Ok(Expr::value(Value::Null));
        };

        if scope.driver_alias() == Some(name) && !steps.is_empty() {
            // `o.status` is `status`.
            match steps.remove(0) {
                Step::Field(field) => {
                    return self.resolve((Head::Name(quoted(field)), steps));
                }
                Step::All(span) => {
                    return Err(self.error(span, format!("`{name}` is an alias, not an array")));
                }
            }
        }
        if let Some(fields) = scope.join(name)
            && !steps.is_empty()
        {
            if scope.fields().iter().any(|f| f.name == name) {
                return Err(self
                    .error(
                        head_ident.span,
                        format!("`{name}` is both a joined alias and a field of the record"),
                    )
                    .help("rename the alias with `as:`"));
            }
            let ty = DataType::Struct(fields.iter().map(|f| f.to_arrow()).collect());
            return self.steps(column_ref(&format!("{JOIN_PREFIX}{name}")), ty, name, steps);
        }
        if let Some(field) = scope.fields().iter().find(|f| f.name == name) {
            return self.steps(column_ref(name), field.ty.to_arrow(), name, steps);
        }
        if scope.is_many(name) {
            return Err(self
                .error(
                    head_ident.span,
                    format!("`{name}` is a `many` join: one record has many rows of it"),
                )
                .help("use it as `nest:` or `unnest:` (0003 §5.2)"));
        }
        if scope.aliases().any(|a| a == name) {
            return Err(self
                .error(
                    head_ident.span,
                    format!("`{name}` is an alias, not a value"),
                )
                .help(format!("name one of its fields: `{name}.<field>`")));
        }
        let candidates = scope
            .fields()
            .iter()
            .map(|f| f.name.as_str())
            .chain(scope.aliases());
        let help = match suggest(name, candidates) {
            Some(s) => format!("did you mean `{s}`?"),
            None => format!(
                "{} has: {}",
                scope.describe(),
                field_list(scope.fields().iter().map(|f| f.name.as_str()))
            ),
        };
        Err(self
            .error(
                head_ident.span,
                format!("no field `{name}` in {}", scope.describe()),
            )
            .help(help))
    }

    /// Applies the steps after a path's head, checking each against the
    /// type it lands on.
    fn steps(
        &self,
        mut expr: Expr,
        mut ty: DataType,
        head: &str,
        steps: Vec<Step>,
    ) -> Result<Expr, ExprError> {
        let mut shown = head.to_owned();
        let mut in_list = false;
        for step in steps {
            match step {
                Step::All(span) => {
                    if in_list {
                        return Err(self.error(span, "a path can have only one `[*]`".into()));
                    }
                    if !matches!(ty, DataType::List(_)) {
                        return Err(self.error(
                            span,
                            format!("`{shown}` is {}, not an array", type_name(&ty)),
                        ));
                    }
                    in_list = true;
                    shown.push_str("[*]");
                }
                Step::Field(ident) => {
                    let fields = match (&ty, in_list) {
                        (DataType::Struct(fields), false) => fields.clone(),
                        (DataType::List(item), true) => match item.data_type() {
                            DataType::Struct(fields) => fields.clone(),
                            other => {
                                return Err(self.error(
                                    ident.span,
                                    format!(
                                        "the elements of `{shown}` are {}, they have no fields",
                                        type_name(other)
                                    ),
                                ));
                            }
                        },
                        (DataType::List(_), false) => {
                            return Err(self
                                .error(ident.span, format!("`{shown}` is an array"))
                                .help(format!(
                                    "`{shown}[*].{}` takes the field of every element",
                                    ident.value
                                )));
                        }
                        (other, _) => {
                            return Err(self.error(
                                ident.span,
                                format!("`{shown}` is {}, it has no fields", type_name(other)),
                            ));
                        }
                    };
                    let Some((_, field)) = fields.find(&ident.value) else {
                        let help = suggest(&ident.value, fields.iter().map(|f| f.name().as_str()))
                            .map(|s| format!("did you mean `{shown}.{s}`?"))
                            .unwrap_or_else(|| {
                                format!(
                                    "`{shown}` has: {}",
                                    field_list(fields.iter().map(|f| f.name().as_str()))
                                )
                            });
                        return Err(self
                            .error(
                                ident.span,
                                format!("`{shown}` has no field `{}`", ident.value),
                            )
                            .help(help));
                    };
                    if in_list {
                        expr = call(LIST_FIELD, vec![expr, string(&ident.value)]);
                        ty = DataType::List(std::sync::Arc::new(
                            arrow_schema::Field::new_list_field(field.data_type().clone(), true),
                        ));
                    } else {
                        expr = call(GET_FIELD, vec![expr, string(&ident.value)]);
                        ty = field.data_type().clone();
                    }
                    shown.push('.');
                    shown.push_str(&ident.value);
                }
            }
        }
        Ok(expr)
    }
}

fn quoted(mut ident: Ident) -> Ident {
    // The first segment after an alias is a field name, never a sigil word.
    ident.quote_style = Some('"');
    ident
}

fn field_list<'a>(names: impl Iterator<Item = &'a str>) -> String {
    let names: Vec<String> = names.map(|n| format!("`{n}`")).collect();
    if names.is_empty() {
        "no fields".into()
    } else {
        names.join(", ")
    }
}

/// How a type reads in messages: the canonical name when there is one.
pub(crate) fn type_name(ty: &DataType) -> String {
    let field = arrow_schema::Field::new("x", ty.clone(), true);
    match crate::types::from_arrow(&field) {
        Ok(Some(t)) => crate::types::name(&t),
        Ok(None) => "null".into(),
        Err(_) => ty.to_string(),
    }
}

fn function_name(function: &Function) -> Option<String> {
    match function.name.0.as_slice() {
        [part] => part.as_ident().map(|i| i.value.to_lowercase()),
        _ => None,
    }
}

fn plain_args(function: &Function) -> Option<Vec<Expr>> {
    match &function.args {
        FunctionArguments::List(list) => list
            .args
            .iter()
            .map(|a| match a {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(e.clone()),
                _ => None,
            })
            .collect(),
        _ => None,
    }
}

fn column_ref(name: &str) -> Expr {
    Expr::Identifier(Ident::with_quote('"', name))
}

fn string(value: &str) -> Expr {
    Expr::value(Value::SingleQuotedString(value.to_owned()))
}

fn call(name: &str, args: Vec<Expr>) -> Expr {
    Expr::Function(Function {
        name: ObjectName::from(vec![Ident::new(name)]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: args
                .into_iter()
                .map(|e| FunctionArg::Unnamed(FunctionArgExpr::Expr(e)))
                .collect(),
            clauses: Vec::new(),
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: Vec::new(),
    })
}
