//! The type checker (B3): every expression of every flow, compiled against
//! the discovered source schemas, and every column's type settled.
//!
//! It runs at `plan` time, once sources have been discovered: `validate`
//! doesn't connect, so it only parses expressions. For each flow it builds
//! the scopes 0003 §5.4 describes (driver record, unnested element, joined
//! rows, `$parent`), compiles each expression (B2), and checks:
//!
//! - every path exists, with the nearest name suggested when it doesn't;
//! - `where:` predicates are booleans;
//! - a column with `type:` converts to it safely, or lossily with
//!   `allow_lossy: true` ([`cast`] has the table), and never otherwise;
//! - `unnest:` reaches an array, `nest:` a `many` join, join keys compare
//!   like with like, and `key:` names columns of the target;
//! - every `expect:` assertion names tables and columns of the targets and
//!   aggregates values it can aggregate ([`crate::expect`], B8).
//!
//! Errors carry the flow, target and column in their field path, the
//! expression, the inferred and the expected type, and the original
//! file:line.
//!
//! **Schemaless sources.** For Mongo the checked schema is the one inferred
//! from a sample at discovery. A document that disagrees with it at run time
//! (a string where an int was inferred) isn't caught here: it becomes an
//! error row and goes to `_ferry_dlq` (or `_ferry_extra` for unknown
//! fields), per `on_error`.

pub mod cast;

use std::collections::BTreeMap;

use dataferry_core::entity::Entity;
use dataferry_core::types::{CanonicalField, CanonicalType};

use crate::config::model::{
    Cardinality, ColumnSpec, Columns, EntityRef, From, NestColumn, OrderedMap, Target, Where,
};
use crate::config::{
    Config, ConfigError, ConfigErrors, ErrorKind, FieldPath, Flow, Location, SourceFile, Spanned,
    suggest,
};
use crate::expect::FlowExpect;
use crate::expr::{Compiled, ExprError, Expression, Scope};
use crate::types;
use cast::Conversion;

/// The discovered entities, by `<connection>.<entity>`.
#[derive(Clone, Debug, Default)]
pub struct Catalog {
    entities: BTreeMap<String, Entity>,
}

impl Catalog {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, connection: &str, entity: Entity) {
        self.entities
            .insert(format!("{connection}.{}", entity.name()), entity);
    }

    pub fn with(mut self, connection: &str, entity: Entity) -> Self {
        self.insert(connection, entity);
        self
    }

    pub fn get(&self, reference: &EntityRef) -> Option<&Entity> {
        self.entities.get(&reference.to_string())
    }

    fn names_on<'a>(&'a self, connection: &'a str) -> impl Iterator<Item = &'a str> {
        self.entities
            .values()
            .zip(self.entities.keys())
            .filter(move |(_, key)| key.split_once('.').map(|(c, _)| c) == Some(connection))
            .map(|(e, _)| e.name())
    }
}

/// A flow whose expressions all compiled.
#[derive(Clone, Debug)]
pub struct CheckedFlow {
    pub name: String,
    /// The flow `where:`, in driver scope.
    pub filter: Vec<Predicate>,
    /// In write order: parents before children.
    pub targets: Vec<CheckedTarget>,
    /// The flow's `expect:` assertions, compiled for the verifier.
    pub expect: FlowExpect,
}

#[derive(Clone, Debug)]
pub struct Predicate {
    /// The predicate's name, or `None` for an unnamed `where:`.
    pub name: Option<String>,
    pub expr: Compiled,
}

#[derive(Clone, Debug)]
pub struct CheckedTarget {
    pub table: String,
    pub parent: Option<String>,
    /// `unnest: <path>`, compiled in the scope its rows come from: the
    /// parent's (0003 §5.7), or the driver record's without `parent:`. Its
    /// type is an array, or `json` for a variant that only holds arrays in
    /// some records.
    pub unnest: Option<Compiled>,
    /// What the target's expressions see: its rows are evaluated on batches
    /// laid out as [`Scope::input_schema`].
    pub scope: Scope,
    pub filter: Vec<Predicate>,
    pub columns: Vec<CheckedColumn>,
}

impl CheckedTarget {
    pub fn column(&self, name: &str) -> Option<&CheckedColumn> {
        self.columns.iter().find(|c| c.field.name == name)
    }
}

#[derive(Clone, Debug)]
pub struct CheckedColumn {
    /// The target column, with its settled type.
    pub field: CanonicalField,
    pub value: ColumnValue,
}

#[derive(Clone, Debug)]
pub enum ColumnValue {
    /// `columns: "*"`: the source field of the same name, unchanged.
    Copy,
    /// An expression, converted to the column's type.
    Expr(Compiled),
    /// An array built from a `many` join.
    Nest {
        alias: String,
        order_by: Vec<(Compiled, bool)>,
        filter: Vec<Predicate>,
        columns: Vec<CheckedColumn>,
    },
}

/// Type-checks every flow of `config` against `catalog`.
pub fn check(config: &Config, catalog: &Catalog) -> Result<Vec<CheckedFlow>, ConfigErrors> {
    let mut flows = Vec::new();
    let mut errors = Vec::new();
    for flow in &config.flows {
        match check_flow(flow, catalog, config.sources()) {
            Ok(checked) => flows.push(checked),
            Err(e) => errors.extend(e),
        }
    }
    if errors.is_empty() {
        Ok(flows)
    } else {
        Err(ConfigErrors(errors))
    }
}

/// Type-checks one flow. `sources` (the config's files) let errors point at
/// the exact column of a name inside an expression.
pub fn check_flow(
    flow: &Flow,
    catalog: &Catalog,
    sources: &[SourceFile],
) -> Result<CheckedFlow, Vec<ConfigError>> {
    let mut checker = Checker {
        flow,
        catalog,
        sources,
        path: FieldPath::root()
            .key("flows")
            .item(0, Some(flow.name.as_str())),
        errors: Vec::new(),
    };
    let checked = checker.flow();
    match checked {
        Some(checked) if checker.errors.is_empty() => Ok(checked),
        _ => Err(checker.errors),
    }
}

struct Checker<'a> {
    flow: &'a Flow,
    catalog: &'a Catalog,
    sources: &'a [SourceFile],
    path: FieldPath,
    errors: Vec<ConfigError>,
}

/// The entities of a flow's `from:`, resolved.
struct Inputs<'a> {
    driver: &'a Entity,
    scope: Scope,
    /// The driver record alone, as its source returns it: what
    /// source-side aggregates see (0003 §5.9).
    record: Scope,
    /// `many` aliases and their entities.
    many: Vec<(String, &'a Entity)>,
}

impl<'a> Checker<'a> {
    fn expr_error(&mut self, error: ExprError, path: &FieldPath) {
        self.errors.push(error.to_config_error(path, self.sources));
    }

    fn type_error(
        &mut self,
        location: &Location,
        path: &FieldPath,
        message: String,
    ) -> &mut ConfigError {
        self.errors.push(
            ConfigError::new(ErrorKind::Type, message)
                .at(location)
                .path(path),
        );
        self.errors.last_mut().expect("just pushed")
    }

    fn compile(
        &mut self,
        text: &str,
        at: &Location,
        scope: &Scope,
        path: &FieldPath,
    ) -> Option<Compiled> {
        match Expression::parse_at(text, at).and_then(|e| e.compile(scope)) {
            Ok(compiled) => Some(compiled),
            Err(e) => {
                self.expr_error(e, path);
                None
            }
        }
    }

    fn entity(
        &mut self,
        reference: &EntityRef,
        at: &Location,
        path: &FieldPath,
    ) -> Option<&'a Entity> {
        if let Some(entity) = self.catalog.get(reference) {
            return Some(entity);
        }
        let connection = reference.connection.as_str();
        let help = suggest(&reference.entity, self.catalog.names_on(connection))
            .map(|s| format!("did you mean `{connection}.{s}`?"))
            .unwrap_or_else(|| {
                let names: Vec<&str> = self.catalog.names_on(connection).collect();
                format!("`{connection}` has: {}", names.join(", "))
            });
        self.errors.push(
            ConfigError::new(
                ErrorKind::Invalid,
                format!("`{reference}` was not found when `{connection}` was discovered"),
            )
            .at(at)
            .path(path)
            .help(help),
        );
        None
    }

    fn flow(&mut self) -> Option<CheckedFlow> {
        let inputs = self.inputs()?;
        let filter = self.predicates(
            self.flow.filter.as_ref(),
            &inputs.scope,
            &self.path.key("where"),
        );
        let mut targets: Vec<CheckedTarget> = Vec::new();
        for (table, target) in write_order(&self.flow.to) {
            let path = self.path.key("to").key(table.as_str());
            if let Some(checked) = self.target(table, target, &inputs, &targets, &path) {
                targets.push(checked);
            }
        }
        // Assertions name the targets' columns: only check them once every
        // target checked, so a broken target isn't reported twice.
        let expect = if targets.len() == self.flow.to.len() {
            let (expect, errors) = crate::expect::check(
                &self.flow.expect,
                &targets,
                &inputs.record,
                &inputs.scope,
                &self.path.key("expect"),
                self.sources,
            );
            self.errors.extend(errors);
            expect
        } else {
            FlowExpect::default()
        };
        Some(CheckedFlow {
            name: self.flow.name.as_str().to_owned(),
            filter,
            targets,
            expect,
        })
    }

    fn inputs(&mut self) -> Option<Inputs<'a>> {
        let path = self.path.key("from");
        match self.flow.from.value() {
            From::Entity(reference) => {
                let driver = self.entity(reference, self.flow.from.location(), &path)?;
                let scope = Scope::entity(format!("entity `{reference}`"), driver);
                Some(Inputs {
                    driver,
                    record: scope.clone(),
                    scope,
                    many: Vec::new(),
                })
            }
            From::Merge(entries) => {
                let (first, rest) = entries.split_first()?;
                let driver = self.entity(
                    &first.entity,
                    first.entity.location(),
                    &path.item(0, None).key("entity"),
                )?;
                let alias = |e: &crate::config::model::FromEntry| {
                    e.alias
                        .as_ref()
                        .map(|a| a.as_str().to_owned())
                        .unwrap_or_else(|| e.entity.default_alias().to_owned())
                };
                let mut scope = Scope::entity(format!("entity `{}`", *first.entity), driver)
                    .with_driver_alias(alias(first));
                let record = scope.clone();
                let mut many = Vec::new();
                for (i, entry) in rest.iter().enumerate() {
                    let entry_path = path.item(i + 1, None);
                    let Some(entity) = self.entity(
                        &entry.entity,
                        entry.entity.location(),
                        &entry_path.key("entity"),
                    ) else {
                        continue;
                    };
                    let Some(join) = &entry.join else { continue };
                    self.join_keys(
                        entity,
                        &join.keys,
                        &scope,
                        &entry_path.key("join").key("keys"),
                    );
                    match join.cardinality.value() {
                        Cardinality::One => {
                            scope = scope.with_join(alias(entry), entity.fields().to_vec())
                        }
                        Cardinality::Many => {
                            scope = scope.with_many(alias(entry));
                            many.push((alias(entry), entity));
                        }
                    }
                }
                Some(Inputs {
                    driver,
                    scope,
                    record,
                    many,
                })
            }
        }
    }

    /// `keys: {id: o.customer_id}`: the left is a field of the joined
    /// entity, the right an earlier entry's column of a comparable type.
    fn join_keys(
        &mut self,
        entity: &Entity,
        keys: &OrderedMap<Spanned<String>>,
        earlier: &Scope,
        path: &FieldPath,
    ) {
        for (column, other) in keys.iter() {
            let key_path = path.key(column.as_str());
            let Some(field) = entity.field(column) else {
                let help = suggest(column, entity.fields().iter().map(|f| f.name.as_str()))
                    .map(|s| format!("did you mean `{s}`?"));
                self.type_error(
                    column.location(),
                    &key_path,
                    format!("`{}` has no field `{}`", entity.name(), column.as_str()),
                )
                .help = help;
                continue;
            };
            let Some(compiled) = self.compile(other, other.location(), earlier, &key_path) else {
                continue;
            };
            let Some(other_type) = compiled.output_type() else {
                continue;
            };
            let comparable = comparable(other_type, &field.ty);
            if !comparable {
                self.type_error(
                    other.location(),
                    &key_path,
                    format!(
                        "join keys must have comparable types: `{}` is {}, `{}` is {}",
                        column.as_str(),
                        types::name(&field.ty),
                        other.as_str(),
                        types::name(other_type)
                    ),
                );
            }
        }
    }

    fn predicates(
        &mut self,
        filter: Option<&Spanned<Where>>,
        scope: &Scope,
        path: &FieldPath,
    ) -> Vec<Predicate> {
        let Some(filter) = filter else {
            return Vec::new();
        };
        let list: Vec<(Option<String>, String, Location, FieldPath)> = match filter.value() {
            Where::Expr(text) => {
                vec![(None, text.clone(), filter.location().clone(), path.clone())]
            }
            Where::Named(named) => named
                .iter()
                .map(|(name, expr)| {
                    (
                        Some(name.as_str().to_owned()),
                        expr.as_str().to_owned(),
                        expr.location().clone(),
                        path.key(name.as_str()),
                    )
                })
                .collect(),
        };
        list.into_iter()
            .filter_map(|(name, text, at, path)| {
                let expr = self.compile(&text, &at, scope, &path)?;
                self.boolean(&expr, &path)?;
                Some(Predicate { name, expr })
            })
            .collect()
    }

    fn boolean(&mut self, expr: &Compiled, path: &FieldPath) -> Option<()> {
        match expr.output_type() {
            Some(CanonicalType::Bool) | None => Some(()),
            Some(other) => {
                let text = expr.text().to_owned();
                let help = format!(
                    "a filter keeps a record when it is true: did you mean `{text} != null` (or `== null`)?"
                );
                self.type_error(
                    expr.location(),
                    path,
                    format!(
                        "a `where:` predicate must be true or false, but `{text}` is {}",
                        types::name(other)
                    ),
                )
                .help = Some(help);
                None
            }
        }
    }

    fn target(
        &mut self,
        table: &Spanned<String>,
        target: &Target,
        inputs: &Inputs<'a>,
        done: &[CheckedTarget],
        path: &FieldPath,
    ) -> Option<CheckedTarget> {
        let parent = match &target.parent {
            Some(parent) => Some(done.iter().find(|t| t.table == parent.as_str())?),
            None => None,
        };
        let (scope, unnest) = self.target_scope(target, inputs, parent, done, path)?;
        let columns = match target.columns.value() {
            Columns::All => inputs
                .driver
                .fields()
                .iter()
                .map(|f| CheckedColumn {
                    field: f.clone(),
                    value: ColumnValue::Copy,
                })
                .collect(),
            Columns::Map(map) => self.columns(map, &scope, inputs, &path.key("columns")),
        };
        let filter = self.predicates(target.filter.as_ref(), &scope, &path.key("where"));
        let mut checked = CheckedTarget {
            table: table.as_str().to_owned(),
            parent: target.parent.as_ref().map(|p| p.as_str().to_owned()),
            unnest,
            scope,
            filter,
            columns,
        };
        if let Some(key) = &target.key {
            for column in key.iter() {
                match checked
                    .columns
                    .iter_mut()
                    .find(|c| c.field.name == column.as_str())
                {
                    // Key columns are implicitly `nullable: false` (0003 §5.5).
                    Some(c) => c.field.nullable = false,
                    None => {
                        let names: Vec<&str> = match target.columns.value() {
                            Columns::Map(map) => map.keys().map(|k| k.as_str()).collect(),
                            Columns::All => inputs
                                .driver
                                .fields()
                                .iter()
                                .map(|f| f.name.as_str())
                                .collect(),
                        };
                        let help = suggest(column, names.iter().copied())
                            .map(|s| format!("did you mean `{s}`?"));
                        self.type_error(
                            column.location(),
                            &path.key("key"),
                            format!(
                                "key column `{}` is not a column of `{}`",
                                column.as_str(),
                                table.as_str()
                            ),
                        )
                        .help = help;
                    }
                }
            }
        }
        Some(checked)
    }

    /// The scope a target's expressions see (0003 §5.4), and its `unnest:`
    /// path compiled in the scope the rows come from.
    fn target_scope(
        &mut self,
        target: &Target,
        inputs: &Inputs<'a>,
        parent: Option<&CheckedTarget>,
        done: &[CheckedTarget],
        path: &FieldPath,
    ) -> Option<(Scope, Option<Compiled>)> {
        let parent_fields = || {
            parent.map(|p| {
                p.columns
                    .iter()
                    .map(|c| c.field.clone())
                    .collect::<Vec<_>>()
            })
        };
        let with_parent = |scope: Scope| match parent_fields() {
            Some(fields) => scope.with_parent(fields),
            None => scope,
        };
        let Some(unnest) = &target.unnest else {
            return Some((with_parent(inputs.scope.clone()), None));
        };
        if let Some((alias, entity)) = inputs.many.iter().find(|(a, _)| a == unnest.as_str()) {
            let scope = Scope::joined_rows(format!("a row of `{alias}`"), entity.fields().to_vec())
                .tracking_missing(entity.tracks_missing());
            return Some((with_parent(scope), None));
        }
        // The path is resolved where the parent's rows come from: its
        // element if it is unnested itself, else the driver record.
        let outer = match parent {
            Some(parent) => {
                let grand = parent
                    .parent
                    .as_deref()
                    .and_then(|g| done.iter().find(|t| t.table == g));
                // The parent compiled already, so this can't add errors.
                let parent_target = self.flow.to.get(&parent.table)?;
                let parent_path = self.path.key("to").key(parent.table.as_str());
                self.target_scope(parent_target, inputs, grand, done, &parent_path)?
                    .0
            }
            None => inputs.scope.clone(),
        };
        let unnest_path = path.key("unnest");
        let compiled = self.compile(unnest, unnest.location(), &outer, &unnest_path)?;
        let element = match compiled.output_type() {
            Some(CanonicalType::List(item)) => item.ty.clone(),
            // A variant (a Mongo path that isn't an array in every sampled
            // document): each element is JSON, and a record whose value
            // isn't an array is an error row at run time (0003 §5.7).
            Some(CanonicalType::Json) => CanonicalType::Json,
            other => {
                let shown = other.map_or("null".to_owned(), types::name);
                self.type_error(
                    unnest.location(),
                    &unnest_path,
                    format!("`unnest: {}` needs an array, but it is {shown}", unnest.as_str()),
                )
                .help = Some("unnest takes an array field (or a `many` alias) and writes one row per element".into());
                return None;
            }
        };
        let scope = Scope::element(format!("an element of `{}`", unnest.as_str()), element)
            .tracking_missing(inputs.driver.tracks_missing());
        Some((with_parent(scope), Some(compiled)))
    }

    fn columns(
        &mut self,
        map: &OrderedMap<Spanned<ColumnSpec>>,
        scope: &Scope,
        inputs: &Inputs<'a>,
        path: &FieldPath,
    ) -> Vec<CheckedColumn> {
        let mut columns = Vec::new();
        for (name, spec) in map.iter() {
            let column_path = path.key(name.as_str());
            let checked = match spec.value() {
                ColumnSpec::Expr(text) => self.column(
                    name,
                    text,
                    spec.location(),
                    None,
                    false,
                    None,
                    scope,
                    &column_path,
                ),
                ColumnSpec::Typed(typed) => self.column(
                    name,
                    &typed.expr,
                    typed.expr.location(),
                    typed.r#type.as_ref(),
                    typed.allow_lossy,
                    typed.nullable,
                    scope,
                    &column_path,
                ),
                ColumnSpec::Nest(nest) => self.nest(name, nest, inputs, &column_path),
            };
            columns.extend(checked);
        }
        columns
    }

    #[allow(clippy::too_many_arguments)]
    fn column(
        &mut self,
        name: &Spanned<String>,
        text: &str,
        at: &Location,
        declared: Option<&Spanned<String>>,
        allow_lossy: bool,
        nullable: Option<bool>,
        scope: &Scope,
        path: &FieldPath,
    ) -> Option<CheckedColumn> {
        let compiled = self.compile(text, at, scope, path)?;
        let declared_type = match declared {
            Some(declared) => match types::parse(declared) {
                Ok(ty) => Some(ty),
                Err(reason) => {
                    let help = suggest(declared, types::SCALAR_NAMES.iter().copied())
                        .map(|s| format!("did you mean `{s}`?"))
                        .unwrap_or_else(|| {
                            format!(
                                "types: {}, decimal(p,s), timestamp, timestamp(<zone>), list(<type>)",
                                types::SCALAR_NAMES.join(", ")
                            )
                        });
                    self.type_error(declared.location(), &path.key("type"), reason)
                        .help = Some(help);
                    return None;
                }
            },
            None => None,
        };
        let inferred = compiled.output_type().cloned();
        let ty = match (inferred, declared_type) {
            (None, None) => {
                self.type_error(at, path, format!("the type of `{text}` can't be inferred"))
                    .help = Some("give the column a `type:`".into());
                return None;
            }
            (Some(inferred), None) => inferred,
            (None, Some(declared)) => declared,
            (Some(inferred), Some(declared)) => match cast::classify(&inferred, &declared) {
                Conversion::Same | Conversion::Safe => declared,
                Conversion::Lossy(_) if allow_lossy => declared,
                Conversion::Lossy(why) => {
                    let help = explicit_conversion_help(text, &inferred, &declared);
                    self.type_error(
                        at,
                        path,
                        format!(
                            "`{text}` is {}, but column `{}` is declared {}: {why}",
                            types::name(&inferred),
                            name.as_str(),
                            types::name(&declared)
                        ),
                    )
                    .help = Some(help);
                    return None;
                }
                Conversion::Impossible => {
                    self.type_error(
                            at,
                            path,
                            format!(
                                "`{text}` is {}, but column `{}` is declared {}, and {} can't be converted to {}",
                                types::name(&inferred),
                                name.as_str(),
                                types::name(&declared),
                                types::name(&inferred),
                                types::name(&declared)
                            ),
                        )
                        .help = Some("compute a value of the declared type in the expression".into());
                    return None;
                }
            },
        };
        let field = CanonicalField::new(
            name.as_str(),
            ty.clone(),
            nullable.unwrap_or_else(|| compiled.nullable()),
        );
        Some(CheckedColumn {
            field,
            value: ColumnValue::Expr(compiled.converting_to(ty)),
        })
    }

    fn nest(
        &mut self,
        name: &Spanned<String>,
        nest: &NestColumn,
        inputs: &Inputs<'a>,
        path: &FieldPath,
    ) -> Option<CheckedColumn> {
        let Some((alias, entity)) = inputs.many.iter().find(|(a, _)| a == nest.nest.as_str())
        else {
            let help = suggest(&nest.nest, inputs.many.iter().map(|(a, _)| a.as_str()))
                .map(|s| format!("did you mean `{s}`?"))
                .unwrap_or_else(|| "`nest:` names a `many` join of this flow's `from:`".into());
            self.type_error(
                nest.nest.location(),
                &path.key("nest"),
                format!("`{}` is not a `many` join of this flow", nest.nest.as_str()),
            )
            .help = Some(help);
            return None;
        };
        let scope = Scope::record(format!("a row of `{alias}`"), entity.fields().to_vec())
            .tracking_missing(entity.tracks_missing());
        let filter = self.predicates(nest.filter.as_ref(), &scope, &path.key("where"));
        let mut order_by = Vec::new();
        for (i, entry) in nest.order_by.iter().enumerate() {
            let (text, descending) = order_term(entry);
            if let Some(compiled) = self.compile(
                text,
                entry.location(),
                &scope,
                &path.key("order_by").item(i, None),
            ) {
                order_by.push((compiled, descending));
            }
        }
        let inner = Inputs {
            driver: entity,
            scope: scope.clone(),
            record: scope.clone(),
            many: Vec::new(),
        };
        let columns = self.columns(&nest.columns, &scope, &inner, &path.key("columns"));
        let element = CanonicalType::Struct(columns.iter().map(|c| c.field.clone()).collect());
        Some(CheckedColumn {
            // A driver with no matches gets `[]`, never null (0003 §5.8).
            field: CanonicalField::new(
                name.as_str(),
                CanonicalType::List(Box::new(CanonicalField::new("item", element, false))),
                false,
            ),
            value: ColumnValue::Nest {
                alias: alias.clone(),
                order_by,
                filter,
                columns,
            },
        })
    }
}

/// Join keys compare like with like: the same type, or one widening to the
/// other. Every type has a text form, but `id = status` isn't a join.
fn comparable(a: &CanonicalType, b: &CanonicalType) -> bool {
    let text = |t: &CanonicalType| matches!(t, CanonicalType::Utf8);
    if text(a) != text(b) {
        return false;
    }
    matches!(cast::classify(a, b), Conversion::Same | Conversion::Safe)
        || matches!(cast::classify(b, a), Conversion::Same | Conversion::Safe)
}

/// `"line_no desc"` is `line_no`, descending.
fn order_term(entry: &str) -> (&str, bool) {
    let trimmed = entry.trim_end();
    for (suffix, descending) in [
        (" desc", true),
        (" DESC", true),
        (" asc", false),
        (" ASC", false),
    ] {
        if let Some(expr) = trimmed.strip_suffix(suffix) {
            return (expr, descending);
        }
    }
    (trimmed, false)
}

/// The help for a lossy conversion: the opt-in, or the explicit function
/// when one says what is meant.
fn explicit_conversion_help(text: &str, from: &CanonicalType, to: &CanonicalType) -> String {
    let function = match (from, to) {
        (CanonicalType::Utf8, t) if t.to_arrow().is_integer() => Some("int"),
        (CanonicalType::Utf8, CanonicalType::Float64) => Some("float"),
        (CanonicalType::Decimal(_), t) if t.to_arrow().is_integer() => Some("int(round(...))"),
        _ => None,
    };
    let opt_in = "add `allow_lossy: true` to the column to accept it (values that don't convert become error rows)";
    match function {
        Some("int(round(...))") => format!("{opt_in}, or round explicitly: `int(round({text}))`"),
        Some(f) => format!("{opt_in}, or convert explicitly: `{f}({text})`"),
        None => opt_in.to_owned(),
    }
}

/// Targets with their parents first, ties by table name (0003 §5.3).
fn write_order(to: &OrderedMap<Target>) -> Vec<(&Spanned<String>, &Target)> {
    let mut remaining: Vec<(&Spanned<String>, &Target)> = to.iter().collect();
    remaining.sort_by(|a, b| a.0.as_str().cmp(b.0.as_str()));
    let mut ordered: Vec<(&Spanned<String>, &Target)> = Vec::new();
    while !remaining.is_empty() {
        let ready = remaining.iter().position(|(_, t)| match &t.parent {
            None => true,
            Some(p) => {
                ordered.iter().any(|(name, _)| name.as_str() == p.as_str())
                    || !to.keys().any(|k| k.as_str() == p.as_str())
            }
        });
        match ready {
            Some(i) => ordered.push(remaining.remove(i)),
            // A `parent:` cycle: reported by config validation.
            None => break,
        }
    }
    ordered
}
