//! Merges (B6, 0003 §5.2): a driver plus joined entries, each with
//! `cardinality:` and `keys:`, and the two ways of running a join (0002).
//!
//! # What a join does to a batch of driver records
//!
//! Joins run in `from:` order, before the flow `where:` (which may read
//! joined columns). Each one evaluates the earlier side of its `keys:` in
//! the scope at that point (the driver, plus the `one` joins before it),
//! and looks the keys up in the joined entity ([`side`](super::side)).
//! Every join is a left join: a null key, or no match, finds nothing.
//!
//! - **`one`**: the match becomes `<alias>.<field>` for every later
//!   expression; no match leaves every joined column null. **A second
//!   match rejects the driver record** (an error row, then `on_error`): the
//!   driver is never fanned out, whatever a sample at plan time said.
//! - **`many`**: the matches are kept, in the joined entity's primary-key
//!   order, for `nest:` columns and `unnest: <alias>` targets. A driver
//!   with none gets `[]` from a nest and no rows from an unnest.
//!
//! # Strategy
//!
//! Chosen at compile time and shown by [`FlowPlan::joins`](super::FlowPlan::joins):
//!
//! - **Co-partitioned streaming** when the joined entry shares the driver's
//!   partition key: a single key pair from a column of this entry to the
//!   driver's single-column primary key (what sources partition on), on
//!   the driver's connection, with bounds of the same kind (0002). The
//!   entry is then an input of the plan, split by the runner on the
//!   driver's boundaries, and each partition's rows are held for the
//!   partition.
//! - **Staging** otherwise (or with `strategy: stage`): the joined entity
//!   is read whole once, before the first partition, and sorted to disk
//!   under a byte budget ([`Staging`]). It isn't an input of the plan, so
//!   the runner doesn't read it; [`FlowPlan::stage_with`](super::FlowPlan::stage_with)
//!   gives the plan its source. The driver is always the side that
//!   streams: its partitions are the units of work (0001), so the joined
//!   side is the one staged, whichever is smaller.
//!
//! `strategy: co_partition` fails the compile when co-partitioning is
//! impossible, instead of falling back.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, ListArray, RecordBatch, StructArray, UInt32Array};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_ord::sort::{SortColumn, SortOptions};
use arrow_schema::{DataType, Field, FieldRef, Fields};
use ferry_core::Source;
use ferry_core::entity::Entity;
use ferry_core::types::{CanonicalField, CanonicalType};

use super::side::{Keyed, Matched, Sorted};
use super::{Run, internal, scoped, take, take_batch};
use crate::config::model::{Cardinality, FromEntry, JoinStrategy};
use crate::config::{ConfigError, ErrorKind, FieldPath};
use crate::expr::{Compiled, Expression, JOIN_PREFIX, Scope};
use crate::typecheck::cast::{Conversion, classify};
use crate::typecheck::{Catalog, CheckedColumn, ColumnValue, Predicate};

/// One joined `from:` entry, as `ferry plan` shows it.
#[derive(Clone, Debug)]
pub struct JoinPlan {
    pub alias: String,
    /// `<connection>.<entity>`, as written.
    pub reference: String,
    pub connection: String,
    /// The discovered entity.
    pub entity: Entity,
    pub cardinality: Cardinality,
    /// `(column of this entry, expression on an earlier entry)`, in
    /// `keys:` order.
    pub keys: Vec<(String, String)>,
    pub strategy: Strategy,
    /// Why the strategy was chosen: what `ferry plan` prints next to it.
    pub reason: String,
}

/// How a join runs (0002).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Strategy {
    /// An input of the plan, split on the driver's boundaries applied to
    /// `column` (ferry-runtime's `Input::co_partition`).
    CoPartition { column: String },
    /// Read whole before the first partition and sorted to disk.
    Stage,
}

impl Strategy {
    pub fn as_str(&self) -> &'static str {
        match self {
            Strategy::CoPartition { .. } => "co_partition",
            Strategy::Stage => "stage",
        }
    }
}

/// A `nest:` column's full ordering, as `ferry plan` shows it: its
/// `order_by`, then the nested entity's primary key (0003 §5.8).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NestOrder {
    pub target: String,
    pub column: String,
    pub alias: String,
    /// `line_no`, `qty desc`, …, then the tie-breaker columns.
    pub order_by: Vec<String>,
}

/// What staged joins need at run time.
#[derive(Clone)]
pub struct Staging {
    /// The source of each staged join, by alias.
    pub sources: BTreeMap<String, Arc<dyn Source>>,
    /// Bytes every staged join of the flow may hold together. Sorting and
    /// the block cache stay within it; what doesn't fit is spilled.
    pub memory: u64,
    /// Where stages spill. Default: the system's temporary directory.
    pub dir: Option<PathBuf>,
}

impl Staging {
    /// 64 MiB: the stages of one flow when nothing else is said.
    pub const DEFAULT_MEMORY: u64 = 64 << 20;
}

/// A compiled join.
#[derive(Clone)]
pub(crate) struct Join {
    pub plan: JoinPlan,
    pub keyed: Arc<Keyed>,
    /// The earlier side of each key pair, compiled in the scope at this
    /// entry.
    pub exprs: Vec<Compiled>,
    /// `strategy: co_partition`: never staged instead.
    pub pinned: bool,
}

/// A `nest:` column, compiled.
#[derive(Clone)]
pub(crate) struct Nest {
    /// Index into the plan's joins.
    pub join: usize,
    pub filter: Vec<Predicate>,
    pub order_by: Vec<(Compiled, bool)>,
    /// The nested entity's primary-key columns not already in `order_by`.
    pub tie: Vec<String>,
    pub columns: Vec<(CanonicalField, Compiled)>,
    /// The list's item field and the struct's fields.
    pub item: FieldRef,
    pub fields: Fields,
}

/// The driver and every join of a merge `from:`.
pub(crate) struct Merge {
    pub driver: Entity,
    pub joins: Vec<Join>,
}

fn invalid(message: String, path: &FieldPath) -> ConfigError {
    ConfigError::new(ErrorKind::Invalid, message).path(path)
}

fn alias(entry: &FromEntry) -> String {
    entry
        .alias
        .as_ref()
        .map(|a| a.as_str().to_owned())
        .unwrap_or_else(|| entry.entity.default_alias().to_owned())
}

/// The kind of partition bound a type gives (ferry-core `KeyValue`).
fn bound_kind(ty: &CanonicalType) -> Option<&'static str> {
    use CanonicalType::*;
    match ty {
        Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 => Some("integer"),
        Utf8 => Some("text"),
        Binary | Uuid => Some("bytes"),
        _ => None,
    }
}

/// Compiles the joins of `entries` (the first is the driver), choosing
/// each one's strategy. Pushes an error, and returns `None`, for what
/// can't be planned.
pub(crate) fn plan(
    flow: &str,
    entries: &[FromEntry],
    catalog: &Catalog,
    path: &FieldPath,
    errors: &mut Vec<ConfigError>,
) -> Option<Merge> {
    let (first, rest) = entries.split_first()?;
    let driver = catalog.get(&first.entity)?.clone();
    let driver_alias = alias(first);
    let mut scope = Scope::entity(format!("entity `{}`", *first.entity), &driver)
        .with_driver_alias(driver_alias.clone());
    let mut joins = Vec::new();
    let mut inputs = vec![driver.name().to_owned()];
    let failed = errors.len();
    for (i, entry) in rest.iter().enumerate() {
        let entry_path = path.item(i + 1, None).key("join");
        let (Some(entity), Some(join)) = (catalog.get(&entry.entity), &entry.join) else {
            return None;
        };
        let alias = alias(entry);
        let mut exprs = Vec::new();
        let mut types = Vec::new();
        let mut keys = Vec::new();
        for (column, other) in join.keys.iter() {
            let key_path = entry_path.key("keys").key(column.as_str());
            let compiled =
                Expression::parse_at(other, other.location()).and_then(|e| e.compile(&scope));
            let (Some(field), Ok(compiled)) = (entity.field(column), compiled) else {
                // The type checker reports these.
                errors.push(invalid(
                    format!(
                        "flow `{flow}`: join key `{}` doesn't compile",
                        column.as_str()
                    ),
                    &key_path,
                ));
                return None;
            };
            let Some(other_ty) = compiled.result_type() else {
                errors.push(invalid(
                    format!(
                        "flow `{flow}`: join key `{}` is a bare null",
                        other.as_str()
                    ),
                    &key_path,
                ));
                return None;
            };
            // Both sides are compared as the wider type.
            let ty = match classify(&field.ty, other_ty) {
                Conversion::Same | Conversion::Safe => other_ty.clone(),
                _ => field.ty.clone(),
            };
            types.push(ty.to_arrow());
            exprs.push(compiled);
            keys.push((column.as_str().to_owned(), other.as_str().to_owned()));
        }
        let shared = shares_partition_key(
            &driver,
            &driver_alias,
            first.entity.connection.as_str(),
            entry,
            entity,
            &inputs,
        );
        let (strategy, reason) = match (join.strategy, shared) {
            (JoinStrategy::Stage, _) => (Strategy::Stage, "pinned by `strategy: stage`".into()),
            (JoinStrategy::CoPartition, Ok(column)) => (
                Strategy::CoPartition { column },
                "pinned by `strategy: co_partition`".into(),
            ),
            (JoinStrategy::Auto, Ok(column)) => {
                let reason = format!(
                    "auto: `{alias}.{column}` is split on the driver's partition boundaries (0002)"
                );
                (Strategy::CoPartition { column }, reason)
            }
            (JoinStrategy::Auto, Err(why)) => (Strategy::Stage, format!("auto: staged, {why}")),
            (JoinStrategy::CoPartition, Err(why)) => {
                errors.push(
                    ConfigError::new(
                        ErrorKind::Invalid,
                        format!("flow `{flow}`: join `{alias}` can't be co-partitioned: {why}"),
                    )
                    .at(join.cardinality.location())
                    .path(&entry_path.key("strategy"))
                    .help("co-partitioning needs one key pair onto the driver's single-column key, on the driver's connection (0002); use `strategy: stage`, or `auto` to stage when it can't"),
                );
                continue;
            }
        };
        if let Strategy::CoPartition { .. } = strategy {
            inputs.push(entity.name().to_owned());
        }
        let reference = entry.entity.to_string();
        let keyed = match Keyed::new(
            entity.clone(),
            reference.clone(),
            keys.iter().map(|(c, _)| c.clone()).collect(),
            types,
        ) {
            Ok(keyed) => Arc::new(keyed),
            Err(e) => {
                errors.push(invalid(format!("flow `{flow}`: {e}"), &entry_path));
                continue;
            }
        };
        let cardinality = *join.cardinality.value();
        scope = match cardinality {
            Cardinality::One => scope.with_join(alias.clone(), entity.fields().to_vec()),
            Cardinality::Many => scope.with_many(alias.clone()),
        };
        joins.push(Join {
            plan: JoinPlan {
                alias,
                reference,
                connection: entry.entity.connection.as_str().to_owned(),
                entity: entity.clone(),
                cardinality,
                keys,
                strategy,
                reason,
            },
            keyed,
            exprs,
            pinned: join.strategy == JoinStrategy::CoPartition,
        });
    }
    (errors.len() == failed).then_some(Merge { driver, joins })
}

/// The column of `entity` that is split like the driver, or why there is
/// none.
fn shares_partition_key(
    driver: &Entity,
    driver_alias: &str,
    driver_connection: &str,
    entry: &FromEntry,
    entity: &Entity,
    inputs: &[String],
) -> Result<String, String> {
    let join = entry.join.as_ref().expect("checked by the caller");
    let connection = entry.entity.connection.as_str();
    if connection != driver_connection {
        return Err(format!(
            "because `{connection}` isn't the driver's connection `{driver_connection}`"
        ));
    }
    let [pk] = driver.primary_key() else {
        return Err(format!(
            "because the driver's primary key ({}) has more than one column",
            driver.primary_key().join(", ")
        ));
    };
    let pairs: Vec<_> = join.keys.iter().collect();
    let [(column, other)] = pairs.as_slice() else {
        return Err("because the join has more than one key pair".into());
    };
    let driver_key = format!("{driver_alias}.{pk}");
    if other.as_str().trim() != driver_key {
        return Err(format!(
            "because `{}` isn't the driver's key `{driver_key}`",
            other.as_str()
        ));
    }
    let (Some(ours), Some(theirs)) = (entity.field(column), driver.field(pk)) else {
        return Err("because a key column wasn't discovered".into());
    };
    match (bound_kind(&ours.ty), bound_kind(&theirs.ty)) {
        (Some(a), Some(b)) if a == b => {}
        _ => {
            return Err(format!(
                "because `{}` and `{driver_key}` don't give partition bounds of the same kind",
                column.as_str()
            ));
        }
    }
    if inputs.iter().any(|i| i == entity.name()) {
        return Err(format!(
            "because the plan already reads `{}` as an input",
            entity.name()
        ));
    }
    Ok(column.as_str().to_owned())
}

/// Compiles a `nest:` column. `order_by` entries must be sortable values.
pub(crate) fn nest(
    field: &CanonicalField,
    value: &ColumnValue,
    joins: &[Join],
    path: &FieldPath,
) -> Result<(Nest, Vec<String>), ConfigError> {
    let ColumnValue::Nest {
        alias,
        order_by,
        filter,
        columns,
    } = value
    else {
        unreachable!("called for nest columns")
    };
    let join = joins
        .iter()
        .position(|j| j.plan.alias == *alias && j.plan.cardinality == Cardinality::Many)
        .ok_or_else(|| invalid(format!("`{alias}` is not a `many` join"), path))?;
    let entity = &joins[join].plan.entity;
    for (expr, _) in order_by {
        if let Some(CanonicalType::List(_) | CanonicalType::Struct(_) | CanonicalType::Map { .. }) =
            expr.output_type()
        {
            return Err(ConfigError::new(
                ErrorKind::Type,
                format!(
                    "`order_by: {}` is a list, struct or map, which has no order",
                    expr.text()
                ),
            )
            .at(expr.location())
            .path(&path.key("order_by")));
        }
    }
    let listed: Vec<&str> = order_by.iter().map(|(e, _)| e.text().trim()).collect();
    let tie: Vec<String> = entity
        .primary_key()
        .iter()
        .filter(|k| !listed.contains(&k.as_str()))
        .cloned()
        .collect();
    let shown = order_by
        .iter()
        .map(|(e, desc)| {
            let text = e.text().trim();
            if *desc {
                format!("{text} desc")
            } else {
                text.to_owned()
            }
        })
        .chain(tie.iter().cloned())
        .collect();
    let columns = columns
        .iter()
        .map(|c: &CheckedColumn| match &c.value {
            ColumnValue::Expr(e) => Ok((c.field.clone(), e.clone())),
            _ => Err(invalid(
                format!("nest column `{}` must be an expression", c.field.name),
                &path.key("columns").key(&c.field.name),
            )),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let DataType::List(item) = field.to_arrow().data_type().clone() else {
        unreachable!("a nest column is a list")
    };
    let DataType::Struct(fields) = item.data_type().clone() else {
        unreachable!("of structs")
    };
    Ok((
        Nest {
            join,
            filter: filter.clone(),
            order_by: order_by.clone(),
            tie,
            columns,
            item,
            fields,
        },
        shown,
    ))
}

/// A joined side for one partition: a co-partitioned input's rows, or a
/// stage.
pub(crate) type Sides = Vec<Arc<Sorted>>;

impl Run<'_> {
    /// Looks every join up for the batch, in `from:` order: `one` joins
    /// become `_ferry_join_<alias>` columns of the driver batch, `many`
    /// joins are kept for nests and unnests.
    pub(super) fn join(&mut self, sides: &Sides) -> ferry_core::Result<()> {
        let rows = self.batch.num_rows();
        for (j, join) in self.plan.joins.iter().enumerate() {
            let alias = &join.plan.alias;
            let mut values = Vec::with_capacity(join.exprs.len());
            let mut failed = vec![false; rows];
            for expr in &join.exprs {
                let (v, errors) = expr.evaluate_rows(&scoped(&self.batch, expr.input_schema())?);
                for e in errors {
                    failed[e.row] = true;
                    self.reject(
                        e.row as u32,
                        None,
                        format!("join `{alias}`: key `{}`: {}", expr.text(), e.message),
                    );
                }
                values.push(v);
            }
            let keys = join.keyed.encode(&values)?;
            let matched = sides[j].lookup(&keys)?;
            match join.plan.cardinality {
                Cardinality::Many => self.many[j] = Some(matched),
                Cardinality::One => {
                    let mut pick = Vec::with_capacity(rows);
                    for (row, &failed) in failed.iter().enumerate() {
                        let found = matched.range(row);
                        if found.len() > 1 && !failed {
                            let shown: Vec<String> = join
                                .plan
                                .keys
                                .iter()
                                .zip(&values)
                                .map(|((column, _), v)| {
                                    let value = arrow_cast::display::array_value_to_string(v, row)
                                        .unwrap_or_else(|_| "?".into());
                                    format!("{column} = {value}")
                                })
                                .collect();
                            self.reject(
                                row as u32,
                                None,
                                format!(
                                    "join `{alias}` is `cardinality: one`, but {} rows of `{}` match {}",
                                    found.len(),
                                    join.plan.reference,
                                    shown.join(", ")
                                ),
                            );
                        }
                        pick.push((found.len() == 1).then_some(found.start as u32));
                    }
                    let column = one_struct(&join.plan.entity, &matched.rows, &pick)?;
                    self.push_driver_column(&format!("{JOIN_PREFIX}{alias}"), column)?;
                }
            }
        }
        Ok(())
    }

    fn push_driver_column(&mut self, name: &str, column: ArrayRef) -> ferry_core::Result<()> {
        let schema = self.batch.schema();
        let mut fields: Vec<FieldRef> = schema.fields().iter().cloned().collect();
        fields.push(Arc::new(Field::new(name, column.data_type().clone(), true)));
        let mut columns = self.batch.columns().to_vec();
        columns.push(column);
        self.batch = RecordBatch::try_new(Arc::new(arrow_schema::Schema::new(fields)), columns)
            .map_err(|e| internal(e.to_string()))?;
        Ok(())
    }

    /// The matches of `many` join `j`, per driver row.
    pub(super) fn matched(&self, j: usize) -> ferry_core::Result<&Matched> {
        self.many[j]
            .as_ref()
            .ok_or_else(|| internal(format!("join {j} wasn't looked up")))
    }

    /// A nest column for the rows `records` of a target: per row, its
    /// driver record's matches, through the nest's `where:`, in its order,
    /// mapped. A record whose nested rows fail is rejected.
    pub(super) fn nest_column(
        &mut self,
        table: &str,
        field: &CanonicalField,
        nest: &Nest,
        records: &[u32],
    ) -> ferry_core::Result<ArrayRef> {
        let context = |message: String| format!("{table}.{}: {message}", field.name);
        // Every target row's nested rows, and which row each belongs to.
        let matched = self.matched(nest.join)?;
        let mut group = Vec::new();
        let mut gather = Vec::new();
        for (i, &r) in records.iter().enumerate() {
            for m in matched.range(r as usize) {
                group.push(i as u32);
                gather.push(m as u32);
            }
        }
        let mut nested = take_batch(&matched.rows, &UInt32Array::from(gather))?;
        let mut group = UInt32Array::from(group);
        let record_of = |g: &UInt32Array, row: usize| records[g.value(row) as usize];

        // The nest's `where:` drops nested rows; an error rejects the record.
        if !nest.filter.is_empty() {
            let results = Self::predicates(&nest.filter, &scoped(&nested, nest.filter_schema())?);
            let mut keep = Vec::with_capacity(results.len());
            for (row, result) in results.into_iter().enumerate() {
                match result {
                    Ok(None) => keep.push(row as u32),
                    Ok(Some(_)) => {}
                    Err(error) => {
                        let record = record_of(&group, row);
                        self.reject(record, Some(table), context(error));
                    }
                }
            }
            let keep = UInt32Array::from(keep);
            nested = take_batch(&nested, &keep)?;
            group = take(&group, &keep)?.as_primitive().clone();
        }

        // Sort by row, then `order_by`, then the primary key.
        let mut sort = vec![SortColumn {
            values: Arc::new(group.clone()),
            options: None,
        }];
        for (expr, descending) in &nest.order_by {
            let (values, errors) = expr.evaluate_rows(&scoped(&nested, expr.input_schema())?);
            for e in errors {
                let record = record_of(&group, e.row);
                let error = context(format!("order_by `{}`: {}", expr.text(), e.message));
                self.reject(record, Some(table), error);
            }
            sort.push(SortColumn {
                values,
                // Postgres' default: nulls last ascending, first descending.
                options: Some(SortOptions {
                    descending: *descending,
                    nulls_first: *descending,
                }),
            });
        }
        for column in &nest.tie {
            let values = nested
                .column_by_name(column)
                .cloned()
                .ok_or_else(|| internal(format!("no primary-key column `{column}`")))?;
            sort.push(SortColumn {
                values,
                options: None,
            });
        }
        let order = arrow_ord::sort::lexsort_to_indices(&sort, None)
            .map_err(|e| internal(context(e.to_string())))?;
        nested = take_batch(&nested, &order)?;
        group = take(&group, &order)?.as_primitive().clone();

        let mut columns = Vec::with_capacity(nest.columns.len());
        for (column, expr) in &nest.columns {
            let (values, errors) = expr.evaluate_rows(&scoped(&nested, expr.input_schema())?);
            for e in errors {
                let record = record_of(&group, e.row);
                let error = context(format!(
                    "{} = `{}`: {}",
                    column.name,
                    expr.text(),
                    e.message
                ));
                self.reject(record, Some(table), error);
            }
            if !column.nullable {
                for row in (0..values.len()).filter(|&r| values.is_null(r)) {
                    let record = record_of(&group, row);
                    let error = context(format!(
                        "{} can't be null, but `{}` is null",
                        column.name,
                        expr.text()
                    ));
                    self.reject(record, Some(table), error);
                }
            }
            columns.push(values);
        }
        // Rejected records' rows go: they may hold the nulls of failures,
        // and their target rows are dropped anyway.
        let keep: Vec<u32> = (0..group.len() as u32)
            .filter(|&row| !self.is_rejected(record_of(&group, row as usize)))
            .collect();
        if keep.len() < group.len() {
            let keep = UInt32Array::from(keep);
            columns = columns
                .iter()
                .map(|c| take(c.as_ref(), &keep))
                .collect::<ferry_core::Result<_>>()?;
            group = take(&group, &keep)?.as_primitive().clone();
        }
        let mut lengths = vec![0usize; records.len()];
        for g in group.values() {
            lengths[*g as usize] += 1;
        }
        let items =
            StructArray::try_new_with_length(nest.fields.clone(), columns, None, group.len())
                .map_err(|e| internal(context(e.to_string())))?;
        let list = ListArray::try_new(
            Arc::clone(&nest.item),
            OffsetBuffer::from_lengths(lengths),
            Arc::new(items),
            None,
        )
        .map_err(|e| internal(context(e.to_string())))?;
        Ok(Arc::new(list))
    }
}

impl Nest {
    /// What a nest's `where:` reads: the nested entity's rows.
    fn filter_schema(&self) -> &arrow_schema::SchemaRef {
        self.filter[0].expr.input_schema()
    }
}

/// A `one` join's column: per driver row, its match as a struct of the
/// joined entity's fields, or null.
fn one_struct(
    entity: &Entity,
    matched: &RecordBatch,
    pick: &[Option<u32>],
) -> ferry_core::Result<ArrayRef> {
    let indices = UInt32Array::from(pick.to_vec());
    let fields: Vec<FieldRef> = entity
        .fields()
        .iter()
        .map(|f| Arc::new(f.to_arrow()))
        .collect();
    let columns = fields
        .iter()
        .map(|f| {
            let column = matched
                .column_by_name(f.name())
                .ok_or_else(|| internal(format!("no joined column `{}`", f.name())))?;
            take(column.as_ref(), &indices)
        })
        .collect::<ferry_core::Result<Vec<_>>>()?;
    // Where nothing matched, the struct is null, and so is every field
    // under it, even one the entity declares not null.
    let nulls = NullBuffer::from(pick.iter().map(Option::is_some).collect::<Vec<_>>());
    let structs =
        StructArray::try_new_with_length(Fields::from(fields), columns, Some(nulls), pick.len())
            .map_err(|e| internal(format!("joined `{}`: {e}", entity.name())))?;
    Ok(Arc::new(structs))
}
