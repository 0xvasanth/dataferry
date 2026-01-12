//! The compiler (B4, B5): a type-checked flow becomes a ferry-core
//! [`TransformPlan`] the runner executes partition by partition, plus the
//! [`TargetSchema`] its sink prepares.
//!
//! # What a plan does with one batch of driver records
//!
//! 1. **Flow `where:`** (0003 §5.6), in driver scope. A predicate that is
//!    false or null *filters* the record: it has no row in any target, and
//!    [`Transformed::filtered`] names the first predicate that failed. A
//!    predicate that errors rejects the record.
//! 2. **Targets, in write order** (parents first, ties by name, 0003 §5.3).
//!    A target's rows come from its parent's rows: one per parent row, or
//!    one per element of the `unnest:` array (0003 §5.7). A target without
//!    `parent:` takes one row per driver record (or per element of the
//!    driver's array). Because rows only ever come from rows that survived
//!    above them, a row filtered out of a target takes its children,
//!    grandchildren and so on with it: that is the propagation rule. A
//!    sibling without a `parent:` link keeps its rows.
//! 3. **Per target**: its `where:` against source values (false or null drops
//!    the row), then every column, converted to its settled type.
//!
//! **Errors reject the whole record** (ferry-core `transform`): an
//! expression that fails, a null in a column that can't be null, a `where:`
//! that errors, an `unnest:` value that isn't an array. The record's rows
//! are removed from every target, and [`TransformOutput::rejected`] says
//! which target and why. Rows of other records in the batch are unaffected.
//!
//! # `unnest:` and the sigils
//!
//! - An empty array, a missing field and an explicit null all give zero
//!   rows, and the parent is still written.
//! - `$index` is the 0-based position in the innermost array, `$item` the
//!   element, `$parent` the parent's **mapped** row, and `$key` the
//!   deterministic UUIDv5 described in [`key`].
//! - `_ferry_missing` follows the element: a Mongo path `items.1.sku` that
//!   was absent is `sku` missing in element 1. This needs `unnest:` to be a
//!   plain path (`items`, `customer.addresses`); for any other expression
//!   nothing in the element counts as missing (null still reads as null).
//! - The type checker guarantees an `unnest:` path is an array, so a
//!   non-array can only show up in a variant (`json`) value, where it is an
//!   error row.
//!
//! # Merges
//!
//! A `from:` list (B6) runs its joins first, in `from:` order, before the
//! flow `where:`: `one` joins become `<alias>.<field>` columns of the
//! driver record, and `many` joins feed `nest:` columns and `unnest:
//! <alias>` targets. A duplicate match on a `one` join rejects the record.
//! [`merge`] has the rules and the two strategies (co-partitioned
//! streaming, or staging), which [`FlowPlan::joins`] shows.

pub mod key;
mod merge;
mod side;

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::builder::{FixedSizeBinaryBuilder, StringBuilder};
use arrow_array::cast::AsArray;
use arrow_array::{
    Array, ArrayRef, BooleanArray, Int64Array, ListArray, RecordBatch, RecordBatchOptions,
    StructArray, UInt32Array, make_array,
};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, Fields, Schema, SchemaRef};
use ferry_core::entity::Entity;
use ferry_core::missing::{MISSING_PATHS_COLUMN, MissingPathsBuilder};
use ferry_core::transform::{
    PartitionInputs, RejectedRecord, SourceRecordId, TargetBatch, TransformOutput, TransformPlan,
    TransformStream,
};
use ferry_core::types::CanonicalField;
use ferry_core::{FerryError, ForeignKey, TargetSchema, TargetTable, WriteMode};
use futures::{StreamExt, TryStreamExt};

use crate::config::model::{self, Mode};
use crate::config::{ConfigError, ConfigErrors, ErrorKind, FieldPath, Flow};
use crate::expr::{Compiled, INDEX_COLUMN, ITEM_COLUMN, KEY_COLUMN, PARENT_COLUMN};
use crate::typecheck::{Catalog, CheckedFlow, ColumnValue, Predicate};
use key::Step;
use merge::{Join, Nest, Sides};
pub use merge::{JoinPlan, NestOrder, Staging, Strategy};
use side::{Matched, Sorted};

/// A flow, compiled: everything the runner needs from the DSL.
#[derive(Clone)]
pub struct CompiledFlow {
    pub name: String,
    pub plan: FlowPlan,
    /// The target tables, in write order, for `Sink::prepare`.
    pub target: TargetSchema,
    pub mode: WriteMode,
    /// What `ferry plan` should point out but doesn't stop the flow: a
    /// child target without `key:` in insert mode (0003 §5.5).
    pub warnings: Vec<String>,
}

/// Compiles one type-checked flow. `checked` must be the type checker's
/// result for `flow` against `catalog`.
///
/// Fails, naming the target, when a `mode: upsert` target has no `key:`
/// (the loader checks this too; the planner doesn't rely on it), and, naming
/// the join, when `strategy: co_partition` is pinned on a join that can't be
/// co-partitioned.
pub fn compile(
    flow: &Flow,
    checked: &CheckedFlow,
    catalog: &Catalog,
) -> Result<CompiledFlow, ConfigErrors> {
    let path = FieldPath::root()
        .key("flows")
        .item(0, Some(flow.name.as_str()));
    let mut errors = Vec::new();
    let (entity, joins) = match flow.from.value() {
        model::From::Entity(reference) => (catalog.get(reference).cloned(), Vec::new()),
        model::From::Merge(entries) => {
            match merge::plan(
                flow.name.as_str(),
                entries,
                catalog,
                &path.key("from"),
                &mut errors,
            ) {
                Some(merge) => (Some(merge.driver), merge.joins),
                None => (None, Vec::new()),
            }
        }
    };
    let mode = match flow.mode {
        Mode::Insert => WriteMode::Insert,
        Mode::Upsert => WriteMode::Upsert,
    };
    let mut warnings = Vec::new();
    let mut tables = Vec::new();
    let mut targets: Vec<PlanTarget> = Vec::new();
    let mut nests = Vec::new();
    for target in &checked.targets {
        let Some((table, model)) = flow.to.iter().find(|(t, _)| t.as_str() == target.table) else {
            continue;
        };
        let target_path = path.key("to").key(&target.table);
        let key: Vec<String> = model
            .key
            .as_ref()
            .map(|k| k.iter().map(|c| c.as_str().to_owned()).collect())
            .unwrap_or_default();
        if model.key.is_none() {
            match mode {
                WriteMode::Upsert => errors.push(
                    ConfigError::new(
                        ErrorKind::Invalid,
                        format!(
                            "flow `{}` is `mode: upsert`, so target `{}` needs `key:`",
                            flow.name.as_str(),
                            target.table
                        ),
                    )
                    .at(table.location())
                    .path(&target_path.key("key"))
                    .help("every target of an upsert flow names its key columns (0003 §5.5)"),
                ),
                WriteMode::Insert if target.parent.is_some() => warnings.push(format!(
                    "flow `{}`: child target `{}` has no `key:`; a re-run can only be made \
                     safe by the sink's strategy (0003 §5.5)",
                    flow.name.as_str(),
                    target.table
                )),
                WriteMode::Insert => {}
            }
        }
        // `unnest: <many alias>`: the type checker leaves `unnest` unset.
        let many = match (&model.unnest, &target.unnest) {
            (Some(alias), None) => {
                let j = joins.iter().position(|j| {
                    j.plan.alias == alias.as_str() && j.plan.cardinality == model::Cardinality::Many
                });
                if j.is_none() {
                    errors.push(
                        ConfigError::new(
                            ErrorKind::Invalid,
                            format!("`unnest: {}` is not a `many` join", alias.as_str()),
                        )
                        .at(alias.location())
                        .path(&target_path.key("unnest")),
                    );
                }
                j
            }
            _ => None,
        };
        let mut columns = Vec::new();
        for column in &target.columns {
            match &column.value {
                ColumnValue::Copy => columns.push(PlanColumn::Copy(column.field.clone())),
                ColumnValue::Expr(expr) => {
                    columns.push(PlanColumn::Expr(column.field.clone(), expr.clone()))
                }
                ColumnValue::Nest { alias, .. } => {
                    let column_path = target_path.key("columns").key(&column.field.name);
                    match merge::nest(&column.field, &column.value, &joins, &column_path) {
                        Ok((nest, order_by)) => {
                            nests.push(NestOrder {
                                target: target.table.clone(),
                                column: column.field.name.clone(),
                                alias: alias.clone(),
                                order_by,
                            });
                            columns.push(PlanColumn::Nest(column.field.clone(), nest));
                        }
                        Err(e) => errors.push(e),
                    }
                }
            }
        }
        let parent = target
            .parent
            .as_ref()
            .and_then(|p| targets.iter().position(|t| t.table == *p));
        let fields: Vec<CanonicalField> = target.columns.iter().map(|c| c.field.clone()).collect();
        let foreign_keys = parent
            .and_then(|p| foreign_key(&tables[p], target))
            .into_iter()
            .collect();
        tables.push(TargetTable {
            name: target.table.clone(),
            fields: fields.clone(),
            primary_key: key,
            foreign_keys,
        });
        targets.push(PlanTarget {
            table: target.table.clone(),
            parent,
            unnest: target.unnest.clone().map(|expr| Unnest {
                missing_prefix: plain_path(expr.text()),
                expr,
            }),
            many,
            input: target.scope.input_schema(),
            filter: target.filter.clone(),
            columns,
            output: Arc::new(Schema::new(
                fields
                    .iter()
                    .map(CanonicalField::to_arrow)
                    .collect::<Vec<_>>(),
            )),
        });
    }
    let target = TargetSchema::new(tables).map_err(|e| {
        ConfigErrors(vec![
            ConfigError::new(ErrorKind::Invalid, e.to_string()).path(&path.key("to")),
        ])
    });
    let (Some(entity), true, Ok(target)) = (entity, errors.is_empty(), &target) else {
        errors.extend(target.err().into_iter().flat_map(|e| e.0));
        return Err(ConfigErrors(errors));
    };
    let plan = FlowPlan {
        inner: Arc::new(Inner {
            name: flow.name.as_str().to_owned(),
            inputs: inputs(&entity, &joins),
            write_order: targets.iter().map(|t| t.table.clone()).collect(),
            stages: Arc::new(Stages::new(joins.len())),
            joins,
            nests,
            entity,
            filter: checked.filter.clone(),
            targets,
        }),
    };
    Ok(CompiledFlow {
        name: flow.name.as_str().to_owned(),
        plan,
        target: target.clone(),
        mode,
        warnings,
    })
}

/// The plan's inputs: the driver, then every co-partitioned join.
fn inputs(driver: &Entity, joins: &[Join]) -> Vec<String> {
    std::iter::once(driver.name().to_owned())
        .chain(
            joins
                .iter()
                .filter(|j| matches!(j.plan.strategy, Strategy::CoPartition { .. }))
                .map(|j| j.plan.entity.name().to_owned()),
        )
        .collect()
}

/// A child that maps every key column of its parent from `$parent.<col>`
/// references it (0003 §5.3).
fn foreign_key(
    parent: &TargetTable,
    child: &crate::typecheck::CheckedTarget,
) -> Option<ForeignKey> {
    if parent.primary_key.is_empty() {
        return None;
    }
    let columns = parent
        .primary_key
        .iter()
        .map(|k| {
            let reference = format!("$parent.{k}");
            child.columns.iter().find_map(|c| match &c.value {
                ColumnValue::Expr(e) if e.text().trim() == reference => Some(c.field.name.clone()),
                _ => None,
            })
        })
        .collect::<Option<Vec<_>>>()?;
    Some(ForeignKey {
        columns,
        references_table: parent.name.clone(),
        references_columns: parent.primary_key.clone(),
    })
}

/// `items` or `customer.addresses`: the Mongo prefix of each element's
/// missing paths. `None` for anything that isn't a plain dotted path.
fn plain_path(text: &str) -> Option<String> {
    let text = text.trim();
    let plain = text.split('.').all(|part| {
        let mut chars = part.chars();
        chars
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
            && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
    });
    plain.then(|| text.to_owned())
}

/// A compiled flow's [`TransformPlan`]. Cheap to clone.
#[derive(Clone)]
pub struct FlowPlan {
    inner: Arc<Inner>,
}

#[derive(Clone)]
struct Inner {
    name: String,
    inputs: Vec<String>,
    write_order: Vec<String>,
    entity: Entity,
    /// The joined `from:` entries, in order.
    joins: Vec<Join>,
    nests: Vec<NestOrder>,
    stages: Arc<Stages>,
    filter: Vec<Predicate>,
    targets: Vec<PlanTarget>,
}

/// The staged joins' sources, and each stage once it is built: by the
/// first partition that needs it, for every partition after.
struct Stages {
    staging: std::sync::Mutex<Option<Staging>>,
    built: Vec<futures::lock::Mutex<Option<Arc<Sorted>>>>,
}

impl Stages {
    fn new(joins: usize) -> Self {
        Self {
            staging: std::sync::Mutex::new(None),
            built: (0..joins)
                .map(|_| futures::lock::Mutex::new(None))
                .collect(),
        }
    }
}

#[derive(Clone)]
struct PlanTarget {
    table: String,
    /// Index of the `parent:` target, which comes earlier.
    parent: Option<usize>,
    unnest: Option<Unnest>,
    /// `unnest: <many alias>`: index of the join.
    many: Option<usize>,
    /// [`Scope::input_schema`](crate::expr::Scope::input_schema) of the
    /// target's scope.
    input: SchemaRef,
    filter: Vec<Predicate>,
    columns: Vec<PlanColumn>,
    output: SchemaRef,
}

#[derive(Clone)]
struct Unnest {
    expr: Compiled,
    missing_prefix: Option<String>,
}

#[derive(Clone)]
enum PlanColumn {
    /// `columns: "*"`: the source field of the same name.
    Copy(CanonicalField),
    Expr(CanonicalField, Compiled),
    /// An array built from a `many` join.
    Nest(CanonicalField, Nest),
}

/// The result of one batch: what the runner gets, plus which records the
/// flow `where:` filtered and why, for the run report (A8).
#[derive(Debug, Clone, Default)]
pub struct Transformed {
    pub output: TransformOutput,
    pub filtered: Vec<Filtered>,
}

/// A record the flow `where:` filtered out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Filtered {
    /// Index into [`TransformOutput::records`].
    pub index: u32,
    /// The first predicate that failed: its name, or the expression text of
    /// an unnamed `where:` (0003 §5.6).
    pub reason: String,
}

impl TransformPlan for FlowPlan {
    fn inputs(&self) -> &[String] {
        &self.inner.inputs
    }

    fn write_order(&self) -> &[String] {
        &self.inner.write_order
    }

    fn execute(&self, mut inputs: PartitionInputs) -> ferry_core::Result<TransformStream> {
        let input = inputs.take(self.inner.entity.name())?;
        let plan = self.clone();
        if self.inner.joins.is_empty() {
            return Ok(input
                .map(move |batch| Ok(plan.transform_batch(&batch?)?.output))
                .boxed());
        }
        let mut co_partitioned = Vec::new();
        for (j, join) in self.inner.joins.iter().enumerate() {
            if let Strategy::CoPartition { .. } = join.plan.strategy {
                co_partitioned.push((j, inputs.take(join.plan.entity.name())?));
            }
        }
        // The joined sides first (this partition's rows of each
        // co-partitioned input, and the stages), then the driver.
        let joined = async move {
            let sides = plan.sides(co_partitioned).await?;
            Ok::<_, FerryError>(input.map(move |batch| {
                Ok(Run::new(&plan.inner, batch?, Some(&sides))?
                    .finish()?
                    .output)
            }))
        };
        Ok(futures::stream::once(joined).try_flatten().boxed())
    }
}

impl FlowPlan {
    /// The driver entity, the plan's first input.
    pub fn entity(&self) -> &Entity {
        &self.inner.entity
    }

    /// The joined `from:` entries, in order, each with its strategy and
    /// why (0003 §5.2): what `ferry plan` shows. The plan's inputs after
    /// the driver ([`TransformPlan::inputs`]) are the co-partitioned ones,
    /// in this order; staged ones aren't inputs (see
    /// [`stage_with`](FlowPlan::stage_with)).
    pub fn joins(&self) -> impl Iterator<Item = &JoinPlan> {
        self.inner.joins.iter().map(|j| &j.plan)
    }

    /// Every `nest:` column's full ordering (0003 §5.8).
    pub fn nest_orders(&self) -> &[NestOrder] {
        &self.inner.nests
    }

    /// Gives the staged joins their sources and memory. Needed before the
    /// first partition runs when any join is staged, and checked here: every
    /// staged join's alias needs a source.
    pub fn stage_with(&self, staging: Staging) -> ferry_core::Result<()> {
        if let Some(join) = self
            .staged()
            .find(|j| !staging.sources.contains_key(&j.plan.alias))
        {
            return Err(FerryError::MissingInput {
                entity: format!(
                    "{} (the source of staged join `{}`)",
                    join.plan.reference, join.plan.alias
                ),
            });
        }
        *self.inner.stages.staging.lock().expect("staging lock") = Some(staging);
        Ok(())
    }

    /// The same plan with every co-partitioned join staged instead: what
    /// the planner falls back to when a source can't split on the driver's
    /// boundaries (0002 step 5). Fails if a join pinned `strategy:
    /// co_partition`.
    pub fn without_co_partitioning(&self) -> ferry_core::Result<FlowPlan> {
        let mut inner = (*self.inner).clone();
        for join in &mut inner.joins {
            let Strategy::CoPartition { column } = &join.plan.strategy else {
                continue;
            };
            if join.pinned {
                return Err(FerryError::CoPartitionUnsupported {
                    column: column.clone(),
                    reason: format!(
                        "join `{}` pins `strategy: co_partition`, so it can't be staged",
                        join.plan.alias
                    ),
                });
            }
            join.plan.strategy = Strategy::Stage;
            join.plan.reason =
                "staged: its source can't split on the driver's boundaries (0002 step 5)".into();
        }
        inner.inputs = inputs(&inner.entity, &inner.joins);
        let staging = self
            .inner
            .stages
            .staging
            .lock()
            .expect("staging lock")
            .clone();
        let stages = Stages::new(inner.joins.len());
        *stages.staging.lock().expect("staging lock") = staging;
        inner.stages = Arc::new(stages);
        Ok(FlowPlan {
            inner: Arc::new(inner),
        })
    }

    fn staged(&self) -> impl Iterator<Item = &Join> {
        self.inner
            .joins
            .iter()
            .filter(|j| j.plan.strategy == Strategy::Stage)
    }

    /// Transforms one batch of driver records, laid out as
    /// [`Entity::arrow_schema`]. Fails only when the batch itself is wrong
    /// (columns that don't match, a null key); a bad value rejects its
    /// record instead. A merge reads its joined inputs too, so it runs
    /// through [`TransformPlan::execute`] only.
    pub fn transform_batch(&self, batch: &RecordBatch) -> ferry_core::Result<Transformed> {
        if !self.inner.joins.is_empty() {
            return Err(internal(format!(
                "flow `{}` is a merge: it runs through `execute`, which reads its joined inputs",
                self.inner.name
            )));
        }
        Run::new(&self.inner, batch.clone(), None)?.finish()
    }

    /// Each join's side for one partition, in join order.
    async fn sides(
        &self,
        co_partitioned: Vec<(usize, ferry_core::BatchStream)>,
    ) -> ferry_core::Result<Sides> {
        let mut streams: BTreeMap<usize, ferry_core::BatchStream> =
            co_partitioned.into_iter().collect();
        let mut sides = Vec::with_capacity(self.inner.joins.len());
        for (j, join) in self.inner.joins.iter().enumerate() {
            let side = match join.plan.strategy {
                Strategy::CoPartition { .. } => {
                    let mut stream =
                        streams.remove(&j).ok_or_else(|| FerryError::MissingInput {
                            entity: join.plan.entity.name().to_owned(),
                        })?;
                    let mut batches = Vec::new();
                    while let Some(batch) = stream.next().await {
                        batches.push(batch?);
                    }
                    Arc::new(Sorted::in_memory(Arc::clone(&join.keyed), &batches)?)
                }
                Strategy::Stage => self.stage(j).await?,
            };
            sides.push(side);
        }
        Ok(sides)
    }

    /// Staged join `j`, built by the first partition that asks.
    async fn stage(&self, j: usize) -> ferry_core::Result<Arc<Sorted>> {
        let join = &self.inner.joins[j];
        let mut built = self.inner.stages.built[j].lock().await;
        if let Some(sorted) = &*built {
            return Ok(Arc::clone(sorted));
        }
        let staging = self
            .inner
            .stages
            .staging
            .lock()
            .expect("staging lock")
            .clone();
        let missing = || FerryError::MissingInput {
            entity: format!(
                "{} (staged join `{}` has no source: see FlowPlan::stage_with)",
                join.plan.reference, join.plan.alias
            ),
        };
        let staging = staging.ok_or_else(missing)?;
        let source = staging.sources.get(&join.plan.alias).ok_or_else(missing)?;
        let memory = staging.memory / self.staged().count().max(1) as u64;
        let dir = staging.dir.clone().unwrap_or_else(std::env::temp_dir);
        let sorted =
            Arc::new(Sorted::stage(Arc::clone(&join.keyed), source.as_ref(), memory, &dir).await?);
        *built = Some(Arc::clone(&sorted));
        Ok(sorted)
    }
}

fn internal(reason: impl Into<String>) -> FerryError {
    FerryError::InvalidTransformOutput {
        reason: reason.into(),
    }
}

/// The rows a target produced for one batch.
struct Rows {
    /// The batch its expressions were evaluated on, one row per row.
    input: RecordBatch,
    /// Index into the records, per row.
    record: Vec<u32>,
    /// The unnest path to each row; empty for a row that isn't an element.
    steps: Vec<Arc<[Step]>>,
    /// The mapped columns, as the target's output schema.
    mapped: Vec<ArrayRef>,
}

impl Rows {
    fn len(&self) -> usize {
        self.record.len()
    }

    /// Keeps the rows at `keep`, in that order.
    fn take(&self, keep: &[u32]) -> ferry_core::Result<Rows> {
        let indices = UInt32Array::from(keep.to_vec());
        Ok(Rows {
            input: take_batch(&self.input, &indices)?,
            record: keep.iter().map(|&i| self.record[i as usize]).collect(),
            steps: keep
                .iter()
                .map(|&i| Arc::clone(&self.steps[i as usize]))
                .collect(),
            mapped: self
                .mapped
                .iter()
                .map(|a| take(a.as_ref(), &indices))
                .collect::<ferry_core::Result<_>>()?,
        })
    }
}

fn take(array: &dyn Array, indices: &UInt32Array) -> ferry_core::Result<ArrayRef> {
    arrow_select::take::take(array, indices, None).map_err(|e| internal(e.to_string()))
}

fn take_batch(batch: &RecordBatch, indices: &UInt32Array) -> ferry_core::Result<RecordBatch> {
    let columns = batch
        .columns()
        .iter()
        .map(|c| take(c.as_ref(), indices))
        .collect::<ferry_core::Result<Vec<_>>>()?;
    batch_of(batch.schema(), columns, indices.len())
}

/// The columns of `batch` that `schema` names, in its order: the batch an
/// expression of that scope is evaluated on.
fn scoped(batch: &RecordBatch, schema: &SchemaRef) -> ferry_core::Result<RecordBatch> {
    let columns = schema
        .fields()
        .iter()
        .map(|f| {
            batch
                .column_by_name(f.name())
                .cloned()
                .ok_or_else(|| internal(format!("no column `{}` in scope", f.name())))
        })
        .collect::<ferry_core::Result<Vec<_>>>()?;
    batch_of(Arc::clone(schema), columns, batch.num_rows())
}

fn batch_of(
    schema: SchemaRef,
    columns: Vec<ArrayRef>,
    rows: usize,
) -> ferry_core::Result<RecordBatch> {
    RecordBatch::try_new_with_options(
        schema,
        columns,
        &RecordBatchOptions::new().with_row_count(Some(rows)),
    )
    .map_err(|e| internal(e.to_string()))
}

/// `items[1].options[0]`, for messages.
fn describe(steps: &[Step]) -> String {
    steps
        .iter()
        .map(|s| format!("{}[{}]", s.path, s.index))
        .collect::<Vec<_>>()
        .join(".")
}

fn at(steps: &[Step]) -> String {
    if steps.is_empty() {
        String::new()
    } else {
        format!(" (at {})", describe(steps))
    }
}

/// The state of one batch going through a plan.
struct Run<'a> {
    plan: &'a Inner,
    /// The driver records, plus a `_ferry_join_<alias>` column per `one`
    /// join: the driver scope's columns.
    batch: RecordBatch,
    records: Vec<SourceRecordId>,
    /// The first error of each rejected record.
    rejected: BTreeMap<u32, RejectedRecord>,
    filtered: Vec<Filtered>,
    /// Driver rows the flow `where:` kept.
    kept: Vec<u32>,
    /// Per target, in write order.
    rows: Vec<Rows>,
    /// Per join, the matches of a `many` join.
    many: Vec<Option<Matched>>,
}

impl<'a> Run<'a> {
    fn new(plan: &'a Inner, batch: RecordBatch, sides: Option<&Sides>) -> ferry_core::Result<Self> {
        let expected = plan.entity.arrow_schema();
        if batch.schema().fields() != expected.fields() {
            return Err(internal(format!(
                "input batch does not match entity `{}`",
                plan.entity.name()
            )));
        }
        let records = plan.entity.record_ids(&batch)?;
        let mut run = Self {
            plan,
            batch,
            records,
            rejected: BTreeMap::new(),
            filtered: Vec::new(),
            kept: Vec::new(),
            rows: Vec::new(),
            many: (0..plan.joins.len()).map(|_| None).collect(),
        };
        if let Some(sides) = sides {
            run.join(sides)?;
        }
        Ok(run)
    }

    fn reject(&mut self, record: u32, target: Option<&str>, error: String) {
        self.rejected.entry(record).or_insert(RejectedRecord {
            index: record,
            target: target.map(str::to_owned),
            error,
        });
    }

    fn is_rejected(&self, record: u32) -> bool {
        self.rejected.contains_key(&record)
    }

    fn finish(mut self) -> ferry_core::Result<Transformed> {
        self.flow_filter()?;
        for t in 0..self.plan.targets.len() {
            let rows = self.target(t)?;
            self.rows.push(rows);
        }
        let mut targets = Vec::new();
        for (t, rows) in self.plan.targets.iter().zip(&self.rows) {
            let keep: Vec<u32> = (0..rows.len() as u32)
                .filter(|&i| !self.is_rejected(rows.record[i as usize]))
                .collect();
            if keep.is_empty() {
                continue;
            }
            let rows = rows.take(&keep)?;
            targets.push(TargetBatch {
                target: t.table.clone(),
                batch: batch_of(Arc::clone(&t.output), rows.mapped, rows.record.len())?,
                source_index: rows.record,
            });
        }
        let output = TransformOutput {
            records: self.records,
            targets,
            rejected: self.rejected.into_values().collect(),
        };
        output.validate(&self.plan.write_order)?;
        Ok(Transformed {
            output,
            filtered: self.filtered,
        })
    }

    /// Evaluates `predicates` on `input`: per row, `Ok(None)` when every one
    /// holds, `Ok(Some(reason))` for the first that is false or null, and
    /// `Err` for one that errors before any fails.
    fn predicates(
        predicates: &[Predicate],
        input: &RecordBatch,
    ) -> Vec<Result<Option<String>, String>> {
        let evaluated: Vec<_> = predicates
            .iter()
            .map(|p| {
                let (values, errors) = p.expr.evaluate_rows(input);
                let errors: BTreeMap<usize, String> =
                    errors.into_iter().map(|e| (e.row, e.message)).collect();
                (p, values, errors)
            })
            .collect();
        (0..input.num_rows())
            .map(|row| {
                for (p, values, errors) in &evaluated {
                    let reason = || p.name.clone().unwrap_or_else(|| p.expr.text().to_owned());
                    if let Some(message) = errors.get(&row) {
                        return Err(format!("where `{}`: {message}", reason()));
                    }
                    let holds = values
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .is_some_and(|b| b.is_valid(row) && b.value(row));
                    if !holds {
                        return Ok(Some(reason()));
                    }
                }
                Ok(None)
            })
            .collect()
    }

    fn flow_filter(&mut self) -> ferry_core::Result<()> {
        let Some(first) = self.plan.filter.first() else {
            self.kept = (0..self.batch.num_rows() as u32).collect();
            return Ok(());
        };
        let input = scoped(&self.batch, first.expr.input_schema())?;
        let results = Self::predicates(&self.plan.filter, &input);
        for (row, result) in results.into_iter().enumerate() {
            let record = row as u32;
            // A join already rejected it.
            if self.is_rejected(record) {
                continue;
            }
            match result {
                Ok(None) => self.kept.push(record),
                Ok(Some(reason)) => self.filtered.push(Filtered {
                    index: record,
                    reason,
                }),
                Err(error) => self.reject(record, None, error),
            }
        }
        Ok(())
    }

    /// The rows of target `t`: built from where they come from, then its
    /// `where:`, then its columns.
    fn target(&mut self, t: usize) -> ferry_core::Result<Rows> {
        let target = &self.plan.targets[t];
        let mut rows = self.candidates(t)?;

        // The target's `where:`, on source values.
        if !target.filter.is_empty() {
            let results = Self::predicates(&target.filter, &rows.input);
            let mut keep = Vec::with_capacity(rows.len());
            for (row, result) in results.into_iter().enumerate() {
                match result {
                    Ok(None) => keep.push(row as u32),
                    Ok(Some(_)) => {}
                    Err(error) => {
                        let error = format!("{}: {error}{}", target.table, at(&rows.steps[row]));
                        self.reject(rows.record[row], Some(&target.table), error);
                    }
                }
            }
            rows = rows.take(&keep)?;
        }
        rows = self.without_rejected(rows)?;

        let mut mapped = Vec::with_capacity(target.columns.len());
        for column in &target.columns {
            let (field, values) = match column {
                PlanColumn::Copy(field) => {
                    let values = rows
                        .input
                        .column_by_name(&field.name)
                        .cloned()
                        .ok_or_else(|| internal(format!("no source field `{}`", field.name)))?;
                    (field, values)
                }
                PlanColumn::Expr(field, expr) => {
                    let (values, errors) = expr.evaluate_rows(&rows.input);
                    for e in errors {
                        let error = format!(
                            "{}.{} = `{}`: {}{}",
                            target.table,
                            field.name,
                            expr.text(),
                            e.message,
                            at(&rows.steps[e.row])
                        );
                        self.reject(rows.record[e.row], Some(&target.table), error);
                    }
                    (field, values)
                }
                PlanColumn::Nest(field, nest) => {
                    let values = self.nest_column(&target.table, field, nest, &rows.record)?;
                    (field, values)
                }
            };
            if !field.nullable && values.null_count() > 0 {
                for row in (0..values.len()).filter(|&r| values.is_null(r)) {
                    let record = rows.record[row];
                    let shown = match column {
                        PlanColumn::Expr(_, e) => format!("`{}`", e.text()),
                        PlanColumn::Copy(_) => "the source value".to_owned(),
                        PlanColumn::Nest(..) => "the nested array".to_owned(),
                    };
                    let error = format!(
                        "{}.{} can't be null, but {shown} is null{}",
                        target.table,
                        field.name,
                        at(&rows.steps[row])
                    );
                    self.reject(record, Some(&target.table), error);
                }
            }
            mapped.push(values);
        }
        rows.mapped = mapped;
        // Children read this target's mapped rows through `$parent`, so the
        // rows of records rejected here go before any child is built.
        self.without_rejected(rows)
    }

    fn without_rejected(&self, rows: Rows) -> ferry_core::Result<Rows> {
        let keep: Vec<u32> = (0..rows.len() as u32)
            .filter(|&i| !self.is_rejected(rows.record[i as usize]))
            .collect();
        if keep.len() == rows.len() {
            return Ok(rows);
        }
        rows.take(&keep)
    }

    /// The rows above target `t`'s: its parent's, or the kept driver
    /// records, without those of rejected records; and for a child, each
    /// one's row in the parent.
    fn outer(&self, target: &PlanTarget) -> ferry_core::Result<(Rows, Vec<u32>)> {
        match target.parent {
            Some(p) => {
                let parent = &self.rows[p];
                let keep: Vec<u32> = (0..parent.len() as u32)
                    .filter(|&i| !self.is_rejected(parent.record[i as usize]))
                    .collect();
                let indices = UInt32Array::from(keep.clone());
                let rows = Rows {
                    input: take_batch(&parent.input, &indices)?,
                    record: keep.iter().map(|&i| parent.record[i as usize]).collect(),
                    steps: keep
                        .iter()
                        .map(|&i| Arc::clone(&parent.steps[i as usize]))
                        .collect(),
                    mapped: Vec::new(),
                };
                Ok((rows, keep))
            }
            None => {
                let keep: Vec<u32> = self
                    .kept
                    .iter()
                    .copied()
                    .filter(|&r| !self.is_rejected(r))
                    .collect();
                let indices = UInt32Array::from(keep.clone());
                let rows = Rows {
                    input: take_batch(&self.batch, &indices)?,
                    steps: vec![Arc::from(Vec::new()); keep.len()],
                    record: keep,
                    mapped: Vec::new(),
                };
                Ok((rows, Vec::new()))
            }
        }
    }

    /// Where target `t`'s rows come from, before its `where:`.
    fn candidates(&mut self, t: usize) -> ferry_core::Result<Rows> {
        let target = &self.plan.targets[t];
        let (outer_rows, parent_rows) = self.outer(target)?;
        let Rows {
            input: outer,
            record: outer_record,
            steps: outer_steps,
            ..
        } = outer_rows;

        if let Some(j) = target.many {
            return self.joined_rows(t, j, outer_record, outer_steps, &parent_rows);
        }
        let Some(unnest) = &target.unnest else {
            // One row per row above. The root is the driver record.
            let driver_rows = UInt32Array::from(outer_record.clone());
            let root = Root::Driver(take_batch(&self.batch, &driver_rows)?);
            let parent = self.parent_struct(target, &parent_rows)?;
            let input = self.input(target, &root, parent, None, &outer_record, &outer_steps)?;
            return Ok(Rows {
                input,
                record: outer_record,
                steps: outer_steps,
                mapped: Vec::new(),
            });
        };

        // One row per element of the array in each outer row.
        let (values, errors) = unnest.expr.evaluate_rows(&outer);
        for e in &errors {
            let error = format!(
                "{}: `unnest: {}`: {}{}",
                target.table,
                unnest.expr.text(),
                e.message,
                at(&outer_steps[e.row])
            );
            self.reject(outer_record[e.row], Some(&target.table), error);
        }
        let failed: Vec<usize> = errors.iter().map(|e| e.row).collect();
        let elements = match values.data_type() {
            DataType::List(_) => self.list_elements(&values, &failed),
            DataType::Utf8 => {
                self.json_elements(target, &values, &failed, &outer_record, &outer_steps)
            }
            other => {
                return Err(internal(format!(
                    "`unnest: {}` evaluated to {other}",
                    unnest.expr.text()
                )));
            }
        }?;

        let record: Vec<u32> = elements
            .outer
            .iter()
            .map(|&o| outer_record[o as usize])
            .collect();
        let steps: Vec<Arc<[Step]>> = elements
            .outer
            .iter()
            .zip(&elements.index)
            .map(|(&o, &index)| {
                let mut steps = outer_steps[o as usize].to_vec();
                steps.push(Step {
                    path: unnest.expr.text().trim().to_owned(),
                    index: index as u64,
                });
                Arc::from(steps)
            })
            .collect();
        let missing = self.element_missing(unnest, &outer, &elements)?;
        let root = Root::Element {
            item: elements.item.clone(),
            missing,
        };
        let parent_of_element: Vec<u32> = match target.parent {
            Some(_) => elements
                .outer
                .iter()
                .map(|&o| parent_rows[o as usize])
                .collect(),
            None => Vec::new(),
        };
        let parent = self.parent_struct(target, &parent_of_element)?;
        let sigils = Sigils {
            index: Arc::new(Int64Array::from(elements.index.clone())),
            item: elements.item,
        };
        let input = self.input(target, &root, parent, Some(sigils), &record, &steps)?;
        Ok(Rows {
            input,
            record,
            steps,
            mapped: Vec::new(),
        })
    }

    /// `unnest: <many alias>`: one row per joined row of each outer row's
    /// driver record, in the joined entity's primary-key order. The root
    /// is the joined row.
    fn joined_rows(
        &mut self,
        t: usize,
        j: usize,
        outer_record: Vec<u32>,
        outer_steps: Vec<Arc<[Step]>>,
        parent_rows: &[u32],
    ) -> ferry_core::Result<Rows> {
        let target = &self.plan.targets[t];
        let alias = &self.plan.joins[j].plan.alias;
        let matched = self.matched(j)?;
        let mut record = Vec::new();
        let mut steps = Vec::new();
        let mut index = Vec::new();
        let mut take_from = Vec::new();
        let mut parent = Vec::new();
        for (o, &r) in outer_record.iter().enumerate() {
            for (i, m) in matched.range(r as usize).enumerate() {
                record.push(r);
                index.push(i as i64);
                take_from.push(m as u32);
                let mut path = outer_steps[o].to_vec();
                path.push(Step {
                    path: alias.clone(),
                    index: i as u64,
                });
                steps.push(Arc::from(path));
                if target.parent.is_some() {
                    parent.push(parent_rows[o]);
                }
            }
        }
        let root = Root::Driver(take_batch(&matched.rows, &UInt32Array::from(take_from))?);
        let parent = self.parent_struct(target, &parent)?;
        let sigils = Sigils {
            index: Arc::new(Int64Array::from(index)),
            item: None,
        };
        let input = self.input(target, &root, parent, Some(sigils), &record, &steps)?;
        Ok(Rows {
            input,
            record,
            steps,
            mapped: Vec::new(),
        })
    }

    /// The elements of a list-typed `unnest:` value. A null list (missing
    /// or explicitly null) has none, like an empty one.
    fn list_elements(&self, values: &ArrayRef, failed: &[usize]) -> ferry_core::Result<Elements> {
        let lists: &ListArray = values.as_list::<i32>();
        let mut outer = Vec::new();
        let mut index = Vec::new();
        let mut take_from = Vec::new();
        for row in 0..lists.len() {
            if lists.is_null(row) || failed.contains(&row) {
                continue;
            }
            let start = lists.value_offsets()[row] as u32;
            for i in 0..lists.value_length(row) as u32 {
                outer.push(row as u32);
                index.push(i64::from(i));
                take_from.push(start + i);
            }
        }
        let item = take(lists.values().as_ref(), &UInt32Array::from(take_from))?;
        Ok(Elements {
            outer,
            index,
            item: Some(item),
        })
    }

    /// The elements of a variant (`json`) value. A JSON array gives one
    /// element per entry, as JSON text; null gives none; anything else is
    /// an error row.
    fn json_elements(
        &mut self,
        target: &PlanTarget,
        values: &ArrayRef,
        failed: &[usize],
        outer_record: &[u32],
        outer_steps: &[Arc<[Step]>],
    ) -> ferry_core::Result<Elements> {
        let text = values.as_string::<i32>();
        let mut outer = Vec::new();
        let mut index = Vec::new();
        let mut item = StringBuilder::new();
        let unnest = target.unnest.as_ref().expect("an unnest target");
        for row in 0..text.len() {
            if text.is_null(row) || failed.contains(&row) {
                continue;
            }
            let kind = match serde_json::from_str::<serde_json::Value>(text.value(row)) {
                Ok(serde_json::Value::Array(entries)) => {
                    for (i, entry) in entries.iter().enumerate() {
                        outer.push(row as u32);
                        index.push(i as i64);
                        item.append_value(entry.to_string());
                    }
                    continue;
                }
                Ok(serde_json::Value::Null) => continue,
                Ok(serde_json::Value::Bool(_)) => "a boolean",
                Ok(serde_json::Value::Number(_)) => "a number",
                Ok(serde_json::Value::String(_)) => "a string",
                Ok(serde_json::Value::Object(_)) => "an object",
                Err(_) => "not JSON",
            };
            let error = format!(
                "{}: `unnest: {}` needs an array, but the value is {kind}{}",
                target.table,
                unnest.expr.text(),
                at(&outer_steps[row])
            );
            self.reject(outer_record[row], Some(&target.table), error);
        }
        Ok(Elements {
            outer,
            index,
            item: Some(Arc::new(item.finish())),
        })
    }

    /// Each element's missing paths, relative to the element: the outer
    /// row's paths under `<path>.<index>.`, with that prefix removed.
    fn element_missing(
        &self,
        unnest: &Unnest,
        outer: &RecordBatch,
        elements: &Elements,
    ) -> ferry_core::Result<Option<ArrayRef>> {
        if !self.plan.entity.tracks_missing() {
            return Ok(None);
        }
        let lists = outer
            .column_by_name(MISSING_PATHS_COLUMN)
            .and_then(|c| c.as_any().downcast_ref::<ListArray>());
        let mut builder = MissingPathsBuilder::new();
        for (&o, &index) in elements.outer.iter().zip(&elements.index) {
            let mut paths = Vec::new();
            if let (Some(lists), Some(prefix)) = (lists, &unnest.missing_prefix)
                && lists.is_valid(o as usize)
            {
                let prefix = format!("{prefix}.{index}.");
                let row = lists.value(o as usize);
                for path in row.as_string::<i32>().iter().flatten() {
                    if let Some(rest) = path.strip_prefix(&prefix) {
                        paths.push(rest.to_owned());
                    }
                }
            }
            builder.append_row(&paths);
        }
        Ok(Some(builder.finish()))
    }

    /// `$parent` for each row: the parent target's mapped row at
    /// `parent_rows`, as a struct. `None` for a target without `parent:`.
    fn parent_struct(
        &self,
        target: &PlanTarget,
        parent_rows: &[u32],
    ) -> ferry_core::Result<Option<ArrayRef>> {
        let Some(p) = target.parent else {
            return Ok(None);
        };
        let parent = &self.rows[p];
        let fields: Fields = self.plan.targets[p].output.fields().clone();
        let all = StructArray::try_new(fields, parent.mapped.clone(), None)
            .map_err(|e| internal(format!("`$parent` of `{}`: {e}", target.table)))?;
        Ok(Some(take(&all, &UInt32Array::from(parent_rows.to_vec()))?))
    }

    /// The batch a target's expressions are evaluated on, column by column
    /// as its scope lays it out.
    fn input(
        &self,
        target: &PlanTarget,
        root: &Root,
        parent: Option<ArrayRef>,
        sigils: Option<Sigils>,
        record: &[u32],
        steps: &[Arc<[Step]>],
    ) -> ferry_core::Result<RecordBatch> {
        let rows = record.len();
        let missing_column = |name: &str| internal(format!("`{}` has no `{name}`", target.table));
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(target.input.fields().len());
        for field in target.input.fields() {
            let name = field.name().as_str();
            let column = match name {
                PARENT_COLUMN => parent.clone().ok_or_else(|| missing_column(name))?,
                INDEX_COLUMN => sigils
                    .as_ref()
                    .map(|s| Arc::clone(&s.index) as ArrayRef)
                    .ok_or_else(|| missing_column(name))?,
                ITEM_COLUMN => sigils
                    .as_ref()
                    .and_then(|s| s.item.clone())
                    .ok_or_else(|| missing_column(name))?,
                KEY_COLUMN => {
                    let mut keys = FixedSizeBinaryBuilder::with_capacity(rows, 16);
                    for (&r, steps) in record.iter().zip(steps) {
                        let key = key::child_key(&self.records[r as usize], steps);
                        keys.append_value(key.as_bytes())
                            .map_err(|e| internal(e.to_string()))?;
                    }
                    Arc::new(keys.finish())
                }
                _ => root.column(name).ok_or_else(|| missing_column(name))?,
            };
            columns.push(column);
        }
        batch_of(Arc::clone(&target.input), columns, rows)
    }
}

/// What bare names resolve to.
enum Root {
    /// The driver record: the driver batch's columns, one row per row.
    Driver(RecordBatch),
    /// The current element: a struct's fields (null where the element is
    /// null), and its missing paths.
    Element {
        item: Option<ArrayRef>,
        missing: Option<ArrayRef>,
    },
}

impl Root {
    fn column(&self, name: &str) -> Option<ArrayRef> {
        match self {
            Root::Driver(batch) => batch.column_by_name(name).cloned(),
            Root::Element { missing, .. } if name == MISSING_PATHS_COLUMN => missing.clone(),
            Root::Element { item, .. } => {
                let structs = item.as_ref()?.as_struct_opt()?;
                let child = structs.column_by_name(name)?;
                // A null element has null fields, whatever its child arrays
                // hold underneath.
                let nulls = NullBuffer::union(structs.nulls(), child.nulls());
                let data = child.to_data().into_builder().nulls(nulls).build().ok()?;
                Some(make_array(data))
            }
        }
    }
}

struct Elements {
    /// Per element, its row in the outer batch.
    outer: Vec<u32>,
    /// Per element, `$index`.
    index: Vec<i64>,
    /// The elements themselves (`$item`).
    item: Option<ArrayRef>,
}

struct Sigils {
    index: ArrayRef,
    item: Option<ArrayRef>,
}
