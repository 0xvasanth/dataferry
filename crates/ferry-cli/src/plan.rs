//! `ferry plan` (B7): `validate`, then discover every source the flows
//! read and ask every sink for its capabilities, then plan (ferry-dsl's
//! planner) and print the flows in run order with their strategies,
//! partitioning, joins, targets and the DDL each sink would run. Writes
//! nothing: discovery and capabilities only read, and the DDL is rendered,
//! never run. `run`, `resume` and `retry` start from the same plan.
//!
//! - A flow may name an entity with its connection's namespace
//!   (`app.public.orders` for the `orders` a Postgres connection on schema
//!   `public` discovers, see [`namespaces`](crate::namespaces)).
//! - A merge's co-partitioned joins are checked with their sources before
//!   the plan is shown (0002 step 5): one whose source can't split on the
//!   driver's boundaries is staged instead, and the plan says why, unless
//!   it pins `strategy: co_partition`, which refuses the plan.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::path::Path;

use ferry_core::{IdempotencyStrategy, TargetSchema, TransformPlan, WriteMode};
use ferry_dsl::compile::Strategy;
use ferry_dsl::config::model::{Cardinality, EntityRef, Where};
use ferry_dsl::config::{Config, ConfigErrors, Flow};
use ferry_dsl::plan::{Plan, PlannedFlow, SettleError, StatePlacement};
use ferry_dsl::typecheck::Catalog;

use crate::Ferry;
use crate::diagnostic::{Diagnostic, Redaction, code};
use crate::exit::Exit;
use crate::validate::{self, Summary};

/// A config that validated and planned.
pub(crate) struct Planned {
    pub summary: Summary,
    pub plan: Plan,
}

/// What stopped planning: the diagnostics to print and the exit code.
pub(crate) struct Refused {
    pub diagnostics: Vec<Diagnostic>,
    pub exit: Exit,
}

/// Validates, discovers and plans `dir`. Warnings come back either way.
pub(crate) async fn plan(
    ferry: &Ferry,
    dir: &Path,
    env: &dyn Fn(&str) -> Option<String>,
    redaction: &mut Redaction,
) -> Result<(Planned, Vec<Diagnostic>), Refused> {
    let report = validate::validate(&ferry.registry, dir, env, redaction);
    let exit = report.exit();
    let mut diagnostics = report.diagnostics;
    let Some(summary) = report.valid else {
        return Err(Refused { diagnostics, exit });
    };
    let config = &summary.config;

    let connect = |diagnostics: &mut Vec<Diagnostic>, d: Diagnostic| {
        diagnostics.push(d);
        Exit::Connect
    };
    let mut catalog = Catalog::new();
    let mut failed = None;
    for name in read_connections(config) {
        let Some(source) = summary.endpoints.get(name).and_then(|e| e.source.as_ref()) else {
            continue;
        };
        match source.discover().await {
            Ok(entities) => {
                let namespace = summary
                    .raw
                    .get(name)
                    .and_then(|raw| ferry.namespaces.of(raw));
                qualify(&mut catalog, config, name, namespace.as_deref(), &entities);
                for entity in entities {
                    catalog.insert(name, entity);
                }
            }
            Err(e) => {
                failed = Some(connect(
                    &mut diagnostics,
                    Diagnostic::error(
                        code::CONNECT,
                        format!("could not discover the entities of `{name}`: {e}"),
                    )
                    .path(format!("connections[{name}]"))
                    .at(config.connection(name).map(|c| c.name.location())),
                ))
            }
        }
    }
    let mut sinks = BTreeMap::new();
    for name in sink_connections(config) {
        let Some(sink) = summary.endpoints.get(name).and_then(|e| e.sink.as_ref()) else {
            continue;
        };
        match sink.capabilities().await {
            Ok(caps) => {
                sinks.insert(name.to_owned(), caps);
            }
            Err(e) => {
                failed = Some(connect(
                    &mut diagnostics,
                    Diagnostic::error(
                        code::CONNECT,
                        format!("could not read the capabilities of the `{name}` sink: {e}"),
                    )
                    .path(format!("connections[{name}]"))
                    .at(config.connection(name).map(|c| c.name.location())),
                ))
            }
        }
    }
    if let Some(exit) = failed {
        return Err(Refused { diagnostics, exit });
    }

    let mut plan = match ferry_dsl::plan::plan(config, &catalog, &sinks) {
        Ok(plan) => plan,
        Err(ConfigErrors(errors)) => {
            diagnostics.extend(errors.into_iter().map(Diagnostic::from));
            return Err(Refused {
                diagnostics,
                exit: Exit::PlanRefused,
            });
        }
    };
    let sources = |name: &str| summary.endpoints.get(name).and_then(|e| e.source.clone());
    match plan.settle_co_partitioning(config, &sources).await {
        Ok(()) => {}
        Err(SettleError::Refused(ConfigErrors(errors))) => {
            diagnostics.extend(errors.into_iter().map(Diagnostic::from));
            return Err(Refused {
                diagnostics,
                exit: Exit::PlanRefused,
            });
        }
        Err(SettleError::Source { connection, error }) => {
            diagnostics.push(
                Diagnostic::error(
                    code::CONNECT,
                    format!(
                        "could not ask `{connection}` whether it can split a joined entity on \
                         the driver's boundaries: {error}"
                    ),
                )
                .path(format!("connections[{connection}]"))
                .at(config.connection(&connection).map(|c| c.name.location())),
            );
            return Err(Refused {
                diagnostics,
                exit: Exit::Connect,
            });
        }
    }
    diagnostics.extend(
        plan.warnings
            .iter()
            .map(|w| Diagnostic::warning(code::PLAN_WARNING, w.clone())),
    );
    Ok((Planned { summary, plan }, diagnostics))
}

/// Makes `<connection>.<namespace>.<entity>` find what `connection`
/// discovered as `<entity>`, for every such reference in the config.
/// Only references the config makes are added, so an entity the config
/// never names qualified is listed once in a typo's suggestions (one it
/// does name is listed under both keys); a discovered name that has the
/// dots itself always wins.
fn qualify(
    catalog: &mut Catalog,
    config: &Config,
    connection: &str,
    namespace: Option<&str>,
    discovered: &[ferry_core::Entity],
) {
    let Some(namespace) = namespace else {
        return;
    };
    let qualified = format!("{connection}.{namespace}");
    for reference in references(config) {
        if reference.connection.as_str() != connection
            || discovered.iter().any(|e| e.name() == reference.entity)
        {
            continue;
        }
        let Some(name) = reference
            .entity
            .strip_prefix(namespace)
            .and_then(|rest| rest.strip_prefix('.'))
        else {
            continue;
        };
        if let Some(entity) = discovered.iter().find(|e| e.name() == name) {
            catalog.insert(&qualified, entity.clone());
        }
    }
}

/// Every entity the flows read: drivers and joined entries.
fn references(config: &Config) -> impl Iterator<Item = &EntityRef> {
    use ferry_dsl::config::model::From;
    config.flows.iter().flat_map(|f| match f.from.value() {
        From::Entity(e) => vec![e],
        From::Merge(entries) => entries.iter().map(|e| e.entity.value()).collect(),
    })
}

/// Every connection a flow reads from, in name order.
fn read_connections(config: &Config) -> BTreeSet<&str> {
    references(config).map(|e| e.connection.as_str()).collect()
}

fn sink_connections(config: &Config) -> BTreeSet<&str> {
    config.flows.iter().map(|f| f.sink.as_str()).collect()
}

/// The target schema a flow's sink prepares: its targets, plus the
/// dead-letter table under `on_error: dead_letter` (A7).
pub(crate) fn prepared_target(config: &Config, flow: &PlannedFlow) -> TargetSchema {
    match config.runtime.on_error {
        ferry_dsl::config::model::OnError::Fail => flow.compiled.target.clone(),
        ferry_dsl::config::model::OnError::DeadLetter => {
            let mut tables = flow.compiled.target.tables().to_vec();
            tables.push(ferry_runtime::dlq_table());
            // The planner refuses a target of that name, so this holds.
            TargetSchema::new(tables).unwrap_or_else(|_| flow.compiled.target.clone())
        }
    }
}

/// The plan as text, for people.
pub(crate) fn render(planned: &Planned) -> String {
    let Planned { summary, plan } = planned;
    let config = &summary.config;
    let mut out = String::new();
    let _ = writeln!(
        out,
        "plan for `{}`, config {}",
        plan.name,
        plan.hash.as_str()
    );
    let state_connection = match &plan.state {
        StatePlacement::File { path } => {
            let _ = writeln!(
                out,
                "state store: file, {}",
                path.as_deref()
                    .map_or("in the work directory".into(), |p| format!("in `{p}`"))
            );
            None
        }
        StatePlacement::Connection {
            backend,
            connection,
        } => {
            let _ = writeln!(out, "state store: {backend}, on connection `{connection}`");
            Some(connection.as_str())
        }
    };
    let _ = writeln!(
        out,
        "on_error: {}, max_errors: {}",
        config.runtime.on_error.as_str(),
        config
            .runtime
            .max_errors
            .map_or("unbounded".into(), |m| m.to_string())
    );

    let _ = writeln!(out, "\nconnections");
    let width = summary
        .connections
        .iter()
        .map(|c| c.0.len())
        .max()
        .unwrap_or(0);
    for (name, kind, roles, description) in &summary.connections {
        let state = match state_connection == Some(name.as_str()) {
            true => ", state store",
            false => "",
        };
        let _ = writeln!(
            out,
            "  {name:width$}  {kind} ({roles}{state}): {description}"
        );
    }

    let _ = writeln!(out, "\nrun order");
    for (i, flow) in plan.flows.iter().enumerate() {
        let waits = match flow.waits_for.is_empty() {
            true => String::new(),
            false => format!(
                ", after {}",
                flow.waits_for
                    .iter()
                    .map(|d| format!("`{}` ({})", d.flow, d.edge.describe()))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        };
        let _ = writeln!(out, "  {}. {}{waits}", i + 1, flow.name);
    }

    for flow in &plan.flows {
        let model = config
            .flow(&flow.name)
            .expect("planned flows are config flows");
        flow_section(&mut out, summary, flow, model);
    }
    out
}

fn flow_section(out: &mut String, summary: &Summary, flow: &PlannedFlow, model: &Flow) {
    let entity = flow.compiled.plan.entity();
    let _ = writeln!(out, "\nflow `{}`", flow.name);
    let _ = writeln!(
        out,
        "  from:        {} (driver, key {})",
        flow.driver,
        entity.primary_key().join(", ")
    );
    let _ = writeln!(
        out,
        "  sink:        {}, mode {}",
        flow.sink,
        match flow.mode() {
            WriteMode::Insert => "insert",
            WriteMode::Upsert => "upsert",
        }
    );
    let strategy = strategy_name(flow.strategy);
    match &flow.fallback {
        None => {
            let _ = writeln!(out, "  strategy:    {strategy}");
        }
        Some(why) => {
            let _ = writeln!(out, "  strategy:    {strategy} ({why})");
        }
    }
    let _ = writeln!(
        out,
        "  partitions:  about {} rows each ({}), stored as `{}/<partition>`",
        flow.partition_rows,
        flow.partition_rows_from.describe(),
        flow.name
    );
    joins_section(out, flow);
    if let Some(filter) = &model.filter {
        let _ = writeln!(out, "  where:       {}", where_text(filter.value()));
    }
    let _ = writeln!(
        out,
        "  write order: {}",
        flow.compiled.plan.write_order().join(" → ")
    );

    let _ = writeln!(out, "  targets:");
    for (table, checked) in flow
        .compiled
        .target
        .tables()
        .iter()
        .zip(&flow.checked.targets)
    {
        let mut about = Vec::new();
        if !table.primary_key.is_empty() {
            about.push(format!("key {}", table.primary_key.join(", ")));
        }
        if let Some(parent) = &checked.parent {
            about.push(format!("parent {parent}"));
        }
        if let Some(unnest) = &checked.unnest {
            about.push(format!("unnest {}", unnest.text()));
        }
        let about = match about.is_empty() {
            true => String::new(),
            false => format!(" ({})", about.join("; ")),
        };
        let _ = writeln!(out, "    {}{about}", table.name);
        let width = table.fields.iter().map(|f| f.name.len()).max().unwrap_or(0);
        for field in &table.fields {
            let null = match field.nullable {
                true => ", nullable",
                false => "",
            };
            let _ = writeln!(
                out,
                "      {:width$}  {}{null}",
                field.name,
                ferry_dsl::types::name(&field.ty)
            );
        }
        for fk in &table.foreign_keys {
            let _ = writeln!(
                out,
                "      foreign key ({}) → {} ({})",
                fk.columns.join(", "),
                fk.references_table,
                fk.references_columns.join(", ")
            );
        }
    }

    if !flow.checked.expect.is_empty() {
        let _ = writeln!(out, "  expect:");
        for a in &flow.checked.expect.assertions {
            let _ = writeln!(out, "    {}: {}", a.name, a.text);
        }
    }

    let target = prepared_target(&summary.config, flow);
    let rendered = summary
        .endpoints
        .get(&flow.sink)
        .and_then(|e| e.ddl.as_ref())
        .map(|ddl| ddl.render_ddl(&target, flow.mode()));
    match rendered {
        Some(statements) => {
            let _ = writeln!(out, "  ddl (not run by plan):");
            for statement in statements {
                for line in format!("{statement};").lines() {
                    let _ = writeln!(out, "    {line}");
                }
            }
        }
        None => {
            let _ = writeln!(
                out,
                "  ddl:         the `{}` sink doesn't render its DDL; it creates the tables above",
                flow.sink
            );
        }
    }
}

/// A merge's joins, each with its strategy and why (0002, 0003 §5.2), and
/// the full ordering of each `nest:` (0003 §5.8).
fn joins_section(out: &mut String, flow: &PlannedFlow) {
    let plan = &flow.compiled.plan;
    let joins: Vec<_> = plan.joins().collect();
    if joins.is_empty() {
        return;
    }
    let _ = writeln!(out, "  joins:");
    for join in joins {
        let cardinality = match join.cardinality {
            Cardinality::One => "one",
            Cardinality::Many => "many",
        };
        let keys: Vec<String> = join
            .keys
            .iter()
            .map(|(column, other)| format!("{}.{column} = {other}", join.alias))
            .collect();
        let _ = writeln!(
            out,
            "    {} {} ({cardinality}, on {})",
            join.alias,
            join.reference,
            keys.join(" and ")
        );
        let strategy = match &join.strategy {
            Strategy::CoPartition { column } => {
                format!("co-partitioned on `{column}`, streamed with each driver partition")
            }
            Strategy::Stage => match flow.stage_memory {
                0 => "staged: read once, sorted and spilled to disk".to_owned(),
                n => format!(
                    "staged: read once, sorted in {} and spilled to disk",
                    bytes(n)
                ),
            },
        };
        let _ = writeln!(out, "      strategy: {strategy}");
        let _ = writeln!(out, "      why:      {}", join.reason);
        if let (Strategy::Stage, Some(why)) = (&join.strategy, &flow.co_partition_fallback) {
            let _ = writeln!(out, "                (the source said: {why})");
        }
    }
    for nest in plan.nest_orders() {
        let _ = writeln!(
            out,
            "  nest:        {}.{} from {}, ordered by {}",
            nest.target,
            nest.column,
            nest.alias,
            nest.order_by.join(", ")
        );
    }
}

/// `256MiB`, `1.5GiB`, `512KiB`, `100B`.
fn bytes(n: u64) -> String {
    const UNITS: [(&str, u64); 3] = [("GiB", 1 << 30), ("MiB", 1 << 20), ("KiB", 1 << 10)];
    for (unit, size) in UNITS {
        if n >= size {
            return match n % size {
                0 => format!("{}{unit}", n / size),
                _ => format!("{:.1}{unit}", n as f64 / size as f64),
            };
        }
    }
    format!("{n}B")
}

fn where_text(filter: &Where) -> String {
    match filter {
        Where::Expr(text) => text.clone(),
        Where::Named(map) => map
            .iter()
            .map(|(name, expr)| format!("{}: {}", name.as_str(), expr.as_str()))
            .collect::<Vec<_>>()
            .join(", "),
    }
}

pub(crate) fn strategy_name(strategy: IdempotencyStrategy) -> &'static str {
    match strategy {
        IdempotencyStrategy::TxnWithState => "TxnWithState",
        IdempotencyStrategy::UpsertByKey => "UpsertByKey",
        IdempotencyStrategy::DeletePartitionThenInsert => "DeletePartitionThenInsert",
        IdempotencyStrategy::StagingSwap => "StagingSwap",
    }
}
