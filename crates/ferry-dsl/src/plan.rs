//! The planner (B7): a loaded config, the entities its sources discovered
//! and the capabilities of its sinks become the flows a run executes, in
//! the order it may start them.
//!
//! For every flow it
//!
//! - type-checks (B3) and compiles (B4, B5) it into a [`CompiledFlow`]:
//!   the transform plan, its write order (parents before children, 0003
//!   §5.3) and the canonical [`TargetSchema`] the sink renders as DDL;
//! - picks the idempotency strategy, `SinkCaps::strategy_for` of the
//!   flow's mode. A sink built on the state connection offers
//!   `TxnWithState`; every other flow gets the sink's next strategy, and the
//!   plan says why (0004 §8). A sink with no strategy for the mode refuses
//!   the flow (0001);
//! - settles how its driver is partitioned: `partition_rows` from the
//!   flow's `runtime:`, else `runtime.yaml`, else the default (0003 §6).
//!
//! # The flow DAG
//!
//! A flow waits for the flows it names in `depends_on:` (B1d, checked at
//! load), and for every flow that writes a table it reads: a `from:`
//! entity on a flow's sink connection whose name is one of that flow's
//! targets. FK discovery is deferred until after S2 (0003 §7), so those
//! are the only edges. Each cycle in `depends_on:` alone is a load error;
//! a cycle that only appears once the reads are added is the planner's,
//! and its error walks the cycle naming why each flow waits for the next.
//! [`Plan::flows`] lists the flows in an order that respects every edge,
//! ties broken by name, so it doesn't depend on which file a flow is in.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use ferry_core::entity::RESERVED_PREFIX;
use ferry_core::{
    BatchStream, Entity, IdempotencyStrategy, Partition, PartitionHint, Result, SinkCaps, Source,
    SourceCaps, WriteMode,
};

use crate::compile::{CompiledFlow, compile};
use crate::config::model::{EntityRef, FILE_BACKEND, From, OnError, Workers};
use crate::config::{
    Config, ConfigError, ConfigErrors, ConfigHash, ErrorKind, FieldPath, Flow, Location,
};
use crate::typecheck::{Catalog, CheckedFlow, check};

/// What a run of the config would do. Built without writing anything.
#[derive(Clone)]
pub struct Plan {
    /// `name:` in `ferry.yaml`.
    pub name: String,
    pub hash: ConfigHash,
    pub state: StatePlacement,
    /// Every flow, each after the flows it waits for; ties by name.
    pub flows: Vec<PlannedFlow>,
    /// What the plan should point out but doesn't stop a run, flows' own
    /// warnings included.
    pub warnings: Vec<String>,
}

impl Plan {
    /// The type-checked flows, for `Expectations` (B8).
    pub fn checked(&self) -> Vec<CheckedFlow> {
        self.flows.iter().map(|f| f.checked.clone()).collect()
    }

    pub fn flow(&self, name: &str) -> Option<&PlannedFlow> {
        self.flows.iter().find(|f| f.name == name)
    }
}

/// Where the run's state store lives (`runtime.state`, 0003 §4).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StatePlacement {
    /// `backend: file`, in `path` (or the CLI's default).
    File { path: Option<String> },
    /// On `connection`, a connection of type `backend`.
    Connection { backend: String, connection: String },
}

impl StatePlacement {
    fn of(config: &Config) -> Self {
        match &config.runtime.state {
            Some(s) if s.backend.as_str() != FILE_BACKEND => StatePlacement::Connection {
                backend: s.backend.as_str().to_owned(),
                connection: s
                    .connection
                    .as_ref()
                    .map(|c| c.as_str().to_owned())
                    .unwrap_or_default(),
            },
            Some(s) => StatePlacement::File {
                path: s.path.as_ref().map(|p| p.as_str().to_owned()),
            },
            None => StatePlacement::File { path: None },
        }
    }
}

/// One flow, planned.
#[derive(Clone)]
pub struct PlannedFlow {
    pub name: String,
    /// The driver entity, `<connection>.<entity>`.
    pub driver: EntityRef,
    /// The connection every target is written to.
    pub sink: String,
    pub compiled: CompiledFlow,
    pub checked: CheckedFlow,
    pub strategy: IdempotencyStrategy,
    /// Why the flow doesn't get `TxnWithState`; `None` when it does.
    pub fallback: Option<String>,
    /// `PartitionHint::target_rows` for the driver.
    pub partition_rows: u64,
    pub partition_rows_from: Setting,
    /// The flows this one waits for, by name, and why.
    pub waits_for: Vec<Dependency>,
}

/// Where a setting's value came from (0003 §6 precedence).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Setting {
    Flow,
    Runtime,
    Default,
}

impl Setting {
    pub fn describe(self) -> &'static str {
        match self {
            Setting::Flow => "the flow's runtime:",
            Setting::Runtime => "runtime.yaml",
            Setting::Default => "the default",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Dependency {
    pub flow: String,
    pub edge: Edge,
}

/// Why one flow waits for another.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Edge {
    /// `depends_on:`.
    Declared,
    /// It reads `entity` (`<connection>.<entity>`), which the other flow
    /// writes.
    Reads(String),
}

impl Edge {
    pub fn describe(&self) -> String {
        match self {
            Edge::Declared => "declared".into(),
            Edge::Reads(entity) => format!("reads `{entity}`"),
        }
    }
}

impl PlannedFlow {
    pub fn mode(&self) -> WriteMode {
        self.compiled.mode
    }

    /// The flow as the runner takes it (A5), reading its driver from
    /// `source` and writing to `sink`.
    pub fn runtime_flow(
        &self,
        source: Arc<dyn Source>,
        sink: Arc<dyn ferry_core::Sink>,
        finalizer: Option<Arc<dyn ferry_registry::Finalizer>>,
    ) -> ferry_runtime::Flow {
        let entity = self.compiled.plan.entity().clone();
        ferry_runtime::Flow {
            name: self.name.clone(),
            inputs: vec![ferry_runtime::Input {
                source: Arc::new(Sized {
                    inner: source,
                    target_rows: self.partition_rows,
                }),
                entity,
                co_partition: None,
            }],
            plan: Arc::new(self.compiled.plan.clone()),
            sink,
            target: self.compiled.target.clone(),
            finalizer,
            mode: self.compiled.mode,
            depends_on: self
                .waits_for
                .iter()
                .map(|d| d.flow.clone())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect(),
        }
    }
}

/// A source whose partitions are sized by its flow: the runner's
/// `partition_rows` is run-wide, a flow's may differ (0003 §6).
/// Co-partitioning boundaries pass through untouched.
struct Sized {
    inner: Arc<dyn Source>,
    target_rows: u64,
}

#[async_trait]
impl Source for Sized {
    async fn discover(&self) -> Result<Vec<Entity>> {
        self.inner.discover().await
    }

    async fn plan_partitions(
        &self,
        entity: &Entity,
        hint: &PartitionHint,
    ) -> Result<Vec<Partition>> {
        let hint = PartitionHint {
            target_rows: self.target_rows,
            co_partition: hint.co_partition.clone(),
        };
        self.inner.plan_partitions(entity, &hint).await
    }

    async fn read(&self, entity: &Entity, partition: &Partition) -> Result<BatchStream> {
        self.inner.read(entity, partition).await
    }

    async fn capabilities(&self) -> Result<SourceCaps> {
        self.inner.capabilities().await
    }
}

/// `runtime.yaml` as the runner's settings. The lease timings are not
/// user settings in v0.1 and keep the runner's defaults.
pub fn settings(config: &Config) -> ferry_runtime::Settings {
    let runtime = &config.runtime;
    let defaults = ferry_runtime::Settings::default();
    ferry_runtime::Settings {
        workers: match runtime.workers {
            Workers::Auto => defaults.workers,
            Workers::Count(n) => n.get() as usize,
        },
        memory_budget: runtime.memory_budget.0.get(),
        partition_rows: runtime.partition_rows.get(),
        source_max_connections: runtime.source_max_connections.map(|n| n.get() as usize),
        source_rows_per_sec: runtime.source_rows_per_sec.map(|n| n.get()),
        on_error: match runtime.on_error {
            OnError::Fail => ferry_runtime::OnError::Fail,
            OnError::DeadLetter => ferry_runtime::OnError::DeadLetter,
        },
        max_errors: runtime.max_errors,
        ..defaults
    }
}

/// Plans every flow of `config`. `catalog` holds what each source
/// connection discovered, and `sinks` the capabilities of each sink
/// connection, as built for the run: the state connection's sink through
/// the state store's ledger (0004 §8).
///
/// Reports every problem it finds: type errors, a sink with no strategy
/// for a flow's mode, a cycle between flows, a target named like one of
/// ferry's own tables, and per-flow settings the runner can't honour yet.
pub fn plan(
    config: &Config,
    catalog: &Catalog,
    sinks: &BTreeMap<String, SinkCaps>,
) -> std::result::Result<Plan, ConfigErrors> {
    let checked = check(config, catalog)?;
    let state = StatePlacement::of(config);
    let mut errors = Vec::new();
    let mut warnings = Vec::new();
    if config.runtime.on_error == OnError::DeadLetter && config.runtime.max_errors.is_none() {
        warnings.push(
            "runtime: `on_error: dead_letter` without `max_errors`: no number of rejected \
             records stops the run"
                .into(),
        );
    }

    let mut planned = Vec::new();
    for (flow, checked) in config.flows.iter().zip(checked) {
        let compiled = match compile(flow, &checked, catalog) {
            Ok(compiled) => compiled,
            Err(e) => {
                errors.extend(e);
                continue;
            }
        };
        warnings.extend(compiled.warnings.iter().cloned());
        errors.extend(reserved_targets(flow));
        errors.extend(unsupported_overrides(config, flow));
        let Some((strategy, fallback)) = strategy(flow, compiled.mode, sinks, &state, &mut errors)
        else {
            continue;
        };
        let (partition_rows, partition_rows_from) = partition_rows(config, flow);
        let From::Entity(driver) = flow.from.value() else {
            unreachable!("compile refuses merges until B6");
        };
        planned.push(PlannedFlow {
            name: flow.name.as_str().to_owned(),
            driver: driver.clone(),
            sink: flow.sink.as_str().to_owned(),
            compiled,
            checked,
            strategy,
            fallback,
            partition_rows,
            partition_rows_from,
            waits_for: Vec::new(),
        });
    }
    if !errors.is_empty() {
        return Err(ConfigErrors(errors));
    }

    let edges = edges(config);
    for flow in &mut planned {
        flow.waits_for = edges.get(flow.name.as_str()).cloned().unwrap_or_default();
    }
    let order = match order(config, &edges) {
        Ok(order) => order,
        Err(e) => return Err(ConfigErrors(vec![e])),
    };
    planned.sort_by_key(|f| order.iter().position(|o| *o == f.name));
    Ok(Plan {
        name: config.name.as_str().to_owned(),
        hash: config.hash(),
        state,
        flows: planned,
        warnings,
    })
}

fn flow_path(flow: &Flow) -> FieldPath {
    FieldPath::root()
        .key("flows")
        .item(0, Some(flow.name.as_str()))
}

fn plan_error(message: impl Into<String>) -> ConfigError {
    ConfigError::new(ErrorKind::Plan, message)
}

/// Target names starting `_ferry_` are the engine's: the runner writes
/// dead letters to `_ferry_dlq` and a sink may keep its own records under
/// the prefix (`_ferry_dropped`), on the same connection as the targets.
fn reserved_targets(flow: &Flow) -> Vec<ConfigError> {
    flow.to
        .keys()
        .filter(|t| t.as_str().starts_with(RESERVED_PREFIX))
        .map(|t| {
            plan_error(format!(
                "flow `{}`: target `{}` uses the prefix `{RESERVED_PREFIX}`, which is reserved \
                 for ferry's own tables (dead letters go to `{}`)",
                flow.name.as_str(),
                t.as_str(),
                ferry_runtime::DLQ_TABLE
            ))
            .at(t.location())
            .path(&flow_path(flow).key("to").key(t.as_str()))
            .help("rename the target")
        })
        .collect()
}

/// Per-flow `on_error` and `max_errors` (0003 §6) need per-flow budgets in
/// the runner, which has run-wide ones only. A flow value that differs
/// from the run-wide one is refused rather than silently ignored.
fn unsupported_overrides(config: &Config, flow: &Flow) -> Vec<ConfigError> {
    let mut errors = Vec::new();
    let path = flow_path(flow).key("runtime");
    let mut refuse = |key: &str, flow_value: String, run_value: String| {
        errors.push(
            plan_error(format!(
                "flow `{}` sets `{key}: {flow_value}`, but this runner applies `{key}` to the \
                 whole run ({run_value})",
                flow.name.as_str()
            ))
            .at(flow.name.location())
            .path(&path.key(key))
            .help(format!(
                "set `{key}` in runtime.yaml instead; per-flow `{key}` isn't supported yet"
            )),
        )
    };
    if let Some(on_error) = flow.runtime.on_error
        && on_error != config.runtime.on_error
    {
        refuse(
            "on_error",
            on_error.as_str().into(),
            config.runtime.on_error.as_str().into(),
        );
    }
    if let Some(max) = flow.runtime.max_errors
        && Some(max) != config.runtime.max_errors
    {
        refuse(
            "max_errors",
            max.to_string(),
            config
                .runtime
                .max_errors
                .map_or("unbounded".into(), |m| m.to_string()),
        );
    }
    errors
}

/// The flow's strategy, and why it isn't `TxnWithState` if it isn't.
/// `None`, with an error pushed, when the sink has none for the mode.
fn strategy(
    flow: &Flow,
    mode: WriteMode,
    sinks: &BTreeMap<String, SinkCaps>,
    state: &StatePlacement,
    errors: &mut Vec<ConfigError>,
) -> Option<(IdempotencyStrategy, Option<String>)> {
    let sink = flow.sink.as_str();
    let mode_name = match mode {
        WriteMode::Insert => "insert",
        WriteMode::Upsert => "upsert",
    };
    let caps = sinks.get(sink).cloned().unwrap_or_default();
    let Some(strategy) = caps.strategy_for(mode) else {
        errors.push(
            plan_error(format!(
                "flow `{}`: the `{sink}` sink has no idempotency strategy for `mode: \
                 {mode_name}` (it offers {}), so re-running a partition could duplicate or \
                 lose rows",
                flow.name.as_str(),
                match caps.strategies.is_empty() {
                    true => "none".to_owned(),
                    false => format!("{:?}", caps.strategies),
                }
            ))
            .at(flow.sink.location())
            .path(&flow_path(flow).key("sink"))
            .help("write this flow to a sink that supports its mode (0001)"),
        );
        return None;
    };
    let fallback = (strategy != IdempotencyStrategy::TxnWithState).then(|| match state {
        StatePlacement::File { .. } => {
            format!("the state store is a file (`backend: file`), not on \"{sink}\"")
        }
        StatePlacement::Connection { connection, .. } if connection != sink => {
            format!("the state store is on \"{connection}\", not \"{sink}\"")
        }
        StatePlacement::Connection { .. } => {
            format!("the \"{sink}\" sink can't commit the state store's record with its rows")
        }
    });
    Some((strategy, fallback))
}

fn partition_rows(config: &Config, flow: &Flow) -> (u64, Setting) {
    match flow.runtime.partition_rows {
        Some(rows) => (rows.get(), Setting::Flow),
        None if config.runtime.partition_rows.get()
            != crate::config::model::DEFAULT_PARTITION_ROWS =>
        {
            (config.runtime.partition_rows.get(), Setting::Runtime)
        }
        None => (config.runtime.partition_rows.get(), Setting::Default),
    }
}

/// What each flow waits for, by flow name: `depends_on:`, and every flow
/// writing a table it reads.
fn edges(config: &Config) -> BTreeMap<&str, Vec<Dependency>> {
    let graph = config.dependency_graph();
    let mut out: BTreeMap<&str, Vec<Dependency>> = BTreeMap::new();
    for flow in &config.flows {
        let name = flow.name.as_str();
        let waits = out.entry(name).or_default();
        waits.extend(graph.dependencies(name).map(|d| Dependency {
            flow: d.to_owned(),
            edge: Edge::Declared,
        }));
        for read in reads(flow) {
            for writer in &config.flows {
                let writes = writer.sink.as_str() == read.connection.as_str()
                    && writer.to.keys().any(|t| t.as_str() == read.entity);
                if writes {
                    waits.push(Dependency {
                        flow: writer.name.as_str().to_owned(),
                        edge: Edge::Reads(read.to_string()),
                    });
                }
            }
        }
        waits.sort();
        waits.dedup();
    }
    out
}

fn reads(flow: &Flow) -> Vec<&EntityRef> {
    match flow.from.value() {
        From::Entity(entity) => vec![entity],
        From::Merge(entries) => entries.iter().map(|e| e.entity.value()).collect(),
    }
}

/// The flows in an order that respects every edge, ties by name; or the
/// error for the first cycle.
fn order(
    config: &Config,
    edges: &BTreeMap<&str, Vec<Dependency>>,
) -> std::result::Result<Vec<String>, ConfigError> {
    let mut remaining: BTreeMap<&str, BTreeSet<&str>> = edges
        .iter()
        .map(|(flow, deps)| (*flow, deps.iter().map(|d| d.flow.as_str()).collect()))
        .collect();
    let mut order = Vec::new();
    while let Some(next) = remaining
        .iter()
        .find(|(_, deps)| deps.is_empty())
        .map(|(f, _)| *f)
    {
        remaining.remove(next);
        for deps in remaining.values_mut() {
            deps.remove(next);
        }
        order.push(next.to_owned());
    }
    match remaining.keys().next() {
        None => Ok(order),
        Some(start) => Err(cycle_error(config, edges, &remaining, start)),
    }
}

/// Walks from `start` along the smallest edge into the flows left on a
/// cycle until a flow repeats, and names every step of the loop.
fn cycle_error(
    config: &Config,
    edges: &BTreeMap<&str, Vec<Dependency>>,
    remaining: &BTreeMap<&str, BTreeSet<&str>>,
    start: &str,
) -> ConfigError {
    let mut path = vec![start];
    let mut current = start;
    loop {
        let next = *remaining[current]
            .iter()
            .next()
            .expect("a flow left after ordering waits for another left");
        if let Some(i) = path.iter().position(|p| *p == next) {
            path.drain(..i);
            break;
        }
        path.push(next);
        current = next;
    }
    let step = |from: &str, to: &str| -> &Dependency {
        edges[from]
            .iter()
            .find(|d| d.flow == to)
            .expect("the walk follows edges")
    };
    let first = path[0];
    let second = *path.get(1).unwrap_or(&first);
    let steps: Vec<String> = path
        .iter()
        .zip(path.iter().cycle().skip(1))
        .map(|(from, to)| match &step(from, to).edge {
            Edge::Declared => format!("`{from}` depends on `{to}`"),
            Edge::Reads(entity) if from == to => {
                format!("`{from}` reads `{entity}`, which it writes itself")
            }
            Edge::Reads(entity) => format!("`{from}` reads `{entity}`, which `{to}` writes"),
        })
        .collect();
    let chain: Vec<&str> = path.iter().copied().chain([first]).collect();
    let flow = config.flow(first).expect("planned flows are config flows");
    let (location, at): (&Location, FieldPath) = match &step(first, second).edge {
        Edge::Declared => {
            let i = flow
                .depends_on
                .iter()
                .position(|d| d.as_str() == second)
                .expect("a declared edge is in depends_on");
            (
                flow.depends_on[i].location(),
                flow_path(flow).key("depends_on").item(i, None),
            )
        }
        Edge::Reads(_) => (flow.from.location(), flow_path(flow).key("from")),
    };
    plan_error(format!(
        "flows wait for each other in a cycle: {} ({})",
        chain.join(" → "),
        steps.join("; ")
    ))
    .at(location)
    .path(&at)
    .help(
        "a flow waits for the flows in its `depends_on:` and for every flow writing a table \
         it reads; remove one of these edges (0003 §7)",
    )
}
