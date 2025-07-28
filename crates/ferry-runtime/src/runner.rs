//! The coordinator: plans each flow once, then leases partitions and hands
//! them to workers until every flow is done, failed or blocked.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use ferry_core::{CoPartition, FerryError, Lease, Partition, PartitionHint, PartitionId, align};
use ferry_registry::StateStore;
use ferry_registry::state::{NewRun, PartitionCounts, RunId, RunTotals};
use tokio::task::JoinSet;
use tokio::time::Instant;

use crate::budget::Budget;
use crate::error::RunError;
use crate::flow::{Flow, namespaced, unit_reads, unnamespaced};
use crate::observe::WriteObserver;
use crate::settings::Settings;
use crate::throttle::Throttle;
use crate::work::{FlowRun, Outcome, Shared, Unit, process};

/// Runs flows on a state store. One per process: every flow of the run
/// shares its workers, memory budget and throttles.
///
/// Everything durable goes through the [`StateStore`] lease interface, so
/// running several processes on one store is a deployment change (v0.2):
/// leases already fence workers off each other.
pub struct Runner {
    store: Arc<dyn StateStore>,
    flows: Vec<Flow>,
    settings: Settings,
    observer: Option<Arc<dyn WriteObserver>>,
}

/// How a run stands when [`Runner::execute`] returns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunReport {
    pub run: RunId,
    pub counts: PartitionCounts,
    pub totals: RunTotals,
    /// Partitions this execution marked failed. Failures from an earlier
    /// execution of the run are in `counts.failed` only.
    pub failures: Vec<PartitionFailure>,
    /// Partitions whose lease was lost mid-way: they were abandoned to
    /// whoever holds them now, or re-leased once the lease expired.
    pub leases_lost: u64,
    /// Flows that didn't start because a flow they depend on has failed
    /// partitions.
    pub blocked: Vec<String>,
}

impl RunReport {
    /// Every partition of every flow is done.
    pub fn is_complete(&self) -> bool {
        self.counts.pending == 0 && self.counts.leased == 0 && self.counts.failed == 0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionFailure {
    pub flow: String,
    pub partition: PartitionId,
    pub error: String,
}

impl Runner {
    /// Checks that the flows fit together: unique names, inputs matching
    /// each plan, a driver plus co-partitioned inputs, and dependencies
    /// that exist and have no cycle.
    pub fn new(
        store: Arc<dyn StateStore>,
        flows: Vec<Flow>,
        settings: Settings,
    ) -> Result<Self, RunError> {
        settings.check().map_err(RunError::Invalid)?;
        let names: BTreeSet<&str> = flows.iter().map(|f| f.name.as_str()).collect();
        if names.len() != flows.len() {
            return Err(RunError::Invalid("two flows have the same name".into()));
        }
        for flow in &flows {
            let invalid =
                |reason: String| RunError::Invalid(format!("flow `{}`: {reason}", flow.name));
            if flow.name.is_empty() || flow.name.contains('/') {
                return Err(invalid("a flow name can't be empty or contain `/`".into()));
            }
            let declared: Vec<&str> = flow.inputs.iter().map(|i| i.entity.name()).collect();
            let planned: Vec<&str> = flow.plan.inputs().iter().map(String::as_str).collect();
            if declared != planned {
                return Err(invalid(format!(
                    "its inputs are {declared:?} but its plan reads {planned:?}"
                )));
            }
            if flow.inputs.is_empty() {
                return Err(invalid("it reads no entity".into()));
            }
            if flow.inputs[0].co_partition.is_some() {
                return Err(invalid("the driver input can't be co-partitioned".into()));
            }
            if let Some(input) = flow.inputs[1..].iter().find(|i| i.co_partition.is_none()) {
                return Err(invalid(format!(
                    "input `{}` has no co-partition column: every input after the \
                     driver is read in step with it (0002)",
                    input.entity.name()
                )));
            }
            if let Some(t) = flow
                .plan
                .write_order()
                .iter()
                .find(|t| flow.target.table(t).is_none())
            {
                return Err(invalid(format!(
                    "its plan writes `{t}`, which isn't a target"
                )));
            }
            if let Some(limit) = settings.source_max_connections
                && flow.inputs.len() > limit
            {
                return Err(invalid(format!(
                    "it reads {} entities at once, more than source_max_connections ({limit})",
                    flow.inputs.len()
                )));
            }
            if let Some(d) = flow.depends_on.iter().find(|d| !names.contains(d.as_str())) {
                return Err(invalid(format!("it depends on unknown flow `{d}`")));
            }
        }
        if let Some(name) = cycle(&flows) {
            return Err(RunError::Invalid(format!(
                "flow `{name}` depends on itself through depends_on"
            )));
        }
        Ok(Self {
            store,
            flows,
            settings,
            observer: None,
        })
    }

    /// Shows every partition's writes to `observer` (the verifier, A8).
    pub fn with_observer(mut self, observer: Arc<dyn WriteObserver>) -> Self {
        self.observer = Some(observer);
        self
    }

    /// Creates a run, then [`execute`](Runner::execute)s it.
    pub async fn start(&self, run: NewRun) -> Result<RunReport, RunError> {
        let run = self.create(run).await?;
        self.execute(&run).await
    }

    /// Creates the run without planning or running anything, so the caller
    /// can record its id before any work starts (`ferry resume` needs it).
    pub async fn create(&self, run: NewRun) -> Result<RunId, RunError> {
        self.store.create_run(run).await.map_err(RunError::Store)
    }

    /// `ferry resume <run-id>`: continues a run pinned to `config_hash` and
    /// `engine_version`, from its stored partitions. Failed partitions stay
    /// failed; see [`retry_failed`](Runner::retry_failed).
    pub async fn resume(
        &self,
        run: &RunId,
        config_hash: &str,
        engine_version: &str,
    ) -> Result<RunReport, RunError> {
        ferry_state::resume(self.store.as_ref(), run, config_hash, engine_version).await?;
        self.execute(run).await
    }

    /// `ferry retry --failed <run-id>`: puts the run's failed partitions
    /// back to pending, then resumes it.
    pub async fn retry_failed(
        &self,
        run: &RunId,
        config_hash: &str,
        engine_version: &str,
    ) -> Result<RunReport, RunError> {
        ferry_state::resume(self.store.as_ref(), run, config_hash, engine_version).await?;
        self.store
            .reset_failed(run)
            .await
            .map_err(RunError::Store)?;
        self.execute(run).await
    }

    /// Plans every flow the run hasn't planned yet, prepares each sink, and
    /// processes partitions until none is left to lease. Partitions are
    /// planned once per run: a flow whose partitions are stored is never
    /// planned again.
    pub async fn execute(&self, run: &RunId) -> Result<RunReport, RunError> {
        let mut flows = Vec::with_capacity(self.flows.len());
        for flow in &self.flows {
            flows.push(Arc::new(self.prepare(run, flow).await?));
        }
        let shared = Arc::new(Shared {
            store: Arc::clone(&self.store),
            run: run.clone(),
            settings: self.settings.clone(),
            budget: Budget::new(self.settings.memory_budget, self.settings.workers),
            throttle: Throttle::new(
                self.settings.source_max_connections,
                self.settings.source_rows_per_sec,
            ),
            observer: self.observer.clone(),
        });
        Coordinator::new(shared, flows).run().await
    }

    async fn prepare(&self, run: &RunId, flow: &Flow) -> Result<FlowRun, RunError> {
        let in_flow = |source: FerryError| RunError::Flow {
            flow: flow.name.clone(),
            source,
        };
        let caps = flow.sink.capabilities().await.map_err(in_flow)?;
        let strategy = caps
            .strategy_for(flow.mode)
            .ok_or_else(|| RunError::NoStrategy {
                flow: flow.name.clone(),
                mode: flow.mode,
                strategies: format!("{:?}", caps.strategies),
            })?;
        flow.sink
            .prepare(&flow.target, flow.mode)
            .await
            .map_err(in_flow)?;
        if let Some(observer) = &self.observer {
            observer.prepared(run, flow).await.map_err(in_flow)?;
        }

        let mut stored = self
            .store
            .load_partitions(run, &flow.name)
            .await
            .map_err(RunError::Store)?;
        if stored.is_empty() {
            let planned = self.plan(flow).await.map_err(in_flow)?;
            stored = planned.iter().map(|p| namespaced(&flow.name, p)).collect();
            self.store
                .store_partitions(run, &flow.name, &stored)
                .await
                .map_err(RunError::Store)?;
        }
        let drivers: Vec<Partition> = stored.iter().map(|p| unnamespaced(&flow.name, p)).collect();
        let reads = unit_reads(flow, &drivers).map_err(in_flow)?;
        let units = stored
            .into_iter()
            .zip(reads)
            .map(|(partition, reads)| (partition.id.clone(), Unit { partition, reads }))
            .collect();
        Ok(FlowRun {
            flow: flow.clone(),
            strategy,
            units,
        })
    }

    /// The driver's partitions, after checking every other input splits on
    /// exactly their boundaries (0002).
    async fn plan(&self, flow: &Flow) -> ferry_core::Result<Vec<Partition>> {
        let driver = &flow.inputs[0];
        let hint = PartitionHint::rows(self.settings.partition_rows);
        let partitions = driver.source.plan_partitions(&driver.entity, &hint).await?;
        for input in &flow.inputs[1..] {
            let column = input.co_partition.as_deref().expect("checked in new");
            let hint = PartitionHint {
                target_rows: self.settings.partition_rows,
                co_partition: Some(CoPartition::from_partitions(column, &partitions)?),
            };
            let theirs = input.source.plan_partitions(&input.entity, &hint).await?;
            align(&partitions, &theirs)?;
        }
        Ok(partitions)
    }
}

/// A flow that depends on itself, if any.
fn cycle(flows: &[Flow]) -> Option<&str> {
    let deps: HashMap<&str, Vec<&str>> = flows
        .iter()
        .map(|f| {
            (
                f.name.as_str(),
                f.depends_on.iter().map(String::as_str).collect(),
            )
        })
        .collect();
    flows.iter().map(|f| f.name.as_str()).find(|&start| {
        let mut stack: Vec<&str> = deps[start].clone();
        let mut seen = BTreeSet::new();
        while let Some(name) = stack.pop() {
            if name == start {
                return true;
            }
            if seen.insert(name) {
                stack.extend(deps.get(name).into_iter().flatten());
            }
        }
        false
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    /// A flow it depends on isn't done yet.
    Waiting,
    /// Leasing.
    Active,
    /// Nothing to lease; this process still works on some of its partitions.
    Draining,
    /// Nothing to lease, and another worker holds some of its partitions
    /// (a crashed run's leases, until they expire): look again at the time.
    Polling(Instant),
    /// Nothing pending or leased. `true` if every partition is done.
    Finished(bool),
    /// A flow it depends on finished with failed partitions.
    Blocked,
}

struct Coordinator {
    shared: Arc<Shared>,
    flows: Vec<Arc<FlowRun>>,
    states: Vec<State>,
    in_flight: Vec<usize>,
    tasks: JoinSet<(usize, Lease, ferry_core::Result<Outcome>)>,
    failures: Vec<PartitionFailure>,
    leases_lost: u64,
    /// Set by the first failure: no new leases, in-flight work finishes.
    stopping: bool,
    fatal: Option<RunError>,
}

impl Coordinator {
    fn new(shared: Arc<Shared>, flows: Vec<Arc<FlowRun>>) -> Self {
        let n = flows.len();
        Self {
            shared,
            flows,
            states: vec![State::Waiting; n],
            in_flight: vec![0; n],
            tasks: JoinSet::new(),
            failures: Vec::new(),
            leases_lost: 0,
            stopping: false,
            fatal: None,
        }
    }

    async fn run(mut self) -> Result<RunReport, RunError> {
        self.unblock();
        loop {
            if !self.stopping {
                self.lease().await;
                // A flow that just finished may let others start.
                if self.unblock() {
                    continue;
                }
            }
            if self.tasks.is_empty()
                && (self.stopping
                    || self
                        .states
                        .iter()
                        .all(|s| matches!(s, State::Finished(_) | State::Blocked)))
            {
                break;
            }
            // Stopping, nothing is leased again: only in-flight work counts.
            let wake = self
                .states
                .iter()
                .filter_map(|s| match s {
                    State::Polling(at) => Some(*at),
                    _ => None,
                })
                .min()
                .filter(|_| !self.stopping);
            if self.tasks.is_empty() && wake.is_none() {
                // Only flows waiting on each other are left, which `new`
                // rules out; don't wait forever if it happens anyway.
                break;
            }
            tokio::select! {
                Some(joined) = self.tasks.join_next() => self.finished(joined),
                _ = sleep_until(wake) => {}
            }
        }
        if let Some(fatal) = self.fatal {
            return Err(fatal);
        }
        let store = &self.shared.store;
        let run = &self.shared.run;
        Ok(RunReport {
            run: run.clone(),
            counts: store.counts(run).await.map_err(RunError::Store)?,
            totals: store.totals(run).await.map_err(RunError::Store)?,
            failures: self.failures,
            leases_lost: self.leases_lost,
            blocked: self
                .flows
                .iter()
                .zip(&self.states)
                .filter(|(_, s)| **s == State::Blocked)
                .map(|(f, _)| f.flow.name.clone())
                .collect(),
        })
    }

    /// Starts flows whose dependencies are all done, and blocks those with
    /// a dependency that failed or is blocked, until nothing changes (a
    /// block passes down a chain of flows). `true` if a flow started.
    fn unblock(&mut self) -> bool {
        let mut started = false;
        loop {
            let by_name: BTreeMap<&str, State> = self
                .flows
                .iter()
                .zip(&self.states)
                .map(|(f, s)| (f.flow.name.as_str(), *s))
                .collect();
            let mut changed = false;
            for (flow, state) in self.flows.iter().zip(&mut self.states) {
                if *state != State::Waiting {
                    continue;
                }
                let deps: Vec<State> = flow
                    .flow
                    .depends_on
                    .iter()
                    .map(|d| by_name[d.as_str()])
                    .collect();
                if deps
                    .iter()
                    .any(|s| matches!(s, State::Finished(false) | State::Blocked))
                {
                    *state = State::Blocked;
                    changed = true;
                } else if deps.iter().all(|s| *s == State::Finished(true)) {
                    *state = State::Active;
                    started = true;
                    changed = true;
                }
            }
            if !changed {
                return started;
            }
        }
    }

    /// Leases partitions for idle workers, flow by flow.
    async fn lease(&mut self) {
        let now = Instant::now();
        for i in 0..self.flows.len() {
            match self.states[i] {
                State::Active => {}
                State::Polling(at) if at <= now => self.states[i] = State::Active,
                _ => continue,
            }
            while self.tasks.len() < self.shared.settings.workers {
                match self.lease_one(i).await {
                    Ok(true) => {}
                    Ok(false) => break,
                    Err(e) => {
                        self.fatal = Some(RunError::Store(e));
                        self.stopping = true;
                        return;
                    }
                }
            }
        }
    }

    /// Leases and spawns one partition of flow `i`. `false`, with the flow's
    /// new state set, when there is nothing to lease.
    async fn lease_one(&mut self, i: usize) -> ferry_core::Result<bool> {
        let shared = &self.shared;
        let name = &self.flows[i].flow.name;
        let ttl = shared.settings.lease_ttl;
        let asked = Instant::now();
        if let Some(lease) = shared.store.lease_next(&shared.run, name, ttl).await? {
            let shared = Arc::clone(shared);
            let flow = Arc::clone(&self.flows[i]);
            self.tasks.spawn(async move {
                let outcome = process(shared, flow, lease.clone(), asked).await;
                (i, lease, outcome)
            });
            self.in_flight[i] += 1;
            return Ok(true);
        }
        let counts = shared.store.flow_counts(&shared.run, name).await?;
        self.states[i] = if counts.pending + counts.leased == 0 && self.in_flight[i] == 0 {
            State::Finished(counts.failed == 0)
        } else if counts.pending > 0 || counts.leased > self.in_flight[i] as u64 {
            State::Polling(Instant::now() + shared.settings.poll)
        } else {
            State::Draining
        };
        Ok(false)
    }

    fn finished(
        &mut self,
        joined: Result<(usize, Lease, ferry_core::Result<Outcome>), tokio::task::JoinError>,
    ) {
        let (i, lease, outcome) = match joined {
            Ok(done) => done,
            Err(e) => {
                // A worker panicked. Its partition stays leased until the
                // lease expires, and a resume picks it up.
                self.fatal = Some(RunError::Worker(e.to_string()));
                self.stopping = true;
                return;
            }
        };
        self.in_flight[i] -= 1;
        if self.states[i] == State::Draining {
            self.states[i] = State::Active;
        }
        match outcome {
            Ok(Outcome::Done) => {}
            Ok(Outcome::Failed(error)) => {
                self.failures.push(PartitionFailure {
                    flow: self.flows[i].flow.name.clone(),
                    partition: lease.partition,
                    error,
                });
                self.stopping = true;
            }
            Ok(Outcome::LeaseLost) => {
                self.leases_lost += 1;
                if self.states[i] != State::Waiting {
                    self.states[i] = State::Active;
                }
            }
            Err(e) => {
                self.fatal = Some(RunError::Store(e));
                self.stopping = true;
            }
        }
    }
}

async fn sleep_until(at: Option<Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}
