//! `ferry run`, `ferry resume <run-id>` and `ferry retry --failed
//! <run-id>` (S2): plan, then run every flow on A5's runner, then verify
//! (A8) and write the run report.
//!
//! - The flows, their sources and sinks come from the plan and the
//!   endpoints `validate` built through the registry, so nothing here
//!   knows a connector type.
//! - `runtime.yaml` becomes the runner's settings. The state store is the
//!   state connection's (`backend: <type>`, built with the sink so its
//!   flows commit `TxnWithState`) or a local directory (`backend: file`).
//! - Two observers watch every partition: the verifier's write-time
//!   checksums and the source-side aggregates of `expect:` (B8). Both keep
//!   their records in files under the work directory, so a run killed
//!   half-way is verified on resume with every attempt's records.
//! - A merge's co-partitioned joins are inputs of the runner; its staged
//!   joins read their sources themselves, in their share of
//!   `memory_budget`, spilling under `<work dir>/stage` (B6, 0002).
//! - After the run, each sink's flows are verified against that sink, with
//!   the run's totals scoped to them, and the report is written to
//!   `<work dir>/runs/<run id>/report.{json,md}`, or
//!   `report-<sink>.{json,md}` per sink when the run writes to several. A
//!   sink this binary can't read back is reported unverified (a warning),
//!   and what finalize couldn't restore on it still fails the run.
//! - Ctrl-C stops the run with exit 130 and releases the leases it held
//!   ([`leases`](crate::leases)), so `ferry resume` doesn't wait for them
//!   to expire.
//!
//! `run` prints the run id before any partition is processed, so a run
//! that is killed can always be resumed.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use ferry_dsl::expect::{Expectations, FileSourceLog, SourceLog};
use ferry_dsl::plan::PlannedFlow;
use ferry_registry::StateStore;
use ferry_registry::state::{NewRun, RunId};
use ferry_runtime::{Flow, RunError, RunReport, Runner, Settings};
use ferry_state::{FileStateStore, ResumeError};
use ferry_verify::{
    ChecksumObserver, FileWriteLog, Report, RunTiming, Status, Target, Verification, Verifier,
    WriteLog,
};

use crate::Ferry;
use crate::cli::Cli;
use crate::diagnostic::{Diagnostic, Printer, code};
use crate::exit::Exit;
use crate::leases::Tracked;
use crate::plan::{self, Planned};

/// The engine version a run is pinned to (A4).
pub(crate) const ENGINE_VERSION: &str = env!("CARGO_PKG_VERSION");

/// How long a lease lasts without a heartbeat. A run resumed after a crash
/// waits at most this long for the crashed process's partitions.
const LEASE_TTL: Duration = Duration::from_secs(30);
const HEARTBEAT: Duration = Duration::from_secs(10);

/// Which command is running.
#[derive(Debug, Clone)]
pub(crate) enum Start {
    New,
    Resume(String),
    Retry(String),
}

pub(crate) async fn run(ferry: &Ferry, cli: &Cli, printer: &mut Printer<'_>, start: Start) -> Exit {
    let planned = plan::plan(
        ferry,
        &cli.config,
        &|name| std::env::var(name).ok(),
        &mut printer.redaction,
    )
    .await;
    let planned = match planned {
        Ok((planned, warnings)) => {
            let shown: Vec<Diagnostic> = warnings.into_iter().filter(|_| !cli.quiet).collect();
            let _ = printer.diagnostics(&shown, &cli.config);
            planned
        }
        Err(refused) => {
            let _ = printer.diagnostics(&refused.diagnostics, &cli.config);
            return refused.exit;
        }
    };
    match Execution::new(ferry, cli, &planned) {
        Ok(execution) => execution.run(cli, printer, start).await,
        Err(d) => {
            let _ = printer.diagnostics(&[d], &cli.config);
            Exit::Internal
        }
    }
}

/// Where `run` keeps what isn't in the state store: write-time records
/// and reports (and the state itself under `backend: file`). Default:
/// `.ferry` next to the config directory.
pub(crate) fn work_dir(cli: &Cli) -> PathBuf {
    match &cli.work_dir {
        Some(dir) => dir.clone(),
        None => cli
            .config
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."))
            .join(".ferry"),
    }
}

/// How one sink's flows are verified (A8).
struct Check {
    sink: String,
    /// The flows writing to it, by name.
    flows: Vec<String>,
    how: How,
}

enum How {
    /// Read back through `target`.
    Verified(Arc<dyn Target>),
    /// Not verified, and why: a warning, not a failure.
    Unverified(String),
}

impl Check {
    /// One check per sink the run writes to, in name order. `Err` when a
    /// sink's target can't be opened: the run must not start, since what
    /// it wrote could never be verified.
    fn of(
        ferry: &Ferry,
        summary: &crate::validate::Summary,
        flows: &[PlannedFlow],
    ) -> Result<Vec<Self>, String> {
        let mut by_sink: BTreeMap<&str, Vec<String>> = BTreeMap::new();
        for flow in flows {
            by_sink
                .entry(flow.sink.as_str())
                .or_default()
                .push(flow.name.clone());
        }
        let mut checks = Vec::new();
        for (sink, flows) in by_sink {
            let raw = summary
                .raw
                .get(sink)
                .ok_or_else(|| format!("connection `{sink}` was not built"))?;
            let how = match ferry.targets.get(&raw.r#type) {
                None => How::Unverified(format!(
                    "this binary can't read `{}` connections back, so `{sink}` isn't verified",
                    raw.r#type
                )),
                Some(factory) => How::Verified(
                    factory(raw)
                        .map_err(|e| format!("could not open `{sink}` to verify it: {e}"))?,
                ),
            };
            checks.push(Check {
                sink: sink.to_owned(),
                flows,
                how,
            });
        }
        Ok(checks)
    }
}

struct Execution<'a> {
    planned: &'a Planned,
    work: PathBuf,
    store: Arc<dyn StateStore>,
    /// `store`, remembering the leases the runner holds.
    leases: Arc<Tracked>,
    write_log: Arc<dyn WriteLog>,
    source_log: Arc<dyn SourceLog>,
    flows: Vec<Flow>,
    checks: Vec<Check>,
    runner: Runner,
}

impl<'a> Execution<'a> {
    fn new(ferry: &Ferry, cli: &Cli, planned: &'a Planned) -> Result<Self, Diagnostic> {
        let Planned { summary, plan } = planned;
        let work = work_dir(cli);
        let inner: Arc<dyn StateStore> = match &summary.state_store {
            Some(store) => Arc::clone(store),
            None => {
                let path = summary
                    .state
                    .as_ref()
                    .and_then(|s| s.path.as_deref())
                    .map_or_else(|| work.join("state"), |p| cli.config.join(p));
                Arc::new(FileStateStore::new(path))
            }
        };
        let leases = Arc::new(Tracked::new(inner));
        let store: Arc<dyn StateStore> = leases.clone();
        let write_log: Arc<dyn WriteLog> = Arc::new(FileWriteLog::new(work.join("writes")));
        let source_log: Arc<dyn SourceLog> = Arc::new(FileSourceLog::new(work.join("sources")));

        let internal = |message: String| Diagnostic::error(code::RUN, message);
        let sources = |name: &str| summary.endpoints.get(name).and_then(|e| e.source.clone());
        let mut flows = Vec::new();
        for planned_flow in &plan.flows {
            let sink_endpoints = summary
                .endpoints
                .get(&planned_flow.sink)
                .ok_or_else(|| internal(format!("`{}` was not built", planned_flow.sink)))?;
            let sink = sink_endpoints
                .sink
                .clone()
                .ok_or_else(|| internal(format!("`{}` has no sink", planned_flow.sink)))?;
            let flow = planned_flow
                .runtime_flow(
                    &sources,
                    sink,
                    sink_endpoints.finalizer.clone(),
                    Some(work.join("stage")),
                )
                .map_err(|e| internal(format!("flow `{}`: {e}", planned_flow.name)))?;
            flows.push(flow);
        }
        let checks = Check::of(ferry, summary, &plan.flows)
            .map_err(|why| Diagnostic::error(code::VERIFY_FAILED, why).help("nothing was run"))?;

        let settings = Settings {
            lease_ttl: LEASE_TTL,
            heartbeat: HEARTBEAT,
            ..plan.settings(&summary.config)
        };
        let mut runner = Runner::new(Arc::clone(&store), flows.clone(), settings)
            .map_err(|e| internal(format!("the planned flows don't fit the runner: {e}")))?;
        // Every flow is recorded, so each sink's verification can take the
        // others' rows out of the run's totals; each is checksummed the way
        // its own sink is read back.
        let targets: BTreeMap<String, Arc<dyn Target>> = checks
            .iter()
            .filter_map(|c| match &c.how {
                How::Verified(target) => Some((c, target)),
                How::Unverified(_) => None,
            })
            .flat_map(|(c, target)| c.flows.iter().map(|f| (f.clone(), Arc::clone(target))))
            .collect();
        runner = runner.with_observer(Arc::new(ChecksumObserver::per_flow(
            Arc::clone(&write_log),
            targets,
        )));
        let expectations = Expectations::new(&plan.checked(), Arc::clone(&source_log));
        runner = runner.with_observer(expectations.observer());
        Ok(Self {
            planned,
            work,
            store,
            leases,
            write_log,
            source_log,
            flows,
            checks,
            runner,
        })
    }

    async fn run(self, cli: &Cli, printer: &mut Printer<'_>, start: Start) -> Exit {
        // Before the run id is printed: from then on Ctrl-C must stop the
        // run with 130 rather than kill the process.
        let mut interrupt = match Interrupt::listen() {
            Ok(interrupt) => interrupt,
            Err(e) => {
                let d = Diagnostic::error(code::RUN, format!("could not listen for Ctrl-C: {e}"));
                let _ = printer.diagnostics(&[d], &cli.config);
                return Exit::Internal;
            }
        };
        let plan = &self.planned.plan;
        let hash = plan.hash.as_str();
        let run = match &start {
            Start::New => {
                let new = NewRun {
                    config_hash: hash.to_owned(),
                    engine_version: ENGINE_VERSION.to_owned(),
                    connections: self
                        .planned
                        .summary
                        .connections
                        .iter()
                        .map(|(name, kind, ..)| (name.clone(), kind.clone()))
                        .collect(),
                };
                match self.runner.create(new).await {
                    Ok(run) => run,
                    Err(e) => return run_error(printer, cli, None, e),
                }
            }
            Start::Resume(id) | Start::Retry(id) => RunId(id.clone()),
        };
        let verb = match start {
            Start::New => "run",
            Start::Resume(_) => "resuming run",
            Start::Retry(_) => "retrying the failed partitions of run",
        };
        let _ = printer.output(&format!("{verb} {}\n", run.0));
        if !cli.quiet {
            let lines: String = plan
                .flows
                .iter()
                .map(|f| {
                    format!(
                        "  flow `{}`: {}{}\n",
                        f.name,
                        plan::strategy_name(f.strategy),
                        f.fallback
                            .as_ref()
                            .map_or(String::new(), |why| format!(" ({why})"))
                    )
                })
                .collect();
            let _ = printer.output(&lines);
        }

        let started = SystemTime::now();
        let execute = async {
            match &start {
                Start::New => self.runner.execute(&run).await,
                Start::Resume(_) => self.runner.resume(&run, hash, ENGINE_VERSION).await,
                Start::Retry(_) => self.runner.retry_failed(&run, hash, ENGINE_VERSION).await,
            }
        };
        let report = tokio::select! {
            report = execute => Some(report),
            _ = interrupt.wait() => None,
        };
        // The run future is dropped by now, and its workers with it.
        let Some(report) = report else {
            let released = self.leases.release().await;
            let d = Diagnostic::error(
                code::INTERRUPTED,
                format!(
                    "interrupted: in-flight partitions of run `{}` were abandoned, and {released} \
                     {} released",
                    run.0,
                    match released {
                        1 => "lease was",
                        _ => "leases were",
                    }
                ),
            )
            .help(format!("continue it with `ferry resume {}`", run.0));
            let _ = printer.diagnostics(&[d], &cli.config);
            return Exit::Interrupted;
        };
        let report = match report {
            Ok(report) => report,
            Err(e) => return run_error(printer, cli, Some(&run), e),
        };
        let finished = SystemTime::now();
        let timing = RunTiming { started, finished };
        let _ = printer.output(&summary_line(&report));
        for f in &report.failures {
            let d = Diagnostic::error(
                code::PARTITION_FAILED,
                format!(
                    "flow `{}`: partition `{}` failed: {}",
                    f.flow, f.partition.0, f.error
                ),
            );
            let _ = printer.diagnostics(&[d], &cli.config);
        }
        for flow in &report.blocked {
            let d = Diagnostic::error(
                code::PARTITION_FAILED,
                format!("flow `{flow}` didn't start: a flow it waits for has failed partitions"),
            );
            let _ = printer.diagnostics(&[d], &cli.config);
        }

        // Verification reads every target back: Ctrl-C stops it too.
        let verified = tokio::select! {
            failed = self.check(cli, printer, &run, &report, timing) => Some(failed),
            _ = interrupt.wait() => None,
        };
        let Some(verification_failed) = verified else {
            let d = Diagnostic::error(
                code::INTERRUPTED,
                format!(
                    "interrupted while verifying run `{}`; its data is written",
                    run.0
                ),
            )
            .help(format!(
                "`ferry resume {}` verifies it again once nothing is left to run",
                run.0
            ));
            let _ = printer.diagnostics(&[d], &cli.config);
            return Exit::Interrupted;
        };

        if report.counts.failed > 0 || !report.blocked.is_empty() {
            let _ = printer.note(&format!(
                "run `{0}` has failed partitions; `ferry retry --failed {0}` re-runs them\n",
                run.0
            ));
            return Exit::RunFailed;
        }
        if !report.is_complete() {
            let _ = printer.note(&format!(
                "run `{0}` has unfinished partitions; `ferry resume {0}` continues it\n",
                run.0
            ));
            return Exit::RunFailed;
        }
        match verification_failed {
            true => Exit::VerifyFailed,
            false => Exit::Success,
        }
    }

    /// Verifies each sink's flows, writes their reports and prints the
    /// outcome. `true` if any verification failed.
    async fn check(
        &self,
        cli: &Cli,
        printer: &mut Printer<'_>,
        run: &RunId,
        report: &RunReport,
        timing: RunTiming,
    ) -> bool {
        let dir = self.report_dir(run);
        let several = self.checks.len() > 1;
        let mut failed = false;
        for check in &self.checks {
            let target = match &check.how {
                How::Verified(target) => target,
                How::Unverified(why) => {
                    let d = Diagnostic::warning(code::UNVERIFIED, format!("not verified: {why}"));
                    let _ = printer.diagnostics(&[d], &cli.config);
                    // Without the verifier, what finalize couldn't restore
                    // is still a failed load.
                    let problems: Vec<Diagnostic> = report
                        .finalize_problems()
                        .filter(|(flow, _)| check.flows.iter().any(|f| f == flow))
                        .map(|(flow, p)| {
                            Diagnostic::error(
                                code::VERIFY_FAILED,
                                format!("flow `{flow}`: finalize couldn't restore {p}"),
                            )
                        })
                        .collect();
                    let _ = printer.diagnostics(&problems, &cli.config);
                    failed |= !problems.is_empty();
                    continue;
                }
            };
            let name = match several {
                true => format!("report-{}", check.sink),
                false => "report".to_owned(),
            };
            let sink = &check.sink;
            match self
                .verify(
                    check,
                    Arc::clone(target),
                    run,
                    report,
                    timing,
                    &dir,
                    &name,
                    printer,
                )
                .await
            {
                Ok(v) => {
                    let text = match v.status {
                        Status::Passed => "passed".to_owned(),
                        Status::Failed => format!("failed ({} problems)", v.problems.len()),
                        Status::Incomplete => "incomplete: the run isn't finished".to_owned(),
                    };
                    let _ = printer.output(&format!(
                        "verification of `{sink}` {text}; report: {}\n",
                        dir.join(format!("{name}.md")).display()
                    ));
                    let diagnostics: Vec<Diagnostic> = match v.status {
                        Status::Failed => v
                            .problems
                            .iter()
                            .map(|p| Diagnostic::error(code::VERIFY_FAILED, p.clone()))
                            .collect(),
                        _ => Vec::new(),
                    };
                    let _ = printer.diagnostics(&diagnostics, &cli.config);
                    failed |= v.status == Status::Failed;
                }
                Err(e) => {
                    let d = Diagnostic::error(
                        code::VERIFY_FAILED,
                        format!("could not verify the flows writing to `{sink}`: {e}"),
                    );
                    let _ = printer.diagnostics(&[d], &cli.config);
                    failed = true;
                }
            }
        }
        failed
    }

    fn report_dir(&self, run: &RunId) -> PathBuf {
        let safe = run
            .0
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
        let name = match safe {
            true => run.0.clone(),
            false => run.0.bytes().map(|b| format!("{b:02x}")).collect(),
        };
        self.work.join("runs").join(name)
    }

    /// Verifies `check`'s flows against `target`, and writes
    /// `<name>.json` and `<name>.md` into `dir`.
    #[allow(clippy::too_many_arguments)]
    async fn verify(
        &self,
        check: &Check,
        target: Arc<dyn Target>,
        run: &RunId,
        report: &RunReport,
        timing: RunTiming,
        dir: &Path,
        name: &str,
        printer: &Printer<'_>,
    ) -> ferry_core::Result<Verification> {
        let ours = |flow: &str| check.flows.iter().any(|f| f == flow);
        let flows: Vec<Flow> = self
            .flows
            .iter()
            .filter(|f| ours(&f.name))
            .cloned()
            .collect();
        let verifier = Verifier::new(
            Arc::clone(&self.store),
            Arc::clone(&self.write_log),
            target,
            &flows,
        )
        .in_run(&self.flows)
        .with_finalized(report);
        let checked: Vec<_> = self
            .planned
            .plan
            .checked()
            .into_iter()
            .filter(|c| ours(&c.name))
            .collect();
        let verifier = Expectations::new(&checked, Arc::clone(&self.source_log)).verifier(verifier);
        let verification = verifier.verify(run).await?;
        let written = Report::new(&verification).with_run_timing(timing);
        std::fs::create_dir_all(dir).map_err(|e| io_error(dir, e))?;
        for (file, text) in [
            (format!("{name}.json"), written.to_json()),
            (format!("{name}.md"), written.to_markdown()),
        ] {
            let path = dir.join(file);
            std::fs::write(&path, printer.redaction.apply(&text))
                .map_err(|e| io_error(&path, e))?;
        }
        Ok(verification)
    }
}

fn io_error(path: &Path, e: std::io::Error) -> ferry_core::FerryError {
    ferry_core::FerryError::Connector {
        connector: "report".into(),
        message: format!("{}: {e}", path.display()),
    }
}

fn summary_line(report: &RunReport) -> String {
    let c = &report.counts;
    let t = &report.totals;
    let state = match report.is_complete() {
        true => "complete".to_owned(),
        false => format!(
            "not complete ({} done, {} failed, {} pending, {} leased)",
            c.done, c.failed, c.pending, c.leased
        ),
    };
    // Dead letters are counted on their own.
    let written: Vec<String> = t
        .written
        .iter()
        .filter(|(table, _)| *table != ferry_runtime::DLQ_TABLE)
        .map(|(table, rows)| format!("{table} {rows}"))
        .collect();
    format!(
        "run {}: {state}; {} partitions; read {}, dead-lettered {}; wrote {}\n",
        report.run.0,
        c.done + c.failed + c.pending + c.leased,
        t.read,
        t.dead_lettered,
        match written.is_empty() {
            true => "nothing".to_owned(),
            false => written.join(", "),
        }
    )
}

/// Prints why the run couldn't run at all, and picks the exit code.
fn run_error(printer: &mut Printer<'_>, cli: &Cli, run: Option<&RunId>, e: RunError) -> Exit {
    let (exit, code, help) = match &e {
        RunError::Resume(ResumeError::NotFound(_)) => (
            Exit::Usage,
            code::RESUME_REFUSED,
            Some("the run id is the one `ferry run` printed first".to_owned()),
        ),
        RunError::Resume(ResumeError::ConfigChanged { .. } | ResumeError::EngineChanged { .. }) => {
            (Exit::ResumeRefused, code::RESUME_REFUSED, None)
        }
        RunError::Resume(ResumeError::Store(_)) | RunError::Store(_) => {
            (Exit::Connect, code::CONNECT, None)
        }
        RunError::NoStrategy { .. } => (Exit::PlanRefused, code::RUN, None),
        RunError::Flow { .. } => (
            Exit::RunFailed,
            code::RUN,
            run.map(|r| format!("fix the cause, then `ferry resume {}`", r.0)),
        ),
        RunError::Invalid(_) | RunError::Worker(_) => (Exit::Internal, code::RUN, None),
    };
    let mut d = Diagnostic::error(code, e.to_string());
    if let Some(help) = help {
        d = d.help(help);
    }
    let _ = printer.diagnostics(&[d], &cli.config);
    exit
}

/// Ctrl-C, caught from the moment `listen` returns.
struct Interrupt {
    #[cfg(unix)]
    signal: tokio::signal::unix::Signal,
}

impl Interrupt {
    fn listen() -> std::io::Result<Self> {
        Ok(Self {
            #[cfg(unix)]
            signal: tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?,
        })
    }

    async fn wait(&mut self) {
        #[cfg(unix)]
        self.signal.recv().await;
        #[cfg(not(unix))]
        let _ = tokio::signal::ctrl_c().await;
    }
}
