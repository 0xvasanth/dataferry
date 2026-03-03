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
//! - After the run, the flows are verified and the report is written to
//!   `<work dir>/runs/<run id>/report.{json,md}`. The verifier checks one
//!   target against the run's totals, so a run is verified when every flow
//!   writes to one sink this binary can read back. Any other run is
//!   reported unverified (a warning), and what finalize couldn't restore
//!   still fails it.
//!
//! `run` prints the run id before any partition is processed, so a run
//! that is killed can always be resumed.

use std::collections::BTreeSet;
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
use crate::plan::{self, Planned};

/// The engine version a run is pinned to (A4).
pub(crate) const ENGINE_VERSION: &str = env!("CARGO_PKG_VERSION");

/// How long a lease lasts without a heartbeat. A run resumed after a crash
/// waits at most this long for the crashed process's partitions.
const LEASE_TTL: Duration = Duration::from_secs(30);
const HEARTBEAT: Duration = Duration::from_secs(10);

/// Overrides [`LEASE_TTL`], in milliseconds, with a heartbeat a third as
/// long: the chaos and conformance suites kill runs and resume them at once.
const LEASE_TTL_ENV: &str = "FERRY_LEASE_TTL_MS";

/// Which command is running.
#[derive(Debug, Clone)]
pub(crate) enum Start {
    New,
    Resume(String),
    Retry(String),
}

pub(crate) async fn run(ferry: &Ferry, cli: &Cli, printer: &mut Printer<'_>, start: Start) -> Exit {
    let lease = match std::env::var(LEASE_TTL_ENV) {
        Err(std::env::VarError::NotPresent) => (LEASE_TTL, HEARTBEAT),
        ms => match ms.ok().and_then(|ms| ms.parse::<u64>().ok()) {
            Some(ms) if ms >= 3 => (Duration::from_millis(ms), Duration::from_millis(ms / 3)),
            _ => {
                let d = Diagnostic::error(
                    code::RUN,
                    format!("{LEASE_TTL_ENV} must be a whole number of milliseconds, at least 3"),
                );
                let _ = printer.diagnostics(&[d], &cli.config);
                return Exit::Usage;
            }
        },
    };
    let planned = plan::plan(
        &ferry.registry,
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
    match Execution::new(ferry, cli, &planned, lease) {
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

/// How a run is verified (A8).
enum Check {
    /// Every flow writes to `sink`, read back through `target`.
    Verified {
        sink: String,
        target: Arc<dyn Target>,
    },
    /// Not verified, and why: a warning, not a failure.
    Unverified(String),
}

impl Check {
    /// `Verifier` compares one target with the run's totals in the state
    /// store, which cover every flow, so only a run with one sink can be
    /// verified as a whole. `Err` when the sink's target can't be opened:
    /// the run must not start, since what it wrote could never be
    /// verified.
    fn of(
        ferry: &Ferry,
        summary: &crate::validate::Summary,
        flows: &[PlannedFlow],
    ) -> Result<Self, String> {
        let sinks: BTreeSet<&str> = flows.iter().map(|f| f.sink.as_str()).collect();
        let [sink] = sinks.into_iter().collect::<Vec<_>>()[..] else {
            let names: Vec<String> = flows
                .iter()
                .map(|f| format!("`{}`", f.sink))
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();
            return Ok(Check::Unverified(format!(
                "the run writes to several sinks ({}); verifying a run across sinks isn't \
                 supported yet",
                names.join(", ")
            )));
        };
        let raw = summary
            .raw
            .get(sink)
            .ok_or_else(|| format!("connection `{sink}` was not built"))?;
        let Some(factory) = ferry.targets.get(&raw.r#type) else {
            return Ok(Check::Unverified(format!(
                "this binary can't read `{}` connections back, so `{sink}` isn't verified",
                raw.r#type
            )));
        };
        let target =
            factory(raw).map_err(|e| format!("could not open `{sink}` to verify it: {e}"))?;
        Ok(Check::Verified {
            sink: sink.to_owned(),
            target,
        })
    }
}

struct Execution<'a> {
    planned: &'a Planned,
    work: PathBuf,
    store: Arc<dyn StateStore>,
    write_log: Arc<dyn WriteLog>,
    source_log: Arc<dyn SourceLog>,
    flows: Vec<Flow>,
    check: Check,
    runner: Runner,
}

impl<'a> Execution<'a> {
    fn new(
        ferry: &Ferry,
        cli: &Cli,
        planned: &'a Planned,
        (lease_ttl, heartbeat): (Duration, Duration),
    ) -> Result<Self, Diagnostic> {
        let Planned { summary, plan } = planned;
        let work = work_dir(cli);
        let store: Arc<dyn StateStore> = match &summary.state_store {
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
        let write_log: Arc<dyn WriteLog> = Arc::new(FileWriteLog::new(work.join("writes")));
        let source_log: Arc<dyn SourceLog> = Arc::new(FileSourceLog::new(work.join("sources")));

        let internal = |message: String| Diagnostic::error(code::RUN, message);
        let mut flows = Vec::new();
        for planned_flow in &plan.flows {
            let source = summary
                .endpoints
                .get(planned_flow.driver.connection.as_str())
                .and_then(|e| e.source.clone())
                .ok_or_else(|| internal(format!("`{}` has no source", planned_flow.driver)))?;
            let sink_endpoints = summary
                .endpoints
                .get(&planned_flow.sink)
                .ok_or_else(|| internal(format!("`{}` was not built", planned_flow.sink)))?;
            let sink = sink_endpoints
                .sink
                .clone()
                .ok_or_else(|| internal(format!("`{}` has no sink", planned_flow.sink)))?;
            flows.push(planned_flow.runtime_flow(source, sink, sink_endpoints.finalizer.clone()));
        }
        let check = Check::of(ferry, summary, &plan.flows)
            .map_err(|why| Diagnostic::error(code::VERIFY_FAILED, why).help("nothing was run"))?;

        let settings = Settings {
            lease_ttl,
            heartbeat,
            ..ferry_dsl::plan::settings(&summary.config)
        };
        let mut runner = Runner::new(Arc::clone(&store), flows.clone(), settings)
            .map_err(|e| internal(format!("the planned flows don't fit the runner: {e}")))?;
        if let Check::Verified { target, .. } = &check {
            runner = runner.with_observer(Arc::new(ChecksumObserver::new(
                Arc::clone(&write_log),
                Arc::clone(target),
            )));
        }
        let expectations = Expectations::new(&plan.checked(), Arc::clone(&source_log));
        runner = runner.with_observer(expectations.observer());
        Ok(Self {
            planned,
            work,
            store,
            write_log,
            source_log,
            flows,
            check,
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
            report = execute => report,
            _ = interrupt.wait() => {
                let d = Diagnostic::error(
                    code::INTERRUPTED,
                    format!("interrupted: in-flight partitions of run `{}` were abandoned", run.0),
                )
                .help(format!("continue it with `ferry resume {}`", run.0));
                let _ = printer.diagnostics(&[d], &cli.config);
                return Exit::Interrupted;
            }
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

    /// Verifies the run, writes its report and prints the outcome. `true`
    /// if verification failed.
    async fn check(
        &self,
        cli: &Cli,
        printer: &mut Printer<'_>,
        run: &RunId,
        report: &RunReport,
        timing: RunTiming,
    ) -> bool {
        let (sink, target) = match &self.check {
            Check::Verified { sink, target } => (sink, target),
            Check::Unverified(why) => {
                let d = Diagnostic::warning(code::UNVERIFIED, format!("not verified: {why}"));
                let _ = printer.diagnostics(&[d], &cli.config);
                // Without the verifier, what finalize couldn't restore is
                // still a failed load.
                let problems: Vec<Diagnostic> = report
                    .finalize_problems()
                    .map(|(flow, p)| {
                        Diagnostic::error(
                            code::VERIFY_FAILED,
                            format!("flow `{flow}`: finalize couldn't restore {p}"),
                        )
                    })
                    .collect();
                let _ = printer.diagnostics(&problems, &cli.config);
                return !problems.is_empty();
            }
        };
        let dir = self.report_dir(run);
        match self
            .verify(Arc::clone(target), run, report, timing, &dir, printer)
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
                    dir.join("report.md").display()
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
                v.status == Status::Failed
            }
            Err(e) => {
                let d = Diagnostic::error(
                    code::VERIFY_FAILED,
                    format!("could not verify the flows writing to `{sink}`: {e}"),
                );
                let _ = printer.diagnostics(&[d], &cli.config);
                true
            }
        }
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

    async fn verify(
        &self,
        target: Arc<dyn Target>,
        run: &RunId,
        report: &RunReport,
        timing: RunTiming,
        dir: &Path,
        printer: &Printer<'_>,
    ) -> ferry_core::Result<Verification> {
        let verifier = Verifier::new(
            Arc::clone(&self.store),
            Arc::clone(&self.write_log),
            target,
            &self.flows,
        )
        .with_finalized(report);
        let checked = self.planned.plan.checked();
        let verifier = Expectations::new(&checked, Arc::clone(&self.source_log)).verifier(verifier);
        let verification = verifier.verify(run).await?;
        let written = Report::new(&verification).with_run_timing(timing);
        std::fs::create_dir_all(dir).map_err(|e| io_error(dir, e))?;
        for (name, text) in [
            ("report.json", written.to_json()),
            ("report.md", written.to_markdown()),
        ] {
            let path = dir.join(name);
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
