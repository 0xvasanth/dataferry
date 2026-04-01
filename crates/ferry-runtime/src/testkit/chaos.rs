//! The kill -9 chaos harness: a run is started in a child process, killed
//! with SIGKILL at random points, resumed each time, and finally left to
//! finish; then the target must hold exactly the expected rows.
//!
//! The harness knows nothing about the target or the state store. A test
//! supplies a [`Scenario`]: how to reset them, the command that runs the
//! child, how far a run got, and how to compare the target with what's
//! expected. A child built on the runner calls [`child_run`], which starts
//! the run the first time and resumes it every time after, so the same
//! command serves every attempt; a child that is a CLI binary is driven by
//! [`conformance`](super::conformance) instead.
//!
//! ```text
//! for each of `runs` runs:
//!     reset the target and the state store
//!     sometimes: spawn the child, kill -9 it as soon as the run exists
//!     1..=max_kills times: spawn the child, watch its progress, kill -9 it
//!         once the partitions done reach the kill point drawn for it
//!     unless a child finished: spawn the child, let it finish (exit 0)
//!     verify the target
//!     no kill landed mid-partition: count the run as redone, run it again
//! ```
//!
//! Kill points are counts of partitions done, read from the state store
//! while the child runs, not times: a busy machine slows the child and the
//! harness alike, so where a kill lands doesn't depend on how loaded the
//! machine is. A kill point is drawn uniformly from the partitions done so
//! far up to `total - in_flight - 1`; the first kill of every run waits for
//! at least one partition, and every kill but an early one waits for a
//! partition to be leased. The kill comes a poll after the child reached
//! its point, by which time it has done little more, so it lands with
//! partitions done, partitions left and one in flight: mid-partition. A
//! kill counts as such only if the store says so once the child is dead, so
//! a run whose kills all missed (the harness was descheduled long enough to
//! watch the child finish, say) is redone rather than counted.
//!
//! The store can't say whose lease it holds: a killed child's lease counts
//! as in flight until it expires. So a kill of a resumed child can count
//! as mid-partition on a dead child's lease alone. When no early kill came
//! first, the first drawn kill of a run can't: its child started the run,
//! with no lease before its own, and must have committed a partition
//! itself. After an early kill, any kill of a resumed child can.

use std::path::{Path, PathBuf};
use std::time::Duration;

use async_trait::async_trait;
use ferry_registry::StateStore;
use ferry_registry::state::{NewRun, RunId};
use tokio::process::{Child, Command};
use tokio::time::Instant;

use crate::{RunError, RunReport, Runner};

/// The file in the run directory that holds the run id once it exists.
pub const RUN_ID_FILE: &str = "run-id";

/// Times a run may be redone because none of its kills landed
/// mid-partition, before the harness gives up.
const MAX_REDOS: usize = 5;

#[derive(Debug, Clone)]
pub struct Config {
    /// Runs, each from an empty target and state store.
    pub runs: usize,
    pub seed: u64,
    /// Kills per run, at most. Each run draws its own number, at least one.
    pub max_kills: usize,
    /// Partitions the child can be processing at once: its workers. A kill
    /// point leaves at least this many partitions, plus one, undone.
    pub in_flight: u64,
    /// How often the child's progress is read while it runs.
    pub poll: Duration,
    /// How long one child may take, killed or not.
    pub timeout: Duration,
}

/// What happened, for the test to assert on and print.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Report {
    /// Runs that verified, each with at least one kill mid-partition.
    pub runs: usize,
    pub kills: usize,
    /// Kills that landed after the run was created and before it was
    /// complete: the ones that test resume.
    pub kills_mid_run: usize,
    /// Kills that landed after some, but not all, partitions were done.
    pub kills_between_partitions: usize,
    /// Kills that landed between partitions with a partition leased and
    /// not committed.
    pub kills_mid_partition: usize,
    /// Children that finished before reaching their kill point.
    pub finished_early: usize,
    /// Runs whose kills all missed, run again from scratch. They were
    /// verified too, but aren't counted in `runs`.
    pub redone: usize,
}

/// How far a run got, read from the state store, between children and
/// while one runs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Progress {
    /// The run exists.
    pub started: bool,
    pub done: u64,
    /// Partitions under a lease that hasn't expired: being processed, or
    /// held by a process that was killed.
    pub leased: u64,
    /// Partitions planned so far. Flows are planned when a child starts
    /// executing the run, so this can grow while the first child runs.
    pub total: u64,
}

impl Progress {
    /// Reads `run`'s progress from `store`.
    pub async fn of(store: &dyn StateStore, run: &RunId) -> ferry_core::Result<Self> {
        let counts = store.counts(run).await?;
        Ok(Self {
            started: true,
            done: counts.done,
            leased: counts.leased,
            total: counts.pending + counts.leased + counts.done + counts.failed,
        })
    }

    /// Created, and neither nothing nor everything done.
    pub fn between_partitions(&self) -> bool {
        self.started && self.done > 0 && self.done < self.total
    }

    /// Between partitions, with one leased and not committed.
    pub fn mid_partition(&self) -> bool {
        self.between_partitions() && self.leased > 0
    }

    fn mid_run(&self) -> bool {
        self.started && (self.total == 0 || self.done < self.total)
    }
}

#[async_trait]
pub trait Scenario: Send + Sync {
    /// Empties the target and the state store before a run.
    async fn reset(&self) -> Result<(), String>;

    /// The command for one attempt at the run kept in `dir`. It must exit
    /// 0 only if the run completed.
    fn child(&self, dir: &Path) -> Command;

    /// How far the run kept in `dir` got. Called while a child runs too,
    /// so it must not disturb it.
    async fn progress(&self, dir: &Path) -> Result<Progress, String>;

    /// `Err` with a description of the difference unless the target holds
    /// exactly the expected rows, no duplicates and no gaps, once the run
    /// kept in `dir` finished.
    async fn verify(&self, dir: &Path) -> Result<(), String>;

    /// Called after each kill, before the run's progress is read: does what
    /// the databases would do on their own once the child's connections
    /// closed, where the test's network hides that they did. A
    /// port-forwarding proxy (Docker Desktop, OrbStack) can keep a killed
    /// client's server-side session open, and with it its transaction and
    /// locks, which the next child or `reset` then waits on for ever.
    async fn killed(&self, _dir: &Path) -> Result<(), String> {
        Ok(())
    }

    /// More about the run kept in `dir` when it fails, such as the last
    /// child's output.
    async fn explain(&self, _dir: &Path) -> String {
        String::new()
    }
}

/// The child's half: resumes the run recorded in `dir`, or creates one,
/// records its id (atomically, before any work starts) and executes it.
pub async fn child_run(runner: &Runner, run: NewRun, dir: &Path) -> Result<RunReport, ChildError> {
    let path = dir.join(RUN_ID_FILE);
    match std::fs::read_to_string(&path) {
        Ok(id) => {
            let id = RunId(id);
            Ok(runner
                .resume(&id, &run.config_hash, &run.engine_version)
                .await?)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // A kill between creating the run and recording it leaves a run
            // nobody resumes; it never leased anything, so it wrote nothing.
            let id = runner.create(run).await?;
            record_run(dir, &id)?;
            Ok(runner.execute(&id).await?)
        }
        Err(e) => Err(e.into()),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ChildError {
    #[error(transparent)]
    Run(#[from] RunError),
    #[error("recording the run id: {0}")]
    Io(#[from] std::io::Error),
}

/// Records `run` as the run kept in `dir`: written to a temp file, synced,
/// then renamed, so a kill leaves either no id or the whole id.
pub fn record_run(dir: &Path, run: &RunId) -> std::io::Result<()> {
    let tmp = dir.join(format!("{RUN_ID_FILE}.tmp"));
    std::fs::write(&tmp, &run.0)?;
    std::fs::File::open(&tmp)?.sync_all()?;
    std::fs::rename(&tmp, dir.join(RUN_ID_FILE))
}

/// The run id recorded in `dir`, if the run was created.
pub fn recorded_run(dir: &Path) -> Option<RunId> {
    std::fs::read_to_string(dir.join(RUN_ID_FILE))
        .ok()
        .map(RunId)
}

/// Runs the chaos test. `work` holds one directory per run. Fails on the
/// first run whose target doesn't verify or whose last child fails, on a
/// child that dies other than by the harness's SIGKILL, and on a run
/// redone more than a few times.
pub async fn run(config: &Config, scenario: &dyn Scenario, work: &Path) -> Result<Report, String> {
    let mut rng = fastrand::Rng::with_seed(config.seed);
    let mut report = Report::default();
    for i in 0..config.runs {
        let mut redos = 0;
        loop {
            let name = match redos {
                0 => format!("run-{i}"),
                n => format!("run-{i}-redo-{n}"),
            };
            let dir = fresh_dir(work, &name)?;
            let landed = match one_run(config, scenario, &dir, &mut rng, &mut report).await {
                Ok(landed) => landed,
                Err(e) => {
                    let more = scenario.explain(&dir).await;
                    return Err(format!("{name} (seed {}): {e}{more}", config.seed));
                }
            };
            if landed {
                report.runs += 1;
                break;
            }
            report.redone += 1;
            redos += 1;
            if redos > MAX_REDOS {
                return Err(format!(
                    "run {i}: no kill landed mid-partition in {redos} attempts: {report:?}"
                ));
            }
        }
    }
    Ok(report)
}

/// One run, from reset to verified. `true` if one of its kills landed
/// mid-partition.
async fn one_run(
    config: &Config,
    scenario: &dyn Scenario,
    dir: &Path,
    rng: &mut fastrand::Rng,
    report: &mut Report,
) -> Result<bool, String> {
    bounded(config, "reset", scenario.reset()).await?;
    // One in four runs first gets an early kill, as soon as the run exists
    // (while it plans, or with its first partitions in flight). Then
    // 1..=max_kills kills, the first once a partition is done. A later
    // kill's point can be what is done already: then the resumed child is
    // killed as it starts. Drawn whether or not they are used, so a seed
    // draws the same points.
    let mut kills = Vec::new();
    if rng.usize(..4) == 0 {
        kills.push(Point::Early);
    }
    for k in 0..rng.usize(1..=config.max_kills.max(1)) {
        kills.push(Point::Drawn {
            at: rng.f64(),
            at_least: u64::from(k == 0),
        });
    }
    let mut landed = false;
    let mut finished = false;
    for point in kills {
        let mut child = spawn(scenario, dir)?;
        match kill_at(config, scenario, dir, &mut child, point).await? {
            Attempt::Killed => {}
            Attempt::Finished => {
                report.finished_early += 1;
                finished = true;
                break;
            }
            // Too far along for a kill to land before the end.
            Attempt::NoRoom => {
                finish_child(config, child).await?;
                finished = true;
                break;
            }
        }
        report.kills += 1;
        bounded(config, "cleaning up after a kill", scenario.killed(dir)).await?;
        let progress = bounded(config, "reading progress", scenario.progress(dir)).await?;
        if progress.mid_run() {
            report.kills_mid_run += 1;
        }
        if progress.between_partitions() {
            report.kills_between_partitions += 1;
        }
        if progress.mid_partition() {
            report.kills_mid_partition += 1;
            landed = true;
        }
    }
    // A child that exited 0 finished the run (and, through a CLI, wrote its
    // report); a kill after the last partition, before the end, didn't.
    if !finished {
        finish_child(config, spawn(scenario, dir)?).await?;
    }
    bounded(config, "verifying", scenario.verify(dir)).await?;
    Ok(landed)
}

#[derive(Debug, Clone, Copy)]
enum Point {
    /// As soon as the run exists.
    Early,
    /// A fraction `at` of the way from `at_least` partitions done (or what
    /// is done already, if more) to the last point a kill can't miss, with
    /// a partition leased.
    Drawn { at: f64, at_least: u64 },
}

enum Attempt {
    Killed,
    Finished,
    NoRoom,
}

/// Watches `child` until the run reaches `point`, then kills it with
/// SIGKILL.
async fn kill_at(
    config: &Config,
    scenario: &dyn Scenario,
    dir: &Path,
    child: &mut Child,
    point: Point,
) -> Result<Attempt, String> {
    let deadline = Instant::now() + config.timeout;
    let mut done_at = None;
    let mut seen_leased = false;
    loop {
        if let Some(status) = child.try_wait().map_err(|e| e.to_string())? {
            return exited(status);
        }
        if Instant::now() > deadline {
            let why = match seen_leased {
                true => "",
                false => " (no partition was ever reported leased)",
            };
            return Err(format!(
                "the child didn't reach its kill point within {:?}{why}",
                config.timeout
            ));
        }
        let p = bounded(config, "reading progress", scenario.progress(dir)).await?;
        seen_leased |= p.leased > 0;
        let reached = match (point, done_at) {
            _ if !p.started => false,
            (Point::Early, _) => true,
            (Point::Drawn { .. }, Some(done_at)) => p.done >= done_at && p.leased > 0,
            // Not planned yet.
            (Point::Drawn { .. }, None) if p.total == 0 => false,
            (Point::Drawn { at, at_least }, None) => {
                let last = p.total.saturating_sub(config.in_flight + 1);
                let first = at_least.max(p.done);
                if first > last {
                    return Ok(Attempt::NoRoom);
                }
                let span = (last - first + 1) as f64;
                let drawn = first + ((at * span) as u64).min(last - first);
                p.done >= *done_at.insert(drawn) && p.leased > 0
            }
        };
        if reached {
            // SIGKILL: no destructors, no flush, no rollback.
            child.start_kill().map_err(|e| e.to_string())?;
            let status = child.wait().await.map_err(|e| e.to_string())?;
            return match killed(status) {
                true => Ok(Attempt::Killed),
                // It exited on its own just before the kill.
                false => exited(status),
            };
        }
        tokio::time::sleep(config.poll).await;
    }
}

/// A child that exited by itself: it finished the run, or failed.
fn exited(status: std::process::ExitStatus) -> Result<Attempt, String> {
    match status.success() {
        true => Ok(Attempt::Finished),
        false => Err(format!("a child failed before its kill: {status}")),
    }
}

/// Whether the child died of the harness's SIGKILL.
#[cfg(unix)]
fn killed(status: std::process::ExitStatus) -> bool {
    use std::os::unix::process::ExitStatusExt;
    status.signal() == Some(9)
}

#[cfg(not(unix))]
fn killed(status: std::process::ExitStatus) -> bool {
    !status.success()
}

/// `f`, given as long as a child: no step of the harness waits for ever.
async fn bounded<T>(
    config: &Config,
    what: &str,
    f: impl std::future::Future<Output = Result<T, String>>,
) -> Result<T, String> {
    tokio::time::timeout(config.timeout, f)
        .await
        .map_err(|_| format!("{what} didn't finish within {:?}", config.timeout))?
}

fn spawn(scenario: &dyn Scenario, dir: &Path) -> Result<Child, String> {
    scenario
        .child(dir)
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| format!("spawning the child: {e}"))
}

/// Waits for `child` to exit, which must be with success.
async fn finish_child(config: &Config, mut child: Child) -> Result<(), String> {
    let status = tokio::time::timeout(config.timeout, child.wait())
        .await
        .map_err(|_| format!("the last child didn't finish within {:?}", config.timeout))?
        .map_err(|e| e.to_string())?;
    if !status.success() {
        return Err(format!("the last child exited with {status}"));
    }
    Ok(())
}

fn fresh_dir(work: &Path, name: &str) -> Result<PathBuf, String> {
    let dir = work.join(name);
    if dir.exists() {
        std::fs::remove_dir_all(&dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    }
    std::fs::create_dir_all(&dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    Ok(dir)
}
