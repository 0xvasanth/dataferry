//! The kill -9 chaos harness: a run is started in a child process, killed
//! with SIGKILL at random points, resumed each time, and finally left to
//! finish; then the target must hold exactly the expected rows.
//!
//! The harness knows nothing about the target or the state store. A test
//! supplies a [`Scenario`]: how to reset them, the command that runs the
//! child, and how to compare the target with what's expected. The child
//! calls [`child_run`], which starts the run the first time and resumes it
//! every time after, so the same command serves every attempt.
//!
//! ```text
//! for each of `runs` runs:
//!     reset the target and the state store
//!     1..=max_kills times: spawn the child, wait a random time, kill -9
//!     spawn the child and let it finish (it must exit 0)
//!     verify the target
//! ```

use std::path::{Path, PathBuf};
use std::time::Duration;

use async_trait::async_trait;
use ferry_registry::state::{NewRun, RunId};
use tokio::process::Command;
use tokio::time::Instant;

use crate::{RunError, RunReport, Runner};

/// Uninterrupted runs timed to find the kill window.
const CALIBRATION_RUNS: usize = 3;

/// The file in the run directory that holds the run id once it exists.
pub const RUN_ID_FILE: &str = "run-id";

#[derive(Debug, Clone)]
pub struct Config {
    /// Runs, each from an empty target and state store.
    pub runs: usize,
    pub seed: u64,
    /// Kills per run, at most. Each run draws its own number, at least one.
    pub max_kills: usize,
    /// Each kill lands at a uniformly random time within this window after
    /// the child is spawned. `None`: the length of the fastest of three
    /// uninterrupted runs, measured first (they are verified too, and not
    /// counted).
    pub kill_window: Option<Duration>,
    /// How long the last, uninterrupted child may take.
    pub timeout: Duration,
}

/// What happened, for the test to assert on and print.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Report {
    pub runs: usize,
    pub kills: usize,
    /// Kills that landed after the run was created and before it was
    /// complete: the ones that test resume.
    pub kills_mid_run: usize,
    /// Kills that landed after some, but not all, partitions were done.
    pub kills_between_partitions: usize,
    /// Children that finished before their kill was due.
    pub finished_early: usize,
    pub kill_window: Duration,
}

/// How far a run got, read from the state store between children.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Progress {
    /// The run exists.
    pub started: bool,
    pub done: u64,
    pub total: u64,
}

#[async_trait]
pub trait Scenario: Send + Sync {
    /// Empties the target and the state store before a run.
    async fn reset(&self) -> Result<(), String>;

    /// The command for one attempt at the run kept in `dir`. It must call
    /// [`child_run`] with `dir`, and exit 0 only if the run completed.
    fn child(&self, dir: &Path) -> Command;

    /// How far the run kept in `dir` got.
    async fn progress(&self, dir: &Path) -> Result<Progress, String>;

    /// `Err` with a description of the difference unless the target holds
    /// exactly the expected rows: no duplicates, no gaps.
    async fn verify(&self) -> Result<(), String>;
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
            let tmp = dir.join(format!("{RUN_ID_FILE}.tmp"));
            std::fs::write(&tmp, &id.0)?;
            std::fs::File::open(&tmp)?.sync_all()?;
            std::fs::rename(&tmp, &path)?;
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

/// The run id recorded in `dir`, if the run was created.
pub fn recorded_run(dir: &Path) -> Option<RunId> {
    std::fs::read_to_string(dir.join(RUN_ID_FILE))
        .ok()
        .map(RunId)
}

/// Runs the chaos test. `work` holds one directory per run. Fails on the
/// first run whose target doesn't verify, or whose last child fails.
pub async fn run(config: &Config, scenario: &dyn Scenario, work: &Path) -> Result<Report, String> {
    let mut rng = fastrand::Rng::with_seed(config.seed);
    let kill_window = match config.kill_window {
        Some(window) => window,
        None => {
            // The fastest of a few, so a cold first start (caches, the
            // database's first connections) doesn't stretch the window
            // past the run.
            let mut fastest = Duration::MAX;
            for i in 0..CALIBRATION_RUNS {
                let dir = fresh_dir(work, &format!("calibrate-{i}"))?;
                scenario.reset().await?;
                let start = Instant::now();
                finish(config, scenario, &dir).await?;
                fastest = fastest.min(start.elapsed());
                scenario
                    .verify()
                    .await
                    .map_err(|e| format!("uninterrupted run {i}: {e}"))?;
            }
            fastest
        }
    };
    let mut report = Report {
        kill_window,
        ..Report::default()
    };
    for i in 0..config.runs {
        let dir = fresh_dir(work, &format!("run-{i}"))?;
        scenario.reset().await?;
        let kills = rng.usize(1..=config.max_kills.max(1));
        for _ in 0..kills {
            let mut child = scenario
                .child(&dir)
                .kill_on_drop(true)
                .spawn()
                .map_err(|e| format!("spawning the child: {e}"))?;
            let delay = kill_window.mul_f64(rng.f64());
            let exited = tokio::time::timeout(delay, child.wait()).await;
            if let Ok(status) = exited {
                let status = status.map_err(|e| e.to_string())?;
                if !status.success() {
                    return Err(format!("run {i}: a child failed before its kill: {status}"));
                }
                report.finished_early += 1;
                break;
            }
            // SIGKILL: no destructors, no flush, no rollback.
            child.start_kill().map_err(|e| e.to_string())?;
            child.wait().await.map_err(|e| e.to_string())?;
            report.kills += 1;
            let progress = scenario.progress(&dir).await?;
            if progress.started && (progress.total == 0 || progress.done < progress.total) {
                report.kills_mid_run += 1;
            }
            if progress.done > 0 && progress.done < progress.total {
                report.kills_between_partitions += 1;
            }
        }
        finish(config, scenario, &dir)
            .await
            .map_err(|e| format!("run {i}: {e}"))?;
        scenario
            .verify()
            .await
            .map_err(|e| format!("run {i} (seed {}): {e}", config.seed))?;
        report.runs += 1;
    }
    Ok(report)
}

/// Runs the child until it exits, which must be with success.
async fn finish(config: &Config, scenario: &dyn Scenario, dir: &Path) -> Result<(), String> {
    let mut child = scenario
        .child(dir)
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| format!("spawning the child: {e}"))?;
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
