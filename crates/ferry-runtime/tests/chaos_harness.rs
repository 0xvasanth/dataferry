//! The chaos harness itself (`testkit::chaos`), on a fake run: a shell
//! child that "commits" 20 partitions one by one into a file in the run
//! directory and picks up where the file says on every attempt. No
//! database, so these check the harness's own promises quickly: every run
//! is killed mid-partition however fast the child is, and a run the
//! harness can't kill that way, a child that fails or a target that
//! doesn't verify fails it instead of passing.

#![cfg(unix)]

use std::path::Path;
use std::time::Duration;

use async_trait::async_trait;
use ferry_runtime::testkit::chaos::{self, Config, Progress, RUN_ID_FILE, Scenario};
use tokio::process::Command;

const PARTITIONS: u64 = 20;

/// Records the run id, then processes the partitions after the one the file
/// says, each for `pause` seconds under a lease (the `leased` file), and
/// commits it. Every attempt first appends where it starts to `starts`:
/// after a kill, that is how many partitions the kill left done. (The
/// harness never kills before the run id exists.)
fn script(pause: &str) -> String {
    format!(
        "i=$(cat done 2>/dev/null || echo 0)
         echo \"$i\" >> starts
         [ -f {RUN_ID_FILE} ] || {{ echo fake > {RUN_ID_FILE}.tmp; mv {RUN_ID_FILE}.tmp {RUN_ID_FILE}; }}
         while [ \"$i\" -lt {PARTITIONS} ]; do
             : > leased; sleep {pause}
             i=$((i + 1)); echo \"$i\" > done.tmp; mv done.tmp done; rm leased
         done"
    )
}

struct Fake {
    script: String,
    /// What `verify` says, if not that every partition is done.
    broken: Option<&'static str>,
}

impl Fake {
    fn new(script: String) -> Self {
        Self {
            script,
            broken: None,
        }
    }
}

fn done(dir: &Path) -> u64 {
    std::fs::read_to_string(dir.join("done"))
        .map(|s| s.trim().parse().unwrap())
        .unwrap_or(0)
}

#[async_trait]
impl Scenario for Fake {
    async fn reset(&self) -> Result<(), String> {
        Ok(())
    }

    fn child(&self, dir: &Path) -> Command {
        let mut cmd = Command::new("sh");
        cmd.args(["-c", &self.script]).current_dir(dir);
        cmd
    }

    async fn progress(&self, dir: &Path) -> Result<Progress, String> {
        Ok(Progress {
            started: dir.join(RUN_ID_FILE).exists(),
            done: done(dir),
            leased: u64::from(dir.join("leased").exists()),
            total: PARTITIONS,
        })
    }

    async fn verify(&self, dir: &Path) -> Result<(), String> {
        if let Some(why) = self.broken {
            return Err(why.into());
        }
        match done(dir) {
            PARTITIONS => Ok(()),
            n => Err(format!("{n} of {PARTITIONS} partitions")),
        }
    }

    async fn explain(&self, dir: &Path) -> String {
        format!(" (done: {})", done(dir))
    }
}

/// Partitions done at each kill of the run kept in `dir`: where every
/// attempt but the first started.
fn kill_points(dir: &Path) -> Vec<u64> {
    let starts = std::fs::read_to_string(dir.join("starts")).unwrap();
    let starts: Vec<u64> = starts.lines().map(|l| l.parse().unwrap()).collect();
    assert_eq!(starts[0], 0, "the first attempt starts the run");
    starts[1..].to_vec()
}

fn config(runs: usize) -> Config {
    Config {
        runs,
        seed: 7,
        max_kills: 3,
        in_flight: 1,
        poll: Duration::from_millis(1),
        timeout: Duration::from_secs(30),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn every_run_is_killed_between_partitions_and_resumed_to_the_end() {
    let fake = Fake::new(script("0.01"));
    let work = tempfile::tempdir().unwrap();

    let report = chaos::run(&config(20), &fake, work.path()).await.unwrap();

    assert_eq!(report.runs, 20, "{report:?}");
    assert!(report.kills_mid_partition >= 20, "{report:?}");
    assert!(report.kills >= report.kills_mid_run, "{report:?}");
    assert!(report.kills_mid_run >= report.kills_between_partitions);
    assert!(report.kills_between_partitions >= report.kills_mid_partition);
    // Every run got to the end, and was killed with partitions done and
    // left: each attempt after the first resumed mid-run.
    let mut kills = 0;
    for i in 0..20 {
        let dir = work.path().join(format!("run-{i}"));
        assert_eq!(done(&dir), PARTITIONS);
        let points = kill_points(&dir);
        assert!(
            points.iter().any(|p| (1..PARTITIONS).contains(p)),
            "{points:?}"
        );
        assert!(points.iter().all(|p| *p < PARTITIONS), "{points:?}");
        kills += points.len();
    }
    // A resumed child can be killed before it ran a command (when its kill
    // point is what is done already), and then logs no start.
    assert!(kills <= report.kills && kills >= report.runs, "{report:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn kill_points_come_from_progress_not_from_how_fast_the_child_is() {
    // A child ten times slower than the one above: wall-clock kill windows
    // calibrated on the fast one would all land in its first partitions.
    // Progress-based points spread over the whole run either way.
    for pause in ["0.002", "0.02"] {
        let work = tempfile::tempdir().unwrap();
        let fake = Fake::new(script(pause));
        let mut config = config(8);
        config.seed = 11;
        let report = chaos::run(&config, &fake, work.path()).await.unwrap();
        assert_eq!(report.runs, 8, "{pause}: {report:?}");
        assert!(report.kills_mid_partition >= 8, "{pause}: {report:?}");
        let points: Vec<u64> = (0..8)
            .flat_map(|i| kill_points(&work.path().join(format!("run-{i}"))))
            .collect();
        let (low, high) = (points.iter().min().unwrap(), points.iter().max().unwrap());
        assert!(*low <= 5 && *high >= 12, "{pause}: kills at {points:?}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_child_too_fast_to_kill_between_partitions_fails_the_harness() {
    // Every partition at once: no kill can land between two of them, so
    // no run may count.
    let fake = Fake::new(format!(
        "echo fake > {RUN_ID_FILE}.tmp; mv {RUN_ID_FILE}.tmp {RUN_ID_FILE}; \
         echo {PARTITIONS} > done.tmp; mv done.tmp done"
    ));
    let work = tempfile::tempdir().unwrap();

    let e = chaos::run(&config(1), &fake, work.path())
        .await
        .unwrap_err();

    assert!(
        e.contains("no kill landed mid-partition in 6 attempts"),
        "{e}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_target_that_does_not_verify_fails_the_run_and_says_why() {
    let mut fake = Fake::new(script("0.002"));
    fake.broken = Some("orders: 3 duplicated");
    let work = tempfile::tempdir().unwrap();

    let e = chaos::run(&config(5), &fake, work.path())
        .await
        .unwrap_err();

    assert!(e.starts_with("run-0 (seed 7): orders: 3 duplicated"), "{e}");
    // With what the scenario adds about the run.
    assert!(e.ends_with(&format!(" (done: {PARTITIONS})")), "{e}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_child_that_fails_fails_the_harness() {
    let fake = Fake::new("exit 3".into());
    let work = tempfile::tempdir().unwrap();

    let e = chaos::run(&config(1), &fake, work.path())
        .await
        .unwrap_err();

    assert!(
        e.contains("a child failed before its kill: exit status: 3"),
        "{e}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_child_that_never_progresses_times_out() {
    let fake = Fake::new(format!("echo fake > {RUN_ID_FILE}; sleep 30"));
    let work = tempfile::tempdir().unwrap();
    let mut config = config(1);
    config.timeout = Duration::from_millis(300);

    let e = chaos::run(&config, &fake, work.path()).await.unwrap_err();

    assert!(
        e.contains(
            "the child didn't reach its kill point within 300ms \
             (no partition was ever reported leased)"
        ),
        "{e}"
    );
}
