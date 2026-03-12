//! The connector conformance suite: what a connector pair must pass to be
//! called certified. It drives a real `ferry` binary (or any binary built
//! on `ferry-cli`, such as a third party's with its own connectors
//! registered) against real databases:
//!
//! 1. **Golden migration.** `validate`, `plan` and `run` on the pair's
//!    config, uninterrupted: each exits 0, the target holds exactly the
//!    expected rows, and the run report (A8) is green: verification
//!    passed, every table's checksum read back equals the one taken at
//!    write time, and every `expect:` assertion holds.
//! 2. **Chaos.** [`chaos::run`]: `runs` runs, each killed with SIGKILL at
//!    points drawn from its progress, `ferry resume <run-id>` after every
//!    kill, and left to finish. Each must end exactly like the golden run:
//!    the expected rows, a green report, and the same checksum for every
//!    table as the uninterrupted run, so a resumed run is
//!    indistinguishable from one that never stopped.
//!
//! A pair is described by a [`Pair`]: the binary and config to run, how to
//! empty the target and the state store, how far a run got, and what the
//! target must hold. `ferry-cli`'s `tests/conformance.rs` certifies Mongo →
//! Postgres and Postgres → Postgres with it.
//!
//! ## Certifying a third-party connector
//!
//! In the crate of the binary that registers the connector (the
//! `acme-ferry` of 0004 §5), add a test that depends on `ferry-runtime`
//! with the `testkit` feature:
//!
//! ```toml
//! [dev-dependencies]
//! ferry-runtime = { version = "...", features = ["testkit"] }
//! ```
//!
//! ```ignore
//! struct ClickHouseFromPostgres { /* the databases, the config dir */ }
//!
//! #[async_trait]
//! impl Pair for ClickHouseFromPostgres {
//!     fn name(&self) -> String { "postgres → clickhouse".into() }
//!     fn command(&self) -> Command {
//!         let mut cmd = Command::new(env!("CARGO_BIN_EXE_acme-ferry"));
//!         cmd.arg("--config").arg(&self.config).env("PG_URL", &self.pg_url);
//!         cmd
//!     }
//!     async fn reset(&self) -> Result<(), String> { /* drop the target tables and _ferry_* */ }
//!     async fn progress(&self, run: &RunId, _work: &Path) -> Result<Progress, String> {
//!         Progress::of(self.state_store.as_ref(), run).await.map_err(|e| e.to_string())
//!     }
//!     async fn verify(&self) -> Result<(), String> { /* the target against the expected rows */ }
//! }
//!
//! #[tokio::test(flavor = "multi_thread")]
//! async fn clickhouse_is_certified() {
//!     let pair = ClickHouseFromPostgres::start().await;
//!     let work = tempfile::tempdir().unwrap();
//!     let certificate = conformance::certify(&pair, &conformance::Config::new(2), work.path())
//!         .await
//!         .unwrap();
//!     assert_eq!(certificate.chaos.runs, 50);
//! }
//! ```
//!
//! The source must hold enough data for a few dozen partitions (set
//! `partition_rows` low in the pair's config), so kills can land between
//! them. The state store is whatever the config's `runtime.state` says;
//! [`Progress::of`] reads any [`StateStore`](ferry_registry::StateStore),
//! including the file store kept in the work directory under
//! `backend: file`.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use async_trait::async_trait;
use ferry_registry::state::RunId;
use serde_json::Value;
use tokio::process::Command;

use super::chaos::{self, Progress, Scenario};

/// The `ferry-cli` variable that shortens leases, so a resume after a kill
/// doesn't wait out the default 30 seconds for the dead process's leases.
pub const LEASE_TTL_ENV: &str = "FERRY_LEASE_TTL_MS";

/// One connector pair, with its databases and its config.
#[async_trait]
pub trait Pair: Send + Sync {
    /// Shown in failures, e.g. `mongo → postgres`.
    fn name(&self) -> String;

    /// The binary with the pair's config (`--config <dir>`) and the
    /// environment it needs. The harness adds `--work-dir`, the lease
    /// variable and the subcommand.
    fn command(&self) -> Command;

    /// Empties the target and the state store. The source stays as it is.
    async fn reset(&self) -> Result<(), String>;

    /// How far `run` got, from the pair's state store. `work` is the run's
    /// work directory, where `backend: file` keeps its state (`state/`).
    /// Called while the binary runs, so it must not disturb it. `leased`
    /// must count the partitions under an unexpired lease: kills wait for
    /// one. [`Progress::of`] reads all of it from a `StateStore`.
    async fn progress(&self, run: &RunId, work: &Path) -> Result<Progress, String>;

    /// `Err` with the difference unless the target holds exactly the
    /// expected rows: no duplicates, no gaps, the same values.
    async fn verify(&self) -> Result<(), String>;
}

#[derive(Debug, Clone)]
pub struct Config {
    /// The chaos runs. `in_flight` must be the config's `workers`.
    pub chaos: chaos::Config,
    /// The lease the binary runs with: a resumed run waits at most this
    /// long for the partitions a killed process held.
    pub lease_ttl: Duration,
}

impl Config {
    /// 50 chaos runs, up to three kills each, for a config with `workers`
    /// workers.
    pub fn new(workers: u64) -> Self {
        Self {
            chaos: chaos::Config {
                runs: 50,
                seed: 0xfe_22_a7,
                max_kills: 3,
                in_flight: workers,
                poll: Duration::from_millis(2),
                timeout: Duration::from_secs(120),
            },
            lease_ttl: Duration::from_millis(1_500),
        }
    }
}

/// What a certified pair passed.
#[derive(Debug, Clone)]
pub struct Certificate {
    /// The uninterrupted run's report.
    pub golden: Checked,
    pub chaos: chaos::Report,
}

/// A run's report, checked green.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checked {
    pub run: String,
    /// Per target table: rows read back and their checksum (hex).
    pub tables: BTreeMap<String, (u64, String)>,
    /// `expect:` assertions by name, all of which held.
    pub assertions: Vec<String>,
}

/// Runs the whole suite on `pair`. `work` holds a directory per run.
pub async fn certify(pair: &dyn Pair, config: &Config, work: &Path) -> Result<Certificate, String> {
    let fail = |e: String| format!("{}: {e}", pair.name());
    let golden = golden(pair, config, &work.join("golden"))
        .await
        .map_err(|e| fail(format!("golden run: {e}")))?;
    let scenario = Chaos {
        pair,
        config,
        golden: &golden,
    };
    let chaos = chaos::run(&config.chaos, &scenario, &work.join("chaos"))
        .await
        .map_err(|e| fail(format!("chaos: {e}")))?;
    Ok(Certificate { golden, chaos })
}

/// `validate`, `plan` and an uninterrupted `run`, each exiting 0, then the
/// target and the report.
async fn golden(pair: &dyn Pair, config: &Config, dir: &Path) -> Result<Checked, String> {
    fresh_dir(dir)?;
    pair.reset().await?;
    for args in [&["validate"][..], &["plan"], &["run"]] {
        let out = command(pair, config, dir)
            .args(args)
            .output()
            .await
            .map_err(|e| format!("running `{}`: {e}", args[0]))?;
        if !out.status.success() {
            return Err(format!(
                "`{}` exited with {}\n--- stdout\n{}\n--- stderr\n{}",
                args[0],
                out.status,
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            ));
        }
        if args[0] == "run" {
            let stdout = String::from_utf8_lossy(&out.stdout);
            let run = run_id(&stdout).ok_or_else(|| format!("no run id in: {stdout}"))?;
            chaos::record_run(dir, &RunId(run)).map_err(|e| e.to_string())?;
        }
    }
    pair.verify().await?;
    let run = chaos::recorded_run(dir).expect("recorded above");
    check_report(&work_dir(dir), &run.0)
}

/// The chaos scenario, run through the binary.
struct Chaos<'a> {
    pair: &'a dyn Pair,
    config: &'a Config,
    golden: &'a Checked,
}

#[async_trait]
impl Scenario for Chaos<'_> {
    async fn reset(&self) -> Result<(), String> {
        self.pair.reset().await
    }

    /// `run` until the run's id is known, `resume <run-id>` after. Each
    /// attempt's output goes to `attempt-<n>.{out,err}` in `dir`.
    fn child(&self, dir: &Path) -> Command {
        let n = attempts(dir);
        let mut cmd = command(self.pair, self.config, dir);
        match known_run(dir) {
            Some(run) => cmd.args(["resume", &run.0]),
            None => cmd.arg("run"),
        };
        let file = |ext: &str| {
            let path = dir.join(format!("attempt-{n}.{ext}"));
            let file =
                std::fs::File::create(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
            Stdio::from(file)
        };
        cmd.stdout(file("out")).stderr(file("err"));
        cmd
    }

    async fn progress(&self, dir: &Path) -> Result<Progress, String> {
        match known_run(dir) {
            Some(run) => self.pair.progress(&run, &work_dir(dir)).await,
            None => Ok(Progress::default()),
        }
    }

    async fn verify(&self, dir: &Path) -> Result<(), String> {
        let run = chaos::recorded_run(dir).ok_or("the run never printed its id")?;
        self.pair.verify().await?;
        let checked = check_report(&work_dir(dir), &run.0)?;
        // An interrupted run must write what an uninterrupted one does.
        if checked.tables != self.golden.tables {
            return Err(format!(
                "the checksums differ from the uninterrupted run's: {:?}, expected {:?}",
                checked.tables, self.golden.tables
            ));
        }
        if checked.assertions != self.golden.assertions {
            return Err(format!(
                "the assertions differ from the uninterrupted run's: {:?}, expected {:?}",
                checked.assertions, self.golden.assertions
            ));
        }
        Ok(())
    }

    async fn explain(&self, dir: &Path) -> String {
        let n = attempts(dir);
        let read = |ext: &str| {
            std::fs::read_to_string(dir.join(format!("attempt-{}.{ext}", n.saturating_sub(1))))
                .unwrap_or_default()
        };
        format!(
            "\n--- last attempt's stdout\n{}\n--- its stderr\n{}",
            read("out"),
            read("err")
        )
    }
}

/// The binary, with its work directory in `dir` and short leases.
fn command(pair: &dyn Pair, config: &Config, dir: &Path) -> Command {
    let mut cmd = pair.command();
    cmd.arg("--work-dir")
        .arg(work_dir(dir))
        .env(LEASE_TTL_ENV, config.lease_ttl.as_millis().to_string())
        .kill_on_drop(true);
    cmd
}

fn work_dir(dir: &Path) -> PathBuf {
    dir.join("work")
}

/// Attempts started in `dir` so far.
fn attempts(dir: &Path) -> usize {
    (0..)
        .find(|n| !dir.join(format!("attempt-{n}.out")).exists())
        .expect("a free attempt number")
}

/// The run kept in `dir`: recorded, or printed by an attempt (`run <id>`
/// is the first line `ferry run` prints, before any work), and then
/// recorded. Attempts run `run` only until one has printed its id.
fn known_run(dir: &Path) -> Option<RunId> {
    if let Some(run) = chaos::recorded_run(dir) {
        return Some(run);
    }
    let run = (0..attempts(dir)).find_map(|n| {
        let out = std::fs::read_to_string(dir.join(format!("attempt-{n}.out"))).ok()?;
        run_id(&out)
    })?;
    let run = RunId(run);
    if let Err(e) = chaos::record_run(dir, &run) {
        panic!("recording run {} in {}: {e}", run.0, dir.display());
    }
    Some(run)
}

/// The id on the first line, `run <id>`, once that line is complete.
fn run_id(stdout: &str) -> Option<String> {
    let (first, _) = stdout.split_once('\n')?;
    Some(first.strip_prefix("run ")?.to_owned())
}

/// Reads `<work>/runs/<run>/report.json` and checks it is green: the run
/// is complete, verification passed, every table's checksum read back is
/// the one taken at write time, and every assertion and the dead-letter
/// count hold.
pub fn check_report(work: &Path, run: &str) -> Result<Checked, String> {
    let path = work.join("runs").join(report_dir(run)).join("report.json");
    let text = std::fs::read_to_string(&path).map_err(|e| format!("{}: {e}", path.display()))?;
    let report: Value =
        serde_json::from_str(&text).map_err(|e| format!("{}: {e}", path.display()))?;
    let bad = |what: String| Err(format!("report {}: {what}", path.display()));
    if report["status"] != "passed" {
        return bad(format!(
            "verification {}: {}",
            report["status"], report["problems"]
        ));
    }
    let p = &report["partitions"];
    if p["done"].as_u64().unwrap_or(0) == 0
        || [&p["failed"], &p["pending"], &p["leased"]]
            .iter()
            .any(|n| n.as_u64() != Some(0))
    {
        return bad(format!("the run isn't complete: {p}"));
    }
    let mut tables = BTreeMap::new();
    for t in report["tables"].as_array().into_iter().flatten() {
        let Some(name) = t["table"].as_str() else {
            return bad(format!("a table without a name: {t}"));
        };
        let (at_write, read_back) = (&t["at_write"], &t["read_back"]);
        if t["passed"] != true || read_back.is_null() || at_write != read_back {
            return bad(format!(
                "table `{name}`: written {at_write}, read back {read_back}"
            ));
        }
        let (Some(rows), Some(sum)) = (read_back["rows"].as_u64(), read_back["sum"].as_str())
        else {
            return bad(format!("table `{name}`: no checksum in {read_back}"));
        };
        if sum.is_empty() || !sum.chars().all(|c| c.is_ascii_hexdigit()) {
            return bad(format!("table `{name}`: checksum {sum:?} isn't hex"));
        }
        tables.insert(name.to_owned(), (rows, sum.to_owned()));
    }
    if tables.is_empty() {
        return bad("no tables were checked".into());
    }
    let mut assertions = Vec::new();
    for a in report["assertions"].as_array().into_iter().flatten() {
        if a["passed"] != true {
            return bad(format!("assertion {a}"));
        }
        let Some(name) = a["name"].as_str() else {
            return bad(format!("an assertion without a name: {a}"));
        };
        assertions.push(name.to_owned());
    }
    if report["dead_letter"]["passed"] != true {
        return bad(format!("dead letters: {}", report["dead_letter"]));
    }
    Ok(Checked {
        run: run.to_owned(),
        tables,
        assertions,
    })
}

/// The report's directory name for `run`, as `ferry-cli` names it.
fn report_dir(run: &str) -> String {
    let safe = run
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
    match safe {
        true => run.to_owned(),
        false => run.bytes().map(|b| format!("{b:02x}")).collect(),
    }
}

fn fresh_dir(dir: &Path) -> Result<(), String> {
    if dir.exists() {
        std::fs::remove_dir_all(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    }
    std::fs::create_dir_all(dir).map_err(|e| format!("{}: {e}", dir.display()))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    /// A green report for run `r1`, as `ferry-verify` writes it.
    fn green() -> Value {
        let sum = |rows: u64, sum: &str| json!({ "rows": rows, "sum": sum });
        json!({
            "status": "passed",
            "partitions": { "pending": 0, "leased": 0, "done": 3, "failed": 0 },
            "tables": [
                { "table": "orders", "at_write": sum(8, "0a"), "read_back": sum(8, "0a"), "passed": true },
                { "table": "order_items", "at_write": sum(7, "0b"), "read_back": sum(7, "0b"), "passed": true },
            ],
            "assertions": [{ "name": "order_count", "passed": true }],
            "dead_letter": { "passed": true },
            "problems": [],
        })
    }

    /// A case's name, how it spoils a green report, and what the error says.
    type Case = (&'static str, fn(&mut Value), &'static str);

    fn check(report: &Value) -> Result<Checked, String> {
        let work = tempfile::tempdir().unwrap();
        let dir = work.path().join("runs/r1");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("report.json"), report.to_string()).unwrap();
        check_report(work.path(), "r1")
    }

    #[test]
    fn a_green_report_gives_each_tables_rows_and_checksum() {
        let checked = check(&green()).unwrap();
        assert_eq!(checked.run, "r1");
        assert_eq!(
            checked.tables,
            BTreeMap::from([
                ("order_items".to_owned(), (7, "0b".to_owned())),
                ("orders".to_owned(), (8, "0a".to_owned())),
            ])
        );
        assert_eq!(checked.assertions, ["order_count"]);
    }

    #[test]
    fn anything_short_of_green_is_refused() {
        let cases: [Case; 11] = [
            (
                "failed",
                |r| r["status"] = "failed".into(),
                "verification \"failed\"",
            ),
            (
                "incomplete",
                |r| r["partitions"]["pending"] = 1.into(),
                "isn't complete",
            ),
            (
                "nothing done",
                |r| r["partitions"]["done"] = 0.into(),
                "isn't complete",
            ),
            (
                "checksum differs",
                |r| r["tables"][1]["read_back"]["sum"] = "0c".into(),
                "table `order_items`",
            ),
            (
                "not read back",
                |r| r["tables"][0]["read_back"] = Value::Null,
                "table `orders`",
            ),
            ("no tables", |r| r["tables"] = json!([]), "no tables"),
            (
                "assertion",
                |r| r["assertions"][0]["passed"] = false.into(),
                "assertion",
            ),
            (
                "dead letters",
                |r| r["dead_letter"]["passed"] = false.into(),
                "dead letters",
            ),
            (
                "unnamed table",
                |r| r["tables"][0]["table"] = Value::Null,
                "without a name",
            ),
            (
                "no checksum",
                |r| {
                    r["tables"][0]["at_write"]["sum"] = Value::Null;
                    r["tables"][0]["read_back"]["sum"] = Value::Null;
                },
                "no checksum",
            ),
            (
                "unnamed assertion",
                |r| r["assertions"][0]["name"] = Value::Null,
                "without a name",
            ),
        ];
        for (case, edit, says) in cases {
            let mut report = green();
            edit(&mut report);
            let e = check(&report).expect_err(case);
            assert!(e.contains(says), "{case}: {e}");
        }
    }

    #[test]
    fn a_missing_report_is_an_error() {
        let work = tempfile::tempdir().unwrap();
        let e = check_report(work.path(), "r1").unwrap_err();
        assert!(e.contains("runs/r1/report.json"), "{e}");
    }

    #[test]
    fn report_directories_are_named_as_ferry_names_them() {
        assert_eq!(report_dir("ab-1_C"), "ab-1_C");
        assert_eq!(report_dir("a/b"), "612f62");
        assert_eq!(run_id("run abc\n  flow"), Some("abc".into()));
        // Not until the line is complete.
        assert_eq!(run_id("run ab"), None);
        assert_eq!(run_id("resuming run abc\n"), None);
    }
}
