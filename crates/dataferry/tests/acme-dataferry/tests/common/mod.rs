//! Helpers shared by the acme-dataferry process tests.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use assert_cmd::Command;

/// A secret-looking password planted in every URL the tests export. It
/// must never appear in anything the binary prints.
pub const SECRET: &str = "pa55-w0rd-never-printed";

/// TEST-NET-1 (RFC 5737): routable nowhere, so a connection attempt would
/// hang until it timed out instead of failing fast.
pub const UNROUTABLE: &str = "192.0.2.1";

pub fn repo(path: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../..")
        .join(path)
}

/// The environment every example config needs, with the secret planted in
/// each URL.
pub fn example_env() -> Vec<(&'static str, String)> {
    let pg = |db: &str| format!("postgres://ferry:{SECRET}@{UNROUTABLE}:5432/{db}");
    vec![
        (
            "FERRY_GOLDEN_MONGO_URL",
            format!("mongodb://ferry:{SECRET}@{UNROUTABLE}:27017"),
        ),
        ("FERRY_GOLDEN_PG_URL", pg("warehouse")),
        ("APP_DATABASE_URL", pg("app")),
        ("REPORTING_DATABASE_URL", pg("reporting")),
        ("FERRY_EDGE_APP_URL", pg("app")),
        ("FERRY_EDGE_REPORTING_URL", pg("reporting")),
        (
            "FERRY_FAKE_URL",
            format!("https://acme:{SECRET}@{UNROUTABLE}/inv"),
        ),
    ]
}

/// `acme-dataferry` run from the repository root with a clean, known
/// environment. A timeout catches a command that tries to connect.
pub fn acme() -> Command {
    let mut cmd = Command::cargo_bin("acme-dataferry").unwrap();
    cmd.current_dir(repo(""))
        .env_clear()
        .timeout(Duration::from_secs(30));
    for (k, v) in example_env() {
        cmd.env(k, v);
    }
    cmd
}

pub struct Output {
    pub code: i32,
    pub stdout: String,
    pub stderr: String,
}

pub fn run(cmd: &mut Command) -> Output {
    let out = cmd.output().unwrap();
    Output {
        code: out
            .status
            .code()
            .expect("exited, not killed by a signal or timeout"),
        stdout: String::from_utf8(out.stdout).unwrap(),
        stderr: String::from_utf8(out.stderr).unwrap(),
    }
}

pub fn validate(config: &Path) -> Output {
    run(acme().arg("validate").arg("--config").arg(config))
}

/// Rows of the first markdown table whose header starts with
/// `first_column`, as maps from header to cell (backticks stripped).
pub fn table(text: &str, first_column: &str) -> Vec<BTreeMap<String, String>> {
    let cells = |line: &str| -> Vec<String> {
        let line = line.trim();
        line.strip_prefix('|')
            .and_then(|l| l.strip_suffix('|'))
            .unwrap_or(line)
            .split('|')
            .map(|c| c.trim().trim_matches('`').to_owned())
            .collect()
    };
    let mut lines = text.lines().skip_while(|l| {
        !l.starts_with('|') || cells(l).first().map(String::as_str) != Some(first_column)
    });
    let header = cells(lines.next().expect("table header"));
    lines
        .skip(1)
        .take_while(|l| l.starts_with('|'))
        .map(|l| header.iter().cloned().zip(cells(l)).collect())
        .collect()
}

/// The rows of `examples/bad/README.md`.
pub fn manifest() -> Vec<BTreeMap<String, String>> {
    let text = std::fs::read_to_string(repo("examples/bad/README.md")).unwrap();
    table(&text, "Case")
}

/// A copy of `examples/golden/ferry` in a temp dir, with `edit` applied to
/// one file's text.
pub fn golden_with(file: &str, edit: impl FnOnce(&str) -> String) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    copy_dir(&repo("examples/golden/ferry"), dir.path());
    let path = dir.path().join(file);
    let text = std::fs::read_to_string(&path).unwrap();
    let edited = edit(&text);
    assert_ne!(text, edited, "the edit changed nothing in {file}");
    std::fs::write(path, edited).unwrap();
    dir
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}
