//! The stock `ferry` binary as a real process: the config path, usage
//! errors, the stub subcommands, and a connection type it doesn't have.

use std::path::{Path, PathBuf};
use std::time::Duration;

use assert_cmd::Command;

fn repo(path: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(path)
}

fn ferry(cwd: &Path) -> Command {
    let mut cmd = Command::cargo_bin("ferry").unwrap();
    cmd.current_dir(cwd)
        .env_clear()
        .timeout(Duration::from_secs(30));
    cmd
}

struct Output {
    code: i32,
    stdout: String,
    stderr: String,
}

fn run(cmd: &mut Command) -> Output {
    let out = cmd.output().unwrap();
    Output {
        code: out.status.code().expect("exited normally"),
        stdout: String::from_utf8(out.stdout).unwrap(),
        stderr: String::from_utf8(out.stderr).unwrap(),
    }
}

#[test]
fn without_a_ferry_directory_the_error_names_the_path_it_looked_for() {
    let cwd = tempfile::tempdir().unwrap();
    let out = run(ferry(cwd.path()).arg("validate"));
    assert_eq!(out.code, 3, "{}", out.stderr);
    let looked_for = cwd.path().canonicalize().unwrap().join("ferry");
    assert!(
        out.stderr.starts_with(&format!(
            "error[F0001]: no config directory at `{}`\n",
            looked_for.display()
        )) || out.stderr.starts_with(&format!(
            "error[F0001]: no config directory at `{}`\n",
            cwd.path().join("ferry").display()
        )),
        "{}",
        out.stderr
    );
    assert!(
        out.stderr
            .contains("help: pass `--config <dir>` or set FERRY_CONFIG")
    );
}

#[test]
fn config_pointing_at_a_file_asks_for_the_directory() {
    let cwd = tempfile::tempdir().unwrap();
    std::fs::create_dir(cwd.path().join("ferry")).unwrap();
    std::fs::write(cwd.path().join("ferry/ferry.yaml"), "version: 1\n").unwrap();
    let out = run(ferry(cwd.path()).args(["validate", "--config", "ferry/ferry.yaml"]));
    assert_eq!(out.code, 3, "{}", out.stderr);
    assert!(
        out.stderr
            .contains("is a file; --config expects the `ferry/` directory that holds ferry.yaml"),
        "{}",
        out.stderr
    );
    assert!(
        out.stderr.contains("help: try `--config ferry`"),
        "{}",
        out.stderr
    );
}

#[test]
fn ferry_config_in_the_environment_picks_the_directory() {
    let cwd = tempfile::tempdir().unwrap();
    let out = run(ferry(cwd.path())
        .env("FERRY_CONFIG", "elsewhere/conf")
        .arg("validate"));
    assert_eq!(out.code, 3);
    assert!(
        out.stderr
            .contains(&format!("elsewhere{}conf`", std::path::MAIN_SEPARATOR)),
        "{}",
        out.stderr
    );
}

#[test]
fn the_stock_binary_rejects_a_type_only_a_third_party_registers() {
    let out = run(ferry(&repo(""))
        .args([
            "validate",
            "--config",
            "crates/ferry-cli/tests/fixtures/fake/ferry",
        ])
        .env("FERRY_FAKE_URL", "https://inv.example.com"));
    assert_eq!(out.code, 3, "{}", out.stderr);
    assert!(
        out.stderr.starts_with(
            "error[F0302]: connections[inventory].type: unknown connection type `fake`\n"
        ),
        "{}",
        out.stderr
    );
    assert!(
        out.stderr
            .contains("crates/ferry-cli/tests/fixtures/fake/ferry/connections.yaml:3:11"),
        "{}",
        out.stderr
    );
    // TODO(C1 follow-up): with the built-ins registered this lists
    // `mongo` (source), `postgres` (source and sink).
    assert!(
        out.stderr
            .contains("help: this binary has no connection types registered")
            || out.stderr.contains("help: registered types: "),
        "{}",
        out.stderr
    );
}

#[test]
fn resume_and_retry_need_a_run_id_and_exit_with_the_usage_code() {
    let cwd = tempfile::tempdir().unwrap();
    for args in [&["resume"][..], &["retry"], &["retry", "--failed"]] {
        let out = run(ferry(cwd.path()).args(args));
        assert_eq!(out.code, 2, "{args:?}: {}", out.stderr);
        assert!(
            out.stderr.starts_with("error: ") && out.stderr.contains("RUN_ID"),
            "{args:?}: {}",
            out.stderr
        );
        assert_eq!(out.stdout, "");
    }
}

#[test]
fn bad_flags_and_a_missing_subcommand_are_usage_errors() {
    let cwd = tempfile::tempdir().unwrap();
    for args in [
        &[][..],
        &["validate", "--frobnicate"],
        &["--format", "xml", "validate"],
    ] {
        let out = run(ferry(cwd.path()).args(args));
        assert_eq!(out.code, 2, "{args:?}: {}", out.stderr);
    }
}

#[test]
fn help_and_version_go_to_stdout_and_succeed() {
    let cwd = tempfile::tempdir().unwrap();
    let help = run(ferry(cwd.path()).arg("--help"));
    assert_eq!(help.code, 0);
    for command in ["validate", "plan", "run", "resume", "retry", "schema"] {
        assert!(help.stdout.contains(command), "{}", help.stdout);
    }
    let version = run(ferry(cwd.path()).arg("--version"));
    assert_eq!(version.code, 0);
    assert_eq!(
        version.stdout,
        format!("ferry {}\n", env!("CARGO_PKG_VERSION"))
    );
}

#[test]
fn stub_subcommands_exit_1_naming_their_owner() {
    let cwd = tempfile::tempdir().unwrap();
    for (args, message) in [
        (&["plan"][..], "`ferry plan` is not implemented yet (B7)"),
        (&["run"], "`ferry run` is not implemented yet (S2)"),
        (
            &["resume", "r-1"],
            "`ferry resume` is not implemented yet (A5)",
        ),
        (
            &["retry", "--failed", "r-1"],
            "`ferry retry` is not implemented yet (A5)",
        ),
    ] {
        let out = run(ferry(cwd.path()).args(args));
        assert_eq!(out.code, 1, "{args:?}: {}", out.stderr);
        assert!(
            out.stderr
                .starts_with(&format!("error[F0003]: {message}\n")),
            "{args:?}: {}",
            out.stderr
        );
        let json = run(ferry(cwd.path()).args(args).args(["--format", "json"]));
        assert_eq!(json.code, 1);
        let d: serde_json::Value = serde_json::from_str(json.stderr.trim()).unwrap();
        assert_eq!(d["code"], "F0003");
    }
}

#[test]
fn schema_writes_one_file_per_config_file_kind() {
    let out_dir = tempfile::tempdir().unwrap();
    let out = run(ferry(out_dir.path()).args(["schema", "--out", "schemas"]));
    assert_eq!(out.code, 0, "{}", out.stderr);
    let mut written: Vec<String> = std::fs::read_dir(out_dir.path().join("schemas"))
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    written.sort();
    assert_eq!(
        written,
        [
            "connections.schema.json",
            "ferry.schema.json",
            "flows.schema.json",
            "runtime.schema.json"
        ]
    );
    // The files that don't depend on the registry are B1's, unchanged.
    for name in ["ferry", "flows", "runtime"] {
        let file = format!("{name}.schema.json");
        let ours = std::fs::read_to_string(out_dir.path().join("schemas").join(&file)).unwrap();
        let ours: serde_json::Value = serde_json::from_str(&ours).unwrap();
        let checked_in: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(repo("schemas/v1").join(&file)).unwrap())
                .unwrap();
        assert_eq!(ours, checked_in, "{file}");
    }
}
