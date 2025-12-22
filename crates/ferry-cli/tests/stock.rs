//! The stock `ferry` binary as a real process: the config path, usage
//! errors, the stub subcommands, `plan` checking the config first, the
//! built-in `postgres` and `mongo` connectors it registers, and a
//! connection type it doesn't have.

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

/// A secret planted in every URL the golden config gets; nothing the
/// binary prints may contain it.
#[cfg(all(feature = "postgres", feature = "mongo"))]
const SECRET: &str = "st0ck-pa55-never-printed";

/// TEST-NET-1 (RFC 5737): a connection attempt would hang, not fail fast.
#[cfg(all(feature = "postgres", feature = "mongo"))]
const UNROUTABLE: &str = "192.0.2.1";

/// `ferry` from the repository root with the golden config's environment.
/// The short timeout fails a command that tries to connect.
#[cfg(all(feature = "postgres", feature = "mongo"))]
fn golden_ferry() -> Command {
    let mut cmd = ferry(&repo(""));
    cmd.timeout(Duration::from_secs(10))
        .env(
            "FERRY_GOLDEN_MONGO_URL",
            format!("mongodb://ferry:{SECRET}@{UNROUTABLE}:27017"),
        )
        .env(
            "FERRY_GOLDEN_PG_URL",
            format!("postgres://ferry:{SECRET}@{UNROUTABLE}:5432/warehouse"),
        );
    cmd
}

/// A copy of `examples/golden/ferry` with `file` edited.
#[cfg(all(feature = "postgres", feature = "mongo"))]
fn golden_with(file: &str, edit: impl FnOnce(&str) -> String) -> tempfile::TempDir {
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
    let dir = tempfile::tempdir().unwrap();
    copy_dir(&repo("examples/golden/ferry"), dir.path());
    let path = dir.path().join(file);
    let text = std::fs::read_to_string(&path).unwrap();
    let edited = edit(&text);
    assert_ne!(text, edited, "the edit changed nothing in {file}");
    std::fs::write(path, edited).unwrap();
    dir
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
#[cfg(all(feature = "postgres", feature = "mongo"))]
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
    assert!(
        out.stderr
            .contains("= help: registered types: `mongo` (source), `postgres` (source and sink)\n"),
        "{}",
        out.stderr
    );
}

/// The built-in connectors build their endpoints without I/O: every URL
/// points at TEST-NET-1, so a connection attempt would hang past the
/// timeout instead of exiting.
#[test]
#[cfg(all(feature = "postgres", feature = "mongo"))]
fn the_stock_binary_validates_the_golden_config_without_connecting() {
    let out = run(golden_ferry().args(["validate", "--config", "examples/golden/ferry"]));
    assert_eq!(out.code, 0, "{}", out.stderr);
    assert_eq!(out.stderr, "");
    assert_eq!(
        out.stdout,
        "`golden_orders` is valid: 2 connections, 1 flow\n"
    );

    let out = run(golden_ferry().args(["validate", "-v", "--config", "examples/golden/ferry"]));
    assert_eq!(out.code, 0, "{}", out.stderr);
    assert!(
        out.stdout.contains(&format!(
            "  shop (mongo, used as source): mongodb://{UNROUTABLE}:27017, database shop\n"
        )),
        "{}",
        out.stdout
    );
    assert!(
        out.stdout.contains(&format!(
            "  warehouse (postgres, used as sink): postgres://ferry@{UNROUTABLE}:5432/warehouse, schema public\n"
        )),
        "{}",
        out.stdout
    );
    assert!(!out.stdout.contains(SECRET) && !out.stderr.contains(SECRET));
}

/// `postgres` is registered with its state backend and `mongo` as a plain
/// source: the golden state on `warehouse` validates (above), and mongo can
/// neither take a flow's rows nor hold the state store.
#[test]
#[cfg(all(feature = "postgres", feature = "mongo"))]
fn the_stock_mongo_is_a_source_without_a_state_backend() {
    let dir = golden_with("flows/orders.yaml", |text| {
        text.replace("sink: warehouse", "sink: shop")
    });
    let out = run(golden_ferry()
        .arg("validate")
        .arg("--config")
        .arg(dir.path()));
    assert_eq!(out.code, 3, "{}", out.stderr);
    assert!(
        out.stderr.starts_with(
            "error[F0303]: flows[orders].sink: flow `orders` writes to connection `shop`, but the `mongo` connector in this build has no sink\n"
        ),
        "{}",
        out.stderr
    );

    let dir = golden_with("runtime.yaml", |text| {
        text.replace("backend: postgres", "backend: mongo")
            .replace("connection: warehouse", "connection: shop")
    });
    let out = run(golden_ferry()
        .arg("validate")
        .arg("--config")
        .arg(dir.path()));
    assert_eq!(out.code, 3, "{}", out.stderr);
    assert!(
        out.stderr.starts_with(
            "error[F0305]: runtime.state.backend: the `mongo` connector in this build can't hold the state store\n"
        ),
        "{}",
        out.stderr
    );
}

#[test]
#[cfg(all(feature = "postgres", feature = "mongo"))]
fn the_stock_schema_has_a_connections_branch_for_postgres_and_mongo() {
    let out = run(ferry(&repo("")).arg("schema"));
    assert_eq!(out.code, 0, "{}", out.stderr);
    let all: serde_json::Value = serde_json::from_str(&out.stdout).unwrap();
    let branches = all["connections.schema.json"]["$defs"]["Connection"]["allOf"][1]["oneOf"]
        .as_array()
        .unwrap_or_else(|| panic!("no oneOf in {}", out.stdout));
    let mut types: Vec<&str> = branches
        .iter()
        .map(|b| b["properties"]["type"]["const"].as_str().unwrap())
        .collect();
    types.sort();
    assert_eq!(types, ["mongo", "postgres"]);
    // Each branch is the connector's own options, secrets marked.
    for branch in branches {
        let ty = &branch["properties"]["type"]["const"];
        assert_eq!(
            branch["properties"]["url"]["x-ferry-secret"], true,
            "{ty}: {branch}"
        );
        assert_eq!(branch["additionalProperties"], false, "{ty}: {branch}");
    }
    let postgres = branches
        .iter()
        .find(|b| b["properties"]["type"]["const"] == "postgres")
        .unwrap();
    assert!(postgres["properties"]["schema"].is_object(), "{postgres}");
    assert_eq!(postgres["properties"]["password"]["x-ferry-secret"], true);
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
        (&["run"][..], "`ferry run` is not implemented yet (S2)"),
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
fn plan_checks_the_config_before_connecting() {
    let cwd = tempfile::tempdir().unwrap();
    let out = run(ferry(cwd.path()).arg("plan"));
    assert_eq!(out.code, 3, "{}", out.stderr);
    assert!(
        out.stderr
            .starts_with("error[F0001]: no config directory at `"),
        "{}",
        out.stderr
    );
    assert_eq!(out.stdout, "");
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
