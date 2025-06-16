//! `acme-ferry validate`, run as a real process: a third-party binary that
//! registered `postgres`, `mongo` and `fake` through the public API, against
//! the repository's example configs.

mod common;

use common::{SECRET, UNROUTABLE, acme, golden_with, manifest, repo, run, validate};

/// The exit code the CLI documents for an invalid config.
const CONFIG_INVALID: i32 = 3;

#[test]
fn golden_config_is_valid_without_connecting() {
    // Every URL points at TEST-NET-1, and every endpoint panics on use, so
    // this passes only if validate never touches a connection.
    let out = validate(&repo("examples/golden/ferry"));
    assert_eq!(out.code, 0, "{}", out.stderr);
    assert_eq!(out.stderr, "");
    assert_eq!(
        out.stdout,
        "`golden_orders` is valid: 2 connections, 1 flow\n"
    );
}

#[test]
fn every_valid_example_validates() {
    for dir in ["examples/pg-merge/ferry", "examples/edge/layout/ferry"] {
        let out = validate(&repo(dir));
        assert_eq!(out.code, 0, "{dir}: {}", out.stderr);
        assert!(out.stdout.contains("is valid"), "{dir}: {}", out.stdout);
    }
}

#[test]
fn verbose_shows_how_each_connection_is_used_and_no_secret() {
    let out = run(acme()
        .args(["validate", "-v", "--config"])
        .arg(repo("examples/golden/ferry")));
    assert_eq!(out.code, 0, "{}", out.stderr);
    assert!(
        out.stdout
            .contains("  shop (mongo, used as source): mongo database shop\n"),
        "{}",
        out.stdout
    );
    assert!(
        out.stdout.contains(&format!(
            "  warehouse (postgres, used as sink): postgres at {UNROUTABLE}:5432/warehouse, schema public\n"
        )),
        "{}",
        out.stdout
    );
}

#[test]
fn every_bad_case_is_rejected_at_the_manifest_line() {
    let rows = manifest();
    assert!(rows.len() >= 20, "manifest has {} rows", rows.len());
    for row in rows {
        let case = &row["Case"];
        let dir = repo(&format!("examples/bad/{case}/ferry"));
        let out = validate(&dir);
        assert_eq!(out.code, CONFIG_INVALID, "{case}: {}", out.stderr);
        // `<dir>/<file>:<line>:<column>`, so `connections.yaml:1` can't match
        // a diagnostic at line 13.
        let at = format!("{}:", dir.join(&row["Error at"]).display());
        assert!(
            out.stderr.contains(&at),
            "{case}: expected {at} in\n{}",
            out.stderr
        );
        let path = &row["Field path"];
        if !path.is_empty() {
            assert!(
                out.stderr.contains(path.as_str()),
                "{case}: {path} not in\n{}",
                out.stderr
            );
        }
        assert!(out.stderr.ends_with(" error\n") || out.stderr.ends_with(" errors\n"));
        assert_eq!(out.stdout, "", "{case}");
    }
}

#[test]
fn representative_bad_cases_render_as_snapshots() {
    // B1 pins every case's loader errors; these pin how the CLI renders
    // them: code, path, file:line, source line, caret and help.
    for case in [
        "unknown-connection-type",
        "state-not-state-capable",
        "yaml-syntax",
        "missing-env",
    ] {
        let out = run(acme()
            .args(["validate", "--config"])
            .arg(format!("examples/bad/{case}/ferry")));
        assert_eq!(out.code, CONFIG_INVALID, "{case}");
        insta::with_settings!({ snapshot_suffix => case, prepend_module_to_snapshot => false }, {
            insta::assert_snapshot!("stderr", out.stderr);
        });
    }
}

#[test]
fn the_planted_secret_never_appears_in_any_output() {
    let mut configs: Vec<String> = manifest()
        .iter()
        .map(|row| format!("examples/bad/{}/ferry", row["Case"]))
        .collect();
    configs.extend(
        ["golden", "pg-merge", "edge/layout"]
            .iter()
            .map(|d| format!("examples/{d}/ferry")),
    );
    for config in &configs {
        for format in ["text", "json"] {
            let out = run(acme().args(["validate", "-v", "--format", format, "--config", config]));
            for text in [&out.stdout, &out.stderr] {
                assert!(!text.contains(SECRET), "{config} ({format}):\n{text}");
            }
        }
    }
}

#[test]
fn a_connector_error_that_quotes_the_url_is_redacted() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("flows")).unwrap();
    std::fs::write(dir.path().join("ferry.yaml"), "version: 1\nname: leaky\n").unwrap();
    std::fs::write(
        dir.path().join("connections.yaml"),
        "connections:\n  - name: inventory\n    type: fake\n    url: ${FERRY_FAKE_URL}\n    leak_url_in_error: true\n",
    )
    .unwrap();
    std::fs::copy(
        repo("crates/ferry-cli/tests/fixtures/fake/ferry/flows/items.yaml"),
        dir.path().join("flows/items.yaml"),
    )
    .unwrap();
    for format in ["text", "json"] {
        let out = run(acme()
            .args(["validate", "--format", format, "--config"])
            .arg(dir.path()));
        assert_eq!(out.code, CONFIG_INVALID, "{}", out.stderr);
        assert!(out.stderr.contains("could not parse ***"), "{}", out.stderr);
        assert!(!out.stderr.contains(SECRET), "{}", out.stderr);
        assert!(!out.stderr.contains(UNROUTABLE), "{}", out.stderr);
    }
}

#[test]
fn a_literal_secret_is_a_warning_and_is_never_printed() {
    let literal = format!("postgres://ferry:{SECRET}@localhost/warehouse");
    let dir = golden_with("connections.yaml", |text| {
        text.replace("${FERRY_GOLDEN_PG_URL}", &literal)
    });
    let out = validate(dir.path());
    assert_eq!(out.code, 0, "{}", out.stderr);
    assert!(
        out.stderr.starts_with(
            "warning[W0301]: connections[warehouse].url: holds a secret as a literal value"
        ),
        "{}",
        out.stderr
    );
    assert!(
        out.stderr.contains("connections.yaml:11:10"),
        "{}",
        out.stderr
    );
    // The source line is shown, with the literal masked.
    assert!(out.stderr.contains("11 |     url: ***\n"), "{}", out.stderr);
    assert!(!out.stderr.contains(SECRET), "{}", out.stderr);
    assert!(out.stdout.ends_with(", 1 warning\n"), "{}", out.stdout);

    let quiet = run(acme().args(["validate", "-q", "--config"]).arg(dir.path()));
    assert_eq!(quiet.code, 0);
    assert_eq!((quiet.stdout.as_str(), quiet.stderr.as_str()), ("", ""));
}

#[test]
fn a_literal_secret_that_fails_to_deserialize_is_masked_in_the_source_line() {
    // Unquoted, so YAML reads a number and `Secret` rejects it. The error
    // points at that line, and the line shown must not give the value away.
    let dir = golden_with("connections.yaml", |text| {
        text.replace(
            "url: ${FERRY_GOLDEN_PG_URL}",
            "url: ${FERRY_GOLDEN_PG_URL}\n    password: 918273645",
        )
    });
    let out = validate(dir.path());
    assert_eq!(out.code, CONFIG_INVALID, "{}", out.stderr);
    assert!(
        out.stderr.starts_with(
            "error[F0301]: connections[warehouse].password: expected a string, got a number"
        ),
        "{}",
        out.stderr
    );
    assert!(
        out.stderr.contains("12 |     password: ***\n"),
        "{}",
        out.stderr
    );
    assert!(!out.stderr.contains("918273645"), "{}", out.stderr);
}

#[test]
fn a_source_only_connection_used_as_a_sink_names_flow_connection_and_line() {
    let dir = golden_with("flows/orders.yaml", |text| {
        text.replace("sink: warehouse", "sink: shop")
    });
    let out = validate(dir.path());
    assert_eq!(out.code, CONFIG_INVALID, "{}", out.stderr);
    assert!(
        out.stderr.starts_with(
            "error[F0303]: flows[orders].sink: flow `orders` writes to connection `shop`, but the `mongo` connector in this build has no sink\n"
        ),
        "{}",
        out.stderr
    );
    let at = format!("{}:4:11", dir.path().join("flows/orders.yaml").display());
    assert!(out.stderr.contains(&at), "{at} not in\n{}", out.stderr);
}

#[test]
fn the_state_connection_must_be_of_the_backend_type() {
    // A postgres connection under `backend: mongo`: the types differ.
    let dir = golden_with("runtime.yaml", |text| {
        text.replace("backend: postgres", "backend: mongo")
    });
    let out = validate(dir.path());
    assert_eq!(out.code, CONFIG_INVALID, "{}", out.stderr);
    assert!(
        out.stderr.starts_with(
            "error[F0304]: runtime.state.connection: the state connection `warehouse` is a `postgres` connection, but `backend:` is `mongo`\n"
        ),
        "{}",
        out.stderr
    );
    let at = format!("{}:8:17", dir.path().join("runtime.yaml").display());
    assert!(out.stderr.contains(&at), "{at} not in\n{}", out.stderr);
}

#[test]
fn the_state_backend_must_be_state_capable() {
    // `backend: mongo` on the mongo connection: the types agree, but mongo
    // can't hold the state store.
    let dir = golden_with("runtime.yaml", |text| {
        text.replace("backend: postgres", "backend: mongo")
            .replace("connection: warehouse", "connection: shop")
    });
    let out = validate(dir.path());
    assert_eq!(out.code, CONFIG_INVALID, "{}", out.stderr);
    assert!(
        out.stderr.starts_with(
            "error[F0305]: runtime.state.backend: the `mongo` connector in this build can't hold the state store\n"
        ),
        "{}",
        out.stderr
    );
    let at = format!("{}:7:14", dir.path().join("runtime.yaml").display());
    assert!(out.stderr.contains(&at), "{at} not in\n{}", out.stderr);
    assert!(
        out.stderr.contains("state-capable type: `postgres`"),
        "{}",
        out.stderr
    );
}

#[test]
fn every_error_is_reported_not_just_the_first() {
    // Three independent defects: a typo'd option, a bad URL scheme and a
    // source-only sink.
    let dir = golden_with("connections.yaml", |text| {
        text.replace("database: shop", "databse: shop").replace(
            "url: ${FERRY_GOLDEN_PG_URL}",
            "url: ${FERRY_GOLDEN_MONGO_URL}",
        )
    });
    let out = validate(dir.path());
    assert_eq!(out.code, CONFIG_INVALID, "{}", out.stderr);
    let codes: Vec<&str> = out
        .stderr
        .lines()
        .filter_map(|l| l.strip_prefix("error["))
        .map(|l| &l[..5])
        .collect();
    assert_eq!(codes, ["F0301", "F0301"], "{}", out.stderr);
    let shop = format!("{}:7:5", dir.path().join("connections.yaml").display());
    assert!(out.stderr.contains(&shop), "{shop} not in\n{}", out.stderr);
    assert!(
        out.stderr.contains("unknown field `databse`"),
        "{}",
        out.stderr
    );
    assert!(
        out.stderr
            .contains("connections[warehouse].url: expected a postgres or postgresql URL"),
        "{}",
        out.stderr
    );
    assert!(out.stderr.ends_with("config is invalid: 2 errors\n"));
}

#[test]
fn json_format_prints_one_diagnostic_per_line_and_a_summary() {
    let out = run(acme().args([
        "validate",
        "--format",
        "json",
        "--config",
        "examples/bad/state-not-state-capable/ferry",
    ]));
    assert_eq!(out.code, CONFIG_INVALID);
    let lines: Vec<serde_json::Value> = out
        .stderr
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(
        lines,
        [serde_json::json!({
            "severity": "error",
            "code": "F0304",
            "message": "the state connection `shop` is a `mongo` connection, but `backend:` is `postgres`",
            "file": "examples/bad/state-not-state-capable/ferry/runtime.yaml",
            "line": 8,
            "column": 17,
            "path": "runtime.state.connection",
            "help": "name a `postgres` connection here, or set `backend:` to the connection's type",
        })]
    );
    let summary: serde_json::Value = serde_json::from_str(out.stdout.trim()).unwrap();
    assert_eq!(
        summary,
        serde_json::json!({"valid": false, "errors": 1, "warnings": 0})
    );

    let ok = run(acme().args([
        "validate",
        "--format",
        "json",
        "--config",
        "examples/golden/ferry",
    ]));
    assert_eq!(ok.code, 0);
    let summary: serde_json::Value = serde_json::from_str(ok.stdout.trim()).unwrap();
    assert_eq!(summary["valid"], true);
    assert_eq!(summary["config"], "golden_orders");
    assert_eq!(summary["connections"][1]["used_as"], "sink");
}

#[test]
fn the_fake_type_validates_here() {
    let out = validate(&repo("crates/ferry-cli/tests/fixtures/fake/ferry"));
    assert_eq!(out.code, 0, "{}", out.stderr);
    assert_eq!(out.stdout, "`fake_demo` is valid: 1 connection, 1 flow\n");
}
