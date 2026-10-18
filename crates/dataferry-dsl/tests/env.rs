//! B1c: `${NAME}` substitution and redaction (0003 §9).

mod common;

use std::collections::HashMap;
use std::path::PathBuf;

use common::repo;
use dataferry_dsl::config::{Config, ErrorKind, load};
use serde_json::json;

const SECRET: &str = "s3cr3t-Pa55w0rd";
const PG_URL: &str = "postgres://ferry:s3cr3t-Pa55w0rd@db.internal:5432/warehouse";

fn golden() -> Config {
    load(repo("examples/golden/ferry")).unwrap()
}

fn env(vars: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
    let vars: HashMap<String, String> = vars
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    move |name| vars.get(name).cloned()
}

fn write(files: &[(&str, &str)]) -> (tempfile::TempDir, PathBuf) {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("ferry");
    let flow = "flows:\n  - name: f\n    from: app.t\n    sink: app\n    to:\n      t:\n        columns: \"*\"\n";
    let base = [
        ("ferry.yaml", "version: 1\nname: t\n"),
        ("flows/f.yaml", flow),
    ];
    for (path, text) in base.iter().chain(files) {
        let path = dir.join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, text).unwrap();
    }
    (tmp, dir)
}

#[test]
fn golden_resolves_into_registry_ready_json() {
    let resolved = golden()
        .resolve_with(env(&[
            ("FERRY_GOLDEN_MONGO_URL", "mongodb://localhost:27017"),
            ("FERRY_GOLDEN_PG_URL", PG_URL),
        ]))
        .unwrap();
    let shop = &resolved.connections[0];
    assert_eq!(
        (shop.name.as_str(), shop.r#type.as_str()),
        ("shop", "mongo")
    );
    assert_eq!(
        shop.fields,
        json!({"url": "mongodb://localhost:27017", "database": "shop"})
    );
    let warehouse = &resolved.connections[1];
    assert_eq!(warehouse.fields, json!({"url": PG_URL}));
    // Registry errors about a field can point back at the line.
    let at = warehouse.location("url").unwrap();
    assert_eq!(
        (&*at.file, at.line, at.column),
        ("connections.yaml", 11, 10)
    );
    let state = resolved.state.as_ref().unwrap();
    assert_eq!(
        (state.backend.as_str(), state.connection.as_deref()),
        ("postgres", Some("warehouse"))
    );
}

#[test]
fn every_missing_variable_is_named_in_one_error_with_its_location() {
    let errors = golden().resolve_with(env(&[])).unwrap_err().0;
    assert_eq!(errors.len(), 1, "{errors:#?}");
    let error = &errors[0];
    assert_eq!(error.kind, ErrorKind::Env);
    assert_eq!(
        error.message,
        "environment variables not set: FERRY_GOLDEN_MONGO_URL (connections.yaml:6:10), FERRY_GOLDEN_PG_URL (connections.yaml:11:10)"
    );
    assert_eq!(error.path, "connections[shop].url");
}

#[test]
fn a_variable_missing_in_several_places_is_listed_once_with_each_location() {
    let (_tmp, dir) = write(&[(
        "connections.yaml",
        "connections:\n  - name: app\n    type: postgres\n    url: ${DB}\n    password: ${DB}\n",
    )]);
    let error = load(&dir)
        .unwrap()
        .resolve_with(env(&[]))
        .unwrap_err()
        .0
        .remove(0);
    assert_eq!(
        error.message,
        "environment variable not set: DB (connections.yaml:4:10, connections.yaml:5:15)"
    );
}

#[test]
fn a_missing_variable_never_prints_another_value() {
    // The bad case refers to FERRY_GOLDEN_MONGO_URI (unset) next to a
    // FERRY_GOLDEN_PG_URL that holds a secret.
    let config = load(repo("examples/bad/missing-env/ferry")).unwrap();
    let errors = config
        .resolve_with(env(&[
            ("FERRY_GOLDEN_PG_URL", PG_URL),
            ("FERRY_GOLDEN_MONGO_URL", "x"),
        ]))
        .unwrap_err();
    let rendered = format!("{errors}\n{errors:?}");
    assert!(
        rendered.contains("FERRY_GOLDEN_MONGO_URI (connections.yaml:6:10)"),
        "{rendered}"
    );
    assert!(!rendered.contains(SECRET), "{rendered}");
    assert!(!rendered.contains("db.internal"), "{rendered}");
}

#[test]
fn substituted_values_never_show_in_debug_output() {
    let config = golden();
    let resolved = config
        .resolve_with(env(&[
            ("FERRY_GOLDEN_MONGO_URL", "mongodb://u:other-secret@m/shop"),
            ("FERRY_GOLDEN_PG_URL", PG_URL),
        ]))
        .unwrap();
    for text in [
        format!("{config:?}"),
        format!("{resolved:?}"),
        format!("{:?}", resolved.connections),
    ] {
        assert!(!text.contains(SECRET), "{text}");
        assert!(!text.contains("other-secret"), "{text}");
    }
    assert!(format!("{resolved:?}").contains("***"));
}

#[test]
fn the_redactor_masks_substituted_values_and_url_passwords() {
    let resolved = golden()
        .resolve_with(env(&[
            ("FERRY_GOLDEN_MONGO_URL", "mongodb://m/shop"),
            ("FERRY_GOLDEN_PG_URL", PG_URL),
        ]))
        .unwrap();
    let redactor = resolved.redactor();
    let driver_error = format!("error connecting to {PG_URL}: timeout");
    assert_eq!(
        redactor.redact(&driver_error),
        "error connecting to ***: timeout"
    );
    // A password typed literally into a URL is masked too.
    assert_eq!(
        redactor.redact("dial postgres://app:literal-pw@h/db"),
        "dial postgres://app:***@h/db"
    );
    // The password inside a substituted URL is masked on its own too.
    assert_eq!(redactor.redact(&format!("pw={SECRET}")), "pw=***");
}

#[test]
fn an_env_value_is_never_expanded_or_parsed() {
    let (_tmp, dir) = write(&[(
        "connections.yaml",
        "connections:\n  - name: app\n    type: postgres\n    url: ${URL}\n    options: {a: [\"pre-${URL}-post\"]}\n",
    )]);
    let config = load(&dir).unwrap();
    for value in ["${OTHER}", "{a: [1, 2]}\nb: c", "x\" , \"y"] {
        let resolved = config
            .resolve_with(env(&[("URL", value), ("OTHER", "nope")]))
            .unwrap();
        assert_eq!(
            resolved.connections[0].fields,
            json!({"url": value, "options": {"a": [format!("pre-{value}-post")]}})
        );
    }
}

#[test]
fn an_empty_value_counts_as_set() {
    // 0003 leaves this open (Open question 2); pinned here until it decides.
    let (_tmp, dir) = write(&[(
        "connections.yaml",
        "connections:\n  - name: app\n    type: postgres\n    url: ${URL}\n",
    )]);
    let resolved = load(&dir)
        .unwrap()
        .resolve_with(env(&[("URL", "")]))
        .unwrap();
    assert_eq!(resolved.connections[0].fields, json!({"url": ""}));
}

#[test]
fn runtime_state_is_substituted_and_its_missing_variables_named() {
    let (_tmp, dir) = write(&[
        (
            "connections.yaml",
            "connections:\n  - name: app\n    type: postgres\n    url: x\n",
        ),
        (
            "runtime.yaml",
            "runtime:\n  state:\n    backend: file\n    path: ${STATE_DIR}/ferry\n",
        ),
    ]);
    let config = load(&dir).unwrap();
    let resolved = config
        .resolve_with(env(&[("STATE_DIR", "/var/lib")]))
        .unwrap();
    assert_eq!(
        resolved.state.unwrap().path.as_deref(),
        Some("/var/lib/ferry")
    );
    let error = config.resolve_with(env(&[])).unwrap_err().0.remove(0);
    assert_eq!(error.path, "runtime.state.path");
    assert_eq!(error.location.unwrap().line, 4);
}

#[test]
fn env_references_anywhere_in_a_flow_are_rejected_at_load() {
    let flow = "flows:\n  - name: f\n    from: app.t\n    sink: app\n    where: \"created_at < '${CUTOFF}'\"\n    to:\n      t:\n        columns: {\"${COL}\": x}\n";
    let (_tmp, dir) = write(&[
        (
            "connections.yaml",
            "connections:\n  - name: app\n    type: postgres\n    url: x\n",
        ),
        ("flows/f.yaml", flow),
    ]);
    let errors = load(&dir).unwrap_err().0;
    let found: Vec<(u32, &str)> = errors
        .iter()
        .map(|e| (e.location.as_ref().unwrap().line, e.path.as_str()))
        .collect();
    assert_eq!(
        found,
        [(5, "flows[f].where"), (8, "flows[f].to.t.columns.${COL}")]
    );
    assert!(errors.iter().all(|e| e.kind == ErrorKind::Env));
}

#[test]
fn malformed_references_in_connections_are_rejected_at_load() {
    for (url, fragment) in [
        ("${file:/run/secrets/pg}", "the only form is `${NAME}`"),
        ("${PG_URL", "without a closing `}`"),
        ("${}", "the only form is `${NAME}`"),
    ] {
        let text = format!("connections:\n  - name: app\n    type: postgres\n    url: \"{url}\"\n");
        let (_tmp, dir) = write(&[("connections.yaml", &text)]);
        let errors = load(&dir).unwrap_err().0;
        assert_eq!(errors.len(), 1, "{url}: {errors:#?}");
        assert_eq!(errors[0].kind, ErrorKind::Env);
        assert_eq!(errors[0].path, "connections[app].url");
        assert_eq!(errors[0].location.as_ref().unwrap().line, 4);
        assert!(errors[0].message.contains(fragment), "{}", errors[0]);
    }
}

#[test]
fn a_literal_state_path_shows_in_debug_and_a_substituted_one_does_not() {
    let connections = (
        "connections.yaml",
        "connections:\n  - name: app\n    type: postgres\n    url: x\n",
    );
    let state = |path: &str| format!("runtime:\n  state:\n    backend: file\n    path: {path}\n");
    let literal = state(".ferry/state");
    let (_t1, dir) = write(&[connections, ("runtime.yaml", &literal)]);
    let resolved = load(&dir).unwrap().resolve_with(env(&[])).unwrap();
    assert!(
        format!("{resolved:?}").contains("\".ferry/state\""),
        "{resolved:?}"
    );

    let from_env = state("${STATE_DIR}");
    let (_t2, dir) = write(&[connections, ("runtime.yaml", &from_env)]);
    let resolved = load(&dir)
        .unwrap()
        .resolve_with(env(&[("STATE_DIR", "/secret/tenant-7")]))
        .unwrap();
    let debug = format!("{resolved:?}");
    assert!(!debug.contains("tenant-7"), "{debug}");
    assert!(debug.contains("***"), "{debug}");
}
