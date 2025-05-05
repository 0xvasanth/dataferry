//! B1b: the JSON Schema is generated from the serde model, checked in, and
//! agrees with serde on every file the examples hold.

mod common;

use std::collections::BTreeMap;
use std::path::Path;

use common::{manifest, repo};
use ferry_dsl::config::{FileKind, check_file, file_to_json, schema};
use serde_json::Value;

fn validator(kind: FileKind) -> jsonschema::Validator {
    let kind = match kind {
        // Included files in the examples hold flows.
        FileKind::Included => FileKind::Flows,
        kind => kind,
    };
    let schema = schema::schema(kind).unwrap();
    jsonschema::validator_for(&schema).unwrap()
}

/// Every YAML file of one `ferry/` directory, by relative path.
fn yaml_files(dir: &Path) -> BTreeMap<String, String> {
    fn walk(root: &Path, dir: &Path, out: &mut BTreeMap<String, String>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(root, &path, out);
            } else if matches!(
                path.extension().and_then(|e| e.to_str()),
                Some("yaml" | "yml")
            ) {
                let rel = path
                    .strip_prefix(root)
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .replace('\\', "/");
                out.insert(rel, std::fs::read_to_string(&path).unwrap());
            }
        }
    }
    let mut out = BTreeMap::new();
    walk(dir, dir, &mut out);
    out
}

/// (serde accepts, schema accepts) for one file, or `None` when it isn't
/// YAML the loader reads (so neither can judge it).
fn verdicts(path: &str, text: &str) -> Option<(bool, bool)> {
    let json = file_to_json(path, text).ok()?;
    let kind = FileKind::for_path(path);
    let serde_ok = check_file(kind, path, text).is_ok();
    let schema_ok = validator(kind).is_valid(&json);
    Some((serde_ok, schema_ok))
}

#[test]
fn every_valid_example_file_passes_serde_and_its_schema() {
    for dir in ["golden", "pg-merge", "edge/layout"] {
        let files = yaml_files(&repo(&format!("examples/{dir}/ferry")));
        assert!(files.len() >= 4, "{dir}");
        for (path, text) in files {
            let kind = FileKind::for_path(&path);
            if let Err(e) = check_file(kind, &path, &text) {
                panic!("{dir}/{path}: serde rejects it: {e}");
            }
            let json = file_to_json(&path, &text).unwrap();
            let validator = validator(kind);
            let problems: Vec<String> = validator
                .iter_errors(&json)
                .map(|e| e.to_string())
                .collect();
            assert!(
                problems.is_empty(),
                "{dir}/{path}: schema rejects it: {problems:#?}"
            );
        }
    }
}

#[test]
fn serde_and_the_schema_agree_on_every_bad_case_file() {
    let mut schema_cases = 0;
    for row in manifest() {
        let case = &row["Case"];
        let files = yaml_files(&repo(&format!("examples/bad/{case}/ferry")));
        for (path, text) in &files {
            let Some((serde_ok, schema_ok)) = verdicts(path, text) else {
                assert_eq!(
                    row["Checked by"], "yaml",
                    "{case}/{path} isn't readable YAML"
                );
                continue;
            };
            assert_eq!(
                serde_ok, schema_ok,
                "{case}/{path}: serde says {serde_ok}, schema says {schema_ok}"
            );
            let defective = *path == row["File"];
            let schema_level = row["Checked by"] == "schema";
            assert_eq!(
                !serde_ok,
                defective && schema_level,
                "{case}/{path}: rejected={} but the manifest says checked by {}",
                !serde_ok,
                row["Checked by"]
            );
        }
        schema_cases += usize::from(row["Checked by"] == "schema");
    }
    assert!(schema_cases >= 9, "{schema_cases}");
}

/// Single-file edits beyond the bad set, each judged by both. `true` means
/// the file is valid.
#[test]
fn serde_and_the_schema_agree_on_single_file_edits() {
    let flow = |body: &str| format!("flows:\n  - name: f\n    from: app.t\n    sink: app\n{body}");
    let to = |columns: &str| flow(&format!("    to:\n      t:\n        columns: {columns}\n"));
    let cases: Vec<(&str, String, bool)> = vec![
        ("ferry.yaml", "version: 1\nname: x\n".into(), true),
        ("ferry.yaml", "version: 1\nname: X\n".into(), false),
        ("ferry.yaml", "version: \"1\"\nname: x\n".into(), false),
        ("ferry.yaml", "version: 1\nname: x\ninclude: [a.yaml]\n".into(), true),
        ("ferry.yaml", "version: 1\nname: x\ninclude: a.yaml\n".into(), false),
        ("runtime.yaml", "runtime: {}\n".into(), true),
        ("runtime.yaml", "runtime:\n  workers: auto\n".into(), true),
        ("runtime.yaml", "runtime:\n  workers: 0\n".into(), false),
        ("runtime.yaml", "runtime:\n  workers: many\n".into(), false),
        ("runtime.yaml", "runtime:\n  memory_budget: 512MiB\n".into(), true),
        ("runtime.yaml", "runtime:\n  memory_budget: 1048576\n".into(), true),
        ("runtime.yaml", "runtime:\n  memory_budget: 1GB\n".into(), false),
        ("runtime.yaml", "runtime:\n  memory_budget: 0MiB\n".into(), false),
        ("runtime.yaml", "runtime:\n  partition_rows: 0\n".into(), false),
        ("runtime.yaml", "runtime:\n  on_error: skip\n".into(), false),
        ("runtime.yaml", "runtime:\n  on_error: dead_letter\n  max_errors: 5\n".into(), true),
        ("runtime.yaml", "runtime:\n  max_errors: -1\n".into(), false),
        ("runtime.yaml", "runtime:\n  state: {backend: file, path: s}\n".into(), true),
        ("runtime.yaml", "runtime:\n  state: {path: s}\n".into(), false),
        ("runtime.yaml", "runtime:\n  state: {backend: file, dir: s}\n".into(), false),
        ("connections.yaml", "connections:\n  - name: a\n    type: postgres\n    url: x\n".into(), true),
        ("connections.yaml", "connections:\n  - name: a\n    url: x\n".into(), false),
        ("connections.yaml", "connections:\n  - name: A\n    type: postgres\n".into(), false),
        ("connections.yaml", "connections: {}\n".into(), false),
        ("flows/f.yaml", "flows: []\n".into(), true),
        ("flows/f.yaml", to("\"*\""), true),
        ("flows/f.yaml", to("all"), false),
        ("flows/f.yaml", to("{a: x, b: \"y + 1\"}"), true),
        ("flows/f.yaml", to("{a: 5}"), false),
        ("flows/f.yaml", to("{a: null}"), false),
        ("flows/f.yaml", to("{a: {expr: x, type: int64, nullable: false, allow_lossy: true}}"), true),
        ("flows/f.yaml", to("{a: {type: int64}}"), false),
        ("flows/f.yaml", to("{a: {expr: x, nest: i, columns: {b: y}}}"), false),
        ("flows/f.yaml", to("{a: {nest: i, order_by: [y], where: \"y > 0\", columns: {b: y}}}"), true),
        ("flows/f.yaml", to("{a: {nest: i}}"), false),
        ("flows/f.yaml", to("{a: {expr: x, columns: {b: y}}}"), false),
        ("flows/f.yaml", to("{a: {nest: i, columns: {b: {nest: j, columns: {c: z}}}}}"), true),
        ("flows/f.yaml", to("{a: {expr: x, tpye: int64}}"), false),
        ("flows/f.yaml", flow("    mode: upsert\n    to:\n      t:\n        key: [id]\n        columns: \"*\"\n"), true),
        ("flows/f.yaml", flow("    mode: merge\n    to:\n      t:\n        columns: \"*\"\n"), false),
        ("flows/f.yaml", flow("    where: x > 1\n    to:\n      t:\n        columns: \"*\"\n"), true),
        ("flows/f.yaml", flow("    where: {a: x > 1, b: y}\n    to:\n      t:\n        columns: \"*\"\n"), true),
        ("flows/f.yaml", flow("    where: [x]\n    to:\n      t:\n        columns: \"*\"\n"), false),
        ("flows/f.yaml", flow("    depends_on: [a, b]\n    to:\n      t:\n        columns: \"*\"\n"), true),
        ("flows/f.yaml", flow("    depends_on: [A]\n    to:\n      t:\n        columns: \"*\"\n"), false),
        ("flows/f.yaml", flow("    depends_on: a\n    to:\n      t:\n        columns: \"*\"\n"), false),
        ("flows/f.yaml", flow("    runtime: {on_error: fail, max_errors: 1, partition_rows: 5}\n    to:\n      t:\n        columns: \"*\"\n"), true),
        ("flows/f.yaml", flow("    runtime: {state: {backend: file}}\n    to:\n      t:\n        columns: \"*\"\n"), false),
        ("flows/f.yaml", flow("    expect: {n: count(t) == source.count()}\n    to:\n      t:\n        columns: \"*\"\n"), true),
        ("flows/f.yaml", flow("    expect: [count(t) == source.count()]\n    to:\n      t:\n        columns: \"*\"\n"), false),
        ("flows/f.yaml", flow("    to:\n      t:\n        parent: p\n        unnest: items\n        key: [a]\n        where: a > 0\n        columns: {a: $index}\n"), true),
        ("flows/f.yaml", flow("    to:\n      t:\n        key: a\n        columns: \"*\"\n"), false),
        ("flows/f.yaml", flow("    to:\n      t: {}\n"), false),
        ("flows/f.yaml", "flows:\n  - name: f\n    from: [{entity: app.o, as: o}, {entity: app.c, join: {cardinality: one, keys: {id: o.c_id}, strategy: stage}}]\n    sink: app\n    to:\n      t:\n        columns: {a: x}\n".into(), true),
        ("flows/f.yaml", "flows:\n  - name: f\n    from: [{entity: app.o}, {entity: app.c, join: {cardinality: some, keys: {id: o.c_id}}}]\n    sink: app\n    to:\n      t:\n        columns: {a: x}\n".into(), false),
        ("flows/f.yaml", "flows:\n  - name: f\n    from: [{entity: app.o}, {entity: app.c, join: {cardinality: one}}]\n    sink: app\n    to:\n      t:\n        columns: {a: x}\n".into(), false),
        ("flows/f.yaml", "flows:\n  - name: f\n    from: [{entity: app.o, on: x}]\n    sink: app\n    to:\n      t:\n        columns: {a: x}\n".into(), false),
        ("flows/f.yaml", "flows:\n  - name: f\n    from: [{entity: app.o, as: O}]\n    sink: app\n    to:\n      t:\n        columns: {a: x}\n".into(), false),
        ("flows/f.yaml", "flows:\n  - name: f\n    from: app\n    sink: app\n    to:\n      t:\n        columns: \"*\"\n".into(), false),
        ("flows/f.yaml", "flows:\n  - name: f\n    from: app.\n    sink: app\n    to:\n      t:\n        columns: \"*\"\n".into(), false),
    ];
    for (path, text, valid) in cases {
        let (serde_ok, schema_ok) = verdicts(path, &text).unwrap_or_else(|| panic!("{text}"));
        assert_eq!(
            serde_ok, schema_ok,
            "serde and schema disagree on {path}:\n{text}"
        );
        assert_eq!(serde_ok, valid, "{path}:\n{text}");
    }
}

#[test]
fn checked_in_schemas_match_the_model() {
    let dir = repo(&schema::directory());
    let update = std::env::var_os("FERRY_UPDATE_SCHEMAS").is_some();
    let documents = schema::documents();
    assert_eq!(documents.len(), 4, "one schema per file kind");
    for (name, document) in documents {
        let path = dir.join(name);
        let rendered = schema::render(&document);
        if update {
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(&path, &rendered).unwrap();
            continue;
        }
        let checked_in = std::fs::read_to_string(&path).unwrap_or_default();
        assert!(
            checked_in == rendered,
            "{} is out of date: run `FERRY_UPDATE_SCHEMAS=1 cargo test -p ferry-dsl --test schema` and commit the result",
            path.display()
        );
        // Each is a valid JSON Schema document in its own right.
        let value: Value = serde_json::from_str(&checked_in).unwrap();
        jsonschema::validator_for(&value).unwrap();
    }
}

#[test]
fn connection_items_are_left_open_for_the_registry() {
    let schema = schema::schema(FileKind::Connections).unwrap();
    let json = serde_json::json!({"connections": [
        {"name": "a", "type": "anything", "url": "x", "port": 5432, "nested": {"k": [1]}}
    ]});
    assert!(jsonschema::validator_for(&schema).unwrap().is_valid(&json));
}
