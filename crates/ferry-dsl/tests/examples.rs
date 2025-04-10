//! The example configs are fixtures for every config-reading ticket, so they
//! have to stay internally consistent: each bad case differs from the golden
//! config in exactly the one file its manifest row names, every file is
//! valid YAML unless the case is about YAML syntax, and the golden flow still
//! implements every rule of the golden dataset.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

fn repo(path: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(path)
}

/// Every file under `dir`, keyed by its `/`-separated path relative to `dir`.
fn files(dir: &Path) -> BTreeMap<String, Vec<u8>> {
    fn walk(root: &Path, dir: &Path, out: &mut BTreeMap<String, Vec<u8>>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(root, &path, out);
            } else {
                let rel = path.strip_prefix(root).unwrap();
                let rel = rel
                    .components()
                    .map(|c| c.as_os_str().to_str().unwrap())
                    .collect::<Vec<_>>()
                    .join("/");
                out.insert(rel, std::fs::read(&path).unwrap());
            }
        }
    }
    let mut out = BTreeMap::new();
    walk(dir, dir, &mut out);
    out
}

/// Rows of the first markdown table in `text` whose header starts with
/// `first_column`, as maps from header to cell (backticks stripped).
fn table(text: &str, first_column: &str) -> Vec<BTreeMap<String, String>> {
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
        .map(|l| {
            let row = cells(l);
            assert_eq!(row.len(), header.len(), "malformed row: {l}");
            header.iter().cloned().zip(row).collect()
        })
        .collect()
}

fn manifest() -> Vec<BTreeMap<String, String>> {
    let text = std::fs::read_to_string(repo("examples/bad/README.md")).unwrap();
    table(&text, "Case")
}

/// Parses `bytes` as plain YAML (syntax only) and requires a single document.
fn parse_yaml(bytes: &[u8]) -> Result<(), String> {
    use saphyr_parser::{Event, Parser};
    let text = std::str::from_utf8(bytes).map_err(|e| e.to_string())?;
    let mut documents = 0;
    for event in Parser::new_from_str(text) {
        match event.map_err(|e| e.to_string())?.0 {
            Event::DocumentStart(_) => documents += 1,
            _ => continue,
        }
    }
    match documents {
        0 | 1 => Ok(()),
        n => Err(format!("{n} documents")),
    }
}

fn is_yaml(path: &str) -> bool {
    path.ends_with(".yaml") || path.ends_with(".yml")
}

#[test]
fn every_bad_case_is_in_the_manifest_and_every_row_has_a_directory() {
    let rows: BTreeSet<String> = manifest().into_iter().map(|r| r["Case"].clone()).collect();
    let dirs: BTreeSet<String> = std::fs::read_dir(repo("examples/bad"))
        .unwrap()
        .map(|e| e.unwrap())
        .filter(|e| e.path().is_dir())
        .map(|e| e.file_name().into_string().unwrap())
        .collect();
    assert_eq!(rows, dirs);
}

#[test]
fn each_bad_case_differs_from_golden_in_exactly_the_named_file() {
    let golden = files(&repo("examples/golden/ferry"));
    for row in manifest() {
        let case = &row["Case"];
        let bad = files(&repo(&format!("examples/bad/{case}/ferry")));
        let changed: BTreeSet<&String> = golden
            .keys()
            .chain(bad.keys())
            .filter(|f| golden.get(*f) != bad.get(*f))
            .collect();
        assert_eq!(
            changed,
            BTreeSet::from([&row["File"]]),
            "{case}: differs from golden in other files than the manifest says"
        );
    }
}

#[test]
fn every_example_file_is_valid_yaml_except_the_syntax_case() {
    let all = files(&repo("examples"));
    let mut checked = 0;
    for (path, bytes) in all.iter().filter(|(p, _)| is_yaml(p)) {
        let result = parse_yaml(bytes);
        if path == "bad/yaml-syntax/ferry/flows/orders.yaml" {
            assert!(result.is_err(), "{path} should not parse");
        } else {
            result.unwrap_or_else(|e| panic!("{path}: {e}"));
        }
        checked += 1;
    }
    assert!(checked > 100, "only {checked} YAML files found");
}

#[test]
fn golden_flow_implements_every_rule_of_the_golden_dataset() {
    let readme = std::fs::read_to_string(repo("fixtures/golden/README.md")).unwrap();
    let flow = std::fs::read_to_string(repo("examples/golden/ferry/flows/orders.yaml")).unwrap();
    let lines: Vec<&str> = flow.lines().collect();
    let rules = table(&readme, "Rule");
    assert_eq!(rules.len(), 12, "a rule was added or dropped");
    for rule in rules {
        let line: usize = rule["Line"].parse().unwrap();
        let text = &rule["Text on that line"];
        assert!(
            lines[line - 1].contains(text.as_str()),
            "rule {:?}: line {line} is {:?}, expected it to contain {text:?}",
            rule["Rule"],
            lines[line - 1]
        );
    }
}

#[test]
fn examples_hold_no_credentials_and_document_every_variable() {
    let readme = std::fs::read_to_string(repo("examples/README.md")).unwrap();
    let all = files(&repo("examples"));
    let mut variables = BTreeSet::new();
    for (path, bytes) in all.iter().filter(|(p, _)| p.ends_with("connections.yaml")) {
        for line in std::str::from_utf8(bytes).unwrap().lines() {
            let Some(value) = line.trim().strip_prefix("url: ") else {
                continue;
            };
            if path == "bad/env-file-reference/ferry/connections.yaml" && value.contains("file:") {
                continue;
            }
            let name = value
                .strip_prefix("${")
                .and_then(|v| v.strip_suffix('}'))
                .unwrap_or_else(|| panic!("{path}: `{value}` is not a ${{ENV}} reference"));
            variables.insert(name.to_owned());
        }
    }
    // The missing-env case refers to a variable that is deliberately unset.
    variables.remove("FERRY_GOLDEN_MONGO_URI");
    for name in &variables {
        assert!(
            readme.contains(&format!("`{name}`")),
            "{name} is not listed in examples/README.md"
        );
    }
    assert!(variables.len() >= 6, "{variables:?}");
}
