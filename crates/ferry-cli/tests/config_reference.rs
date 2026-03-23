//! `docs/config-reference.md` against the schemas `ferry schema` prints:
//! every key the schemas have is documented, every documented key exists,
//! and every value a key enumerates is named in its row. The format the
//! doc follows is in a comment at its top.

mod common;

use std::collections::{BTreeMap, BTreeSet};

use common::{ferry, output, repo};
use serde_json::Value;

/// Doc section heading → schema file.
const FILES: [(&str, &str); 4] = [
    ("ferry.yaml", "ferry.schema.json"),
    ("connections.yaml", "connections.schema.json"),
    ("runtime.yaml", "runtime.schema.json"),
    ("flows/**/*.yaml", "flows.schema.json"),
];

/// Key path → the string values its schema enumerates.
type Keys = BTreeMap<String, BTreeSet<String>>;

/// Every key path of a schema document. A connector's branch of the
/// connection entry is marked `{type=<type>}`; a definition already being
/// walked isn't entered again (a nest column holds column specs).
fn keys(doc: &Value) -> Keys {
    fn walk(doc: &Value, node: &Value, path: &str, stack: &[String], out: &mut Keys) {
        let Some(node) = node.as_object() else {
            return;
        };
        if let Some(Value::String(target)) = node.get("$ref") {
            let name = target.rsplit('/').next().unwrap().to_owned();
            if !stack.contains(&name) {
                let def = &doc["$defs"][&name];
                assert!(!def.is_null(), "dangling {target}");
                let stack = [stack, &[name]].concat();
                walk(doc, def, path, &stack, out);
            }
        }
        let values = node
            .get("enum")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .chain(node.get("const"))
            .filter_map(Value::as_str);
        for value in values {
            out.entry(path.to_owned())
                .or_default()
                .insert(value.to_owned());
        }
        for combinator in ["allOf", "anyOf", "oneOf"] {
            for branch in node
                .get(combinator)
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                let path = match branch.pointer("/properties/type/const") {
                    Some(Value::String(ty)) => format!("{path}{{type={ty}}}"),
                    _ => path.to_owned(),
                };
                walk(doc, branch, &path, stack, out);
            }
        }
        if let Some(Value::Object(properties)) = node.get("properties") {
            for (key, schema) in properties {
                let path = if path.is_empty() {
                    key.clone()
                } else {
                    format!("{path}.{key}")
                };
                out.entry(path.clone()).or_default();
                walk(doc, schema, &path, stack, out);
            }
        }
        if let Some(items) = node.get("items") {
            walk(doc, items, &format!("{path}[]"), stack, out);
        }
        if let Some(values @ Value::Object(_)) = node.get("additionalProperties") {
            let path = format!("{path}.<key>");
            out.entry(path.clone()).or_default();
            walk(doc, values, &path, stack, out);
        }
    }
    let mut out = Keys::new();
    walk(doc, doc, "", &[], &mut out);
    // A connector's `name` and `type` are the entry's own, documented once.
    let covered: Vec<String> = out
        .keys()
        .filter(|path| {
            path.split_once('{')
                .and_then(|(head, rest)| Some(format!("{head}{}", rest.split_once('}')?.1)))
                .is_some_and(|plain| out.contains_key(&plain))
        })
        .cloned()
        .collect();
    for path in covered {
        out.remove(&path);
    }
    out
}

/// `<table>`, `<column>`, `<name>`: any placeholder is a map key.
fn normalize(path: &str) -> String {
    let mut out = String::new();
    let mut rest = path;
    while let Some(open) = rest.find('<') {
        let close = rest[open..].find('>').expect("an unclosed <placeholder>") + open;
        out.push_str(&rest[..open]);
        out.push_str("<key>");
        rest = &rest[close + 1..];
    }
    out + rest
}

/// Per schema file: documented key path → the text of its row.
fn documented(text: &str) -> Result<BTreeMap<&'static str, BTreeMap<String, String>>, String> {
    let mut out: BTreeMap<&'static str, BTreeMap<String, String>> = BTreeMap::new();
    let mut file: Option<&'static str> = None;
    let mut connector: Option<String> = None;
    let mut key_table = false;
    for (n, line) in text.lines().enumerate() {
        let at = || format!("config-reference.md:{}", n + 1);
        if let Some(heading) = line.strip_prefix("## ") {
            file = FILES
                .iter()
                .find(|(name, _)| heading == format!("`{name}`"))
                .map(|(_, schema)| *schema);
            connector = None;
        } else if let Some(heading) = line.strip_prefix("### ") {
            connector = heading
                .strip_prefix("`type: ")
                .and_then(|h| h.strip_suffix('`'))
                .map(str::to_owned);
        }
        if !line.starts_with('|') {
            key_table = false;
            continue;
        }
        let first = line.split('|').nth(1).unwrap_or_default().trim();
        if first == "Key" {
            key_table = true;
            continue;
        }
        if !key_table || first.starts_with("---") {
            continue;
        }
        let Some(file) = file else {
            return Err(format!("{}: a key row outside a file's section", at()));
        };
        let path = first
            .strip_prefix('`')
            .and_then(|p| p.strip_suffix('`'))
            .ok_or_else(|| format!("{}: a key row's first cell is `<path>`", at()))?;
        let path = normalize(path);
        let path = match &connector {
            Some(ty) => match path.strip_prefix("connections[].") {
                Some(rest) => format!("connections[]{{type={ty}}}.{rest}"),
                None => return Err(format!("{}: `{path}` under `type: {ty}`", at())),
            },
            None => path,
        };
        if out
            .entry(file)
            .or_default()
            .insert(path.clone(), line.to_owned())
            .is_some()
        {
            return Err(format!("{}: `{path}` documented twice", at()));
        }
    }
    Ok(out)
}

/// Everything wrong with `doc` against `schemas` (file name → schema).
fn problems(doc: &str, schemas: &Value) -> Vec<String> {
    let documented = match documented(doc) {
        Ok(documented) => documented,
        Err(problem) => return vec![problem],
    };
    let mut problems = Vec::new();
    for (_, file) in FILES {
        let keys = keys(&schemas[file]);
        let rows = documented.get(file).cloned().unwrap_or_default();
        for (path, values) in &keys {
            let Some(row) = rows.get(path) else {
                problems.push(format!("{file}: `{path}` is not documented"));
                continue;
            };
            for value in values {
                if !row.contains(&format!("`{value}`")) && !row.contains(&format!("`\"{value}\"`"))
                {
                    problems.push(format!("{file}: `{path}` doesn't name `{value}`"));
                }
            }
        }
        for path in rows.keys().filter(|path| !keys.contains_key(*path)) {
            problems.push(format!(
                "{file}: `{path}` is documented but not in the schema"
            ));
        }
    }
    problems
}

/// What the stock binary's `ferry schema` prints: one object per file.
fn stock_schemas() -> Value {
    let out = output(ferry(&repo("")).arg("schema")).expect(0);
    serde_json::from_str(&out.stdout).unwrap()
}

fn reference() -> String {
    std::fs::read_to_string(repo("docs/config-reference.md")).unwrap()
}

#[test]
fn every_schema_key_is_documented_and_every_documented_key_exists() {
    let schemas = stock_schemas();
    let problems = problems(&reference(), &schemas);
    assert!(
        problems.is_empty(),
        "docs/config-reference.md is out of date with `ferry schema`:\n  {}",
        problems.join("\n  ")
    );
    // The stock binary documents both built-in connectors' fields.
    let connections = keys(&schemas["connections.schema.json"]);
    for key in [
        "connections[]{type=postgres}.url",
        "connections[]{type=postgres}.foreign_keys",
        "connections[]{type=mongo}.database",
    ] {
        assert!(connections.contains_key(key), "{key}");
    }
}

#[test]
fn the_checked_in_schemas_have_no_key_the_binary_lacks() {
    // `schemas/v1` is B1's model with connection entries left open, so its
    // keys are the binary's minus the connector fields.
    let schemas = stock_schemas();
    for (_, file) in FILES {
        let text = std::fs::read_to_string(repo(&format!("schemas/v1/{file}"))).unwrap();
        let checked_in = keys(&serde_json::from_str(&text).unwrap());
        let binary = keys(&schemas[file]);
        let missing: Vec<&String> = checked_in
            .keys()
            .filter(|k| !binary.contains_key(*k))
            .collect();
        assert!(missing.is_empty(), "{file}: {missing:?}");
        if file != "connections.schema.json" {
            assert_eq!(checked_in, binary, "{file}");
        }
    }
}

#[test]
fn the_check_catches_a_missing_key_an_unknown_key_and_a_missing_value() {
    let schemas = stock_schemas();
    let doc = reference();
    let row = |key: &str| {
        doc.lines()
            .find(|line| line.starts_with(&format!("| `{key}` |")))
            .unwrap_or_else(|| panic!("no row for {key}"))
            .to_owned()
    };
    let workers = row("runtime.workers");
    let database = row("connections[].database");
    let mode = row("flows[].mode");
    let edited = doc
        .replacen(&format!("{workers}\n"), "", 1)
        .replacen(&format!("{database}\n"), "", 1)
        .replacen(&mode, &mode.replace("`upsert`", "upsert"), 1)
        .replacen(
            &row("runtime.max_errors"),
            &format!(
                "{}\n| `runtime.wrokers` | integer | | A typo. |",
                row("runtime.max_errors")
            ),
            1,
        );
    assert_eq!(
        problems(&edited, &schemas),
        [
            "connections.schema.json: `connections[]{type=mongo}.database` is not documented",
            "runtime.schema.json: `runtime.workers` is not documented",
            "runtime.schema.json: `runtime.wrokers` is documented but not in the schema",
            "flows.schema.json: `flows[].mode` doesn't name `upsert`",
        ]
    );

    // A connector field outside its connector's section is a format error.
    let misplaced = doc.replacen(&format!("{database}\n"), "", 1).replacen(
        "| `runtime.max_errors` |",
        "| `connections[].database` | x | | x |\n| `runtime.max_errors` |",
        1,
    );
    let problems = problems(&misplaced, &schemas);
    assert!(
        problems.contains(
            &"runtime.schema.json: `connections[].database` is documented but not in the schema"
                .to_owned()
        ),
        "{problems:?}"
    );
}
