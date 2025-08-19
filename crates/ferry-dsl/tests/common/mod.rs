//! Helpers shared by the integration tests.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::path::PathBuf;

pub fn repo(path: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(path)
}

/// Rows of the first markdown table in `text` whose header starts with
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
        .map(|l| {
            let row = cells(l);
            assert_eq!(row.len(), header.len(), "malformed row: {l}");
            header.iter().cloned().zip(row).collect()
        })
        .collect()
}

/// The rows of `examples/bad/README.md`.
pub fn manifest() -> Vec<BTreeMap<String, String>> {
    let text = std::fs::read_to_string(repo("examples/bad/README.md")).unwrap();
    table(&text, "Case")
}

pub mod golden;
