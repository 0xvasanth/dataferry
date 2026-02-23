//! Crates.io readiness, from `cargo metadata`: every crate declares the same
//! minimum Rust version and the metadata crates.io shows, and the crates CI
//! leaves out of `cargo package` are exactly those that can't be published
//! (test-only, or depending on a git dependency such as the DataFusion pin).

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::process::Command;

use common::repo;
use serde_json::Value;

fn metadata() -> Value {
    let out = Command::new(env!("CARGO"))
        .args(["metadata", "--format-version", "1", "--locked"])
        .current_dir(repo(""))
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).unwrap()
}

/// Workspace members by name.
fn members(metadata: &Value) -> BTreeMap<String, Value> {
    let ids: BTreeSet<&str> = metadata["workspace_members"]
        .as_array()
        .unwrap()
        .iter()
        .map(|id| id.as_str().unwrap())
        .collect();
    metadata["packages"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|p| ids.contains(p["id"].as_str().unwrap()))
        .map(|p| (p["name"].as_str().unwrap().to_owned(), p.clone()))
        .collect()
}

fn publishable(package: &Value) -> bool {
    package["publish"] != serde_json::json!([])
}

/// Members whose normal or build dependencies reach, transitively, a
/// package crates.io can't hold (from git).
fn depend_on_git(metadata: &Value) -> BTreeSet<String> {
    let packages: BTreeMap<&str, &Value> = metadata["packages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| (p["id"].as_str().unwrap(), p))
        .collect();
    let deps: BTreeMap<&str, Vec<&str>> = metadata["resolve"]["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|node| {
            let deps = node["deps"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|dep| {
                    dep["dep_kinds"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|kind| kind["kind"].as_str() != Some("dev"))
                })
                .map(|dep| dep["pkg"].as_str().unwrap())
                .collect();
            (node["id"].as_str().unwrap(), deps)
        })
        .collect();
    let mut out = BTreeSet::new();
    for id in metadata["workspace_members"].as_array().unwrap() {
        let id = id.as_str().unwrap();
        let mut seen = BTreeSet::new();
        let mut todo = vec![id];
        while let Some(next) = todo.pop() {
            if !seen.insert(next) {
                continue;
            }
            let source = packages[next]["source"].as_str().unwrap_or_default();
            if source.starts_with("git+") {
                out.insert(packages[id]["name"].as_str().unwrap().to_owned());
                break;
            }
            todo.extend(deps.get(next).into_iter().flatten());
        }
    }
    out
}

/// The `--exclude`s of the `cargo package` line in `text`.
fn excluded(text: &str, what: &str) -> BTreeSet<String> {
    let line = text
        .lines()
        .find(|line| line.contains("cargo package --workspace"))
        .unwrap_or_else(|| panic!("{what} has no `cargo package --workspace` line"));
    let words: Vec<&str> = line.split_whitespace().collect();
    words
        .windows(2)
        .filter(|w| w[0] == "--exclude")
        .map(|w| w[1].to_owned())
        .collect()
}

#[test]
fn every_crate_declares_the_same_minimum_rust_version_that_ci_builds_with() {
    let members = members(&metadata());
    let versions: BTreeSet<&str> = members
        .values()
        .map(|p| p["rust_version"].as_str().expect("rust-version declared"))
        .collect();
    assert_eq!(versions.len(), 1, "one MSRV for every crate: {versions:?}");
    let msrv = versions.into_iter().next().unwrap();

    let ci = std::fs::read_to_string(repo(".github/workflows/ci.yml")).unwrap();
    assert!(
        ci.contains(&format!("dtolnay/rust-toolchain@{msrv}\n")),
        "CI's msrv job builds with {msrv}"
    );
    let readme = std::fs::read_to_string(repo("README.md")).unwrap();
    assert!(readme.contains(&format!("Rust {msrv} or newer")));
}

#[test]
fn every_publishable_crate_has_the_metadata_crates_io_shows() {
    let members = members(&metadata());
    assert!(
        !publishable(&members["acme-ferry"]),
        "acme-ferry is test-only"
    );
    for (name, package) in members.iter().filter(|(_, p)| publishable(p)) {
        for field in ["description", "license", "repository", "homepage", "readme"] {
            assert!(
                package[field].as_str().is_some_and(|v| !v.is_empty()),
                "{name}: no {field}"
            );
        }
        let readme = std::path::Path::new(package["manifest_path"].as_str().unwrap())
            .parent()
            .unwrap()
            .join(package["readme"].as_str().unwrap());
        assert!(readme.is_file(), "{name}: {}", readme.display());
        // crates.io's limits: at most five keywords of at most 20
        // characters, at most five categories.
        let keywords = package["keywords"].as_array().unwrap();
        assert!((1..=5).contains(&keywords.len()), "{name}: {keywords:?}");
        for keyword in keywords {
            let keyword = keyword.as_str().unwrap();
            assert!(
                keyword.len() <= 20
                    && keyword
                        .chars()
                        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'),
                "{name}: keyword {keyword:?}"
            );
        }
        let categories = package["categories"].as_array().unwrap();
        assert!(
            (1..=5).contains(&categories.len()),
            "{name}: {categories:?}"
        );
    }
}

#[test]
fn ci_packages_every_crate_that_can_be_published() {
    let metadata = metadata();
    let members = members(&metadata);
    let git = depend_on_git(&metadata);
    // Today: the DataFusion pin, in ferry-dsl and through it ferry-cli.
    assert!(git.contains("ferry-dsl"), "{git:?}");
    let mut blocked: BTreeSet<String> = git;
    blocked.extend(
        members
            .iter()
            .filter(|(_, p)| !publishable(p))
            .map(|(name, _)| name.clone()),
    );

    let ci = std::fs::read_to_string(repo(".github/workflows/ci.yml")).unwrap();
    assert_eq!(
        excluded(&ci, "ci.yml"),
        blocked,
        "CI's `cargo package` must leave out exactly the crates that can't be published"
    );
    let readme = std::fs::read_to_string(repo("README.md")).unwrap();
    assert_eq!(excluded(&readme, "README.md"), blocked);
}
