//! Crates.io readiness: every crate declares the same minimum Rust version
//! and the metadata crates.io shows, nothing depends on a git source
//! (crates.io refuses them), the crates CI leaves out of `cargo package` are
//! exactly the test-only ones, and every other crate really packages. And
//! the release (RELEASING.md): one version everywhere, internal dependencies
//! pinned to it, a CHANGELOG section for it, and a publish order that puts
//! every crate after what it needs.

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
        !publishable(&members["acme-dataferry"]),
        "acme-dataferry is test-only"
    );
    for (name, package) in members.iter().filter(|(_, p)| publishable(p)) {
        for field in ["description", "license", "repository", "homepage", "readme"] {
            assert!(
                package[field].as_str().is_some_and(|v| !v.is_empty()),
                "{name}: no {field}"
            );
        }
        assert_eq!(
            package["documentation"].as_str(),
            Some(format!("https://docs.rs/{name}").as_str()),
            "{name}: documentation links its docs.rs page"
        );
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

/// Members that are never published (`publish = false`).
fn test_only(members: &BTreeMap<String, Value>) -> BTreeSet<String> {
    members
        .iter()
        .filter(|(_, p)| !publishable(p))
        .map(|(name, _)| name.clone())
        .collect()
}

#[test]
fn no_crate_depends_on_a_git_source() {
    // A git dependency anywhere under a crate's normal dependencies keeps
    // it off crates.io (DataFusion was one until 55.2).
    let git = depend_on_git(&metadata());
    assert!(git.is_empty(), "these depend on git: {git:?}");
}

#[test]
fn ci_packages_every_crate_but_the_test_only_ones() {
    let members = members(&metadata());
    let test_only = test_only(&members);
    assert_eq!(test_only, BTreeSet::from(["acme-dataferry".to_owned()]));

    let ci = std::fs::read_to_string(repo(".github/workflows/ci.yml")).unwrap();
    assert_eq!(
        excluded(&ci, "ci.yml"),
        test_only,
        "CI's `cargo package` must leave out exactly the test-only crates"
    );
    let readme = std::fs::read_to_string(repo("README.md")).unwrap();
    assert_eq!(excluded(&readme, "README.md"), test_only);
}

/// `cargo package` on the whole workspace, as CI runs it, but without
/// building each tarball (CI's package job does that): a new crate that is
/// missing a version on a path dependency, a readme, or anything else
/// crates.io needs fails here.
#[test]
fn every_publishable_crate_packages() {
    let members = members(&metadata());
    let target = tempfile::tempdir().unwrap();
    let mut cmd = Command::new(env!("CARGO"));
    cmd.args(["package", "--workspace", "--locked", "--no-verify"])
        .arg("--allow-dirty")
        .arg("--target-dir")
        .arg(target.path())
        .current_dir(repo(""));
    for name in test_only(&members) {
        cmd.args(["--exclude", &name]);
    }
    let out = cmd.output().unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let packaged: BTreeSet<String> = std::fs::read_dir(target.path().join("package"))
        .unwrap()
        .filter_map(|entry| {
            let name = entry.unwrap().file_name().into_string().unwrap();
            name.strip_suffix(".crate").map(str::to_owned)
        })
        .collect();
    let expected: BTreeSet<String> = members
        .iter()
        .filter(|(_, p)| publishable(p))
        .map(|(name, p)| format!("{name}-{}", p["version"].as_str().unwrap()))
        .collect();
    assert_eq!(packaged, expected);
}

/// The one version every publishable crate has.
fn release_version(members: &BTreeMap<String, Value>) -> String {
    let versions: BTreeSet<&str> = members
        .values()
        .filter(|p| publishable(p))
        .map(|p| p["version"].as_str().unwrap())
        .collect();
    assert_eq!(
        versions.len(),
        1,
        "one version for every crate: {versions:?}"
    );
    versions.into_iter().next().unwrap().to_owned()
}

/// A workspace dependency `cargo publish` keeps: normal and build ones, and
/// dev ones with a version (a path-only dev-dependency is dropped).
fn kept<'a>(package: &'a Value, members: &BTreeMap<String, Value>) -> Vec<&'a Value> {
    package["dependencies"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|dep| {
            let name = dep["name"].as_str().unwrap();
            members.contains_key(name)
                && name != package["name"].as_str().unwrap()
                && !(dep["kind"].as_str() == Some("dev") && dep["req"] == "*")
        })
        .collect()
}

#[test]
fn internal_dependencies_pin_this_release_exactly() {
    let members = members(&metadata());
    let version = release_version(&members);
    for (name, package) in members.iter().filter(|(_, p)| publishable(p)) {
        for dep in kept(package, &members) {
            assert_eq!(
                dep["req"].as_str().unwrap(),
                format!("={version}"),
                "{name} -> {}: crates of one release depend on each other at exactly its version",
                dep["name"]
            );
        }
    }
}

fn release_script(args: &[&str]) -> std::process::Output {
    Command::new(repo("scripts/release.py"))
        .args(args)
        .current_dir(repo(""))
        .output()
        .unwrap()
}

#[test]
fn the_publish_order_puts_every_crate_after_what_it_needs() {
    let members = members(&metadata());
    let out = release_script(&["order"]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let order: Vec<String> = String::from_utf8(out.stdout)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect();
    let publishable: BTreeSet<String> = members
        .iter()
        .filter(|(_, p)| publishable(p))
        .map(|(name, _)| name.clone())
        .collect();
    assert_eq!(order.iter().cloned().collect::<BTreeSet<_>>(), publishable);
    assert_eq!(order.len(), publishable.len(), "each crate once: {order:?}");
    let position = |name: &str| order.iter().position(|n| n == name).unwrap();
    for name in &order {
        for dep in kept(&members[name], &members) {
            let dep = dep["name"].as_str().unwrap();
            assert!(
                position(dep) < position(name),
                "{name} is published before {dep}, which it needs: {order:?}"
            );
        }
    }
    // The CLI needs everything, so it goes last.
    assert_eq!(order.last().map(String::as_str), Some("dataferry"));
}

#[test]
fn the_changelog_and_readme_are_ready_for_this_version() {
    let version = release_version(&members(&metadata()));
    let out = release_script(&["check-tag", &format!("v{version}")]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(String::from_utf8(out.stdout).unwrap().trim(), version);
    // Any other tag is refused before anything is built.
    let out = release_script(&["check-tag", &format!("v{version}.1")]);
    assert!(!out.status.success());

    // The release notes: this version's section, without the next one's.
    let out = release_script(&["notes", &version]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let notes = String::from_utf8(out.stdout).unwrap();
    assert!(notes.contains("### Added"), "{notes}");
    assert!(!notes.contains("## ["), "{notes}");
    assert!(!release_script(&["notes", "0.0.0"]).status.success());

    // Both READMEs (the repository's, and this crate's on crates.io)
    // install this version from crates.io: a pre-release has to be asked
    // for by version.
    for readme in ["README.md", "crates/dataferry/README.md"] {
        let text = std::fs::read_to_string(repo(readme)).unwrap();
        assert!(
            text.contains(&format!(
                "cargo install --locked dataferry --version {version}\n"
            )),
            "{readme} installs dataferry {version}"
        );
    }
}
