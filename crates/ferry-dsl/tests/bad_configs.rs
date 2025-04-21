//! Every case in `examples/bad` fails the way its manifest row says: the
//! first error points at the documented file, line and field path, and the
//! full rendered errors are pinned by snapshots.

mod common;

use std::collections::BTreeMap;

use common::{manifest, repo};
use ferry_dsl::config::{ConfigErrors, load};

/// Tickets whose checks are in place. Rows owned by others are skipped.
const CHECKED: &[&str] = &["B1a", "B1b"];

fn load_case(case: &str) -> Result<(), ConfigErrors> {
    load(repo(&format!("examples/bad/{case}/ferry"))).map(|_| ())
}

fn checked_rows() -> Vec<BTreeMap<String, String>> {
    manifest()
        .into_iter()
        .filter(|row| CHECKED.contains(&row["Owner"].as_str()))
        .collect()
}

#[test]
fn every_checked_case_fails_where_the_manifest_says() {
    let rows = checked_rows();
    assert!(!rows.is_empty());
    for row in rows {
        let case = &row["Case"];
        let errors = match load_case(case) {
            Ok(()) => panic!("{case}: loaded without error"),
            Err(errors) => errors,
        };
        let first = &errors.0[0];
        let location = first
            .location
            .as_ref()
            .unwrap_or_else(|| panic!("{case}: error has no location: {first}"));
        assert_eq!(
            format!("{}:{}", location.file, location.line),
            row["Error at"],
            "{case}: {errors}"
        );
        assert_eq!(first.path, row["Field path"], "{case}: {errors}");
    }
}

#[test]
fn rendered_errors_match_their_snapshots() {
    for row in checked_rows() {
        let case = &row["Case"];
        let rendered = load_case(case).unwrap_err().to_string();
        insta::with_settings!({ snapshot_suffix => case.as_str(), prepend_module_to_snapshot => false }, {
            insta::assert_snapshot!("bad", rendered);
        });
    }
}

#[test]
fn cases_owned_by_the_registry_load_here() {
    // ferry-dsl can't know which types exist or which are state-capable;
    // `ferry validate` (C1) rejects these through the registry.
    for row in manifest().into_iter().filter(|r| r["Owner"] == "C1") {
        let case = &row["Case"];
        if let Err(e) = load_case(case) {
            panic!("{case} should load in ferry-dsl: {e}");
        }
    }
}
