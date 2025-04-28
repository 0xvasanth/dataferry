//! Every case in `examples/bad` fails the way its manifest row says: the
//! first error points at the documented file, line and field path, and the
//! full rendered errors are pinned by snapshots.

mod common;

use std::collections::BTreeMap;

use common::{manifest, repo};
use ferry_dsl::config::{ConfigErrors, load};

/// Tickets whose checks are in place. Rows owned by others are skipped.
const CHECKED: &[&str] = &["B1a", "B1b", "B1c", "B1d"];

/// A secret-looking value for the one variable every case sets. It must
/// never show up in an error.
const SECRET: &str = "pa55-w0rd-never-printed";

/// Loads a case and substitutes `${ENV}` from a fixed environment, as
/// `ferry validate` does.
fn load_case(case: &str) -> Result<(), ConfigErrors> {
    let config = load(repo(&format!("examples/bad/{case}/ferry")))?;
    config
        .resolve_with(|name| match name {
            "FERRY_GOLDEN_MONGO_URL" => Some("mongodb://localhost:27017".into()),
            "FERRY_GOLDEN_PG_URL" => Some(format!("postgres://ferry:{SECRET}@db/warehouse")),
            _ => None,
        })
        .map(|_| ())
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
        let errors = load_case(case).unwrap_err();
        let rendered = errors.to_string();
        assert!(!format!("{rendered}{errors:?}").contains(SECRET), "{case}");
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

#[test]
fn every_case_is_checked_here_or_owned_by_the_registry() {
    for row in manifest() {
        let owner = row["Owner"].as_str();
        assert!(
            CHECKED.contains(&owner) || owner == "C1",
            "{}: owner {owner} is neither checked here nor the registry's",
            row["Case"]
        );
    }
}
