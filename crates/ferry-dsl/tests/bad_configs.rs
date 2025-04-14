//! Every case in `examples/bad` fails the way its manifest row says: the
//! first error points at the documented file, line and field path.

mod common;

use std::collections::BTreeMap;

use common::{manifest, repo};
use ferry_dsl::config::{ConfigErrors, load};

/// Tickets whose checks are in place. Rows owned by others are skipped.
const CHECKED: &[&str] = &["B1a"];

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
