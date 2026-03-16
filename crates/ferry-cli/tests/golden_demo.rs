//! The golden demo (S3): `examples/golden/demo.sh` run as a user runs it,
//! on the golden Mongo and an empty Postgres, and the report it shows; and
//! a deliberately broken copy of the golden mapping, caught by its
//! `expect:` assertion and shown in the report with both values.

mod common;

use std::process::Stdio;

use common::{SECRET, Shop, Warehouse, edit, golden_copy, golden_ferry, output, repo};
use serde_json::Value;

/// The report's section headings, in order.
const SECTIONS: [&str; 6] = [
    "## Run",
    "## Counts",
    "## Tables",
    "## Constraints",
    "## Assertions",
    "## Dead letters",
];

/// `report.md`'s lines that start with `#`.
fn headings(md: &str) -> Vec<&str> {
    md.lines().filter(|l| l.starts_with('#')).collect()
}

/// The table row whose first cells are `cells`, e.g. `["orders"]`.
fn row<'a>(md: &'a str, cells: &[&str]) -> &'a str {
    let prefix = format!("| {} |", cells.join(" | "));
    let rows: Vec<&str> = md.lines().filter(|l| l.starts_with(&prefix)).collect();
    match rows[..] {
        [row] => row,
        _ => panic!("expected one row starting {prefix:?}, found {rows:?}\n{md}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_golden_demo_runs_end_to_end_and_shows_a_readable_green_report() {
    let (shop, warehouse) = tokio::join!(Shop::golden(), Warehouse::start());
    let work = tempfile::tempdir().unwrap();
    // The databases are given, so the script starts no container.
    let out = std::process::Command::new("bash")
        .arg(repo("examples/golden/demo.sh"))
        .env_clear()
        .env("PATH", std::env::var("PATH").unwrap())
        .env("FERRY", env!("CARGO_BIN_EXE_ferry"))
        .env("FERRY_GOLDEN_MONGO_URL", &shop.url)
        .env("FERRY_GOLDEN_PG_URL", &warehouse.url)
        .env("FERRY_WORK_DIR", work.path())
        .stdin(Stdio::null())
        .output()
        .unwrap();
    let stdout = String::from_utf8(out.stdout).unwrap();
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert!(out.status.success(), "{}\n{stdout}\n{stderr}", out.status);
    assert!(!stdout.contains(SECRET) && !stderr.contains(SECRET));

    // validate, plan and run, in that order, each saying what it did.
    let steps: Vec<&str> = stdout.lines().filter(|l| l.starts_with("==> ")).collect();
    assert_eq!(
        steps.iter().map(|s| &s[4..14]).collect::<Vec<_>>(),
        ["ferry vali", "ferry plan", "ferry run:", "report.md "],
        "{stdout}"
    );
    assert!(
        stdout.contains("write order: orders → order_items"),
        "{stdout}"
    );
    // `ferry run` prints the run id first.
    let mut lines = stdout
        .lines()
        .skip_while(|l| !l.starts_with("==> ferry run:"));
    let run = lines
        .nth(1)
        .and_then(|l| l.strip_prefix("run "))
        .unwrap_or_else(|| panic!("no run id: {stdout}"));
    assert!(
        stdout.contains(&format!(
            "run {run}: complete; 3 partitions; read 10, dead-lettered 1; wrote order_items 7, \
             orders 8\n"
        )),
        "{stdout}"
    );

    // The report the script shows is the one the run wrote.
    let md = std::fs::read_to_string(work.path().join("runs").join(run).join("report.md")).unwrap();
    assert!(
        stdout.ends_with(&md),
        "the script prints report.md last\n{stdout}"
    );
    assert!(!md.contains(SECRET));
    assert!(!md.contains(&warehouse.url) && !md.contains(&shop.url));

    // Readable: a title saying it passed, the sections, short lines of text
    // and tables rather than JSON.
    let headings = headings(&md);
    assert_eq!(
        headings[0],
        format!("# Run {run}: verification PASSED"),
        "{md}"
    );
    assert_eq!(headings[1..], SECTIONS, "{md}");
    assert!(!md.contains('{'), "no JSON in the report\n{md}");
    assert!(md.contains("- Connection `warehouse`: postgres\n"), "{md}");
    assert!(md.contains("- Partitions: 3 done, 0 failed, 0 pending, 0 leased\n"));
    // Per flow and per table counts, each table's checksum read back.
    assert_eq!(
        row(&md, &["orders", "10"]),
        "| orders | 10 | 8 | 1 | 1 | 0 |"
    );
    assert_eq!(
        row(&md, &["orders", "8"]),
        "| orders | 8 | 8 | 8 | matches | pass |"
    );
    assert_eq!(
        row(&md, &["order_items", "7"]),
        "| order_items | 7 | 7 | 7 | matches | pass |"
    );
    for (name, values) in [
        ("order_count", "8 == 8"),
        ("order_total", "5326 == 5326"),
        ("item_count", "7 == 7"),
    ] {
        let row = row(&md, &["orders", name]);
        assert!(row.ends_with(&format!(" | {values} | pass |")), "{row}");
    }
    assert!(!md.contains("**fail**"), "{md}");
    // The dead letter, with the reason a person can act on.
    let letter = row(&md, &["orders", "order_items"]);
    assert!(
        letter.contains("`\"three\"` is not an integer (at items[1])"),
        "{letter}"
    );
    assert!(
        letter.ends_with("| 1 | `[\"650000000000000000000006\"]` |"),
        "{letter}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_broken_mapping_is_caught_by_its_assertion_and_shown_with_both_values() {
    let (shop, warehouse) = tokio::join!(Shop::golden(), Warehouse::start());
    let dir = golden_copy();
    // Every total lands ten times too large.
    edit(
        &dir.path().join("ferry/flows/orders.yaml"),
        "total_cents: int(round(total * 100))",
        "total_cents: int(round(total * 100)) * 10",
    );
    let out = output(golden_ferry(dir.path(), &shop, &warehouse).arg("run"))
        .expect(7)
        .no_secret();
    let run = out
        .stdout
        .lines()
        .next()
        .unwrap()
        .strip_prefix("run ")
        .unwrap();
    assert!(
        out.stdout
            .contains("verification of `warehouse` failed (1 problems)"),
        "{}",
        out.stdout
    );
    assert!(out.stderr.contains("order_total"), "{}", out.stderr);

    // The data was written, wrong as the mapping made it.
    let totals = warehouse
        .strings("SELECT sum(total_cents)::text FROM public.orders")
        .await;
    assert_eq!(totals, ["53260"]);

    let reports = dir.path().join(".ferry/runs").join(run);
    let md = std::fs::read_to_string(reports.join("report.md")).unwrap();
    assert!(!md.contains(SECRET));
    assert_eq!(
        headings(&md)[0],
        format!("# Run {run}: verification FAILED"),
        "{md}"
    );
    // Both values, and the assertion that compared them.
    assert_eq!(
        row(&md, &["orders", "order_total"]),
        "| orders | order_total | `sum(orders.total_cents) == source.sum(int(round(total * 100)))` \
         | 53260 == 5326 | **fail** |"
    );
    // The rest holds: the wrong values landed exactly as computed, which is
    // all verification can prove, and the other assertions pass.
    assert!(row(&md, &["orders", "order_count"]).ends_with(" | 8 == 8 | pass |"));
    assert!(row(&md, &["orders", "item_count"]).ends_with(" | 7 == 7 | pass |"));
    assert_eq!(
        row(&md, &["orders", "8"]),
        "| orders | 8 | 8 | 8 | matches | pass |"
    );
    assert_eq!(md.matches("**fail**").count(), 1, "{md}");
    let problems = section(&md, "## Problems");
    assert!(problems.contains("order_total"), "{problems}");

    let json: Value =
        serde_json::from_str(&std::fs::read_to_string(reports.join("report.json")).unwrap())
            .unwrap();
    assert_eq!(json["status"], "failed");
    let failed: Vec<&Value> = json["assertions"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|a| a["passed"] == false)
        .collect();
    assert_eq!(failed.len(), 1, "{json:#}");
    assert_eq!(failed[0]["name"], "order_total");
}

/// The text of the `## ` section `heading`, up to the next one.
fn section<'a>(md: &'a str, heading: &str) -> &'a str {
    let start = md
        .find(&format!("{heading}\n"))
        .unwrap_or_else(|| panic!("no {heading}\n{md}"));
    let rest = &md[start + heading.len()..];
    &rest[..rest.find("\n## ").unwrap_or(rest.len())]
}
