//! `examples/pg-merge` end to end through the real `dataferry` binary (S3): a
//! Postgres `app` seeded from `examples/pg-merge/source.sql`, merged into
//! a second, empty Postgres `reporting`. The flows name their tables
//! `app.public.<table>`, which Postgres discovers as `<table>`; the merge
//! stages its `one` join and co-partitions its pinned `many` join (B6).

mod common;

use std::path::Path;

use common::{Warehouse, copy_dir, edit, output, repo};
use serde_json::{Value, json};

/// `app` holds `source.sql`; `reporting` is empty.
async fn databases() -> (Warehouse, Warehouse) {
    let (app, reporting) = tokio::join!(Warehouse::start(), Warehouse::start());
    let seed = std::fs::read_to_string(repo("examples/pg-merge/source.sql")).unwrap();
    app.client.batch_execute(&seed).await.unwrap();
    (app, reporting)
}

/// `dataferry` with the example's variables pointing at `app` and `reporting`.
fn ferry(cwd: &Path, app: &Warehouse, reporting: &Warehouse) -> assert_cmd::Command {
    let mut cmd = common::ferry(cwd);
    cmd.env("APP_DATABASE_URL", &app.url)
        .env("REPORTING_DATABASE_URL", &reporting.url);
    cmd
}

/// A copy of `examples/pg-merge/ferry`, to run from (the run writes its
/// work directory next to the config).
fn example() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    copy_dir(&repo("examples/pg-merge/ferry"), &dir.path().join("ferry"));
    dir
}

fn normalized(text: &str, app: &Warehouse, reporting: &Warehouse) -> String {
    text.replace(&format!(":{}/", app.port), ":<app-port>/")
        .replace(&format!(":{}/", reporting.port), ":<reporting-port>/")
}

#[tokio::test(flavor = "multi_thread")]
async fn plan_shows_the_merge_strategy_of_each_join_as_a_stable_snapshot() {
    let (app, reporting) = databases().await;
    let before = reporting.catalog().await;
    let out = output(
        ferry(&repo("examples/pg-merge"), &app, &reporting)
            .arg("plan")
            .arg("--config")
            .arg("ferry"),
    )
    .expect(0)
    .no_secret();
    insta::assert_snapshot!("pg_merge_plan", normalized(&out.stdout, &app, &reporting));
    // Planning a merge writes nothing.
    assert_eq!(reporting.catalog().await, before);
}

/// `reporting.order_summaries`, one row per order, as JSON.
async fn summaries(reporting: &Warehouse) -> Vec<Value> {
    reporting
        .json_rows("public.order_summaries", "order_id")
        .await
}

fn expected_summaries() -> Vec<Value> {
    vec![
        // Items sorted by `line_no`, though inserted out of order.
        json!({
            "order_id": 10, "customer_id": 1, "customer_name": "Ana", "status": "paid",
            "placed_at": "2026-03-01T10:00:00.000000Z", "total_cents": 2550,
            "items": [
                {"sku": "SKU-1", "qty": 2, "unit_cents": 1000},
                {"sku": "SKU-2", "qty": 1, "unit_cents": 550},
            ],
        }),
        // 11 is cancelled: filtered. 12 has no items: `[]`, not null.
        json!({
            "order_id": 12, "customer_id": 2, "customer_name": "Bo", "status": "paid",
            "placed_at": "2026-03-03T10:00:00.000000Z", "total_cents": 0,
            "items": [],
        }),
        // 13 has no customer, and its qty-0 item is dropped by the nest's
        // `where:`.
        json!({
            "order_id": 13, "customer_id": null, "customer_name": null, "status": "pending",
            "placed_at": "2026-03-04T10:00:00.000000Z", "total_cents": 300,
            "items": [{"sku": "SKU-6", "qty": 3, "unit_cents": 100}],
        }),
    ]
}

fn run_id(stdout: &str) -> String {
    stdout
        .lines()
        .next()
        .and_then(|l| l.strip_prefix("run "))
        .unwrap_or_else(|| panic!("the first line names the run: {stdout}"))
        .to_owned()
}

#[tokio::test(flavor = "multi_thread")]
async fn ferry_run_merges_the_example_into_reporting_and_verifies_it() {
    let (app, reporting) = databases().await;
    let dir = example();
    output(ferry(dir.path(), &app, &reporting).arg("validate"))
        .expect(0)
        .no_secret();
    let out = output(ferry(dir.path(), &app, &reporting).arg("run"))
        .expect(0)
        .no_secret();
    let run = run_id(&out.stdout);
    assert!(
        out.stdout.contains("  flow `customers`: TxnWithState\n")
            && out
                .stdout
                .contains("  flow `order_summaries`: TxnWithState\n"),
        "{}",
        out.stdout
    );
    assert!(
        out.stdout
            .contains("; read 6, dead-lettered 0; wrote customers 2, order_summaries 3\n"),
        "{}",
        out.stdout
    );
    assert!(
        out.stdout
            .contains("verification of `reporting` passed; report: "),
        "{}",
        out.stdout
    );
    assert!(!out.stderr.contains("W0701"), "{}", out.stderr);

    assert_eq!(summaries(&reporting).await, expected_summaries());
    assert_eq!(
        reporting.json_rows("public.customers", "id").await,
        [
            json!({"id": 1, "name": "Ana", "email": "ana@example.dev"}),
            json!({"id": 2, "name": "Bo", "email": null}),
        ]
    );
    // `items` is stored as jsonb.
    assert_eq!(
        reporting
            .strings(
                "SELECT data_type::text FROM information_schema.columns \
                 WHERE table_name = 'order_summaries' AND column_name = 'items'"
            )
            .await,
        ["jsonb"]
    );

    // The report: every table and both assertions, green.
    let report_dir = dir.path().join(".ferry/runs").join(&run);
    let md = std::fs::read_to_string(report_dir.join("report.md")).unwrap();
    assert!(
        md.starts_with(&format!("# Run {run}: verification PASSED")),
        "{md}"
    );
    assert!(!md.contains(common::SECRET));
    let report: Value =
        serde_json::from_str(&std::fs::read_to_string(report_dir.join("report.json")).unwrap())
            .unwrap();
    assert_eq!(report["status"], "passed", "{report:#}");
    let tables: Vec<(&str, bool)> = report["tables"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| (t["table"].as_str().unwrap(), t["passed"].as_bool().unwrap()))
        .collect();
    assert_eq!(tables, [("customers", true), ("order_summaries", true)]);
    let assertions: Vec<(&str, &Value, &Value)> = report["assertions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| {
            (
                a["name"].as_str().unwrap(),
                &a["outcome"]["left"],
                &a["passed"],
            )
        })
        .collect();
    assert_eq!(
        assertions,
        [
            (
                "one_row_per_order",
                &json!({"unscaled": "3", "scale": 0}),
                &json!(true)
            ),
            (
                "totals_match",
                &json!({"unscaled": "2850", "scale": 0}),
                &json!(true)
            ),
        ]
    );
    // 4 orders read for the merge: 3 written, 1 filtered.
    let merge = report["counts"]["flows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|f| f["flow"] == "order_summaries")
        .unwrap();
    assert_eq!(merge["read"], 4);
    assert_eq!(
        merge["records"],
        json!({"written": 3, "filtered": 1, "rejected": 0})
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_duplicate_customer_fails_the_run_naming_the_join_under_on_error_fail() {
    let (app, reporting) = databases().await;
    // A second customer 2: under the source's primary key on `id` that
    // can't happen, so the key moves to a surrogate column.
    app.client
        .batch_execute(
            "ALTER TABLE orders DROP CONSTRAINT orders_customer_id_fkey; \
             ALTER TABLE customers DROP CONSTRAINT customers_pkey; \
             ALTER TABLE customers ADD COLUMN row_id serial PRIMARY KEY; \
             INSERT INTO customers (id, name) VALUES (2, 'Bo again');",
        )
        .await
        .unwrap();
    let dir = example();
    // `customers` is copied by key `id`, which now repeats: copy it by
    // `row_id` instead.
    edit(
        &dir.path().join("ferry/legacy/customers.yaml"),
        "key: [id]",
        "key: [row_id]",
    );
    let out = output(ferry(dir.path(), &app, &reporting).arg("run"))
        .expect(6)
        .no_secret();
    assert!(
        out.stderr.contains(
            "join `c` is `cardinality: one`, but 2 rows of `app.public.customers` match id = 2"
        ),
        "{}",
        out.stderr
    );
}
