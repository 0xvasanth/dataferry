//! `dataferry run`, `dataferry resume` and `dataferry retry --failed` (S2) as real
//! processes: the golden Mongo `orders` into a real Postgres `orders` and
//! `order_items`, compared with `fixtures/golden/expected` row for row.

mod common;

use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Child, Stdio};
use std::time::{Duration, Instant};

use common::{Shop, Warehouse, edit, fixture, golden_copy, golden_ferry, output};
use mongodb::bson::doc;
use serde_json::Value;

const DEAD_LETTERED: &str = "650000000000000000000006";

async fn assert_golden_rows(warehouse: &Warehouse) {
    assert_eq!(
        warehouse.json_rows("public.orders", "id").await,
        fixture("expected/orders.json").as_array().unwrap().clone()
    );
    assert_eq!(
        warehouse
            .json_rows("public.order_items", "order_id, line_no")
            .await,
        fixture("expected/order_items.json")
            .as_array()
            .unwrap()
            .clone()
    );
}

/// Exactly one dead letter, …0006 as a whole (the raw document, its bad
/// item included), written by `run`, for the reason
/// `fixtures/golden/expected/dead_letter.json` gives.
async fn assert_one_dead_letter(warehouse: &Warehouse, run: &str) {
    let letters = fixture("expected/dead_letter.json");
    let [expected] = letters.as_array().unwrap().as_slice() else {
        panic!("one expected dead letter")
    };
    assert_eq!(expected["_id"], DEAD_LETTERED);
    assert_eq!(expected["path"], "items[1].qty");
    let letters = warehouse
        .strings(
            "SELECT concat_ws(' | ', flow, source_id, run, target, error, record->>'_id', \
                    record->'items'->1->>'qty') \
             FROM public._ferry_dlq",
        )
        .await;
    // The compiled plan names the column (`qty`), the element
    // (`items[1]`) and the value.
    assert_eq!(
        letters,
        [format!(
            "orders | [\"{DEAD_LETTERED}\"] | {run} | order_items | order_items.qty = `int(qty)`: \
             int(): `\"three\"` is not an integer (at items[1]) | {DEAD_LETTERED} | \"three\""
        )]
    );
}

/// The report the run wrote, after checking it holds no secret.
fn report(work: &Path, run: &str) -> Value {
    let dir = work.join("runs").join(run);
    let md = std::fs::read_to_string(dir.join("report.md")).unwrap();
    assert!(!md.contains(common::SECRET));
    let json = std::fs::read_to_string(dir.join("report.json")).unwrap();
    assert!(!json.contains(common::SECRET));
    serde_json::from_str(&json).unwrap()
}

fn run_id(stdout: &str) -> String {
    let first = stdout.lines().next().unwrap_or_default();
    first
        .strip_prefix("run ")
        .unwrap_or_else(|| panic!("the first line names the run: {stdout}"))
        .to_owned()
}

#[tokio::test(flavor = "multi_thread")]
async fn ferry_run_moves_the_golden_orders_into_postgres_exactly() {
    let (shop, warehouse) = tokio::join!(Shop::golden(), Warehouse::start());
    let dir = golden_copy();
    let work = dir.path().join(".ferry");

    output(golden_ferry(dir.path(), &shop, &warehouse).arg("validate")).expect(0);
    let out = output(golden_ferry(dir.path(), &shop, &warehouse).arg("run"))
        .expect(0)
        .no_secret();
    let run = run_id(&out.stdout);
    // State is on the flow's sink: exactly-once.
    assert!(
        out.stdout.contains("\n  flow `orders`: TxnWithState\n"),
        "{}",
        out.stdout
    );
    assert!(
        out.stdout.contains(&format!(
            "run {run}: complete; 3 partitions; read 10, dead-lettered 1; wrote order_items 7, orders 8\n"
        )),
        "{}",
        out.stdout
    );
    assert!(
        out.stdout.contains("verification of `warehouse` passed"),
        "{}",
        out.stdout
    );

    assert_golden_rows(&warehouse).await;
    assert_one_dead_letter(&warehouse, &run).await;
    // The done-records are in the warehouse, next to the rows.
    let done = warehouse
        .strings(&format!(
            "SELECT count(*)::text FROM _ferry_partitions WHERE status = 'done' AND run_id = '{run}'"
        ))
        .await;
    assert_eq!(done, ["3"], "three partitions of four documents");

    let report = report(&work, &run);
    assert_eq!(report["status"], "passed", "{report:#}");
    let assertions: Vec<(&str, bool)> = report["assertions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| (a["name"].as_str().unwrap(), a["passed"].as_bool().unwrap()))
        .collect();
    assert_eq!(
        assertions,
        [
            ("order_count", true),
            ("order_total", true),
            ("item_count", true)
        ]
    );
}

/// `dataferry run` as a child process, with its first line (the run id) read.
fn spawn_run(dir: &Path, shop: &Shop, warehouse: &Warehouse) -> (Child, String) {
    let mut child = std::process::Command::new(assert_cmd::cargo::cargo_bin("dataferry"))
        .current_dir(dir)
        .env_clear()
        .env("FERRY_GOLDEN_MONGO_URL", &shop.url)
        .env("FERRY_GOLDEN_PG_URL", &warehouse.url)
        .arg("run")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut first = String::new();
    BufReader::new(child.stdout.as_mut().unwrap())
        .read_line(&mut first)
        .unwrap();
    (child, run_id(&first))
}

#[tokio::test(flavor = "multi_thread")]
async fn a_run_killed_mid_way_resumes_to_exactly_the_golden_rows() {
    let (shop, warehouse) = tokio::join!(Shop::golden(), Warehouse::start());
    let dir = golden_copy();
    let runtime = dir.path().join("ferry/runtime.yaml");
    // Slow enough to kill between partitions: two documents a second, one
    // partition at a time. Throttles aren't in the config hash, so the
    // resume can drop them (0003 §10.5).
    edit(
        &runtime,
        "  workers: 2\n",
        "  workers: 1\n  source_rows_per_sec: 2\n",
    );

    let (mut child, run) = spawn_run(dir.path(), &shop, &warehouse);
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        // The first partition has committed: its orders are visible.
        let committed = warehouse
            .strings(
                "SELECT count(*)::text FROM pg_tables WHERE schemaname = 'public' \
                 AND tablename = 'orders'",
            )
            .await
            == ["1"]
            && warehouse.count("public.orders").await > 0;
        if committed {
            break;
        }
        assert!(Instant::now() < deadline, "no partition committed in time");
        assert!(
            child.try_wait().unwrap().is_none(),
            "the run ended before it was killed"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    child.kill().unwrap(); // SIGKILL: no cleanup, leases stay held
    let status = child.wait().unwrap();
    assert!(!status.success());
    let partial = warehouse.count("public.orders").await;
    assert!(
        (1..8).contains(&partial),
        "killed mid-way: {partial} of 8 orders"
    );

    edit(&runtime, "  source_rows_per_sec: 2\n", "");
    // A changed mapping is refused: the run is pinned to its config.
    let flows = dir.path().join("ferry/flows/orders.yaml");
    edit(&flows, "status: lower(status)", "status: upper(status)");
    output(golden_ferry(dir.path(), &shop, &warehouse).args(["resume", &run]))
        .expect(8)
        .no_secret();
    edit(&flows, "status: upper(status)", "status: lower(status)");

    let out = output(golden_ferry(dir.path(), &shop, &warehouse).args(["resume", &run]))
        .expect(0)
        .no_secret();
    assert!(
        out.stdout.starts_with(&format!("resuming run {run}\n")),
        "{}",
        out.stdout
    );
    assert!(
        out.stdout.contains("verification of `warehouse` passed"),
        "{}",
        out.stdout
    );

    // Every row once: the tables' keys would refuse a duplicate, and the
    // counts match the fixtures exactly.
    assert_golden_rows(&warehouse).await;
    assert_one_dead_letter(&warehouse, &run).await;
    assert_eq!(report(&dir.path().join(".ferry"), &run)["status"], "passed");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_partition_exits_6_and_retry_failed_reruns_it_once_fixed() {
    let (shop, warehouse) = tokio::join!(Shop::golden(), Warehouse::start());
    let dir = golden_copy();
    // `on_error: fail`: …0006's bad item fails its partition.
    edit(
        &dir.path().join("ferry/runtime.yaml"),
        "  on_error: dead_letter\n  max_errors: 10\n",
        "  on_error: fail\n",
    );
    let out = output(golden_ferry(dir.path(), &shop, &warehouse).arg("run"))
        .expect(6)
        .no_secret();
    let run = run_id(&out.stdout);
    assert!(out.stderr.contains(DEAD_LETTERED), "{}", out.stderr);
    assert!(
        out.stderr
            .contains(&format!("`dataferry retry --failed {run}`")),
        "{}",
        out.stderr
    );
    // The failed partition wrote nothing; the other two are in.
    let orders = warehouse.count("public.orders").await;
    assert!(orders < 8, "{orders}");

    // Fix the document at the source, then re-run only what failed.
    shop.orders()
        .update_one(
            doc! { "_id": mongodb::bson::oid::ObjectId::parse_str(DEAD_LETTERED).unwrap() },
            doc! { "$set": { "items.1.qty": 3 } },
        )
        .await
        .unwrap();
    let out = output(golden_ferry(dir.path(), &shop, &warehouse).args(["retry", "--failed", &run]))
        .expect(0)
        .no_secret();
    assert!(
        out.stdout.contains("verification of `warehouse` passed"),
        "{}",
        out.stdout
    );
    let ids = warehouse
        .strings("SELECT id FROM public.orders ORDER BY id")
        .await;
    assert_eq!(ids.len(), 9);
    assert!(ids.contains(&DEAD_LETTERED.to_owned()));
}

#[tokio::test(flavor = "multi_thread")]
async fn an_expectation_that_fails_exits_7_with_the_data_written() {
    let (shop, warehouse) = tokio::join!(Shop::golden(), Warehouse::start());
    let dir = golden_copy();
    edit(
        &dir.path().join("ferry/flows/orders.yaml"),
        "order_count: count(orders) == source.count()",
        "order_count: count(orders) == source.count() + 1",
    );
    let out = output(golden_ferry(dir.path(), &shop, &warehouse).arg("run"))
        .expect(7)
        .no_secret();
    let run = run_id(&out.stdout);
    assert!(
        out.stdout.contains("verification of `warehouse` failed"),
        "{}",
        out.stdout
    );
    assert!(out.stderr.contains("order_count"), "{}", out.stderr);
    assert_golden_rows(&warehouse).await;
    let report = report(&dir.path().join(".ferry"), &run);
    assert_eq!(report["status"], "failed");
}

#[tokio::test(flavor = "multi_thread")]
async fn ctrl_c_stops_a_run_with_exit_130_and_says_how_to_resume() {
    let (shop, warehouse) = tokio::join!(Shop::golden(), Warehouse::start());
    let dir = golden_copy();
    edit(
        &dir.path().join("ferry/runtime.yaml"),
        "  workers: 2\n",
        "  workers: 1\n  source_rows_per_sec: 1\n",
    );
    let mut child = std::process::Command::new(assert_cmd::cargo::cargo_bin("dataferry"))
        .current_dir(dir.path())
        .env_clear()
        .env("FERRY_GOLDEN_MONGO_URL", &shop.url)
        .env("FERRY_GOLDEN_PG_URL", &warehouse.url)
        .arg("run")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut first = String::new();
    BufReader::new(child.stdout.as_mut().unwrap())
        .read_line(&mut first)
        .unwrap();
    let run = run_id(&first);
    // SIGINT, as a terminal sends on Ctrl-C.
    let kill = std::process::Command::new("kill")
        .args(["-INT", &child.id().to_string()])
        .status()
        .unwrap();
    assert!(kill.success());
    let out = child.wait_with_output().unwrap();
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert_eq!(out.status.code(), Some(130), "{stderr}");
    assert!(
        stderr.contains(&format!("continue it with `dataferry resume {run}`")),
        "{stderr}"
    );
    assert!(!stderr.contains(common::SECRET));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_run_into_two_sinks_verifies_each_sink_on_its_own() {
    let (shop, warehouse) = tokio::join!(Shop::golden(), Warehouse::start());
    let dir = golden_copy();
    // A second Postgres connection, another schema of the same database.
    edit(
        &dir.path().join("ferry/connections.yaml"),
        "    url: ${FERRY_GOLDEN_PG_URL}\n",
        "    url: ${FERRY_GOLDEN_PG_URL}\n\n  - name: reporting\n    type: postgres\n    url: ${FERRY_GOLDEN_PG_URL}\n    schema: reporting\n",
    );
    std::fs::write(
        dir.path().join("ferry/flows/report.yaml"),
        "flows:\n  - name: statuses\n    from: shop.orders\n    sink: reporting\n    to:\n      order_status:\n        key: [id]\n        columns:\n          id: string(_id)\n          status: lower(status)\n    expect:\n      every_order: count(order_status) == source.count()\n",
    )
    .unwrap();
    let out = output(golden_ferry(dir.path(), &shop, &warehouse).arg("run"))
        .expect(0)
        .no_secret();
    let run = run_id(&out.stdout);
    // The second sink isn't the state connection: it falls back.
    assert!(
        out.stdout.contains(
            "  flow `statuses`: DeletePartitionThenInsert (the state store is on \"warehouse\", \
             not \"reporting\")"
        ),
        "{}",
        out.stdout
    );
    // Each sink is verified against its own flows' share of the run.
    let reports = dir.path().join(".ferry/runs").join(&run);
    for sink in ["reporting", "warehouse"] {
        let line = format!(
            "verification of `{sink}` passed; report: {}\n",
            Path::new(".")
                .join(".ferry/runs")
                .join(&run)
                .join(format!("report-{sink}.md"))
                .display()
        );
        assert!(out.stdout.contains(&line), "{line}\n{}", out.stdout);
        let json: Value = serde_json::from_str(
            &std::fs::read_to_string(reports.join(format!("report-{sink}.json"))).unwrap(),
        )
        .unwrap();
        assert_eq!(json["status"], "passed", "{json:#}");
    }
    assert!(!out.stderr.contains("W0701"), "{}", out.stderr);
    assert!(!reports.join("report.json").exists());

    let warehouse_report: Value = serde_json::from_str(
        &std::fs::read_to_string(reports.join("report-warehouse.json")).unwrap(),
    )
    .unwrap();
    // 10 golden documents per flow, not the run's 20; …0006 is the
    // warehouse's one dead letter.
    assert_eq!(warehouse_report["counts"]["read"], 10);
    assert_eq!(warehouse_report["counts"]["dead_lettered"], 1);
    assert_eq!(warehouse_report["dead_letter"]["rows"], 1);
    let reporting_report: Value = serde_json::from_str(
        &std::fs::read_to_string(reports.join("report-reporting.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(reporting_report["counts"]["read"], 10);
    assert_eq!(reporting_report["counts"]["dead_lettered"], 0);
    assert_eq!(
        reporting_report["counts"]["written"],
        serde_json::json!({"order_status": 10})
    );
    // Each report holds its own flows' assertions only.
    let names = |report: &Value| -> Vec<String> {
        report["assertions"]
            .as_array()
            .unwrap()
            .iter()
            .map(|a| a["name"].as_str().unwrap().to_owned())
            .collect()
    };
    assert_eq!(names(&reporting_report), ["every_order"]);
    assert_eq!(
        names(&warehouse_report),
        ["order_count", "order_total", "item_count"]
    );

    assert_golden_rows(&warehouse).await;
    // …0004 is filtered by `orders` only; `statuses` has no `where:`, and
    // …0006's items don't matter to it.
    assert_eq!(warehouse.count("reporting.order_status").await, 10);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_sink_password_given_apart_from_its_url_is_used_to_verify_too() {
    let (shop, warehouse) = tokio::join!(Shop::golden(), Warehouse::start());
    let dir = golden_copy();
    edit(
        &dir.path().join("ferry/connections.yaml"),
        "    url: ${FERRY_GOLDEN_PG_URL}\n",
        "    url: ${FERRY_GOLDEN_PG_URL}\n    password: ${FERRY_GOLDEN_PG_PASSWORD}\n",
    );
    let out = output(
        common::ferry(dir.path())
            .env("FERRY_GOLDEN_MONGO_URL", &shop.url)
            // No password in the URL: only the `password:` field has it.
            .env(
                "FERRY_GOLDEN_PG_URL",
                format!("postgres://postgres@127.0.0.1:{}/postgres", warehouse.port),
            )
            .env("FERRY_GOLDEN_PG_PASSWORD", common::SECRET)
            .arg("run"),
    )
    .expect(0)
    .no_secret();
    assert!(
        out.stdout.contains("verification of `warehouse` passed"),
        "{}",
        out.stdout
    );
    let report = report(&dir.path().join(".ferry"), &run_id(&out.stdout));
    assert_eq!(report["status"], "passed");
    assert_golden_rows(&warehouse).await;
}

/// Live leases of `run`: leased partitions whose lease hasn't expired.
async fn live_leases(warehouse: &Warehouse, run: &str) -> i64 {
    warehouse
        .client
        .query_one(
            "SELECT count(*) FROM _ferry_partitions \
             WHERE run_id = $1 AND status = 'leased' AND expires_at > now()",
            &[&run],
        )
        .await
        .unwrap()
        .get(0)
}

#[tokio::test(flavor = "multi_thread")]
async fn ctrl_c_releases_the_runs_leases_so_resume_starts_at_once() {
    let (shop, warehouse) = tokio::join!(Shop::golden(), Warehouse::start());
    let dir = golden_copy();
    edit(
        &dir.path().join("ferry/runtime.yaml"),
        "  workers: 2\n",
        "  workers: 2\n  source_rows_per_sec: 1\n",
    );
    let mut child = std::process::Command::new(assert_cmd::cargo::cargo_bin("dataferry"))
        .current_dir(dir.path())
        .env_clear()
        .env("FERRY_GOLDEN_MONGO_URL", &shop.url)
        .env("FERRY_GOLDEN_PG_URL", &warehouse.url)
        .arg("run")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut first = String::new();
    BufReader::new(child.stdout.as_mut().unwrap())
        .read_line(&mut first)
        .unwrap();
    let run = run_id(&first);
    // Wait until both workers hold a partition.
    let deadline = Instant::now() + Duration::from_secs(60);
    while live_leases(&warehouse, &run).await < 2 {
        assert!(Instant::now() < deadline, "no partition was leased");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let kill = std::process::Command::new("kill")
        .args(["-INT", &child.id().to_string()])
        .status()
        .unwrap();
    assert!(kill.success());
    let status = child.wait().unwrap();
    let mut stderr = String::new();
    std::io::Read::read_to_string(child.stderr.as_mut().unwrap(), &mut stderr).unwrap();
    assert_eq!(status.code(), Some(130), "{stderr}");
    assert!(
        stderr.contains(&format!(
            "interrupted: in-flight partitions of run `{run}` were abandoned, and 2 leases \
             were released"
        )),
        "{stderr}"
    );
    // Nothing is left leased: the leases' 30 s don't hold up a resume.
    assert_eq!(live_leases(&warehouse, &run).await, 0);

    edit(
        &dir.path().join("ferry/runtime.yaml"),
        "  source_rows_per_sec: 1\n",
        "",
    );
    let started = Instant::now();
    output(
        golden_ferry(dir.path(), &shop, &warehouse)
            .arg("resume")
            .arg(&run),
    )
    .expect(0)
    .no_secret();
    assert!(
        started.elapsed() < Duration::from_secs(20),
        "resume waited {:?}",
        started.elapsed()
    );
    assert_golden_rows(&warehouse).await;
    assert_one_dead_letter(&warehouse, &run).await;
}
