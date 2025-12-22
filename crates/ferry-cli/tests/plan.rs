//! `ferry plan` (B7) as a real process, against a real Mongo (the golden
//! documents) and a real, empty Postgres.

mod common;

use std::path::Path;

use common::{Shop, Warehouse, edit, ferry, golden_copy, golden_ferry, output, repo};

/// The plan text with what changes from one container to the next (ports)
/// replaced, so it can be snapshotted.
fn normalized(text: &str, shop: &Shop, warehouse: &Warehouse) -> String {
    let mongo_port = shop.url.rsplit(':').next().unwrap();
    text.replace(&format!(":{mongo_port}"), ":<mongo-port>")
        .replace(&format!(":{}", warehouse.port), ":<pg-port>")
}

#[tokio::test(flavor = "multi_thread")]
async fn plan_on_the_golden_config_is_a_stable_snapshot_that_hides_the_secret() {
    let (shop, warehouse) = tokio::join!(Shop::golden(), Warehouse::start());
    let out = output(golden_ferry(&repo(""), &shop, &warehouse).args([
        "plan",
        "--config",
        "examples/golden/ferry",
    ]))
    .expect(0)
    .no_secret();
    insta::assert_snapshot!("golden_plan", normalized(&out.stdout, &shop, &warehouse));
    assert_eq!(out.stderr, "", "the golden config plans without warnings");

    // The same flow in another file, in a nested directory with another
    // extension, plans the same.
    let moved = golden_copy();
    let flows = moved.path().join("ferry/flows");
    std::fs::create_dir_all(flows.join("shop/2026")).unwrap();
    std::fs::rename(flows.join("orders.yaml"), flows.join("shop/2026/all.yml")).unwrap();
    let again = output(golden_ferry(moved.path(), &shop, &warehouse).arg("plan"))
        .expect(0)
        .no_secret();
    assert_eq!(again.stdout, out.stdout);
}

/// Two flows besides the golden one, written in `order` across `files`.
fn three_flows(dir: &Path, layout: &[(&str, &[&str])]) {
    let flows = dir.join("ferry/flows");
    std::fs::remove_dir_all(&flows).unwrap();
    let golden = std::fs::read_to_string(repo("examples/golden/ferry/flows/orders.yaml")).unwrap();
    let golden = golden.strip_prefix("flows:\n").unwrap().to_owned();
    let audit = "  - name: audit\n    from: shop.orders\n    sink: warehouse\n    depends_on: [orders]\n    to:\n      order_audit:\n        key: [id]\n        columns:\n          id: string(_id)\n          status: status\n";
    let copy = "  - name: copy\n    from: shop.orders\n    sink: warehouse\n    to:\n      order_copy:\n        key: [id]\n        columns:\n          id: string(_id)\n";
    for (file, names) in layout {
        let mut text = String::from("flows:\n");
        for name in *names {
            text.push_str(match *name {
                "orders" => &golden,
                "audit" => audit,
                "copy" => copy,
                other => panic!("{other}"),
            });
        }
        let path = flows.join(file);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, text).unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn plan_output_does_not_depend_on_files_or_the_order_flows_are_written_in() {
    let (shop, warehouse) = tokio::join!(Shop::golden(), Warehouse::start());
    let one = golden_copy();
    three_flows(one.path(), &[("all.yaml", &["orders", "audit", "copy"])]);
    let other = golden_copy();
    three_flows(
        other.path(),
        &[
            ("z/copy.yml", &["copy"]),
            ("a.yaml", &["audit"]),
            ("m/n/orders.yaml", &["orders"]),
        ],
    );
    let first = output(golden_ferry(one.path(), &shop, &warehouse).arg("plan")).expect(0);
    let second = output(golden_ferry(other.path(), &shop, &warehouse).arg("plan")).expect(0);
    assert_eq!(first.stdout, second.stdout);
    // `audit` runs after `orders`, which it depends on; `copy` waits for
    // nothing, and ties go by name.
    let order: Vec<&str> = first
        .stdout
        .lines()
        .skip_while(|l| *l != "run order")
        .skip(1)
        .take_while(|l| !l.is_empty())
        .collect();
    assert_eq!(
        order,
        [
            "  1. copy",
            "  2. orders",
            "  3. audit, after `orders` (declared)"
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn plan_against_a_real_postgres_leaves_it_unchanged() {
    let (shop, warehouse) = tokio::join!(Shop::golden(), Warehouse::start());
    // A table already there, as on a database a run has written to.
    warehouse
        .client
        .batch_execute("CREATE TABLE public.legacy (id int8 PRIMARY KEY, note text)")
        .await
        .unwrap();
    let before = warehouse.catalog().await;
    assert!(before.contains(&"schema public".to_owned()), "{before:#?}");

    let out = output(golden_ferry(&repo(""), &shop, &warehouse).args([
        "plan",
        "--config",
        "examples/golden/ferry",
    ]))
    .expect(0);
    assert!(
        out.stdout.contains("CREATE TABLE \"public\".\"orders\""),
        "{}",
        out.stdout
    );

    // No DDL ran: not the targets', not the dead-letter table's, not the
    // state store's (it lives on this connection).
    assert_eq!(warehouse.catalog().await, before);
    // Nor was any row written anywhere.
    let written = warehouse
        .strings(
            "SELECT count(*)::text FROM pg_stat_user_tables \
             WHERE n_tup_ins + n_tup_upd + n_tup_del > 0",
        )
        .await;
    assert_eq!(written, ["0"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cycle_through_depends_on_and_a_read_refuses_the_plan_naming_it() {
    let (shop, warehouse) = tokio::join!(Shop::golden(), Warehouse::start());
    // `report` reads the table `copy` writes; `copy` waits for `report`.
    warehouse
        .client
        .batch_execute("CREATE TABLE public.order_copy (id text PRIMARY KEY)")
        .await
        .unwrap();
    let dir = golden_copy();
    std::fs::write(
        dir.path().join("ferry/flows/more.yaml"),
        "flows:\n  - name: copy\n    from: shop.orders\n    sink: warehouse\n    depends_on: [report]\n    to:\n      order_copy:\n        key: [id]\n        columns:\n          id: string(_id)\n  - name: report\n    from: warehouse.order_copy\n    sink: warehouse\n    to:\n      order_report:\n        key: [id]\n        columns:\n          id: id\n",
    )
    .unwrap();

    // `validate` sees no cycle: it can't know what `report` reads.
    output(golden_ferry(dir.path(), &shop, &warehouse).arg("validate")).expect(0);
    let out = output(golden_ferry(dir.path(), &shop, &warehouse).arg("plan"))
        .expect(4)
        .no_secret();
    assert_eq!(out.stdout, "");
    assert!(
        out.stderr.starts_with(
            "error[F0112]: flows[copy].depends_on[0]: flows wait for each other in a cycle: \
             copy → report → copy (`copy` depends on `report`; `report` reads \
             `warehouse.order_copy`, which `copy` writes)\n --> ferry/flows/more.yaml:5:18\n"
        ),
        "{}",
        out.stderr
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_flow_off_the_state_connection_plans_its_fallback_and_why() {
    let (shop, warehouse) = tokio::join!(Shop::golden(), Warehouse::start());
    let dir = golden_copy();
    edit(
        &dir.path().join("ferry/runtime.yaml"),
        "  state:\n    backend: postgres\n    connection: warehouse\n",
        "  state:\n    backend: file\n",
    );
    let out = output(golden_ferry(dir.path(), &shop, &warehouse).arg("plan")).expect(0);
    assert!(
        out.stdout.contains(
            "  strategy:    DeletePartitionThenInsert (the state store is a file \
             (`backend: file`), not on \"warehouse\")"
        ),
        "{}",
        out.stdout
    );
    assert!(
        out.stdout
            .contains("state store: file, in the work directory"),
        "{}",
        out.stdout
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_source_that_cant_be_reached_exits_5_and_a_type_error_exits_4() {
    let warehouse = Warehouse::start().await;
    // Nothing listens on port 1.
    let mut cmd = ferry(&repo(""));
    cmd.env(
        "FERRY_GOLDEN_MONGO_URL",
        "mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=500",
    )
    .env("FERRY_GOLDEN_PG_URL", &warehouse.url)
    .args(["plan", "--config", "examples/golden/ferry"]);
    let out = output(&mut cmd).expect(5).no_secret();
    assert!(
        out.stderr.starts_with(
            "error[F0501]: connections[shop]: could not discover the entities of `shop`"
        ),
        "{}",
        out.stderr
    );

    let shop = Shop::golden().await;
    let dir = golden_copy();
    edit(
        &dir.path().join("ferry/flows/orders.yaml"),
        "status: lower(status)",
        "status: lower(stauts)",
    );
    let out = output(golden_ferry(dir.path(), &shop, &warehouse).arg("plan"))
        .expect(4)
        .no_secret();
    assert!(out.stderr.contains("stauts"), "{}", out.stderr);
    assert!(
        out.stderr.contains("status"),
        "the nearest name is suggested: {}",
        out.stderr
    );
}
