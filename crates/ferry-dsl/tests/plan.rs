//! The planner (B7) without connecting: configs on disk, discovered
//! entities and sink capabilities given by hand. The real binary against
//! real databases is in ferry-cli's `plan` tests.

mod common;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use common::golden;
use common::plan::{golden_catalog, project};
use common::repo;
use ferry_core::testkit::MemSink;
use ferry_core::types::{CanonicalField, CanonicalType};
use ferry_core::{
    BatchStream, Entity, IdempotencyStrategy, Partition, PartitionHint, Result, SinkCaps, Source,
    SourceCaps, TransformPlan,
};
use ferry_dsl::config::{ConfigErrors, ErrorKind, load};
use ferry_dsl::plan::{Dependency, Edge, Plan, Setting, StatePlacement, plan, settings};
use ferry_dsl::typecheck::Catalog;
use ferry_runtime::OnError;

use IdempotencyStrategy::{DeletePartitionThenInsert, TxnWithState, UpsertByKey};

/// What the Postgres sink offers: `TxnWithState` only when it was built on
/// the state connection (0004 §8).
fn caps(on_state_connection: bool) -> SinkCaps {
    let mut strategies = vec![DeletePartitionThenInsert, UpsertByKey];
    if on_state_connection {
        strategies.insert(0, TxnWithState);
    }
    SinkCaps {
        strategies,
        bulk_copy: true,
    }
}

fn sinks(entries: &[(&str, SinkCaps)]) -> BTreeMap<String, SinkCaps> {
    entries
        .iter()
        .map(|(name, caps)| (name.to_string(), caps.clone()))
        .collect()
}

fn write(dir: &tempfile::TempDir, path: &str, text: &str) {
    let path = dir.path().join(path);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, text).unwrap();
}

fn plan_dir(
    dir: &std::path::Path,
    catalog: &Catalog,
    sinks: &BTreeMap<String, SinkCaps>,
) -> std::result::Result<Plan, ConfigErrors> {
    let config = load(dir).unwrap_or_else(|e| panic!("{e}"));
    plan(&config, catalog, sinks)
}

fn refused(result: std::result::Result<Plan, ConfigErrors>) -> ConfigErrors {
    match result {
        Ok(_) => panic!("the plan should have been refused"),
        Err(e) => e,
    }
}

/// A Postgres-shaped table, for flows that read what another writes.
fn table(name: &str) -> Entity {
    Entity::new(
        name,
        vec![
            CanonicalField::new("id", CanonicalType::Utf8, false),
            CanonicalField::new("status", CanonicalType::Utf8, true),
        ],
        vec!["id".into()],
    )
    .unwrap()
}

/// A copy of one mapped column of the golden documents: enough for flows
/// whose shape, not mapping, is under test.
fn copy_flow(name: &str, from: &str, to: &str, extra: &str) -> String {
    let column = match from.starts_with("shop.") {
        true => "string(_id)",
        false => "id",
    };
    format!(
        "  - name: {name}\n    from: {from}\n    sink: warehouse\n{extra}    to:\n      {to}:\n        key: [id]\n        columns:\n          id: {column}\n"
    )
}

#[test]
fn the_golden_flow_gets_txn_with_state_its_partition_rows_and_its_write_order() {
    let plan = plan_dir(
        &repo("examples/golden/ferry"),
        &golden_catalog(),
        &sinks(&[("warehouse", caps(true))]),
    )
    .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(plan.name, "golden_orders");
    assert_eq!(
        plan.state,
        StatePlacement::Connection {
            backend: "postgres".into(),
            connection: "warehouse".into()
        }
    );
    let [flow] = plan.flows.as_slice() else {
        panic!("one flow")
    };
    assert_eq!(flow.driver.to_string(), "shop.orders");
    assert_eq!(flow.strategy, TxnWithState);
    assert_eq!(flow.fallback, None);
    assert_eq!(
        (flow.partition_rows, flow.partition_rows_from),
        (4, Setting::Flow)
    );
    assert_eq!(flow.compiled.plan.write_order(), ["orders", "order_items"]);
    assert!(flow.waits_for.is_empty());
    // The canonical target the sink renders as DDL, with the FK the child
    // declares through `$parent.id`.
    let items = flow.compiled.target.table("order_items").unwrap();
    assert_eq!(items.primary_key, ["order_id", "line_no"]);
    assert_eq!(items.foreign_keys[0].references_table, "orders");
    // B8's assertions are type-checked as part of the plan.
    let names: Vec<&str> = flow
        .checked
        .expect
        .assertions
        .iter()
        .map(|a| a.name.as_str())
        .collect();
    assert_eq!(names, ["order_count", "order_total", "item_count"]);
    assert_eq!(
        plan.hash,
        load(repo("examples/golden/ferry")).unwrap().hash()
    );
}

#[test]
fn a_sink_off_the_state_connection_falls_back_and_the_plan_says_why() {
    let dir = project(&format!(
        "flows:\n{}",
        copy_flow("orders", "shop.orders", "orders", "")
    ));
    write(
        &dir,
        "connections.yaml",
        "connections:\n  - name: shop\n    type: mongo\n    url: x\n  - name: warehouse\n    type: postgres\n    url: y\n  - name: control\n    type: postgres\n    url: z\n",
    );
    write(
        &dir,
        "runtime.yaml",
        "runtime:\n  state: {backend: postgres, connection: control}\n",
    );
    let plan = plan_dir(
        dir.path(),
        &golden_catalog(),
        &sinks(&[("warehouse", caps(false))]),
    )
    .unwrap_or_else(|e| panic!("{e}"));
    let flow = &plan.flows[0];
    assert_eq!(flow.strategy, DeletePartitionThenInsert);
    assert_eq!(
        flow.fallback.as_deref(),
        Some("the state store is on \"control\", not \"warehouse\"")
    );
}

#[test]
fn under_a_file_state_store_every_flow_falls_back_and_the_plan_says_why() {
    let dir = project(&format!(
        "flows:\n{}",
        copy_flow("orders", "shop.orders", "orders", "")
    ));
    write(&dir, "runtime.yaml", "runtime:\n  state: {backend: file}\n");
    let plan = plan_dir(
        dir.path(),
        &golden_catalog(),
        &sinks(&[("warehouse", caps(false))]),
    )
    .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(plan.state, StatePlacement::File { path: None });
    assert_eq!(plan.flows[0].strategy, DeletePartitionThenInsert);
    assert_eq!(
        plan.flows[0].fallback.as_deref(),
        Some("the state store is a file (`backend: file`), not on \"warehouse\"")
    );
    // No runtime.yaml at all is the same: the default backend is a file.
    std::fs::remove_file(dir.path().join("runtime.yaml")).unwrap();
    let plan = plan_dir(
        dir.path(),
        &golden_catalog(),
        &sinks(&[("warehouse", caps(false))]),
    )
    .unwrap();
    assert_eq!(plan.flows[0].strategy, DeletePartitionThenInsert);
    assert!(plan.flows[0].fallback.is_some());
}

#[test]
fn a_sink_with_no_strategy_for_the_mode_refuses_the_flow() {
    // A sink that can only swap a staged copy in can't take an upsert.
    let staging_only = SinkCaps {
        strategies: vec![IdempotencyStrategy::StagingSwap],
        bulk_copy: false,
    };
    let dir = project(&format!(
        "flows:\n{}",
        copy_flow("orders", "shop.orders", "orders", "    mode: upsert\n")
    ));
    let errors = refused(plan_dir(
        dir.path(),
        &golden_catalog(),
        &sinks(&[("warehouse", staging_only.clone())]),
    ));
    let [e] = errors.0.as_slice() else {
        panic!("{errors}")
    };
    assert_eq!(e.kind, ErrorKind::Plan);
    assert_eq!(e.path, "flows[orders].sink");
    assert!(
        e.message
            .contains("the `warehouse` sink has no idempotency strategy for `mode: upsert`")
            && e.message.contains("StagingSwap"),
        "{e}"
    );
    assert_eq!(
        e.location.as_ref().unwrap().to_string(),
        "flows/f.yaml:4:11"
    );

    // The same sink takes the flow in insert mode.
    let dir = project(&format!(
        "flows:\n{}",
        copy_flow("orders", "shop.orders", "orders", "")
    ));
    let plan = plan_dir(
        dir.path(),
        &golden_catalog(),
        &sinks(&[("warehouse", staging_only)]),
    )
    .unwrap();
    assert_eq!(plan.flows[0].strategy, IdempotencyStrategy::StagingSwap);
}

#[test]
fn a_cycle_made_by_depends_on_and_a_read_is_a_plan_error_naming_it() {
    // `audit` reads `warehouse.order_copy`, which `copy` writes, so it
    // waits for `copy`; and `copy` declares it waits for `audit`. Neither
    // edge is a cycle on its own: `ferry validate` accepts the config.
    let dir = project(&format!(
        "flows:\n{}{}",
        copy_flow("audit", "warehouse.order_copy", "order_audit", ""),
        copy_flow(
            "copy",
            "shop.orders",
            "order_copy",
            "    depends_on: [audit]\n"
        ),
    ));
    let config = load(dir.path()).unwrap_or_else(|e| panic!("{e}"));
    let catalog = golden_catalog().with("warehouse", table("order_copy"));
    let errors = refused(plan(
        &config,
        &catalog,
        &sinks(&[("warehouse", caps(true))]),
    ));
    let [e] = errors.0.as_slice() else {
        panic!("{errors}")
    };
    assert_eq!(e.kind, ErrorKind::Plan);
    assert_eq!(
        e.message,
        "flows wait for each other in a cycle: audit → copy → audit (`audit` reads \
         `warehouse.order_copy`, which `copy` writes; `copy` depends on `audit`)"
    );
    // At the read that closes it: `audit`'s `from:`.
    assert_eq!(e.path, "flows[audit].from");
    assert_eq!(
        e.location.as_ref().unwrap().to_string(),
        "flows/f.yaml:3:11"
    );
    assert!(e.help.as_deref().unwrap().contains("depends_on"), "{e:?}");
}

#[test]
fn a_flow_reading_the_table_it_writes_is_a_cycle_of_one() {
    let dir = project(&format!(
        "flows:\n{}",
        copy_flow("rewrite", "warehouse.orders", "orders", "")
    ));
    let catalog = Catalog::new().with("warehouse", table("orders"));
    let errors = refused(plan_dir(
        dir.path(),
        &catalog,
        &sinks(&[("warehouse", caps(true))]),
    ));
    assert_eq!(
        errors.0[0].message,
        "flows wait for each other in a cycle: rewrite → rewrite (`rewrite` reads \
         `warehouse.orders`, which it writes itself)"
    );
}

#[test]
fn flows_run_after_what_they_wait_for_and_otherwise_by_name() {
    // `b_report` reads what `c_copy` writes; `a_late` declares it waits for
    // `b_report`. `d_free` waits for nothing.
    let dir = project(&format!(
        "flows:\n{}{}{}{}",
        copy_flow("d_free", "shop.orders", "free", ""),
        copy_flow(
            "a_late",
            "shop.orders",
            "late",
            "    depends_on: [b_report]\n"
        ),
        copy_flow("c_copy", "shop.orders", "order_copy", ""),
        copy_flow("b_report", "warehouse.order_copy", "report", ""),
    ));
    let catalog = golden_catalog().with("warehouse", table("order_copy"));
    let plan = plan_dir(dir.path(), &catalog, &sinks(&[("warehouse", caps(true))]))
        .unwrap_or_else(|e| panic!("{e}"));
    let order: Vec<&str> = plan.flows.iter().map(|f| f.name.as_str()).collect();
    assert_eq!(order, ["c_copy", "b_report", "a_late", "d_free"]);
    let report = plan.flow("b_report").unwrap();
    assert_eq!(
        report.waits_for,
        [Dependency {
            flow: "c_copy".into(),
            edge: Edge::Reads("warehouse.order_copy".into())
        }]
    );
    let late = plan.flow("a_late").unwrap();
    assert_eq!(late.waits_for[0].edge, Edge::Declared);

    // The runner gets every edge, the inferred one included.
    let flow = report.runtime_flow(
        Arc::new(Spy::default()),
        Arc::new(MemSink::new(vec![DeletePartitionThenInsert])),
        None,
    );
    assert_eq!(flow.depends_on, ["c_copy"]);
}

#[test]
fn the_plan_does_not_depend_on_which_file_a_flow_is_in_or_its_order() {
    let flows = [
        copy_flow("d_free", "shop.orders", "free", ""),
        copy_flow(
            "a_late",
            "shop.orders",
            "late",
            "    depends_on: [c_copy]\n",
        ),
        copy_flow("c_copy", "shop.orders", "order_copy", ""),
    ];
    let one_file = project(&format!("flows:\n{}{}{}", flows[0], flows[1], flows[2]));
    let spread = project(&format!("flows:\n{}", flows[2]));
    write(
        &spread,
        "flows/z/nested.yaml",
        &format!("flows:\n{}", flows[0]),
    );
    write(&spread, "flows/a.yml", &format!("flows:\n{}", flows[1]));
    let summary = |dir: &tempfile::TempDir| {
        let plan = plan_dir(
            dir.path(),
            &golden_catalog(),
            &sinks(&[("warehouse", caps(true))]),
        )
        .unwrap_or_else(|e| panic!("{e}"));
        (
            plan.hash.as_str().to_owned(),
            plan.flows
                .iter()
                .map(|f| (f.name.clone(), f.waits_for.clone(), f.strategy))
                .collect::<Vec<_>>(),
        )
    };
    assert_eq!(summary(&one_file), summary(&spread));
    assert_eq!(summary(&one_file).1[0].0, "c_copy");
}

#[test]
fn a_per_flow_on_error_the_runner_cant_apply_is_refused_not_ignored() {
    let dir = project(&format!(
        "flows:\n{}",
        copy_flow(
            "orders",
            "shop.orders",
            "orders",
            "    runtime: {on_error: dead_letter, max_errors: 3}\n"
        )
    ));
    let errors = refused(plan_dir(
        dir.path(),
        &golden_catalog(),
        &sinks(&[("warehouse", caps(true))]),
    ));
    let messages: Vec<&str> = errors.0.iter().map(|e| e.message.as_str()).collect();
    assert_eq!(
        messages,
        [
            "flow `orders` sets `on_error: dead_letter`, but this runner applies `on_error` to \
             the whole run (fail)",
            "flow `orders` sets `max_errors: 3`, but this runner applies `max_errors` to the \
             whole run (unbounded)",
        ]
    );
    assert_eq!(errors.0[0].path, "flows[orders].runtime.on_error");

    // The run-wide value written again on the flow changes nothing.
    write(
        &dir,
        "runtime.yaml",
        "runtime:\n  on_error: dead_letter\n  max_errors: 3\n",
    );
    let plan = plan_dir(
        dir.path(),
        &golden_catalog(),
        &sinks(&[("warehouse", caps(true))]),
    )
    .unwrap_or_else(|e| panic!("{e}"));
    assert!(plan.warnings.is_empty(), "{:?}", plan.warnings);
}

#[test]
fn a_target_named_like_ferrys_own_tables_is_refused() {
    let dir = project(&format!(
        "flows:\n{}",
        copy_flow("orders", "shop.orders", "_ferry_dlq", "")
    ));
    let errors = refused(plan_dir(
        dir.path(),
        &golden_catalog(),
        &sinks(&[("warehouse", caps(true))]),
    ));
    let [e] = errors.0.as_slice() else {
        panic!("{errors}")
    };
    assert_eq!(e.kind, ErrorKind::Plan);
    assert_eq!(e.path, "flows[orders].to._ferry_dlq");
    assert_eq!(
        e.message,
        "flow `orders`: target `_ferry_dlq` uses the prefix `_ferry_`, which is reserved for \
         ferry's own tables (dead letters go to `_ferry_dlq`)"
    );
}

#[test]
fn type_errors_and_unknown_entities_refuse_the_plan() {
    let dir = project(&format!(
        "flows:\n{}",
        copy_flow("orders", "shop.ordres", "orders", "")
    ));
    let errors = refused(plan_dir(
        dir.path(),
        &golden_catalog(),
        &sinks(&[("warehouse", caps(true))]),
    ));
    assert!(errors.0[0].message.contains("ordres"), "{errors}");
}

#[test]
fn dead_letters_without_max_errors_and_keyless_children_are_warnings() {
    let dir = project(
        "flows:\n  - name: orders\n    from: shop.orders\n    sink: warehouse\n    to:\n      orders:\n        key: [id]\n        columns:\n          id: string(_id)\n      order_items:\n        parent: orders\n        unnest: items\n        columns:\n          order_id: $parent.id\n          sku: sku\n",
    );
    write(&dir, "runtime.yaml", "runtime:\n  on_error: dead_letter\n");
    let plan = plan_dir(
        dir.path(),
        &golden_catalog(),
        &sinks(&[("warehouse", caps(true))]),
    )
    .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(plan.warnings.len(), 2, "{:?}", plan.warnings);
    assert!(
        plan.warnings[0].contains("without `max_errors`"),
        "{:?}",
        plan.warnings
    );
    assert!(
        plan.warnings[1].contains("child target `order_items` has no `key:`"),
        "{:?}",
        plan.warnings
    );
}

#[test]
fn runtime_yaml_becomes_the_runner_settings() {
    let config = load(repo("examples/golden/ferry")).unwrap();
    let s = settings(&config);
    assert_eq!(s.workers, 2);
    assert_eq!(s.memory_budget, 256 << 20);
    assert_eq!(s.on_error, OnError::DeadLetter);
    assert_eq!(s.max_errors, Some(10));
    assert_eq!(
        s.partition_rows, 100_000,
        "run-wide; the flow's 4 is its own"
    );
    assert_eq!(
        (s.source_max_connections, s.source_rows_per_sec),
        (None, None)
    );

    let dir = project(&format!(
        "flows:\n{}",
        copy_flow("orders", "shop.orders", "orders", "")
    ));
    write(
        &dir,
        "runtime.yaml",
        "runtime:\n  workers: 3\n  partition_rows: 50\n  source_max_connections: 2\n  source_rows_per_sec: 100\n",
    );
    let config = load(dir.path()).unwrap();
    let s = settings(&config);
    assert_eq!(
        (
            s.workers,
            s.partition_rows,
            s.source_max_connections,
            s.source_rows_per_sec
        ),
        (3, 50, Some(2), Some(100))
    );
    assert_eq!(s.on_error, OnError::Fail);
    let plan = plan(
        &config,
        &golden_catalog(),
        &sinks(&[("warehouse", caps(true))]),
    )
    .unwrap();
    assert_eq!(
        (
            plan.flows[0].partition_rows,
            plan.flows[0].partition_rows_from
        ),
        (50, Setting::Runtime)
    );
}

/// Records the hints it is asked to partition with.
#[derive(Default)]
struct Spy {
    hints: Mutex<Vec<PartitionHint>>,
}

#[async_trait]
impl Source for Spy {
    async fn discover(&self) -> Result<Vec<Entity>> {
        Ok(vec![golden::entity()])
    }

    async fn plan_partitions(&self, _: &Entity, hint: &PartitionHint) -> Result<Vec<Partition>> {
        self.hints.lock().unwrap().push(hint.clone());
        Ok(Vec::new())
    }

    async fn read(&self, _: &Entity, _: &Partition) -> Result<BatchStream> {
        unreachable!("planning only")
    }

    async fn capabilities(&self) -> Result<SourceCaps> {
        Ok(SourceCaps::default())
    }
}

#[tokio::test]
async fn the_driver_is_partitioned_with_the_flows_own_partition_rows() {
    let plan = plan_dir(
        &repo("examples/golden/ferry"),
        &golden_catalog(),
        &sinks(&[("warehouse", caps(true))]),
    )
    .unwrap();
    let spy = Arc::new(Spy::default());
    let flow = plan.flows[0].runtime_flow(
        spy.clone(),
        Arc::new(MemSink::new(vec![TxnWithState])),
        None,
    );
    assert_eq!(flow.name, "orders");
    assert_eq!(flow.inputs[0].entity.name(), "orders");
    // The runner asks with its run-wide default...
    flow.inputs[0]
        .source
        .plan_partitions(&flow.inputs[0].entity, &PartitionHint::rows(100_000))
        .await
        .unwrap();
    // ...and the source is asked for the flow's `runtime: partition_rows`.
    assert_eq!(*spy.hints.lock().unwrap(), [PartitionHint::rows(4)]);
}
