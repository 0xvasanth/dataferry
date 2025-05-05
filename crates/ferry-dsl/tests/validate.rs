//! B1b: the rules of 0003 §3–§5 that need more than one value to check, run
//! on the merged model at load.

use std::path::PathBuf;

use ferry_dsl::config::{ConfigError, ErrorKind, load};

const FERRY: &str = "version: 1\nname: test\n";
const CONNECTIONS: &str = "connections:\n  - name: app\n    type: postgres\n    url: ${APP_URL}\n  - name: dw\n    type: postgres\n    url: ${DW_URL}\n";

fn write(files: &[(&str, &str)]) -> (tempfile::TempDir, PathBuf) {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("ferry");
    let base = [("ferry.yaml", FERRY), ("connections.yaml", CONNECTIONS)];
    for (path, text) in base.iter().chain(files) {
        let path = dir.join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, text).unwrap();
    }
    (tmp, dir)
}

fn errors(files: &[(&str, &str)]) -> Vec<ConfigError> {
    let (_tmp, dir) = write(files);
    match load(&dir) {
        Ok(_) => Vec::new(),
        Err(e) => e.0,
    }
}

/// The one error a single flow file gives, as `(line, path, message)`.
fn flow_error(flow: &str) -> (u32, String, String) {
    let errors = errors(&[("flows/f.yaml", flow)]);
    assert_eq!(errors.len(), 1, "{errors:#?}");
    let e = &errors[0];
    assert_eq!(e.kind, ErrorKind::Invalid, "{e}");
    (
        e.location.as_ref().unwrap().line,
        e.path.clone(),
        e.message.clone(),
    )
}

fn assert_valid(files: &[(&str, &str)]) {
    let errors = errors(files);
    assert!(errors.is_empty(), "{errors:#?}");
}

const MERGE: &str = "\
flows:
  - name: f
    from:
      - entity: app.public.orders
        as: o
      - entity: app.public.customers
        as: c
        join:
          cardinality: one
          keys: {id: o.customer_id}
      - entity: app.public.items
        as: i
        join:
          cardinality: many
          keys: {order_id: o.id}
    sink: dw
    to:
      t:
        columns: {id: id}
";

#[test]
fn a_valid_merge_loads() {
    assert_valid(&[("flows/f.yaml", MERGE)]);
}

#[test]
fn the_driver_has_no_join_and_every_other_entry_has_one() {
    let driver_joins = MERGE.replace(
        "        as: o\n",
        "        as: o\n        join:\n          cardinality: one\n          keys: {id: c.id}\n",
    );
    let (line, path, message) = flow_error(&driver_joins);
    assert_eq!((line, path.as_str()), (7, "flows[f].from[0].join"));
    assert!(message.contains("the driver"), "{message}");

    let no_join = MERGE.replace(
        "        join:\n          cardinality: one\n          keys: {id: o.customer_id}\n",
        "",
    );
    let (line, path, _) = flow_error(&no_join);
    assert_eq!((line, path.as_str()), (6, "flows[f].from[1].join"));
}

#[test]
fn join_keys_reference_an_earlier_entry_that_is_not_many() {
    let later = MERGE.replace("keys: {id: o.customer_id}", "keys: {id: i.customer_id}");
    let (line, path, message) = flow_error(&later);
    assert_eq!((line, path.as_str()), (10, "flows[f].from[1].join.keys.id"));
    assert!(message.contains("earlier `from:` entry"), "{message}");

    let through_many = MERGE.replace(
        "    sink: dw\n",
        "      - entity: app.public.options\n        join:\n          cardinality: one\n          keys: {item_id: i.id}\n    sink: dw\n",
    );
    let (_, path, message) = flow_error(&through_many);
    assert_eq!(path, "flows[f].from[3].join.keys.item_id");
    assert!(message.contains("a `many` entry"), "{message}");
}

#[test]
fn aliases_are_unique_and_default_to_the_last_name_part() {
    let clash = MERGE.replace("        as: c\n", "        as: o\n");
    let (line, path, message) = flow_error(&clash);
    assert_eq!((line, path.as_str()), (7, "flows[f].from[1].as"));
    assert!(message.contains("alias `o`"), "{message}");

    // `app.public.orders` defaults to `orders`, which the second entry also
    // gets from `app.archive.orders`.
    let defaults = MERGE
        .replace("        as: o\n", "")
        .replace("o.customer_id", "orders.customer_id")
        .replace("o.id", "orders.id")
        .replace("app.public.customers\n        as: c", "app.archive.orders");
    let (_, path, message) = flow_error(&defaults);
    assert_eq!(path, "flows[f].from[1].as");
    assert!(message.contains("alias `orders`"), "{message}");
}

#[test]
fn every_entity_names_a_defined_connection() {
    let (line, path, message) = flow_error(&MERGE.replace("app.public.customers", "crm.customers"));
    assert_eq!((line, path.as_str()), (6, "flows[f].from[1].entity"));
    assert_eq!(message, "no connection named `crm`");
}

const SPLIT: &str = "\
flows:
  - name: f
    from: app.public.orders
    sink: dw
    to:
      orders:
        key: [id]
        columns: {id: id}
      items:
        parent: orders
        unnest: items
        columns: {order_id: $parent.id}
";

#[test]
fn parent_must_be_another_target_of_the_flow() {
    let (line, path, message) = flow_error(&SPLIT.replace("parent: orders", "parent: order"));
    assert_eq!((line, path.as_str()), (10, "flows[f].to.items.parent"));
    assert!(message.contains("not a target"), "{message}");

    let (_, path, message) = flow_error(&SPLIT.replace("parent: orders", "parent: items"));
    assert_eq!(path, "flows[f].to.items.parent");
    assert!(message.contains("its own parent"), "{message}");
}

#[test]
fn parent_links_must_not_form_a_cycle() {
    let cycle = SPLIT.replace(
        "        key: [id]\n",
        "        parent: items\n        key: [id]\n",
    );
    let (_, path, message) = flow_error(&cycle);
    assert_eq!(path, "flows[f].to.orders.parent");
    assert_eq!(
        message,
        "`parent:` links form a cycle: orders → items → orders"
    );
}

#[test]
fn star_columns_only_on_a_plain_single_entity_target() {
    let (line, path, _) =
        flow_error(&SPLIT.replace("columns: {order_id: $parent.id}", "columns: \"*\""));
    assert_eq!((line, path.as_str()), (12, "flows[f].to.items.columns"));
    let (_, path, _) = flow_error(&MERGE.replace("columns: {id: id}", "columns: \"*\""));
    assert_eq!(path, "flows[f].to.t.columns");
}

#[test]
fn upsert_needs_a_key_on_every_target() {
    let (line, path, message) =
        flow_error(&SPLIT.replace("    sink: dw\n", "    sink: dw\n    mode: upsert\n"));
    assert_eq!((line, path.as_str()), (10, "flows[f].to.items.key"));
    assert!(message.contains("needs `key:`"), "{message}");
}

#[test]
fn a_target_name_has_no_dot() {
    let (_, path, message) = flow_error(
        &SPLIT
            .replace("      orders:\n", "      public.orders:\n")
            .replace("parent: orders", "parent: public.orders"),
    );
    assert_eq!(path, "flows[f].to.public.orders");
    assert!(message.contains("contains a dot"), "{message}");
}

#[test]
fn a_flow_needs_a_target() {
    let (_, path, _) =
        flow_error("flows:\n  - name: f\n    from: app.t\n    sink: dw\n    to: {}\n");
    assert_eq!(path, "flows[f].to");
}

#[test]
fn a_table_on_one_sink_has_one_writer() {
    let one = "flows:\n  - name: a\n    from: app.t\n    sink: dw\n    to:\n      t:\n        columns: \"*\"\n";
    let two = one.replace("name: a", "name: b");
    let errors = errors(&[("flows/a.yaml", one), ("flows/b.yaml", &two)]);
    assert_eq!(errors.len(), 1, "{errors:#?}");
    assert_eq!(errors[0].path, "flows[b].to.t");
    assert!(
        errors[0]
            .message
            .contains("flow `a` also writes `t` on `dw`"),
        "{}",
        errors[0]
    );
    // The same table on another sink is a different table.
    let other_sink = two.replace("sink: dw", "sink: app");
    assert_valid(&[("flows/a.yaml", one), ("flows/b.yaml", &other_sink)]);
}

#[test]
fn the_state_store_fits_its_backend() {
    let flow = (
        "flows/f.yaml",
        "flows:\n  - name: f\n    from: app.t\n    sink: dw\n    to:\n      t:\n        columns: \"*\"\n",
    );
    for (runtime, path, fragment) in [
        (
            "runtime:\n  state: {backend: file, connection: dw}\n",
            "runtime.state.connection",
            "takes `path:`",
        ),
        (
            "runtime:\n  state: {backend: postgres}\n",
            "runtime.state.connection",
            "needs `connection:`",
        ),
        (
            "runtime:\n  state: {backend: postgres, connection: dw, path: x}\n",
            "runtime.state.path",
            "only applies to `backend: file`",
        ),
        (
            "runtime:\n  state: {backend: postgres, connection: warehouse}\n",
            "runtime.state.connection",
            "no connection named",
        ),
    ] {
        let errors = errors(&[flow, ("runtime.yaml", runtime)]);
        assert_eq!(errors.len(), 1, "{runtime}: {errors:#?}");
        assert_eq!(errors[0].path, path, "{runtime}");
        assert!(
            errors[0].message.contains(fragment),
            "{runtime}: {}",
            errors[0]
        );
    }
    assert_valid(&[
        flow,
        (
            "runtime.yaml",
            "runtime:\n  state: {backend: postgres, connection: dw}\n",
        ),
    ]);
    assert_valid(&[
        flow,
        (
            "runtime.yaml",
            "runtime:\n  state: {backend: file, path: /var/ferry}\n",
        ),
    ]);
}

#[test]
fn a_file_that_fails_to_parse_does_not_cascade_into_reference_errors() {
    let bad_connections = format!("{CONNECTIONS}    bogus: [\n");
    let (_tmp, dir) = write(&[
        ("connections.yaml", &bad_connections),
        (
            "flows/f.yaml",
            "flows:\n  - name: f\n    from: app.t\n    sink: dw\n    to:\n      t:\n        columns: \"*\"\n",
        ),
    ]);
    let errors = load(&dir).unwrap_err().0;
    assert_eq!(errors.len(), 1, "{errors:#?}");
    assert_eq!(errors[0].kind, ErrorKind::Yaml);
}
