//! B1c: the config hash (0003 §10).

mod common;

use std::path::{Path, PathBuf};

use common::repo;
use ferry_dsl::config::{Config, load};

/// A copy of `examples/<example>/ferry` in a temp dir.
struct Copy {
    _tmp: tempfile::TempDir,
    dir: PathBuf,
}

impl Copy {
    fn of(example: &str) -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("ferry");
        copy_dir(&repo(&format!("examples/{example}/ferry")), &dir);
        Copy { _tmp: tmp, dir }
    }

    fn read(&self, path: &str) -> String {
        std::fs::read_to_string(self.dir.join(path)).unwrap()
    }

    fn write(&self, path: &str, text: &str) -> &Self {
        let path = self.dir.join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, text).unwrap();
        self
    }

    fn remove(&self, path: &str) -> &Self {
        std::fs::remove_file(self.dir.join(path)).unwrap();
        self
    }

    /// Replaces the one occurrence of `old` in `path`.
    fn edit(&self, path: &str, old: &str, new: &str) -> &Self {
        let text = self.read(path);
        assert_eq!(text.matches(old).count(), 1, "{path}: {old:?}");
        self.write(path, &text.replace(old, new))
    }

    fn config(&self) -> Config {
        load(&self.dir).unwrap_or_else(|e| panic!("{e}"))
    }

    fn hash(&self) -> String {
        self.config().hash().to_string()
    }
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let path = entry.unwrap().path();
        let target = to.join(path.file_name().unwrap());
        if path.is_dir() {
            copy_dir(&path, &target);
        } else {
            std::fs::copy(&path, &target).unwrap();
        }
    }
}

fn golden_hash() -> String {
    Copy::of("golden").hash()
}

const FLOW: &str = "flows/orders.yaml";

#[test]
fn golden_config_hash_is_pinned() {
    // Changing how the hash is computed changes this value, and blocks
    // resuming every run started by an older ferry (A4). If that is
    // intended, update the literal and say so in the release notes.
    assert_eq!(
        golden_hash(),
        "sha256:c4e1f41657f0c914abccab48976f2122ca2041b6769bd83a031f758b787109e4"
    );
}

#[test]
fn the_hash_is_a_sha256_string() {
    let hash = golden_hash();
    let hex = hash.strip_prefix("sha256:").unwrap();
    assert_eq!(hex.len(), 64);
    assert!(
        hex.bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    );
}

// ------------------------------------------------------------ unchanged

#[test]
fn moving_a_flow_to_another_file_keeps_the_hash() {
    let copy = Copy::of("golden");
    let text = copy.read(FLOW);
    copy.remove(FLOW).write("flows/sub/b.yml", &text);
    assert_eq!(copy.hash(), golden_hash());
}

#[test]
fn inlining_an_included_file_keeps_the_hash() {
    let base = Copy::of("pg-merge").hash();
    let copy = Copy::of("pg-merge");
    let legacy = copy.read("legacy/customers.yaml");
    copy.remove("legacy/customers.yaml")
        .write("flows/customers.yaml", &legacy)
        .edit("ferry.yaml", "include:\n  - legacy/*.yaml\n", "");
    assert_eq!(copy.hash(), base);
}

#[test]
fn comments_whitespace_and_key_order_keep_the_hash() {
    let copy = Copy::of("golden");
    copy.edit(
        FLOW,
        "    from: shop.orders\n    sink: warehouse\n",
        "\n    # moved\n    sink:    warehouse\n    from: \"shop.orders\"\n",
    )
    .edit(
        FLOW,
        "        key: [id]\n",
        "        key:\n          - id\n",
    )
    .edit(
        "connections.yaml",
        "    type: mongo\n    url: ${FERRY_GOLDEN_MONGO_URL}\n",
        "    url: '${FERRY_GOLDEN_MONGO_URL}'   # quoted now\n    type: mongo\n",
    );
    assert_eq!(copy.hash(), golden_hash());
}

#[test]
fn writing_a_default_out_keeps_the_hash() {
    let copy = Copy::of("golden");
    // `mode: insert` is already explicit in golden; drop it, and spell out
    // the default state store in a config that has none.
    copy.edit(FLOW, "    mode: insert\n", "");
    assert_eq!(copy.hash(), golden_hash());

    let base = Copy::of("edge/layout");
    let without_state = base.hash();
    base.write("runtime.yaml", "runtime: {}\n");
    let implicit = base.hash();
    base.write("runtime.yaml", "runtime:\n  state: {backend: file}\n");
    assert_eq!(base.hash(), implicit);
    assert_ne!(implicit, without_state, "edge/layout sets a state path");
}

#[test]
fn a_bare_expression_and_an_expr_object_hash_the_same() {
    let copy = Copy::of("golden");
    copy.edit(
        FLOW,
        "          status: lower(status)\n",
        "          status: {expr: lower(status)}\n",
    );
    assert_eq!(copy.hash(), golden_hash());
}

#[test]
fn rotating_an_env_value_keeps_the_hash() {
    let config = Copy::of("golden").config();
    let before = config.hash();
    for password in ["one", "two"] {
        let url = format!("postgres://ferry:{password}@db/warehouse");
        config
            .resolve_with(|name| match name {
                "FERRY_GOLDEN_PG_URL" => Some(url.clone()),
                _ => Some("mongodb://localhost/shop".into()),
            })
            .unwrap();
        assert_eq!(config.hash(), before);
    }
    assert!(!config.hash_input().contains("ferry:one"));
}

#[test]
fn reordering_depends_on_keeps_the_hash() {
    let copy = Copy::of("edge/layout");
    let before = copy.hash();
    copy.edit(
        "flows/sales/orders.yml",
        "[customers, products]",
        "[products, customers]",
    );
    assert_eq!(copy.hash(), before);
}

#[test]
fn reordering_join_keys_keeps_the_hash() {
    let copy = Copy::of("pg-merge");
    let path = "flows/order_summaries.yaml";
    copy.edit(
        path,
        "keys: {order_id: o.id}",
        "keys: {order_id: o.id, line_scope: o.scope}",
    );
    let before = copy.hash();
    copy.edit(
        path,
        "keys: {order_id: o.id, line_scope: o.scope}",
        "keys: {line_scope: o.scope, order_id: o.id}",
    );
    assert_eq!(copy.hash(), before);
}

/// Tuning settings are left out (0003 §10.5): one test per key, in
/// runtime.yaml and, where allowed, in a flow's `runtime:`.
macro_rules! tuning_key_is_not_hashed {
    ($name:ident, $runtime_old:expr, $runtime_new:expr) => {
        #[test]
        fn $name() {
            let copy = Copy::of("golden");
            copy.edit("runtime.yaml", $runtime_old, $runtime_new);
            assert_eq!(copy.hash(), golden_hash());
        }
    };
}

tuning_key_is_not_hashed!(workers_is_not_hashed, "  workers: 2\n", "  workers: 16\n");
tuning_key_is_not_hashed!(
    memory_budget_is_not_hashed,
    "  memory_budget: 256MiB\n",
    "  memory_budget: 8GiB\n"
);
tuning_key_is_not_hashed!(
    partition_rows_in_runtime_yaml_is_not_hashed,
    "  workers: 2\n",
    "  workers: 2\n  partition_rows: 500\n"
);
tuning_key_is_not_hashed!(
    source_max_connections_is_not_hashed,
    "  workers: 2\n",
    "  workers: 2\n  source_max_connections: 3\n"
);
tuning_key_is_not_hashed!(
    source_rows_per_sec_is_not_hashed,
    "  workers: 2\n",
    "  workers: 2\n  source_rows_per_sec: 1000\n"
);

#[test]
fn partition_rows_in_a_flow_is_not_hashed() {
    let copy = Copy::of("golden");
    copy.edit(
        FLOW,
        "      partition_rows: 4\n",
        "      partition_rows: 4000\n",
    );
    assert_eq!(copy.hash(), golden_hash());
    copy.edit(FLOW, "    runtime:\n      partition_rows: 4000\n", "");
    assert_eq!(copy.hash(), golden_hash());
}

// ------------------------------------------------------------ changed

fn changes_hash(example: &str, path: &str, old: &str, new: &str) {
    let base = Copy::of(example).hash();
    let copy = Copy::of(example);
    copy.edit(path, old, new);
    assert_ne!(copy.hash(), base, "{path}: {old:?} → {new:?}");
}

#[test]
fn editing_a_mapping_expression_changes_the_hash() {
    changes_hash(
        "golden",
        FLOW,
        "status: lower(status)",
        "status: upper(status)",
    );
}

#[test]
fn renaming_an_env_reference_changes_the_hash() {
    changes_hash(
        "golden",
        "connections.yaml",
        "${FERRY_GOLDEN_PG_URL}",
        "${FERRY_GOLDEN_PG_URL_NEW}",
    );
}

#[test]
fn on_error_changes_the_hash_in_runtime_yaml_and_per_flow() {
    changes_hash(
        "golden",
        "runtime.yaml",
        "on_error: dead_letter",
        "on_error: fail",
    );
    changes_hash(
        "golden",
        FLOW,
        "      partition_rows: 4\n",
        "      partition_rows: 4\n      on_error: fail\n",
    );
}

#[test]
fn max_errors_changes_the_hash_in_runtime_yaml_and_per_flow() {
    changes_hash("golden", "runtime.yaml", "max_errors: 10", "max_errors: 11");
    changes_hash(
        "golden",
        FLOW,
        "      partition_rows: 4\n",
        "      partition_rows: 4\n      max_errors: 1\n",
    );
}

#[test]
fn the_state_store_changes_the_hash() {
    changes_hash(
        "golden",
        "runtime.yaml",
        "    backend: postgres\n    connection: warehouse\n",
        "    backend: file\n",
    );
    changes_hash(
        "edge/layout",
        "runtime.yaml",
        "path: .ferry/state",
        "path: .ferry/other",
    );
}

#[test]
fn adding_or_removing_a_dependency_changes_the_hash() {
    changes_hash(
        "edge/layout",
        "flows/sales/orders.yml",
        "[customers, products]",
        "[customers]",
    );
    changes_hash(
        "edge/layout",
        "flows/customers.yaml",
        "    sink: reporting\n",
        "    sink: reporting\n    depends_on: [categories]\n",
    );
}

#[test]
fn other_semantic_edits_change_the_hash() {
    let flow = "flows/order_summaries.yaml";
    for (example, path, old, new) in [
        // Column order is DDL order.
        (
            "golden",
            FLOW,
            "          line_no: $index\n          sku: sku\n",
            "          sku: sku\n          line_no: $index\n",
        ),
        // Named predicates are reported in order.
        (
            "golden",
            FLOW,
            "      not_deleted: deleted_at == null\n",
            "      not_deleted: deleted_at == null\n      has_id: _id != null\n",
        ),
        ("golden", FLOW, "not_deleted:", "kept:"),
        (
            "golden",
            FLOW,
            "        key: [id]\n",
            "        key: [id, status]\n",
        ),
        ("golden", FLOW, "    mode: insert\n", "    mode: upsert\n"),
        (
            "golden",
            FLOW,
            "        unnest: items",
            "        unnest: lines",
        ),
        (
            "golden",
            FLOW,
            "            type: timestamp(UTC)\n",
            "            type: timestamp(UTC)\n            nullable: false\n",
        ),
        (
            "golden",
            FLOW,
            "      order_count: count(orders) == source.count()\n",
            "",
        ),
        (
            "golden",
            "connections.yaml",
            "    database: shop\n",
            "    database: shop2\n",
        ),
        (
            "golden",
            "connections.yaml",
            "    type: postgres\n",
            "    type: postgres\n    schema: staging\n",
        ),
        (
            "golden",
            "ferry.yaml",
            "name: golden_orders",
            "name: golden_orders_2",
        ),
        // `from:` entry order: the driver comes first.
        (
            "pg-merge",
            flow,
            "strategy: co_partition",
            "strategy: stage",
        ),
        ("pg-merge", flow, "cardinality: one ", "cardinality: many "),
        ("pg-merge", flow, "        as: c\n", "        as: cu\n"),
        (
            "pg-merge",
            flow,
            "            order_by: [line_no]",
            "            order_by: [line_no desc]",
        ),
        ("pg-merge", flow, "            where: qty > 0\n", ""),
    ] {
        changes_hash(example, path, old, new);
    }
}

#[test]
fn the_hash_input_holds_references_not_values_and_no_tuning() {
    let input = Copy::of("golden").config().hash_input();
    assert!(input.contains("${FERRY_GOLDEN_PG_URL}"));
    for tuning in [
        "workers",
        "memory_budget",
        "partition_rows",
        "source_max_connections",
        "source_rows_per_sec",
    ] {
        assert!(!input.contains(tuning), "{tuning} is in the hash input");
    }
    assert!(
        !input.contains("orders.yaml"),
        "file names are in the hash input"
    );
}
