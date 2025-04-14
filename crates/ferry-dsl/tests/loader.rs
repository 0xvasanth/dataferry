//! B1a: finding and reading a `ferry/` directory (0003 §2, §8).

use std::path::{Path, PathBuf};

use ferry_dsl::config::{Config, ConfigError, ConfigErrors, ErrorKind, load};

fn repo(path: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(path)
}

const FERRY: &str = "version: 1\nname: test\n";
const CONNECTIONS: &str = "connections:\n  - name: app\n    type: postgres\n    url: ${APP_URL}\n";

fn flow(name: &str) -> String {
    format!(
        "flows:\n  - name: {name}\n    from: app.public.{name}\n    sink: app\n    to:\n      {name}_copy:\n        columns: \"*\"\n"
    )
}

/// Writes `files` (path, contents) under a fresh `ferry/` directory, in the
/// order given.
fn write(files: &[(&str, &str)]) -> (tempfile::TempDir, PathBuf) {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("ferry");
    for (path, text) in files {
        let path = dir.join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, text).unwrap();
    }
    (tmp, dir)
}

fn load_ok(dir: &Path) -> Config {
    load(dir).unwrap_or_else(|e| panic!("{e}"))
}

fn load_err(dir: &Path) -> Vec<ConfigError> {
    match load(dir) {
        Ok(_) => panic!("{} loaded, expected an error", dir.display()),
        Err(ConfigErrors(errors)) => errors,
    }
}

fn only(errors: Vec<ConfigError>) -> ConfigError {
    assert_eq!(errors.len(), 1, "{errors:#?}");
    errors.into_iter().next().unwrap()
}

fn line(error: &ConfigError) -> (String, u32) {
    let location = error.location.as_ref().expect("error has a location");
    (location.file.to_string(), location.line)
}

fn flow_names(config: &Config) -> Vec<&str> {
    config.flows.iter().map(|f| f.name.as_str()).collect()
}

fn source_paths(config: &Config) -> Vec<&str> {
    config.sources().iter().map(|s| &*s.path).collect()
}

#[test]
fn every_example_config_loads() {
    for dir in ["golden", "pg-merge", "edge/layout"] {
        let config = load_ok(&repo(&format!("examples/{dir}/ferry")));
        assert!(!config.flows.is_empty(), "{dir}");
    }
}

#[test]
fn golden_config_reads_into_the_expected_model() {
    let config = load_ok(&repo("examples/golden/ferry"));
    assert_eq!(config.name.as_str(), "golden_orders");
    assert_eq!(flow_names(&config), ["orders"]);
    let orders = &config.flows[0];
    assert_eq!(orders.sink.as_str(), "warehouse");
    let targets: Vec<&str> = orders.to.keys().map(|k| k.as_str()).collect();
    assert_eq!(targets, ["orders", "order_items"]);
    // Expressions keep their text and the line they were written on.
    let where_ = orders.filter.as_ref().unwrap();
    assert_eq!(where_.location().line, 11);
    let connections: Vec<&str> = config.connections.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(connections, ["shop", "warehouse"]);
    assert_eq!(config.runtime.max_errors, Some(10));
}

#[test]
fn file_creation_order_does_not_change_the_model() {
    let files = [
        ("ferry.yaml", FERRY.to_owned()),
        ("connections.yaml", CONNECTIONS.to_owned()),
        ("flows/b.yaml", flow("bravo")),
        ("flows/a/x.yml", flow("alpha")),
        ("flows/c.yaml", flow("charlie")),
    ];
    let forward: Vec<(&str, &str)> = files.iter().map(|(p, t)| (*p, t.as_str())).collect();
    let backward: Vec<(&str, &str)> = forward.iter().rev().copied().collect();
    let (_t1, one) = write(&forward);
    let (_t2, two) = write(&backward);
    let (one, two) = (load_ok(&one), load_ok(&two));
    assert_eq!(flow_names(&one), ["alpha", "bravo", "charlie"]);
    assert_eq!(one, two);
    // Debug shows locations too, so this also checks every line and file.
    let debug = |c: &Config| format!("{:?}", (&c.name, &c.connections, &c.runtime, &c.flows));
    assert_eq!(debug(&one), debug(&two));
}

#[test]
fn files_load_in_byte_wise_path_order() {
    // Byte-wise: `B` (0x42) sorts before `a`, and `a.yaml` before `a/b.yaml`
    // because `.` (0x2e) sorts before `/` (0x2f). A case-folding, locale or
    // per-component sort gets at least one of these wrong.
    let z = flow("zulu");
    let b = flow("bravo");
    let a = flow("alpha");
    let ab = flow("alpha_beta");
    let (_tmp, dir) = write(&[
        ("ferry.yaml", FERRY),
        ("connections.yaml", CONNECTIONS),
        ("flows/z.yml", &z),
        ("flows/a/b.yaml", &ab),
        ("flows/B.yaml", &b),
        ("flows/a.yaml", &a),
    ]);
    let config = load_ok(&dir);
    assert_eq!(
        source_paths(&config),
        [
            "ferry.yaml",
            "connections.yaml",
            "flows/B.yaml",
            "flows/a.yaml",
            "flows/a/b.yaml",
            "flows/z.yml",
        ]
    );
    assert_eq!(
        flow_names(&config),
        ["bravo", "alpha", "alpha_beta", "zulu"]
    );
}

#[test]
fn a_flow_name_used_in_two_files_is_one_error_naming_both() {
    let orders = flow("orders");
    let (_tmp, dir) = write(&[
        ("ferry.yaml", FERRY),
        ("connections.yaml", CONNECTIONS),
        ("flows/a.yaml", &orders),
        ("flows/sub/c.yml", &orders),
    ]);
    let error = only(load_err(&dir));
    assert_eq!(error.kind, ErrorKind::Duplicate);
    assert_eq!(line(&error), ("flows/sub/c.yml".into(), 2));
    assert_eq!(error.path, "flows[orders].name");
    assert!(error.message.contains("flows/a.yaml:2:11"), "{error}");
}

#[test]
fn a_flow_name_used_twice_in_one_file_is_an_error() {
    let text = format!("{}{}", flow("orders"), &flow("orders")["flows:\n".len()..]);
    let (_tmp, dir) = write(&[
        ("ferry.yaml", FERRY),
        ("connections.yaml", CONNECTIONS),
        ("flows/orders.yaml", &text),
    ]);
    let error = only(load_err(&dir));
    assert_eq!(error.kind, ErrorKind::Duplicate);
    assert_eq!(line(&error), ("flows/orders.yaml".into(), 8));
    assert!(error.message.contains("flows/orders.yaml:2:11"), "{error}");
}

#[test]
fn a_connection_name_used_in_two_files_is_an_error_naming_both() {
    let ferry = format!("{FERRY}include: [shared/connections.yaml]\n");
    let orders = flow("orders");
    let (_tmp, dir) = write(&[
        ("ferry.yaml", &ferry),
        ("connections.yaml", CONNECTIONS),
        ("shared/connections.yaml", CONNECTIONS),
        ("flows/orders.yaml", &orders),
    ]);
    let error = only(load_err(&dir));
    assert_eq!(error.kind, ErrorKind::Duplicate);
    assert_eq!(line(&error), ("shared/connections.yaml".into(), 2));
    assert_eq!(error.path, "connections[app].name");
    assert!(error.message.contains("connections.yaml:2:11"), "{error}");
}

#[test]
fn include_in_an_included_file_is_an_error_at_that_line() {
    let ferry = format!("{FERRY}include: [shared/extra.yaml]\n");
    let orders = flow("orders");
    let extra = format!("{}include: [more.yaml]\n", flow("extra"));
    let (_tmp, dir) = write(&[
        ("ferry.yaml", &ferry),
        ("connections.yaml", CONNECTIONS),
        ("flows/orders.yaml", &orders),
        ("shared/extra.yaml", &extra),
        ("shared/more.yaml", &flow("more")),
    ]);
    let error = only(load_err(&dir));
    assert_eq!(error.kind, ErrorKind::Model);
    assert_eq!(line(&error), ("shared/extra.yaml".into(), 8));
    assert_eq!(error.path, "include");
    assert!(error.help.unwrap().contains("only allowed in ferry.yaml"));
}

#[test]
fn include_in_a_flow_file_is_an_error_with_the_same_hint() {
    let orders = format!("include: [x.yaml]\n{}", flow("orders"));
    let (_tmp, dir) = write(&[
        ("ferry.yaml", FERRY),
        ("connections.yaml", CONNECTIONS),
        ("flows/orders.yaml", &orders),
    ]);
    let error = only(load_err(&dir));
    assert_eq!(line(&error), ("flows/orders.yaml".into(), 1));
    assert!(error.help.unwrap().contains("only allowed in ferry.yaml"));
}

#[test]
fn a_file_reached_several_ways_loads_once() {
    let config = load_ok(&repo("examples/edge/layout/ferry"));
    assert_eq!(
        source_paths(&config),
        [
            "ferry.yaml",
            "connections.yaml",
            "flows/customers.yaml",
            "flows/sales/orders.yml",
            "runtime.yaml",
            "shared/catalog/categories.yaml",
            "shared/products.yaml",
        ]
    );
    let mut names = flow_names(&config);
    names.sort();
    assert_eq!(names, ["categories", "customers", "orders", "products"]);
}

#[test]
fn includes_may_point_outside_the_directory() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("ferry");
    std::fs::create_dir_all(dir.join("flows")).unwrap();
    std::fs::create_dir_all(tmp.path().join("shared")).unwrap();
    std::fs::write(
        dir.join("ferry.yaml"),
        format!("{FERRY}include: [../shared/*.yaml]\n"),
    )
    .unwrap();
    std::fs::write(dir.join("connections.yaml"), CONNECTIONS).unwrap();
    std::fs::write(tmp.path().join("shared/orders.yaml"), flow("orders")).unwrap();
    let config = load_ok(&dir);
    assert_eq!(flow_names(&config), ["orders"]);
    assert_eq!(
        source_paths(&config),
        ["ferry.yaml", "../shared/orders.yaml", "connections.yaml"]
    );
}

#[test]
fn a_schema_error_in_an_included_file_points_at_that_file() {
    let ferry = format!("{FERRY}include: [legacy/*.yaml]\n");
    let broken = flow("legacy").replace("    sink: app\n", "    sink: app\n    mdoe: upsert\n");
    let (_tmp, dir) = write(&[
        ("ferry.yaml", &ferry),
        ("connections.yaml", CONNECTIONS),
        ("legacy/old.yaml", &broken),
    ]);
    let error = only(load_err(&dir));
    assert_eq!(error.kind, ErrorKind::Model);
    assert_eq!(line(&error), ("legacy/old.yaml".into(), 5));
    assert_eq!(error.path, "flows[legacy].mdoe");
    assert_eq!(error.help.as_deref(), Some("did you mean `mode`?"));
}

#[test]
fn missing_include_targets_are_errors_at_the_include_line() {
    for (pattern, message) in [
        ("shared/missing.yaml", "does not exist"),
        ("shared/*.yaml", "matches no .yaml or .yml file"),
    ] {
        let ferry = format!("{FERRY}include:\n  - flows/orders.yaml\n  - {pattern}\n");
        let orders = flow("orders");
        let (_tmp, dir) = write(&[
            ("ferry.yaml", &ferry),
            ("connections.yaml", CONNECTIONS),
            ("flows/orders.yaml", &orders),
        ]);
        let error = only(load_err(&dir));
        assert_eq!(error.kind, ErrorKind::Include, "{pattern}");
        assert_eq!(line(&error), ("ferry.yaml".into(), 5), "{pattern}");
        assert_eq!(error.path, "include[1]");
        assert!(error.message.contains(message), "{error}");
    }
}

#[test]
fn non_yaml_files_and_dotfiles_under_flows_are_ignored() {
    let orders = flow("orders");
    let (_tmp, dir) = write(&[
        ("ferry.yaml", FERRY),
        ("connections.yaml", CONNECTIONS),
        ("flows/orders.yaml", &orders),
        ("flows/README.md", "not: [yaml"),
        ("flows/.orders.yaml.swp", "\u{0}\u{1}binary"),
        ("flows/.hidden.yaml", "not: [yaml"),
        ("flows/.git/config.yaml", "not: [yaml"),
    ]);
    let config = load_ok(&dir);
    assert_eq!(flow_names(&config), ["orders"]);
    assert_eq!(config.sources().len(), 3);
}

#[test]
fn a_config_without_flows_is_an_error() {
    // An empty flows/, no flows/ at all, and only empty `flows:` lists.
    let cases: [&[(&str, &str)]; 3] = [
        &[
            ("ferry.yaml", FERRY),
            ("connections.yaml", CONNECTIONS),
            ("flows/README.md", "x"),
        ],
        &[("ferry.yaml", FERRY), ("connections.yaml", CONNECTIONS)],
        &[
            ("ferry.yaml", FERRY),
            ("connections.yaml", CONNECTIONS),
            ("flows/a.yaml", "flows: []\n"),
        ],
    ];
    for files in cases {
        let (_tmp, dir) = write(files);
        let error = only(load_err(&dir));
        assert_eq!(error.kind, ErrorKind::Layout);
        assert_eq!(error.message, "the config has no flows");
    }
}

#[test]
fn an_empty_flows_list_is_allowed_next_to_real_flows() {
    let orders = flow("orders");
    let (_tmp, dir) = write(&[
        ("ferry.yaml", FERRY),
        ("connections.yaml", CONNECTIONS),
        ("flows/orders.yaml", &orders),
        ("flows/later.yaml", "# not written yet\nflows: []\n"),
    ]);
    assert_eq!(flow_names(&load_ok(&dir)), ["orders"]);
}

#[test]
fn an_empty_flow_file_is_an_error() {
    let orders = flow("orders");
    let (_tmp, dir) = write(&[
        ("ferry.yaml", FERRY),
        ("connections.yaml", CONNECTIONS),
        ("flows/orders.yaml", &orders),
        ("flows/returns.yaml", "# placeholder\n"),
    ]);
    let error = only(load_err(&dir));
    assert_eq!(error.kind, ErrorKind::Layout);
    assert_eq!(line(&error), ("flows/returns.yaml".into(), 1));
    assert!(error.help.unwrap().contains("flows: []"));
}

#[test]
fn flows_need_connections_yaml() {
    let orders = flow("orders");
    let (_tmp, dir) = write(&[("ferry.yaml", FERRY), ("flows/orders.yaml", &orders)]);
    let error = only(load_err(&dir));
    assert_eq!(error.kind, ErrorKind::Layout);
    assert_eq!(error.message, "connections.yaml is missing");
}

#[test]
fn a_duplicate_key_is_an_error_at_the_second_occurrence() {
    let error = only(load_err(&repo("examples/bad/duplicate-key/ferry")));
    assert_eq!(error.kind, ErrorKind::Yaml);
    assert_eq!(line(&error), ("flows/orders.yaml".into(), 6));
    assert_eq!(error.path, "flows[orders].mode");
}

#[test]
fn a_missing_ferry_yaml_names_the_full_path_it_expected() {
    let orders = flow("orders");
    let (_tmp, dir) = write(&[
        ("connections.yaml", CONNECTIONS),
        ("flows/orders.yaml", &orders),
    ]);
    let error = only(load_err(&dir));
    assert_eq!(error.kind, ErrorKind::Layout);
    let expected = std::path::absolute(dir.join("ferry.yaml")).unwrap();
    assert!(
        error.message.contains(&expected.display().to_string()),
        "{error}"
    );
}

#[test]
fn a_missing_directory_is_an_error() {
    let tmp = tempfile::tempdir().unwrap();
    let error = only(load_err(&tmp.path().join("nope")));
    assert_eq!(error.kind, ErrorKind::Io);
}

#[test]
fn yaml_and_yml_side_by_side_is_an_error() {
    let orders = flow("orders");
    let (_tmp, dir) = write(&[
        ("ferry.yaml", FERRY),
        ("connections.yaml", CONNECTIONS),
        ("connections.yml", CONNECTIONS),
        ("flows/orders.yaml", &orders),
    ]);
    let error = only(load_err(&dir));
    assert_eq!(error.kind, ErrorKind::Layout);
    assert!(
        error
            .message
            .contains("connections.yaml and connections.yml"),
        "{error}"
    );
}

#[test]
fn keys_in_the_wrong_file_get_a_hint() {
    let orders = flow("orders");
    let cases = [
        (
            "ferry.yaml",
            format!("{FERRY}flows: []\n"),
            "flows",
            "flows go in files under flows/",
        ),
        (
            "ferry.yaml",
            format!("{FERRY}connections: []\n"),
            "connections",
            "connections go in connections.yaml",
        ),
        (
            "connections.yaml",
            format!("{CONNECTIONS}runtime: {{}}\n"),
            "runtime",
            "runtime settings go in runtime.yaml",
        ),
        (
            "runtime.yaml",
            "runtime: {}\nflows: []\n".to_owned(),
            "flows",
            "flows go in files under flows/",
        ),
        (
            "flows/orders.yaml",
            format!("{orders}version: 1\n"),
            "version",
            "goes in ferry.yaml",
        ),
    ];
    for (file, text, key, hint) in cases {
        let mut files = vec![
            ("ferry.yaml", FERRY),
            ("connections.yaml", CONNECTIONS),
            ("flows/orders.yaml", orders.as_str()),
        ];
        files.retain(|(p, _)| *p != file);
        files.push((file, &text));
        let (_tmp, dir) = write(&files);
        let error = only(load_err(&dir));
        assert_eq!(error.path, key, "{file}");
        assert_eq!(error.location.as_ref().unwrap().file.as_ref(), file);
        assert!(
            error.help.as_deref().unwrap_or_default().contains(hint),
            "{file}: {error}"
        );
    }
}

#[test]
fn errors_from_several_files_are_all_reported() {
    let bad_one = flow("one").replace("sink: app", "snk: app");
    let bad_two = flow("two").replace("from: app.public.two", "from: two");
    let (_tmp, dir) = write(&[
        ("ferry.yaml", FERRY),
        ("connections.yaml", CONNECTIONS),
        ("flows/one.yaml", &bad_one),
        ("flows/two.yaml", &bad_two),
    ]);
    let errors = load_err(&dir);
    let files: Vec<String> = errors.iter().map(|e| line(e).0).collect();
    assert_eq!(files, ["flows/one.yaml", "flows/two.yaml"]);
}

#[test]
fn values_checked_after_reading_point_at_themselves_not_their_parent() {
    // An invalid name and an unsupported version are rejected once the value
    // is read. The error must still point at that value, not the map around
    // it.
    let orders = flow("orders").replace("name: orders", "name: Orders");
    let (_tmp, dir) = write(&[
        ("ferry.yaml", "# header\nname: test\nversion: 3\n"),
        ("connections.yaml", CONNECTIONS),
        ("flows/orders.yaml", &orders),
    ]);
    let errors = load_err(&dir);
    assert_eq!(errors.len(), 2, "{errors:#?}");
    assert_eq!(line(&errors[0]), ("ferry.yaml".into(), 3));
    assert_eq!(errors[0].path, "version");
    assert!(
        errors[0].message.contains("reads version 1"),
        "{}",
        errors[0]
    );
    assert_eq!(line(&errors[1]), ("flows/orders.yaml".into(), 2));
    assert_eq!(errors[1].location.as_ref().unwrap().column, 11);
    assert_eq!(errors[1].path, "flows[Orders].name");
}
