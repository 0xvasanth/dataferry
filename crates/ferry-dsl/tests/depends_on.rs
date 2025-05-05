//! B1d: `depends_on:` between flows (0003 §7).

use std::path::PathBuf;

use ferry_dsl::config::{Config, ConfigError, ErrorKind, load};

const FERRY: &str = "version: 1\nname: test\n";
const CONNECTIONS: &str = "connections:\n  - name: app\n    type: postgres\n    url: ${APP_URL}\n";

/// A flow named `name` waiting for `deps`, as a one-flow file.
fn flow(name: &str, deps: &[&str]) -> String {
    let deps = match deps {
        [] => String::new(),
        deps => format!("    depends_on: [{}]\n", deps.join(", ")),
    };
    format!(
        "flows:\n  - name: {name}\n    from: app.public.{name}\n    sink: app\n{deps}    to:\n      {name}:\n        columns: \"*\"\n"
    )
}

fn write(files: &[(&str, String)]) -> (tempfile::TempDir, PathBuf) {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("ferry");
    let base = [
        ("ferry.yaml", FERRY.to_owned()),
        ("connections.yaml", CONNECTIONS.to_owned()),
    ];
    for (path, text) in base.iter().chain(files) {
        let path = dir.join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, text).unwrap();
    }
    (tmp, dir)
}

fn load_ok(files: &[(&str, String)]) -> Config {
    let (_tmp, dir) = write(files);
    load(&dir).unwrap_or_else(|e| panic!("{e}"))
}

fn load_err(files: &[(&str, String)]) -> ConfigError {
    let (_tmp, dir) = write(files);
    let errors = load(&dir).expect_err("expected an error").0;
    assert_eq!(errors.len(), 1, "{errors:#?}");
    errors.into_iter().next().unwrap()
}

fn edges(config: &Config) -> Vec<(String, String)> {
    config
        .dependency_graph()
        .edges()
        .map(|(a, b)| (a.to_owned(), b.to_owned()))
        .collect()
}

#[test]
fn a_dependency_across_files_reaches_the_graph_wherever_the_flows_live() {
    let one = load_ok(&[
        ("flows/customers.yaml", flow("customers", &[])),
        ("flows/sales/q3/orders.yml", flow("orders", &["customers"])),
    ]);
    let two = load_ok(&[(
        "flows/all.yaml",
        format!(
            "{}{}",
            flow("orders", &["customers"]),
            &flow("customers", &[])["flows:\n".len()..]
        ),
    )]);
    let three = load_ok(&[
        ("flows/x/customers.yml", flow("customers", &[])),
        ("flows/orders.yaml", flow("orders", &["customers"])),
    ]);
    let expected = vec![("orders".to_owned(), "customers".to_owned())];
    assert_eq!(edges(&one), expected);
    assert_eq!(one.dependency_graph(), two.dependency_graph());
    assert_eq!(one.dependency_graph(), three.dependency_graph());
    let graph = one.dependency_graph();
    assert_eq!(graph.flows().collect::<Vec<_>>(), ["customers", "orders"]);
    assert_eq!(
        graph.dependencies("orders").collect::<Vec<_>>(),
        ["customers"]
    );
    assert_eq!(graph.dependencies("customers").count(), 0);
}

#[test]
fn a_dependency_on_an_included_flow_resolves() {
    let ferry = format!("{FERRY}include: [../shared/*.yaml]\n");
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("ferry");
    std::fs::create_dir_all(dir.join("flows")).unwrap();
    std::fs::create_dir_all(tmp.path().join("shared")).unwrap();
    std::fs::write(dir.join("ferry.yaml"), ferry).unwrap();
    std::fs::write(dir.join("connections.yaml"), CONNECTIONS).unwrap();
    std::fs::write(
        dir.join("flows/orders.yaml"),
        flow("orders", &["customers"]),
    )
    .unwrap();
    std::fs::write(
        tmp.path().join("shared/customers.yaml"),
        flow("customers", &[]),
    )
    .unwrap();
    let config = load(&dir).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(
        edges(&config),
        [("orders".to_owned(), "customers".to_owned())]
    );
}

#[test]
fn a_cycle_of_two_is_one_error_listing_it() {
    let error = load_err(&[
        ("flows/a.yaml", flow("alpha", &["bravo"])),
        ("flows/sub/b.yml", flow("bravo", &["alpha"])),
    ]);
    assert_eq!(error.kind, ErrorKind::DependsOn);
    assert_eq!(
        error.message,
        "`depends_on` forms a cycle: alpha (flows/a.yaml:2:11) → bravo (flows/sub/b.yml:2:11) → alpha"
    );
    let at = error.location.as_ref().unwrap();
    assert_eq!((&*at.file, at.line), ("flows/a.yaml", 5));
    assert_eq!(error.path, "flows[alpha].depends_on[0]");
}

#[test]
fn a_cycle_of_three_across_three_files_lists_every_flow_and_location() {
    let error = load_err(&[
        ("flows/c.yaml", flow("charlie", &["alpha"])),
        ("flows/a.yaml", flow("alpha", &["bravo"])),
        ("flows/nested/b.yaml", flow("bravo", &["charlie"])),
        ("flows/d.yaml", flow("delta", &["alpha"])),
    ]);
    assert_eq!(
        error.message,
        "`depends_on` forms a cycle: alpha (flows/a.yaml:2:11) → bravo (flows/nested/b.yaml:2:11) → charlie (flows/c.yaml:2:11) → alpha"
    );
    assert_eq!(error.path, "flows[alpha].depends_on[0]");
}

#[test]
fn two_separate_cycles_are_two_errors() {
    let (_tmp, dir) = write(&[
        ("flows/a.yaml", flow("a", &["b"])),
        ("flows/b.yaml", flow("b", &["a"])),
        ("flows/x.yaml", flow("x", &["y"])),
        ("flows/y.yaml", flow("y", &["x"])),
    ]);
    let errors = load(&dir).unwrap_err().0;
    let messages: Vec<&str> = errors.iter().map(|e| e.message.as_str()).collect();
    assert_eq!(messages.len(), 2, "{messages:#?}");
    assert!(
        messages[0].contains("a (flows/a.yaml:2:11) → b"),
        "{}",
        messages[0]
    );
    assert!(
        messages[1].contains("x (flows/x.yaml:2:11) → y"),
        "{}",
        messages[1]
    );
}

#[test]
fn a_diamond_is_accepted() {
    let config = load_ok(&[
        ("flows/a.yaml", flow("a", &["b", "c"])),
        ("flows/b.yaml", flow("b", &["d"])),
        ("flows/c.yaml", flow("c", &["d"])),
        ("flows/d.yaml", flow("d", &[])),
    ]);
    assert_eq!(edges(&config).len(), 4);
}

#[test]
fn a_typo_points_at_the_entry_and_suggests_the_name() {
    let error = load_err(&[
        ("flows/customers.yaml", flow("customers", &[])),
        (
            "flows/orders.yaml",
            flow("orders", &["customer", "customers"]),
        ),
    ]);
    assert_eq!(error.kind, ErrorKind::DependsOn);
    assert_eq!(error.message, "no flow named `customer`");
    let at = error.location.as_ref().unwrap();
    assert_eq!(
        (&*at.file, at.line, at.column),
        ("flows/orders.yaml", 5, 18)
    );
    assert_eq!(error.path, "flows[orders].depends_on[0]");
    assert_eq!(error.help.as_deref(), Some("did you mean `customers`?"));
}

#[test]
fn depending_on_itself_is_an_error() {
    let error = load_err(&[("flows/orders.yaml", flow("orders", &["orders"]))]);
    assert_eq!(error.message, "flow `orders` depends on itself");
    assert_eq!(error.path, "flows[orders].depends_on[0]");
}

#[test]
fn a_name_listed_twice_is_an_error_at_the_second() {
    let error = load_err(&[
        ("flows/customers.yaml", flow("customers", &[])),
        (
            "flows/orders.yaml",
            flow("orders", &["customers", "customers"]),
        ),
    ]);
    assert_eq!(error.message, "`customers` is listed twice");
    assert_eq!(error.path, "flows[orders].depends_on[1]");
}
