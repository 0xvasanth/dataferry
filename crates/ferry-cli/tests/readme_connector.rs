//! The README's "Adding a connector" example, compiled and registered.
//! `readme.rs` checks the README shows the code between the `readme:`
//! markers verbatim, so the example can't drift from the trait.

use std::ffi::OsString;
use std::process::ExitCode;

// readme:connector:start
use ferry_registry::{BuildContext, BuildError, Connector, Endpoints, Roles, Secret};
use schemars::JsonSchema;
use serde::Deserialize;

pub struct ClickHouse;

/// A ClickHouse database. Doc comments become editor hover text.
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ClickHouseOptions {
    /// `clickhouse://` URL. A `Secret` is redacted from everything printed.
    url: Secret,
    /// The database written to.
    database: String,
}

impl Connector for ClickHouse {
    const TYPE: &'static str = "clickhouse";
    const ROLES: Roles = Roles::BOTH;
    type Options = ClickHouseOptions;

    fn build(
        &self,
        ctx: &BuildContext<'_>,
        options: ClickHouseOptions,
        wants: Roles,
    ) -> Result<Endpoints, BuildError> {
        // No I/O here: `ferry validate` calls this. Keep the URL for the
        // endpoints, which connect on their first trait call.
        let ClickHouseOptions { url, database } = options;
        let _ = (ctx, url, database);
        todo!("the Source and/or Sink that {wants} asks for")
    }

    /// One line for `ferry plan` and the run report. No secrets.
    fn describe(&self, options: &ClickHouseOptions) -> String {
        format!("clickhouse, database {}", options.database)
    }
}
// readme:connector:end

#[allow(dead_code)]
// readme:main:start
fn main() -> std::process::ExitCode {
    ferry_cli::Ferry::with_builtins()
        .register(ClickHouse)
        .name("acme-ferry")
        .main()
}
// readme:main:end

fn run(dir: &std::path::Path, args: &[&str]) -> (ExitCode, String, String) {
    let mut out = Vec::new();
    let mut err = Vec::new();
    let args = ["acme-ferry", "--color", "never", "--config"]
        .into_iter()
        .map(OsString::from)
        .chain([dir.as_os_str().to_owned()])
        .chain(args.iter().map(OsString::from));
    let code = ferry_cli::Ferry::with_builtins()
        .register(ClickHouse)
        .name("acme-ferry")
        .run_with(args, &mut out, &mut err);
    (
        code,
        String::from_utf8(out).unwrap(),
        String::from_utf8(err).unwrap(),
    )
}

#[test]
fn the_readme_connector_registers_its_type_and_options() {
    let dir = tempfile::tempdir().unwrap();
    let (code, stdout, stderr) = run(dir.path(), &["schema"]);
    assert_eq!(code, ExitCode::SUCCESS, "{stderr}");
    let schemas: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    let branches = schemas["connections.schema.json"]["$defs"]["Connection"]["allOf"][1]["oneOf"]
        .as_array()
        .unwrap();
    let clickhouse = branches
        .iter()
        .find(|b| b["properties"]["type"]["const"] == "clickhouse")
        .expect("a clickhouse branch next to the built-ins");
    assert_eq!(clickhouse["properties"]["url"]["x-ferry-secret"], true);
    assert_eq!(clickhouse["additionalProperties"], false);
    assert_eq!(branches.len(), 3, "postgres, mongo and clickhouse");
}

#[test]
fn the_readme_connector_rejects_an_unknown_field_before_building() {
    let dir = tempfile::tempdir().unwrap();
    let ferry = dir.path().join("ferry");
    std::fs::create_dir_all(ferry.join("flows")).unwrap();
    std::fs::write(ferry.join("ferry.yaml"), "version: 1\nname: demo\n").unwrap();
    std::fs::write(
        ferry.join("connections.yaml"),
        "connections:\n  - name: app\n    type: postgres\n    url: postgres://192.0.2.1/app\n  \
         - name: olap\n    type: clickhouse\n    url: clickhouse://192.0.2.1\n    database: olap\n    \
         hots: typo\n",
    )
    .unwrap();
    std::fs::write(ferry.join("runtime.yaml"), "runtime: {}\n").unwrap();
    std::fs::write(
        ferry.join("flows/copy.yaml"),
        "flows:\n  - name: copy\n    from: app.public.t\n    sink: olap\n    to:\n      t:\n        columns: \"*\"\n",
    )
    .unwrap();
    // `build` is `todo!()`: reaching it would panic, so a clean exit 3
    // means the options were checked first.
    let (code, _, stderr) = run(&ferry, &["validate"]);
    assert_eq!(code, ExitCode::from(3), "{stderr}");
    assert!(stderr.contains("hots"), "{stderr}");
    assert!(stderr.contains("F0301"), "{stderr}");
}
