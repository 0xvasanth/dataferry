//! A CLI built from `dataferry::Ferry` in-process, through `run_with`:
//! registration mistakes, and the schema the registered types compose.

mod common;

use std::ffi::OsString;
use std::process::ExitCode;

use acme_dataferry::{Backend, Mongo, Postgres};
use common::{acme, repo, run};
use dataferry::{Exit, Ferry};
use serde_json::Value;

fn run_with(ferry: Ferry, args: &[&str]) -> (ExitCode, String, String) {
    let args: Vec<OsString> = std::iter::once("acme-dataferry")
        .chain(args.iter().copied())
        .map(Into::into)
        .collect();
    let (mut out, mut err) = (Vec::new(), Vec::new());
    let code = ferry.run_with(args, &mut out, &mut err);
    (
        code,
        String::from_utf8(out).unwrap(),
        String::from_utf8(err).unwrap(),
    )
}

#[test]
fn registering_postgres_twice_fails_at_startup_naming_the_type() {
    let twice = || {
        Ferry::new()
            .register_state_capable(Postgres, Backend)
            .register(Postgres)
    };
    // Every command, even one that wouldn't touch the registry.
    for args in [&["validate"][..], &["schema"], &["--help"]] {
        let (code, out, err) = run_with(twice(), args);
        assert_eq!(code, ExitCode::from(Exit::Internal), "{args:?}");
        assert_eq!(out, "");
        assert!(
            err.starts_with(
                "error[F0002]: this binary is miswired: connector type `postgres` is already registered\n"
            ),
            "{err}"
        );
    }
}

#[test]
fn replacing_a_type_that_isnt_registered_is_a_wiring_error_too() {
    let (code, _, err) = run_with(Ferry::new().replace(Mongo), &["validate"]);
    assert_eq!(code, ExitCode::from(Exit::Internal));
    assert!(
        err.contains("connector type `mongo` is not registered, so it can't be replaced"),
        "{err}"
    );
    // Replacing a registered one on purpose is fine.
    let (code, _, err) = run_with(
        Ferry::new().register(Mongo).replace(Mongo),
        &[
            "--config",
            repo("examples/golden/ferry").to_str().unwrap(),
            "schema",
        ],
    );
    assert_eq!(code, ExitCode::from(Exit::Success), "{err}");
}

#[test]
fn name_and_version_show_up_in_help_and_errors() {
    let ferry = || Ferry::new().name("acme-dataferry").version("9.9.9");
    let (code, out, _) = run_with(ferry(), &["--version"]);
    assert_eq!(code, ExitCode::from(Exit::Success));
    assert_eq!(out, "acme-dataferry 9.9.9\n");
    let (code, _, err) = run_with(ferry(), &["resume"]);
    assert_eq!(code, ExitCode::from(Exit::Usage));
    assert!(
        err.contains("Usage: acme-dataferry resume <RUN_ID>"),
        "{err}"
    );
}

fn schemas(stdout: &str) -> Value {
    serde_json::from_str(stdout).expect("dataferry schema prints one JSON object")
}

/// The `type` const of every `connections` branch.
fn branch_types(connections: &Value) -> Vec<String> {
    let connection = &connections["$defs"]["Connection"];
    connection["allOf"][1]["oneOf"]
        .as_array()
        .unwrap_or_else(|| panic!("no oneOf in {connection}"))
        .iter()
        .map(|b| {
            b["properties"]["type"]["const"]
                .as_str()
                .unwrap()
                .to_owned()
        })
        .collect()
}

#[test]
fn schema_has_one_connections_branch_per_registered_type() {
    let out = run(acme().arg("schema"));
    assert_eq!(out.code, 0, "{}", out.stderr);
    let all = schemas(&out.stdout);
    let keys: Vec<&String> = all.as_object().unwrap().keys().collect();
    assert_eq!(keys.len(), 4, "one schema per config file kind: {keys:?}");
    assert_eq!(
        branch_types(&all["connections.schema.json"]),
        ["fake", "mongo", "postgres"]
    );

    // Built from the lib with one type fewer, the schema has one branch fewer.
    let (code, out, _) = run_with(Ferry::new().register(Mongo), &["schema"]);
    assert_eq!(code, ExitCode::from(Exit::Success));
    assert_eq!(
        branch_types(&schemas(&out)["connections.schema.json"]),
        ["mongo"]
    );
}

/// The composed `connections.yaml` schema, ready to validate documents.
fn connections_validator() -> jsonschema::Validator {
    let out = run(acme().arg("schema"));
    let schema = schemas(&out.stdout)["connections.schema.json"].clone();
    jsonschema::validator_for(&schema).expect("the composed schema is a valid JSON Schema")
}

fn connections_json(text: &str) -> Value {
    dataferry_dsl::config::file_to_json("connections.yaml", text).unwrap()
}

#[test]
fn the_composed_schema_accepts_the_golden_connections_and_flags_typos() {
    let validator = connections_validator();
    let golden = std::fs::read_to_string(repo("examples/golden/ferry/connections.yaml")).unwrap();
    assert!(validator.is_valid(&connections_json(&golden)));

    // What an editor flags as you type: an unknown field, a missing
    // required one, a type nobody registered, a reserved key.
    for (from, to) in [
        ("database: shop", "databse: shop"),
        ("    database: shop\n", ""),
        ("type: postgres", "type: postgresql"),
        ("database: shop", "database: shop\n    description: x"),
    ] {
        let bad = golden.replace(from, to);
        assert_ne!(bad, golden);
        assert!(
            !validator.is_valid(&connections_json(&bad)),
            "accepted: {bad}"
        );
    }
    // `name` keeps B1's pattern inside every branch.
    let bad = golden.replace("name: shop", "name: Shop");
    assert!(!validator.is_valid(&connections_json(&bad)));
}

#[test]
fn schema_out_writes_the_composed_connections_file() {
    let dir = tempfile::tempdir().unwrap();
    let out = run(acme().arg("schema").arg("--out").arg(dir.path()));
    assert_eq!(out.code, 0, "{}", out.stderr);
    assert_eq!(out.stdout.lines().count(), 4, "{}", out.stdout);
    let file = std::fs::read_to_string(dir.path().join("connections.schema.json")).unwrap();
    let connections: Value = serde_json::from_str(&file).unwrap();
    assert_eq!(branch_types(&connections), ["fake", "mongo", "postgres"]);
    // A secret option is marked for editors.
    let postgres = &connections["$defs"]["Connection"]["allOf"][1]["oneOf"][2];
    assert_eq!(
        postgres["properties"]["url"]["x-ferry-secret"], true,
        "{postgres}"
    );
}

#[test]
fn a_type_named_after_a_b1_definition_gets_its_own() {
    // `Ident` is one of B1's definitions in connections.schema.json; a
    // connector whose options define their own `Ident` must not clobber it.
    use dataferry_registry::{BuildContext, BuildError, Connector, Endpoints, Roles};

    #[derive(serde::Deserialize, schemars::JsonSchema)]
    struct Ident {
        #[allow(dead_code)]
        part: String,
    }

    #[derive(serde::Deserialize, schemars::JsonSchema)]
    #[serde(deny_unknown_fields)]
    struct Opts {
        #[allow(dead_code)]
        id: Ident,
    }

    struct Clash;
    impl Connector for Clash {
        const TYPE: &'static str = "clash";
        const ROLES: Roles = Roles::SOURCE;
        type Options = Opts;
        fn build(&self, _: &BuildContext<'_>, _: Opts, _: Roles) -> Result<Endpoints, BuildError> {
            Ok(Endpoints::default())
        }
        fn describe(&self, _: &Opts) -> String {
            String::new()
        }
    }

    let (code, out, _) = run_with(Ferry::new().register(Clash), &["schema"]);
    assert_eq!(code, ExitCode::from(Exit::Success));
    let connections = schemas(&out)["connections.schema.json"].clone();
    let defs = &connections["$defs"];
    assert_eq!(defs["Ident"]["pattern"], "^[a-z][a-z0-9_]*$", "{defs}");
    assert_eq!(
        defs["Ident_"]["properties"]["part"]["type"], "string",
        "{defs}"
    );
    let validator = jsonschema::validator_for(&connections).unwrap();
    let doc =
        serde_json::json!({"connections": [{"name": "c", "type": "clash", "id": {"part": "x"}}]});
    assert!(validator.is_valid(&doc));
    let doc = serde_json::json!({"connections": [{"name": "c", "type": "clash", "id": "x"}]});
    assert!(!validator.is_valid(&doc));
}

#[test]
fn no_registered_types_means_no_connection_entry_is_valid() {
    let (code, out, _) = run_with(Ferry::new(), &["schema"]);
    assert_eq!(code, ExitCode::from(Exit::Success));
    let connections = schemas(&out)["connections.schema.json"].clone();
    let validator = jsonschema::validator_for(&connections).unwrap();
    assert!(validator.is_valid(&serde_json::json!({"connections": []})));
    assert!(
        !validator
            .is_valid(&serde_json::json!({"connections": [{"name": "a", "type": "postgres"}]}))
    );
}
