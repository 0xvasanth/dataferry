//! `connections_schema()`: one `oneOf` branch per registered type, checked
//! with a real JSON Schema validator against the golden `connections.yaml`.
//!
//! The postgres and mongo connectors here only have the option shapes 0004
//! §2 gives the built-ins; the real ones register in A1/A2/A3.

use std::path::Path;
use std::sync::Arc;

use dataferry_core::IdempotencyStrategy;
use dataferry_core::testkit::{MemSink, MemSource};
use dataferry_registry::{
    BuildContext, BuildError, Connector, ConnectorRegistry, Endpoints, RawConnection, Roles, Secret,
};
use schemars::{JsonSchema, SchemaGenerator};
use serde::Deserialize;
use serde_json::{Value, json};

struct PgLike;

/// A PostgreSQL database.
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[allow(dead_code)]
struct PgOptions {
    /// libpq URL or key=value string; `sslmode` goes here.
    url: Secret,
    /// Overrides a password in `url`.
    password: Option<Secret>,
    /// Read from as a source, written to as a sink.
    #[serde(default = "public")]
    schema: String,
}

fn public() -> String {
    "public".into()
}

impl Connector for PgLike {
    const TYPE: &'static str = "postgres";
    const ROLES: Roles = Roles::BOTH;
    type Options = PgOptions;

    fn build(
        &self,
        _: &BuildContext<'_>,
        _: PgOptions,
        wants: Roles,
    ) -> Result<Endpoints, BuildError> {
        Ok(Endpoints {
            source: wants.source.then(|| Arc::new(MemSource::new()) as _),
            sink: wants.sink.then(|| {
                Arc::new(MemSink::new(vec![
                    IdempotencyStrategy::DeletePartitionThenInsert,
                ])) as _
            }),
            finalizer: None,
            ddl: None,
        })
    }

    fn describe(&self, o: &PgOptions) -> String {
        format!("postgres, schema {}", o.schema)
    }
}

struct MongoLike;

/// A MongoDB database.
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[allow(dead_code)]
struct MongoOptions {
    /// mongodb:// or mongodb+srv://
    url: Secret,
    database: String,
}

impl Connector for MongoLike {
    const TYPE: &'static str = "mongo";
    const ROLES: Roles = Roles::SOURCE;
    type Options = MongoOptions;

    fn build(
        &self,
        _: &BuildContext<'_>,
        _: MongoOptions,
        _: Roles,
    ) -> Result<Endpoints, BuildError> {
        Ok(Endpoints {
            source: Some(Arc::new(MemSource::new())),
            sink: None,
            finalizer: None,
            ddl: None,
        })
    }

    fn describe(&self, o: &MongoOptions) -> String {
        format!("mongo, database {}", o.database)
    }
}

/// Typed, non-string options, to check `${ENV}` widening.
struct Tuned;

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[allow(dead_code)]
struct TunedOptions {
    /// TCP port.
    port: u16,
    #[serde(default)]
    compress: bool,
    timeout_secs: Option<f64>,
    mode: Mode,
    tls: Option<Tls>,
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
#[allow(dead_code)]
enum Mode {
    Fast,
    Safe,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[allow(dead_code)]
struct Tls {
    verify: bool,
}

impl Connector for Tuned {
    const TYPE: &'static str = "tuned";
    const ROLES: Roles = Roles::SOURCE;
    type Options = TunedOptions;

    fn build(
        &self,
        _: &BuildContext<'_>,
        _: TunedOptions,
        _: Roles,
    ) -> Result<Endpoints, BuildError> {
        Ok(Endpoints {
            source: Some(Arc::new(MemSource::new())),
            sink: None,
            finalizer: None,
            ddl: None,
        })
    }

    fn describe(&self, _: &TunedOptions) -> String {
        "tuned".into()
    }
}

fn builtins_shaped() -> ConnectorRegistry {
    let mut registry = ConnectorRegistry::new();
    registry.register(PgLike).register(MongoLike);
    registry
}

fn validator(registry: &ConnectorRegistry) -> jsonschema::Validator {
    jsonschema::validator_for(registry.connections_file_schema().as_value())
        .expect("the composed schema is itself valid JSON Schema")
}

fn golden() -> Value {
    let path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/golden/ferry/connections.yaml");
    serde_norway::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

fn connections(entries: Value) -> Value {
    json!({ "connections": entries })
}

fn errors(validator: &jsonschema::Validator, instance: &Value) -> Vec<String> {
    validator
        .iter_errors(instance)
        .map(|e| format!("{} at {}", e, e.instance_path()))
        .collect()
}

#[test]
fn one_branch_per_registered_type() {
    let mut registry = builtins_shaped();
    registry.register(Tuned);
    let schema = registry.connections_schema(&mut SchemaGenerator::default());
    let branches = schema.get("oneOf").and_then(Value::as_array).unwrap();
    let types: Vec<_> = branches
        .iter()
        .map(|b| b["properties"]["type"]["const"].as_str().unwrap())
        .collect();
    assert_eq!(types, ["mongo", "postgres", "tuned"]);
    for branch in branches {
        assert_eq!(branch["additionalProperties"], false, "{branch}");
        let required = branch["required"].as_array().unwrap();
        assert_eq!(required[..2], [json!("name"), json!("type")]);
    }
}

#[test]
fn golden_connections_validate() {
    let validator = validator(&builtins_shaped());
    let golden = golden();
    assert_eq!(errors(&validator, &golden), Vec::<String>::new());
}

/// The schema and the Rust type must agree: every golden entry the schema
/// accepts also deserializes and builds (after `${ENV}` substitution, which
/// B1 does).
#[test]
fn golden_connections_build_through_the_registry() {
    let registry = builtins_shaped();
    let golden = golden();
    for entry in golden["connections"].as_array().unwrap() {
        let mut raw: RawConnection = serde_json::from_value(entry.clone()).unwrap();
        for value in raw.fields.values_mut() {
            if value.as_str().is_some_and(|s| s.starts_with("${")) {
                *value = json!("scheme://user:pw@192.0.2.1/db");
            }
        }
        let built = registry.build(&raw, Roles::SOURCE).unwrap();
        assert!(built.endpoints.source.is_some());
    }
}

#[test]
fn an_unknown_field_is_rejected() {
    let validator = validator(&builtins_shaped());
    let typo = connections(json!([
        { "name": "warehouse", "type": "postgres", "url": "${PG_URL}", "shcema": "public" }
    ]));
    assert!(!validator.is_valid(&typo));

    // The same entry with the field spelled right is fine.
    let fixed = connections(json!([
        { "name": "warehouse", "type": "postgres", "url": "${PG_URL}", "schema": "public" }
    ]));
    assert_eq!(errors(&validator, &fixed), Vec::<String>::new());

    // A field of another type's branch is unknown here.
    let crossed = connections(json!([
        { "name": "warehouse", "type": "postgres", "url": "${PG_URL}", "database": "shop" }
    ]));
    assert!(!validator.is_valid(&crossed));
}

#[test]
fn entries_that_break_0003_section_3_are_rejected() {
    let validator = validator(&builtins_shaped());
    let cases = [
        (
            "unknown type",
            json!({ "name": "x", "type": "fake", "url": "u" }),
        ),
        (
            "no type",
            json!({ "name": "x", "url": "u", "database": "d" }),
        ),
        (
            "no name",
            json!({ "type": "mongo", "url": "u", "database": "d" }),
        ),
        (
            "bad name",
            json!({ "name": "Shop", "type": "mongo", "url": "u", "database": "d" }),
        ),
        // Reserved, and undefined in v0.1.
        (
            "description",
            json!({ "name": "x", "type": "mongo", "url": "u", "database": "d", "description": "hi" }),
        ),
        (
            "missing required option",
            json!({ "name": "x", "type": "mongo", "url": "u" }),
        ),
        (
            "secret not a string",
            json!({ "name": "x", "type": "mongo", "url": 5, "database": "d" }),
        ),
    ];
    for (case, entry) in cases {
        assert!(
            !validator.is_valid(&connections(json!([entry]))),
            "{case} was accepted"
        );
    }
}

#[test]
fn secret_fields_are_marked_in_the_schema() {
    let schema = builtins_shaped().connections_file_schema();
    let branches = schema
        .pointer("/properties/connections/items/oneOf")
        .unwrap();
    let pg = &branches[1];
    assert_eq!(pg["properties"]["url"]["x-ferry-secret"], true);
    assert_eq!(pg["properties"]["url"]["writeOnly"], true);
    assert_eq!(pg["properties"]["schema"]["default"], "public");
    // Doc comments become hover text.
    assert_eq!(pg["description"], "A PostgreSQL database.");
    assert!(
        pg["properties"]["url"]["description"]
            .as_str()
            .unwrap()
            .contains("sslmode")
    );
}

#[test]
fn non_string_scalars_also_accept_an_env_reference() {
    let mut registry = ConnectorRegistry::new();
    registry.register(Tuned);
    let validator = validator(&registry);
    let entry = |fields: Value| {
        let mut e = json!({ "name": "t", "type": "tuned", "mode": "fast" });
        e.as_object_mut()
            .unwrap()
            .extend(fields.as_object().unwrap().clone());
        connections(json!([e]))
    };

    for ok in [
        json!({ "port": 5432 }),
        json!({ "port": "${PG_PORT}" }),
        json!({ "port": 1, "compress": "${COMPRESS}", "timeout_secs": "${T}" }),
        json!({ "port": 1, "timeout_secs": null }),
        // Nested definitions are widened too.
        json!({ "port": 1, "tls": { "verify": "${VERIFY}" } }),
    ] {
        assert_eq!(
            errors(&validator, &entry(ok.clone())),
            Vec::<String>::new(),
            "{ok}"
        );
    }
    for bad in [
        json!({ "port": "5432" }),
        json!({ "port": "${PG PORT}" }),
        json!({ "port": "prefix-${PG_PORT}" }),
        json!({ "port": 70000 }),
        json!({ "port": 1, "tls": { "verify": "yes" } }),
    ] {
        assert!(
            !validator.is_valid(&entry(bad.clone())),
            "{bad} was accepted"
        );
    }
    // A string enum isn't widened: `${MODE}` is already a string.
    assert!(!validator.is_valid(&connections(json!([
        { "name": "t", "type": "tuned", "port": 1, "mode": "${MODE}" }
    ]))));
}

#[test]
fn widening_keeps_hover_text_on_the_field() {
    let mut registry = ConnectorRegistry::new();
    registry.register(Tuned);
    let schema = registry.connections_schema(&mut SchemaGenerator::default());
    let port = schema.pointer("/oneOf/0/properties/port").unwrap();
    assert_eq!(port["description"], "TCP port.");
    assert_eq!(port["anyOf"].as_array().unwrap().len(), 2);
}

#[test]
fn an_empty_registry_accepts_no_connection() {
    let validator = validator(&ConnectorRegistry::new());
    assert!(validator.is_valid(&connections(json!([]))));
    assert!(!validator.is_valid(&connections(json!([{ "name": "x", "type": "postgres" }]))));
}
