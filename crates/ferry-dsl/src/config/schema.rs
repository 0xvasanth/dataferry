//! JSON Schema for each kind of config file, generated from the model so
//! the two can't drift (0003, 0004 §3).
//!
//! There is one document per file kind. `connections` items are left open:
//! each connector type's fields come from the registry, which composes the
//! full schema for a given binary (`ferry schema`, 0004 §3). The documents
//! for this grammar version are checked in under `schemas/v1/`, and a test
//! fails when they no longer match what the model generates.

use schemars::generate::SchemaSettings;
use schemars::{JsonSchema, Schema};
use serde_json::{Map, Value};

use super::load::FileKind;
use super::model::{ConnectionsFile, FerryFile, FlowsFile, RuntimeFile, VERSION};

/// The schema documents, by file name under `schemas/v<VERSION>/`.
pub fn documents() -> Vec<(&'static str, Value)> {
    [
        FileKind::Ferry,
        FileKind::Connections,
        FileKind::Runtime,
        FileKind::Flows,
    ]
    .into_iter()
    .filter_map(|kind| Some((file_name(kind)?, schema(kind)?)))
    .collect()
}

/// The directory the documents live in, relative to the repository root.
pub fn directory() -> String {
    format!("schemas/v{VERSION}")
}

fn file_name(kind: FileKind) -> Option<&'static str> {
    match kind {
        FileKind::Ferry => Some("ferry.schema.json"),
        FileKind::Connections => Some("connections.schema.json"),
        FileKind::Runtime => Some("runtime.schema.json"),
        FileKind::Flows => Some("flows.schema.json"),
        FileKind::Included => None,
    }
}

/// The schema for one file kind. Included files have none of their own:
/// they hold `flows:` or `connections:` and match those schemas.
pub fn schema(kind: FileKind) -> Option<Value> {
    let (schema, applies_to) = match kind {
        FileKind::Ferry => (generate::<FerryFile>(), "ferry/ferry.yaml"),
        FileKind::Connections => (generate::<ConnectionsFile>(), "ferry/connections.yaml"),
        FileKind::Runtime => (generate::<RuntimeFile>(), "ferry/runtime.yaml"),
        FileKind::Flows => (generate::<FlowsFile>(), "ferry/flows/**/*.yaml and *.yml"),
        FileKind::Included => return None,
    };
    let mut value = schema.to_value();
    if let Value::Object(map) = &mut value {
        map.insert(
            "description".into(),
            Value::String(format!(
                "0xferry config grammar version {VERSION}: {applies_to} (decision 0003)."
            )),
        );
    }
    Some(sorted(value))
}

fn generate<T: JsonSchema>() -> Schema {
    SchemaSettings::draft2020_12()
        .for_deserialize()
        .into_generator()
        .into_root_schema_for::<T>()
}

/// Object keys in sorted order at every level, so the checked-in files are
/// byte-stable whatever map ordering `serde_json` was built with.
fn sorted(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut entries: Vec<(String, Value)> = map.into_iter().collect();
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            Value::Object(
                entries
                    .into_iter()
                    .map(|(k, v)| (k, sorted(v)))
                    .collect::<Map<_, _>>(),
            )
        }
        Value::Array(items) => Value::Array(items.into_iter().map(sorted).collect()),
        other => other,
    }
}

/// A document as it is checked in: pretty JSON with a trailing newline.
pub fn render(value: &Value) -> String {
    let mut text = serde_json::to_string_pretty(value).expect("a schema serializes");
    text.push('\n');
    text
}
