//! `dataferry schema`: the JSON Schema of every config file, for this binary
//! (0004 §3). B1 generates each file's schema with `connections` items left
//! open; this fills them with one branch per registered connection type, so
//! a third-party binary's schema covers its own connectors.

use schemars::SchemaGenerator;
use serde_json::{Map, Value, json};

use dataferry_dsl::config::schema as base;
use dataferry_registry::ConnectorRegistry;

const CONNECTIONS: &str = "connections.schema.json";

/// `(file name, schema)` for every config file kind.
pub(crate) fn documents(registry: &ConnectorRegistry) -> Vec<(&'static str, Value)> {
    base::documents()
        .into_iter()
        .map(|(name, doc)| match name {
            CONNECTIONS => (name, with_connections(doc, registry)),
            _ => (name, doc),
        })
        .collect()
}

/// B1's `Connection` definition (`name`, `type`, no `description`) still
/// applies; each entry must also match one registered type's branch.
fn with_connections(mut doc: Value, registry: &ConnectorRegistry) -> Value {
    let mut generator = SchemaGenerator::default();
    let mut items = registry.connections_schema(&mut generator).to_value();
    let mut added = generator.take_definitions(true);

    let defs = doc
        .as_object_mut()
        .expect("a schema document is an object")
        .entry("$defs")
        .or_insert_with(|| Value::Object(Map::new()))
        .as_object_mut()
        .expect("$defs is an object");
    // A connector's type may share a name with one of B1's definitions:
    // move the connector's aside and point its references there.
    let taken: Vec<String> = added
        .keys()
        .filter(|name| defs.contains_key(*name))
        .cloned()
        .collect();
    for name in taken {
        let mut renamed = format!("{name}_");
        while defs.contains_key(&renamed) || added.contains_key(&renamed) {
            renamed.push('_');
        }
        let from = format!("#/$defs/{name}");
        let to = format!("#/$defs/{renamed}");
        rename_refs(&mut items, &from, &to);
        for def in added.values_mut() {
            rename_refs(def, &from, &to);
        }
        let def = added.remove(&name).expect("listed above");
        added.insert(renamed, def);
    }
    let open = defs
        .remove("Connection")
        .expect("B1's connections schema defines Connection");
    let description = open.get("description").cloned();
    let mut connection = json!({ "allOf": [open, items] });
    if let Some(description) = description {
        connection["description"] = description;
    }
    defs.insert("Connection".into(), connection);
    defs.extend(added);
    doc
}

fn rename_refs(value: &mut Value, from: &str, to: &str) {
    match value {
        Value::Object(map) => {
            for (key, v) in map.iter_mut() {
                match v {
                    Value::String(s) if key == "$ref" && s == from => *s = to.to_owned(),
                    _ => rename_refs(v, from, to),
                }
            }
        }
        Value::Array(items) => items.iter_mut().for_each(|v| rename_refs(v, from, to)),
        _ => {}
    }
}

/// Every document in one JSON object, by file name: what `dataferry schema`
/// prints without `--out`.
pub(crate) fn render_all(documents: &[(&'static str, Value)]) -> String {
    let all: Map<String, Value> = documents
        .iter()
        .map(|(name, doc)| ((*name).to_owned(), doc.clone()))
        .collect();
    base::render(&Value::Object(all))
}

pub(crate) fn render(doc: &Value) -> String {
    base::render(doc)
}
