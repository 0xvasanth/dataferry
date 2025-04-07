//! Composing connector option schemas into the `connections[]` item schema
//! (0004 §3), and reading them back for reserved keys and secret fields.
//!
//! Option schemas only describe the editor experience. Validation always
//! goes through serde into the connector's `Options` type.

use std::collections::BTreeSet;

use schemars::{Schema, json_schema};
use serde_json::{Map, Value};

use crate::secret::SECRET_KEYWORD;

/// Keys 0003 §3 owns in every connection entry. A connector's `Options`
/// may not use them.
pub const RESERVED_KEYS: [&str; 3] = ["name", "type", "description"];

/// A whole-value `${NAME}` reference, as 0003 §9 allows in connections.
pub const ENV_REFERENCE_PATTERN: &str = r"^\$\{[A-Za-z_][A-Za-z0-9_]*\}$";

/// Following `$ref`s and combinators deeper than this means the schema is
/// recursive; the walk stops there.
const MAX_DEPTH: usize = 64;

/// Keywords that describe a field rather than constrain it. When a field is
/// widened to accept `${ENV}`, they move to the wrapper so editors still show
/// them on hover.
const ANNOTATIONS: [&str; 7] = [
    "title",
    "description",
    "default",
    "examples",
    "deprecated",
    "readOnly",
    "writeOnly",
];

const COMBINATORS: [&str; 3] = ["allOf", "anyOf", "oneOf"];

/// The property names an options schema accepts at its top level, looking
/// through `$ref` and `allOf`/`anyOf`/`oneOf` (`root` holds the `$defs`).
pub(crate) fn top_level_properties(root: &Value) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    collect_properties(root, root, &mut out, 0);
    out
}

fn collect_properties(node: &Value, root: &Value, out: &mut BTreeSet<String>, depth: usize) {
    if depth > MAX_DEPTH {
        return;
    }
    if let Some(target) = resolve_ref(node, root) {
        collect_properties(target, root, out, depth + 1);
    }
    if let Some(properties) = node.get("properties").and_then(Value::as_object) {
        out.extend(properties.keys().cloned());
    }
    for sub in subschemas(node, &COMBINATORS) {
        collect_properties(sub, root, out, depth + 1);
    }
}

/// Every string in `value` that sits where the schema has `x-ferry-secret`.
///
/// Walks the raw entry rather than the deserialized `Options`, so it works
/// for any connector, and still finds every secret when deserialization
/// fails partway. Under a combinator every branch is followed, which can
/// only over-collect.
pub(crate) fn collect_secrets(root: &Value, value: &Value) -> Vec<String> {
    let mut out = Vec::new();
    walk_secrets(root, root, value, &mut out, 0);
    out
}

fn walk_secrets(schema: &Value, root: &Value, value: &Value, out: &mut Vec<String>, depth: usize) {
    if depth > MAX_DEPTH {
        return;
    }
    if let Some(target) = resolve_ref(schema, root) {
        walk_secrets(target, root, value, out, depth + 1);
    }
    if schema.get(SECRET_KEYWORD) == Some(&Value::Bool(true))
        && let Some(s) = value.as_str()
    {
        out.push(s.to_owned());
    }
    for sub in subschemas(schema, &COMBINATORS) {
        walk_secrets(sub, root, value, out, depth + 1);
    }
    match value {
        Value::Object(fields) => {
            let properties = schema.get("properties").and_then(Value::as_object);
            let additional = schema.get("additionalProperties").filter(|s| s.is_object());
            for (key, v) in fields {
                if let Some(sub) = properties.and_then(|p| p.get(key)).or(additional) {
                    walk_secrets(sub, root, v, out, depth + 1);
                }
            }
        }
        Value::Array(items) => {
            let prefix = schema.get("prefixItems").and_then(Value::as_array);
            let rest = schema.get("items").filter(|s| s.is_object());
            for (i, v) in items.iter().enumerate() {
                if let Some(sub) = prefix.and_then(|p| p.get(i)).or(rest) {
                    walk_secrets(sub, root, v, out, depth + 1);
                }
            }
        }
        _ => {}
    }
}

fn resolve_ref<'a>(node: &Value, root: &'a Value) -> Option<&'a Value> {
    let reference = node.get("$ref")?.as_str()?;
    root.pointer(reference.strip_prefix('#')?)
}

fn subschemas<'a>(node: &'a Value, keywords: &[&str]) -> impl Iterator<Item = &'a Value> {
    keywords
        .iter()
        .filter_map(|k| node.get(*k).and_then(Value::as_array))
        .flatten()
}

/// Lets every non-string scalar field also hold a `${NAME}` reference, so
/// `port: ${PG_PORT}` isn't flagged by an editor. Substitution happens
/// before deserialization, so the value the connector sees is still typed.
pub(crate) fn widen_for_env(node: &mut Value) {
    if is_non_string_scalar(node) {
        let Value::Object(original) = std::mem::take(node) else {
            unreachable!("checked by is_non_string_scalar")
        };
        let mut original = original;
        let mut wrapper = Map::new();
        for key in ANNOTATIONS {
            if let Some(v) = original.remove(key) {
                wrapper.insert(key.to_owned(), v);
            }
        }
        wrapper.insert(
            "anyOf".into(),
            Value::Array(vec![Value::Object(original), env_reference()]),
        );
        *node = Value::Object(wrapper);
        return;
    }
    let Value::Object(object) = node else {
        return;
    };
    for key in ["properties", "patternProperties", "$defs"] {
        if let Some(Value::Object(children)) = object.get_mut(key) {
            children.values_mut().for_each(widen_for_env);
        }
    }
    for key in ["additionalProperties", "items"] {
        if let Some(child @ Value::Object(_)) = object.get_mut(key) {
            widen_for_env(child);
        }
    }
    for key in ["prefixItems", "allOf", "anyOf", "oneOf"] {
        if let Some(Value::Array(children)) = object.get_mut(key) {
            children.iter_mut().for_each(widen_for_env);
        }
    }
}

fn is_non_string_scalar(node: &Value) -> bool {
    let scalar = |t: &Value| matches!(t.as_str(), Some("integer" | "number" | "boolean"));
    match node.get("type") {
        Some(t @ Value::String(_)) => scalar(t),
        Some(Value::Array(types)) => {
            types.iter().any(scalar)
                && types
                    .iter()
                    .all(|t| scalar(t) || t.as_str() == Some("null"))
        }
        _ => false,
    }
}

fn env_reference() -> Value {
    serde_json::json!({
        "type": "string",
        "pattern": ENV_REFERENCE_PATTERN,
        "description": "A ${NAME} environment variable reference",
    })
}

/// One `oneOf` branch: the connector's options plus `name` and `type`.
///
/// `name` and `type` are repeated in every branch because
/// `additionalProperties: false` doesn't see properties declared in a
/// sibling subschema. An options schema that isn't a plain object (a `$ref`
/// or a combinator at the top) is wrapped in `allOf` instead; if it also
/// closes itself with `additionalProperties: false`, editors will flag
/// `name` and `type` in that branch, so connectors should derive a plain
/// struct.
pub(crate) fn compose_branch(type_name: &str, mut options: Schema) -> Schema {
    let name = serde_json::json!({
        "type": "string",
        "pattern": "^[a-z][a-z0-9_]*$",
        "description": "The connection's name, used by flows in `from: <name>.<entity>` and `sink: <name>`",
    });
    let r#type = serde_json::json!({
        "const": type_name,
        "description": "The connector type",
    });
    let object = options.ensure_object();
    let plain = !object.contains_key("$ref")
        && !COMBINATORS.iter().any(|k| object.contains_key(*k))
        && object
            .get("type")
            .is_none_or(|t| t.as_str() == Some("object"));
    if !plain {
        return json_schema!({
            "type": "object",
            "properties": { "name": name, "type": r#type },
            "required": ["name", "type"],
            "allOf": [options],
        });
    }
    object.insert("type".into(), "object".into());
    let properties = object
        .entry("properties")
        .or_insert_with(|| Value::Object(Map::new()));
    if let Value::Object(properties) = properties {
        properties.insert("name".into(), name);
        properties.insert("type".into(), r#type);
    }
    let mut required = vec![Value::from("name"), Value::from("type")];
    if let Some(Value::Array(existing)) = object.get("required") {
        for key in existing {
            if !required.contains(key) {
                required.push(key.clone());
            }
        }
    }
    object.insert("required".into(), Value::Array(required));
    options
}
