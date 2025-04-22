//! The config hash (0003 §10). A run pins it and `resume` refuses to go on
//! when it changes (A4), so it has to change exactly when what a run writes
//! could change.
//!
//! It is SHA-256 over a canonical JSON form of the merged model, taken
//! before `${ENV}` substitution and with defaults filled in:
//!
//! - file layout doesn't count: connections, flows and targets are sorted
//!   by name, and nothing records which file a value came from;
//! - order is kept where it means something (columns, `from:` entries,
//!   named `where:` predicates, `expect:`), and dropped where it doesn't
//!   (`depends_on`, join key pairs);
//! - tuning settings (`workers`, `memory_budget`, `partition_rows`,
//!   `source_max_connections`, `source_rows_per_sec`) are left out, so a run
//!   can resume with more memory or at a lower rate.
//!
//! Changing anything here changes every hash, which blocks resuming runs
//! started by an older ferry. That is deliberate: `golden_config_hash_is_pinned`
//! fails first, and the change has to say so in its release notes.

use std::fmt;

use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use super::Config;
use super::model::{
    ColumnSpec, Columns, Connection, FILE_BACKEND, Flow, From, OrderedMap, RawKind, Target, Where,
};
use super::span::Spanned;

/// `sha256:<64 hex digits>`. A plain string, so `ferry-state` can store and
/// compare it without depending on this crate.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ConfigHash(String);

impl ConfigHash {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

impl fmt::Display for ConfigHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Config {
    /// The hash a run pins (0003 §10).
    pub fn hash(&self) -> ConfigHash {
        let digest = Sha256::digest(self.hash_input().as_bytes());
        let hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
        ConfigHash(format!("sha256:{hex}"))
    }

    /// The canonical JSON the hash is computed over.
    pub fn hash_input(&self) -> String {
        let mut out = String::new();
        write_canonical(&canonical(self), &mut out);
        out
    }
}

fn canonical(config: &Config) -> Value {
    let mut connections: Vec<&Connection> = config.connections.iter().collect();
    connections.sort_by(|a, b| a.name.as_str().cmp(b.name.as_str()));
    let mut flows: Vec<&Flow> = config.flows.iter().collect();
    flows.sort_by(|a, b| a.name.as_str().cmp(b.name.as_str()));
    let runtime = &config.runtime;
    let state = match &runtime.state {
        None => json!({"backend": FILE_BACKEND, "path": null, "connection": null}),
        Some(state) => json!({
            "backend": state.backend.as_str(),
            "path": state.path.as_ref().map(|p| p.as_str()),
            "connection": state.connection.as_ref().map(|c| c.as_str()),
        }),
    };
    json!({
        "version": config.version.get(),
        "name": config.name.as_str(),
        "connections": connections.iter().map(|c| json!({
            "name": c.name.as_str(),
            "type": c.r#type.as_str(),
            "fields": raw_map(&c.fields),
        })).collect::<Vec<_>>(),
        "runtime": {
            "state": state,
            "on_error": runtime.on_error.as_str(),
            "max_errors": runtime.max_errors,
        },
        "flows": flows.iter().map(|f| flow(f)).collect::<Vec<_>>(),
    })
}

fn raw_map(map: &OrderedMap<Spanned<RawKind>>) -> Value {
    Value::Object(
        map.iter()
            .map(|(k, v)| (k.as_str().to_owned(), v.to_json()))
            .collect(),
    )
}

fn flow(flow: &Flow) -> Value {
    let from = match flow.from.value() {
        From::Entity(entity) => json!(entity.to_string()),
        From::Merge(entries) => Value::Array(
            entries
                .iter()
                .map(|e| {
                    json!({
                        "entity": e.entity.to_string(),
                        "as": e.alias.as_ref().map(|a| a.as_str()).unwrap_or(e.entity.default_alias()),
                        "join": e.join.as_ref().map(|j| {
                            let mut keys: Vec<(&str, &str)> =
                                j.keys.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
                            keys.sort();
                            json!({
                                "cardinality": j.cardinality.as_str(),
                                "keys": keys,
                                "strategy": j.strategy.as_str(),
                            })
                        }),
                    })
                })
                .collect(),
        ),
    };
    let mut depends_on: Vec<&str> = flow.depends_on.iter().map(|d| d.as_str()).collect();
    depends_on.sort();
    let mut targets: Vec<(&Spanned<String>, &Target)> = flow.to.iter().collect();
    targets.sort_by(|a, b| a.0.as_str().cmp(b.0.as_str()));
    json!({
        "name": flow.name.as_str(),
        "from": from,
        "sink": flow.sink.as_str(),
        "mode": flow.mode.as_str(),
        "where": filter(flow.filter.as_ref()),
        "depends_on": depends_on,
        "runtime": {
            "on_error": flow.runtime.on_error.map(|e| e.as_str()),
            "max_errors": flow.runtime.max_errors,
        },
        "to": targets.iter().map(|(table, t)| json!([table.as_str(), target(t)])).collect::<Vec<_>>(),
        "expect": pairs(&flow.expect),
    })
}

fn target(target: &Target) -> Value {
    json!({
        "parent": target.parent.as_ref().map(|p| p.as_str()),
        "unnest": target.unnest.as_ref().map(|u| u.as_str()),
        "key": target.key.as_ref().map(|k| k.iter().map(|c| c.as_str()).collect::<Vec<_>>()),
        "where": filter(target.filter.as_ref()),
        "columns": match target.columns.value() {
            Columns::All => json!("*"),
            Columns::Map(map) => columns(map),
        },
    })
}

fn columns(map: &OrderedMap<Spanned<ColumnSpec>>) -> Value {
    Value::Array(
        map.iter()
            .map(|(name, spec)| json!([name.as_str(), column(spec)]))
            .collect(),
    )
}

/// A column written as a bare expression and as `{expr: ...}` with nothing
/// else are the same column.
fn column(spec: &ColumnSpec) -> Value {
    match spec {
        ColumnSpec::Expr(expr) => json!({"expr": expr}),
        ColumnSpec::Typed(t) if t.r#type.is_none() && t.nullable.is_none() && !t.allow_lossy => {
            json!({"expr": t.expr.as_str()})
        }
        ColumnSpec::Typed(t) => json!({
            "expr": t.expr.as_str(),
            "type": t.r#type.as_ref().map(|t| t.as_str()),
            "nullable": t.nullable,
            "allow_lossy": t.allow_lossy,
        }),
        ColumnSpec::Nest(n) => json!({
            "nest": n.nest.as_str(),
            "order_by": n.order_by.iter().map(|o| o.as_str()).collect::<Vec<_>>(),
            "where": filter(n.filter.as_ref()),
            "columns": columns(&n.columns),
        }),
    }
}

fn filter(filter: Option<&Spanned<Where>>) -> Value {
    match filter.map(Spanned::value) {
        None => Value::Null,
        Some(Where::Expr(expr)) => json!({"expr": expr}),
        Some(Where::Named(named)) => json!({"named": pairs(named)}),
    }
}

fn pairs(map: &OrderedMap<Spanned<String>>) -> Value {
    Value::Array(
        map.iter()
            .map(|(k, v)| json!([k.as_str(), v.as_str()]))
            .collect(),
    )
}

/// Compact JSON with object keys sorted, whatever map type `serde_json` uses.
fn write_canonical(value: &Value, out: &mut String) {
    match value {
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            out.push('{');
            for (i, key) in keys.into_iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&Value::String(key.clone()).to_string());
                out.push(':');
                write_canonical(&map[key], out);
            }
            out.push('}');
        }
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_canonical(item, out);
            }
            out.push(']');
        }
        scalar => out.push_str(&scalar.to_string()),
    }
}
