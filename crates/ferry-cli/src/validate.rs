//! `ferry validate`: everything that can be checked without connecting
//! (0004 §6). Loads the directory (B1), substitutes `${ENV}`, then builds
//! every connection through the registry with the roles its flows use.
//! `build` must not do I/O, so nothing here ever connects. Reports every
//! error it finds, not just the first.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::path::Path;

use ferry_dsl::config::model::{FILE_BACKEND, From, OrderedMap, RawKind, RawValue};
use ferry_dsl::config::{self, Config, Connection, Location, ResolvedConnection, ResolvedState};
use ferry_registry::secret::SECRET_KEYWORD;
use ferry_registry::{
    BuildError, Built, ConnectorRegistry, DynConnector, Endpoints, RawConnection, Roles,
};
use schemars::SchemaGenerator;
use serde_json::Value;

use crate::diagnostic::{Diagnostic, Redaction, code};
use crate::exit::Exit;

/// What `validate` found.
pub(crate) struct Report {
    pub diagnostics: Vec<Diagnostic>,
    /// Set when the config is valid.
    pub valid: Option<Summary>,
}

impl Report {
    fn invalid(diagnostics: Vec<Diagnostic>) -> Self {
        Self {
            diagnostics,
            valid: None,
        }
    }

    pub fn exit(&self) -> Exit {
        match self.valid {
            Some(_) => Exit::Success,
            None => Exit::ConfigInvalid,
        }
    }
}

pub(crate) struct Summary {
    pub name: String,
    pub flows: usize,
    /// `(name, type, roles, description)`, in load order.
    pub connections: Vec<(String, String, Roles, String)>,
    /// The config, for `plan`.
    pub config: Config,
    /// What each connection built, by name: built once, with the roles its
    /// flows use, so `plan` reaches the databases through the same
    /// endpoints `validate` checked.
    pub endpoints: BTreeMap<String, Endpoints>,
}

/// Checks that `dir` is a `ferry/` directory before the loader looks
/// inside, so the error names the path and says what was expected.
pub(crate) fn check_config_dir(dir: &Path) -> Result<(), Diagnostic> {
    let shown = absolute(dir);
    if dir.is_dir() {
        return Ok(());
    }
    if dir.exists() {
        let parent = dir
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        return Err(Diagnostic::error(
            code::CONFIG_DIR,
            format!(
                "`{}` is a file; --config expects the `ferry/` directory that holds ferry.yaml",
                shown.display()
            ),
        )
        .help(format!("try `--config {}`", parent.display())));
    }
    Err(Diagnostic::error(
        code::CONFIG_DIR,
        format!("no config directory at `{}`", shown.display()),
    )
    .help("pass `--config <dir>` or set FERRY_CONFIG; the default is `./ferry`"))
}

fn absolute(path: &Path) -> std::path::PathBuf {
    std::path::absolute(path).unwrap_or_else(|_| path.to_owned())
}

pub(crate) fn validate(
    registry: &ConnectorRegistry,
    dir: &Path,
    env: &dyn Fn(&str) -> Option<String>,
    redaction: &mut Redaction,
) -> Report {
    if let Err(d) = check_config_dir(dir) {
        return Report::invalid(vec![d]);
    }
    let config = match config::load(dir) {
        Ok(config) => config,
        Err(errors) => return Report::invalid(errors.into_iter().map(Into::into).collect()),
    };

    // Every value read from the environment is redacted, even when
    // substitution fails partway (a missing variable next to a set one).
    let seen = RefCell::new(Vec::new());
    let resolved = config.resolve_with(|name| {
        let value = env(name);
        if let Some(v) = &value {
            seen.borrow_mut().push(v.clone());
        }
        value
    });
    for value in seen.into_inner() {
        redaction.add(&value);
    }
    let resolved = match resolved {
        Ok(resolved) => resolved,
        Err(errors) => return Report::invalid(errors.into_iter().map(Into::into).collect()),
    };

    let uses = uses(&config);
    let mut checker = Checker {
        registry,
        diagnostics: Vec::new(),
        redaction,
    };
    let mut connections = Vec::new();
    let mut endpoints = BTreeMap::new();
    for (raw, resolved_conn) in config.connections.iter().zip(&resolved.connections) {
        let used = uses
            .get(resolved_conn.name.as_str())
            .map_or(&[][..], Vec::as_slice);
        if let Some((entry, built)) =
            checker.connection(&config, raw, resolved_conn, used, resolved.state.as_ref())
        {
            endpoints.insert(entry.0.clone(), built);
            connections.push(entry);
        }
    }

    let diagnostics = checker.diagnostics;
    if diagnostics.iter().any(Diagnostic::is_error) {
        return Report::invalid(diagnostics);
    }
    Report {
        diagnostics,
        valid: Some(Summary {
            name: config.name.as_str().to_owned(),
            flows: config.flows.len(),
            connections,
            config,
            endpoints,
        }),
    }
}

/// One place a flow uses a connection.
struct Use<'a> {
    flow: &'a str,
    role: Roles,
    location: &'a Location,
}

/// How each connection is used, by connection name.
fn uses(config: &Config) -> BTreeMap<&str, Vec<Use<'_>>> {
    let mut out: BTreeMap<&str, Vec<Use<'_>>> = BTreeMap::new();
    for flow in &config.flows {
        let name = flow.name.as_str();
        let mut add = |connection, role, location| {
            out.entry(connection).or_default().push(Use {
                flow: name,
                role,
                location,
            })
        };
        match flow.from.value() {
            From::Entity(entity) => add(
                entity.connection.as_str(),
                Roles::SOURCE,
                flow.from.location(),
            ),
            From::Merge(entries) => {
                for entry in entries {
                    add(
                        entry.entity.connection.as_str(),
                        Roles::SOURCE,
                        entry.entity.location(),
                    );
                }
            }
        }
        add(flow.sink.as_str(), Roles::SINK, flow.sink.location());
    }
    out
}

struct Checker<'a> {
    registry: &'a ConnectorRegistry,
    diagnostics: Vec<Diagnostic>,
    redaction: &'a mut Redaction,
}

impl Checker<'_> {
    /// Checks and builds one connection: its summary line and its
    /// endpoints. `None` if it couldn't be built.
    fn connection(
        &mut self,
        config: &Config,
        raw: &Connection,
        conn: &ResolvedConnection,
        uses: &[Use<'_>],
        state: Option<&ResolvedState>,
    ) -> Option<((String, String, Roles, String), Endpoints)> {
        let entry = format!("connections[{}]", conn.name);
        let Some(connector) = self.registry.connector(&conn.r#type) else {
            self.diagnostics.push(
                Diagnostic::error(
                    code::UNKNOWN_TYPE,
                    format!("unknown connection type `{}`", conn.r#type),
                )
                .at(Some(raw.r#type.location()))
                .path(format!("{entry}.type"))
                .help(self.registered_types()),
            );
            return None;
        };
        let provides = connector.roles();
        let type_name = connector.type_name().to_owned();
        // Before building, so a secret is masked in the source line shown
        // with an error even when its value fails to deserialize.
        for secret in secret_values(connector, &conn.fields) {
            self.redaction.add(&secret);
        }

        let mut wants = Roles::NONE;
        for u in uses {
            if provides.contains(u.role) {
                wants = wants.union(u.role);
                continue;
            }
            let (verb, key, missing) = match u.role == Roles::SINK {
                true => ("writes to", "sink", "sink"),
                false => ("reads from", "from", "source"),
            };
            self.diagnostics.push(
                Diagnostic::error(
                    code::ROLE,
                    format!(
                        "flow `{}` {verb} connection `{}`, but the `{type_name}` connector in this build has no {missing}",
                        u.flow, conn.name
                    ),
                )
                .at(Some(u.location))
                .path(format!("flows[{}].{key}", u.flow))
                .help(format!("`{type_name}` provides: {provides}")),
            );
        }

        let state_ok = match state {
            Some(s)
                if s.backend != FILE_BACKEND
                    && s.connection.as_deref() == Some(conn.name.as_str()) =>
            {
                self.state_connection(config, conn, s)
            }
            _ => false,
        };

        let raw_conn = RawConnection {
            name: conn.name.clone(),
            r#type: conn.r#type.clone(),
            fields: conn.fields.as_object().cloned().unwrap_or_default(),
        };
        let built = match state_ok {
            true => self
                .registry
                .build_with_state(&raw_conn, wants)
                .map(|(built, _store)| built),
            false => self.registry.build(&raw_conn, wants),
        };
        match built {
            Ok(built) => {
                for secret in &built.secrets {
                    self.redaction.add(secret);
                }
                self.literal_secrets(raw, &built, &entry);
                Some((
                    (conn.name.clone(), type_name, wants, built.description),
                    built.endpoints,
                ))
            }
            Err(error) => {
                self.build_error(raw, conn, &entry, error);
                None
            }
        }
    }

    fn registered_types(&self) -> String {
        let types: Vec<String> = self
            .registry
            .types()
            .map(|(name, roles)| format!("`{name}` ({roles})"))
            .collect();
        match types.is_empty() {
            true => "this binary has no connection types registered".to_owned(),
            false => format!("registered types: {}", types.join(", ")),
        }
    }

    /// `runtime.state.connection` names `conn` (0004 §8): its type must be
    /// the backend, and that type must be able to hold the state store.
    /// Errors point at `runtime.yaml`.
    fn state_connection(
        &mut self,
        config: &Config,
        conn: &ResolvedConnection,
        state: &ResolvedState,
    ) -> bool {
        let written = config.runtime.state.as_ref();
        let backend_at = written.map(|s| s.backend.location());
        let connection_at = written
            .and_then(|s| s.connection.as_ref())
            .map(|c| c.location());
        if conn.r#type != state.backend {
            self.diagnostics.push(
                Diagnostic::error(
                    code::STATE_TYPE,
                    format!(
                        "the state connection `{}` is a `{}` connection, but `backend:` is `{}`",
                        conn.name, conn.r#type, state.backend
                    ),
                )
                .at(connection_at)
                .path("runtime.state.connection")
                .help(format!(
                    "name a `{}` connection here, or set `backend:` to the connection's type",
                    state.backend
                )),
            );
            return false;
        }
        let capable = self
            .registry
            .connector(&state.backend)
            .is_some_and(|c| c.state_capable());
        if !capable {
            let capable: Vec<String> = self
                .registry
                .types()
                .filter(|(t, _)| {
                    self.registry
                        .connector(t)
                        .is_some_and(|c| c.state_capable())
                })
                .map(|(t, _)| format!("`{t}`"))
                .collect();
            self.diagnostics.push(
                Diagnostic::error(
                    code::STATE_NOT_CAPABLE,
                    format!(
                        "the `{}` connector in this build can't hold the state store",
                        state.backend
                    ),
                )
                .at(backend_at)
                .path("runtime.state.backend")
                .help(match capable.is_empty() {
                    true => {
                        "use `backend: file`: no connector in this build can hold state".to_owned()
                    }
                    false => format!(
                        "use `backend: file`, or a connection of a state-capable type: {}",
                        capable.join(", ")
                    ),
                }),
            );
            return false;
        }
        true
    }

    fn build_error(
        &mut self,
        raw: &Connection,
        conn: &ResolvedConnection,
        entry: &str,
        error: BuildError,
    ) {
        let entry_at = raw.name.location();
        match error {
            BuildError::Options(errors) => {
                for e in errors {
                    // An unknown field is wrong as a key; anything else is
                    // a wrong value.
                    let at_key = e.message.starts_with("unknown field");
                    let (path, at) = match e.path.as_str() {
                        "" => (entry.to_owned(), entry_at),
                        p => (
                            format!("{entry}.{p}"),
                            at_key
                                .then(|| key_location(&raw.fields, p))
                                .flatten()
                                .or_else(|| conn.location(p))
                                .unwrap_or(entry_at),
                        ),
                    };
                    self.diagnostics.push(
                        Diagnostic::error(code::OPTIONS, e.message)
                            .at(Some(at))
                            .path(path),
                    );
                }
            }
            BuildError::Other(e) => self.diagnostics.push(
                Diagnostic::error(code::BUILD, e.to_string())
                    .at(Some(entry_at))
                    .path(entry),
            ),
        }
    }

    /// Warns about secret values written into the config as literals, since
    /// config directories get committed (0004 §4). Not an error: local runs
    /// against throwaway databases legitimately do it.
    fn literal_secrets(&mut self, raw: &Connection, built: &Built, entry: &str) {
        let mut found = Vec::new();
        for (key, value) in raw.fields.iter() {
            literals(value, key.as_str().to_owned(), &built.secrets, &mut found);
        }
        for (path, location) in found {
            self.diagnostics.push(
                Diagnostic::warning(
                    code::SECRET_LITERAL,
                    "holds a secret as a literal value, and config directories get committed",
                )
                .at(Some(&location))
                .path(format!("{entry}.{path}"))
                .help("write `${NAME}` and export the value in the environment instead"),
            );
        }
    }
}

/// The value of every field the connector's schema marks `x-ferry-secret`,
/// numbers included: `password: 123456` is still a password.
fn secret_values(connector: &dyn DynConnector, fields: &Value) -> Vec<String> {
    let mut generator = SchemaGenerator::default();
    let schema = connector.options_schema(&mut generator).to_value();
    let defs = Value::Object(generator.take_definitions(true));
    let mut out = Vec::new();
    walk_secrets(&schema, &defs, fields, &mut out, 0);
    out
}

fn walk_secrets(schema: &Value, defs: &Value, value: &Value, out: &mut Vec<String>, depth: usize) {
    // Deeper than any real options type: the schema is recursive.
    if depth > 32 {
        return;
    }
    if let Some(name) = schema
        .get("$ref")
        .and_then(Value::as_str)
        .and_then(|r| r.strip_prefix("#/$defs/"))
        && let Some(target) = defs.get(name)
    {
        walk_secrets(target, defs, value, out, depth + 1);
    }
    if schema.get(SECRET_KEYWORD) == Some(&Value::Bool(true)) {
        match value {
            Value::String(s) => out.push(s.clone()),
            Value::Number(n) => out.push(n.to_string()),
            _ => {}
        }
    }
    for combinator in ["allOf", "anyOf", "oneOf"] {
        for sub in schema
            .get(combinator)
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            walk_secrets(sub, defs, value, out, depth + 1);
        }
    }
    if let (Some(properties), Value::Object(fields)) =
        (schema.get("properties").and_then(Value::as_object), value)
    {
        for (key, field) in fields {
            if let Some(sub) = properties.get(key) {
                walk_secrets(sub, defs, field, out, depth + 1);
            }
        }
    }
}

/// Where the key at the end of a dotted `path` was written. `None` for a
/// path through a list, or one that isn't in the config.
fn key_location<'a>(fields: &'a OrderedMap<RawValue>, path: &str) -> Option<&'a Location> {
    let (first, rest) = match path.split_once('.') {
        Some((first, rest)) => (first, Some(rest)),
        None => (path, None),
    };
    let (key, value) = fields.iter().find(|(k, _)| k.as_str() == first)?;
    match (rest, value.value()) {
        (None, _) => Some(key.location()),
        (Some(rest), RawKind::Map(map)) => key_location(map, rest),
        _ => None,
    }
}

/// Strings with no `${` that contain one of `secrets`, with their paths.
fn literals(value: &RawValue, path: String, secrets: &[String], out: &mut Vec<(String, Location)>) {
    match value.value() {
        RawKind::Str(text) => {
            if !text.contains("${")
                && secrets
                    .iter()
                    .any(|s| !s.is_empty() && text.contains(s.as_str()))
            {
                out.push((path, value.location().clone()));
            }
        }
        RawKind::Seq(items) => {
            for (i, item) in items.iter().enumerate() {
                literals(item, format!("{path}[{i}]"), secrets, out);
            }
        }
        RawKind::Map(map) => {
            for (key, item) in map.iter() {
                literals(item, format!("{path}.{}", key.as_str()), secrets, out);
            }
        }
        _ => {}
    }
}
