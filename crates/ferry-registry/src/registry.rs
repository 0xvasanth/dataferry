//! `ConnectorRegistry`: maps `connections[].type` to a connector, builds
//! connections and composes their schema (0004 §2, §3).

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use ferry_core::FerryError;
use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::connector::{
    BuildContext, BuildError, Built, Connector, Endpoints, OptionsError, RawConnection, Roles,
};
use crate::schema::{
    RESERVED_KEYS, collect_secrets, compose_branch, top_level_properties, widen_for_env,
};
use crate::secret::Redactor;
use crate::state::{StateBackend, StateCapable, StateStore};

/// The object-safe, type-erased form of [`Connector`], so connectors with
/// different `Options` types live in one map. It is also the boundary a
/// future subprocess connector would implement (0004 §7): options arrive as
/// JSON, the schema is JSON Schema, roles are two booleans.
pub trait DynConnector: Send + Sync {
    fn type_name(&self) -> &str;
    fn roles(&self) -> Roles;

    /// The schema of the type-specific fields. Definitions it depends on go
    /// into `generator`.
    fn options_schema(&self, generator: &mut SchemaGenerator) -> Schema;

    /// `options` is the connection entry minus `name` and `type`. Errors
    /// must already be redacted; the registry redacts again with the
    /// secrets it finds through the schema.
    fn build(
        &self,
        ctx: &BuildContext<'_>,
        options: Value,
        wants: Roles,
    ) -> Result<Built, BuildError>;

    /// Only connectors registered with `register_state_capable` override
    /// these.
    fn state_capable(&self) -> bool {
        false
    }

    fn build_with_state(
        &self,
        ctx: &BuildContext<'_>,
        options: Value,
        wants: Roles,
    ) -> Result<(Built, Arc<dyn StateStore>), BuildError> {
        let _ = (ctx, options, wants);
        Err(not_state_capable(self.type_name()))
    }
}

fn not_state_capable(type_name: &str) -> BuildError {
    BuildError::Other(FerryError::Connector {
        connector: type_name.to_owned(),
        message: format!("the `{type_name}` connector in this build can't hold the state store"),
    })
}

/// Why a connector could not be registered. Both are wiring bugs in the
/// binary: [`ConnectorRegistry::register`] and friends panic with this
/// message, and the `try_` forms return it.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RegisterError {
    #[error("connector type `{type_name}` is already registered")]
    DuplicateType { type_name: String },
    #[error("connector type `{type_name}` is not registered, so it can't be replaced")]
    NotRegistered { type_name: String },
    #[error(
        "connector type `{type_name}` is state-capable; replacing it with `replace` would drop that, use `replace_state_capable`"
    )]
    WouldDropStateCapability { type_name: String },
    #[error("connector type must not be empty")]
    EmptyType,
    #[error(
        "connector type `{type_name}`: option `{key}` uses a reserved connection key (reserved: {})",
        RESERVED_KEYS.join(", ")
    )]
    ReservedKey { type_name: String, key: String },
}

struct Entry {
    connector: Box<dyn DynConnector>,
    /// The options schema with its `$defs`, for finding secret fields.
    options_schema: Value,
}

/// Every connection type a binary knows.
#[derive(Default)]
pub struct ConnectorRegistry {
    entries: BTreeMap<String, Entry>,
}

impl ConnectorRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Panics if `C::TYPE` is already registered, or if `C::Options`'s
    /// schema has a property named after a reserved key (0003 §3): both are
    /// wiring bugs in the binary, found the first time it runs.
    pub fn register<C: Connector>(&mut self, connector: C) -> &mut Self {
        self.try_register(connector)
            .unwrap_or_else(|e| panic!("{e}"))
    }

    pub fn try_register<C: Connector>(&mut self, connector: C) -> Result<&mut Self, RegisterError> {
        self.insert(Box::new(Typed(connector)), false)
    }

    /// Replaces a registered type on purpose, e.g. a patched postgres.
    /// Panics if the type isn't registered yet (a typo would otherwise add a
    /// second type silently), if its options use a reserved key, or if the
    /// registered type is state-capable: use
    /// [`replace_state_capable`](Self::replace_state_capable) to keep it so.
    pub fn replace<C: Connector>(&mut self, connector: C) -> &mut Self {
        self.try_replace(connector)
            .unwrap_or_else(|e| panic!("{e}"))
    }

    pub fn try_replace<C: Connector>(&mut self, connector: C) -> Result<&mut Self, RegisterError> {
        self.insert(Box::new(Typed(connector)), true)
    }

    /// `replace` for a state-capable type, with its backend. Same panics,
    /// except that the registered type may be state-capable.
    pub fn replace_state_capable<C, B>(&mut self, connector: C, backend: B) -> &mut Self
    where
        C: StateCapable,
        B: StateBackend<Pool = C::Pool, Conn = C::Conn>,
    {
        self.try_replace_state_capable(connector, backend)
            .unwrap_or_else(|e| panic!("{e}"))
    }

    pub fn try_replace_state_capable<C, B>(
        &mut self,
        connector: C,
        backend: B,
    ) -> Result<&mut Self, RegisterError>
    where
        C: StateCapable,
        B: StateBackend<Pool = C::Pool, Conn = C::Conn>,
    {
        self.insert(Box::new(StateTyped { connector, backend }), true)
    }

    /// For connectors that aren't a Rust `Options` type (0004 §7). Same
    /// panics as `register`.
    pub fn register_dyn(&mut self, connector: Box<dyn DynConnector>) -> &mut Self {
        self.try_register_dyn(connector)
            .unwrap_or_else(|e| panic!("{e}"))
    }

    pub fn try_register_dyn(
        &mut self,
        connector: Box<dyn DynConnector>,
    ) -> Result<&mut Self, RegisterError> {
        self.insert(connector, false)
    }

    /// Registers a connector that can host the state store, paired with
    /// its backend (0004 §8). The `where` clause is where the compiler
    /// checks that both name the same driver handles. Same panics as
    /// `register`.
    pub fn register_state_capable<C, B>(&mut self, connector: C, backend: B) -> &mut Self
    where
        C: StateCapable,
        B: StateBackend<Pool = C::Pool, Conn = C::Conn>,
    {
        self.try_register_state_capable(connector, backend)
            .unwrap_or_else(|e| panic!("{e}"))
    }

    pub fn try_register_state_capable<C, B>(
        &mut self,
        connector: C,
        backend: B,
    ) -> Result<&mut Self, RegisterError>
    where
        C: StateCapable,
        B: StateBackend<Pool = C::Pool, Conn = C::Conn>,
    {
        self.insert(Box::new(StateTyped { connector, backend }), false)
    }

    fn insert(
        &mut self,
        connector: Box<dyn DynConnector>,
        replace: bool,
    ) -> Result<&mut Self, RegisterError> {
        let type_name = connector.type_name().to_owned();
        if type_name.is_empty() {
            return Err(RegisterError::EmptyType);
        }
        match (self.entries.get(&type_name), replace) {
            (Some(_), false) => return Err(RegisterError::DuplicateType { type_name }),
            (None, true) => return Err(RegisterError::NotRegistered { type_name }),
            (Some(old), true) if old.connector.state_capable() && !connector.state_capable() => {
                return Err(RegisterError::WouldDropStateCapability { type_name });
            }
            _ => {}
        }
        let options_schema = standalone_schema(connector.as_ref());
        let properties = top_level_properties(&options_schema);
        if let Some(key) = RESERVED_KEYS.iter().find(|k| properties.contains(**k)) {
            return Err(RegisterError::ReservedKey {
                type_name,
                key: (*key).to_owned(),
            });
        }
        self.entries.insert(
            type_name,
            Entry {
                connector,
                options_schema,
            },
        );
        Ok(self)
    }

    /// Registered types and their roles, sorted by type.
    pub fn types(&self) -> impl Iterator<Item = (&str, Roles)> {
        self.entries
            .iter()
            .map(|(name, e)| (name.as_str(), e.connector.roles()))
    }

    pub fn connector(&self, type_name: &str) -> Option<&dyn DynConnector> {
        self.entries.get(type_name).map(|e| e.connector.as_ref())
    }

    /// Builds `conn` with the roles its flows use. No I/O.
    pub fn build(&self, conn: &RawConnection, wants: Roles) -> Result<Built, BuildError> {
        self.build_inner(conn, wants, |c, ctx, options| {
            c.build(ctx, options, wants).map(|built| (built, None))
        })
        .map(|(built, _)| built)
    }

    /// The state connection: `wants` plus the state store, on one pool.
    pub fn build_with_state(
        &self,
        conn: &RawConnection,
        wants: Roles,
    ) -> Result<(Built, Arc<dyn StateStore>), BuildError> {
        self.build_inner(conn, wants, |c, ctx, options| {
            if !c.state_capable() {
                return Err(not_state_capable(c.type_name()));
            }
            c.build_with_state(ctx, options, wants)
                .map(|(built, store)| (built, Some(store)))
        })
        .map(|(built, store)| (built, store.expect("state-capable build returns a store")))
    }

    fn build_inner(
        &self,
        conn: &RawConnection,
        wants: Roles,
        build: impl FnOnce(
            &dyn DynConnector,
            &BuildContext<'_>,
            Value,
        ) -> Result<(Built, Option<Arc<dyn StateStore>>), BuildError>,
    ) -> Result<(Built, Option<Arc<dyn StateStore>>), BuildError> {
        let entry = self.entries.get(&conn.r#type).ok_or_else(|| {
            let known: Vec<_> = self.entries.keys().map(|k| format!("`{k}`")).collect();
            BuildError::option(
                "type",
                format!(
                    "unknown connection type `{}`; registered types: {}",
                    conn.r#type,
                    if known.is_empty() {
                        "none".to_owned()
                    } else {
                        known.join(", ")
                    }
                ),
            )
        })?;
        let connector = entry.connector.as_ref();
        let type_name = connector.type_name();
        let roles = connector.roles();
        if !roles.contains(wants) {
            return Err(BuildError::Other(FerryError::Connector {
                connector: type_name.to_owned(),
                message: format!(
                    "connection `{}` is used as a {wants}, but the `{type_name}` connector in this build is only a {roles}",
                    conn.name
                ),
            }));
        }
        // 0003 §3 owns these keys. A connector with closed options would
        // reject them anyway, but a free-form one would take them as its own.
        if let Some(key) = RESERVED_KEYS.iter().find(|k| conn.fields.contains_key(**k)) {
            return Err(BuildError::option(
                *key,
                format!("`{key}` is a reserved connection key and has no meaning in this version"),
            ));
        }
        let options = Value::Object(conn.fields.clone());
        let mut secrets = collect_secrets(&entry.options_schema, &options);
        let redactor = Redactor::new(secrets.iter().cloned());
        let ctx = BuildContext::new(&conn.name);
        let (mut built, store) =
            build(connector, &ctx, options).map_err(|e| e.redact(&redactor, type_name))?;
        let got = built.endpoints.roles();
        if got != wants {
            return Err(BuildError::Other(FerryError::Connector {
                connector: type_name.to_owned(),
                message: format!(
                    "connector bug: connection `{}` asked for {wants}, build returned {got}",
                    conn.name
                ),
            }));
        }
        for secret in std::mem::take(&mut built.secrets) {
            if !secret.is_empty() && !secrets.contains(&secret) {
                secrets.push(secret);
            }
        }
        built.description = Redactor::new(secrets.iter().cloned())
            .redact(&built.description)
            .into_owned();
        built.secrets = secrets;
        Ok((built, store))
    }

    /// The `connections[]` item schema: one `oneOf` branch per registered
    /// type, each a closed object with `name`, `type` (as a `const`) and the
    /// type's options. Non-string scalar options also accept `${NAME}`.
    /// Definitions go into `generator`.
    pub fn connections_schema(&self, generator: &mut SchemaGenerator) -> Schema {
        let branches: Vec<Value> = self
            .entries
            .values()
            .map(|e| {
                let before: Vec<String> = generator.definitions().keys().cloned().collect();
                let mut options = e.connector.options_schema(generator).to_value();
                widen_for_env(&mut options);
                for (name, def) in generator.definitions_mut().iter_mut() {
                    if !before.contains(name) {
                        widen_for_env(def);
                    }
                }
                let options = Schema::try_from(options).expect("still an object or bool");
                compose_branch(e.connector.type_name(), options).to_value()
            })
            .collect();
        if branches.is_empty() {
            return json_schema!(false);
        }
        json_schema!({ "oneOf": branches })
    }

    /// A whole `connections.yaml` schema: `connections:` holding
    /// [`connections_schema`](Self::connections_schema) items, with
    /// definitions under `$defs`. B1 generates the other files' schemas.
    pub fn connections_file_schema(&self) -> Schema {
        let mut generator = SchemaGenerator::default();
        let items = self.connections_schema(&mut generator);
        let defs = generator.take_definitions(true);
        let mut schema = json_schema!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "connections.yaml",
            "type": "object",
            "properties": {
                "connections": { "type": "array", "items": items },
            },
            "required": ["connections"],
            "additionalProperties": false,
        });
        if !defs.is_empty() {
            schema.insert("$defs".into(), Value::Object(defs));
        }
        schema
    }
}

impl fmt::Debug for ConnectorRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map().entries(self.types()).finish()
    }
}

/// The connector's options schema as a root, with its own `$defs`.
fn standalone_schema(connector: &dyn DynConnector) -> Value {
    let mut generator = SchemaGenerator::default();
    let mut schema = connector.options_schema(&mut generator).to_value();
    let defs = generator.take_definitions(true);
    if let (Value::Object(object), false) = (&mut schema, defs.is_empty()) {
        object.insert("$defs".into(), Value::Object(defs));
    }
    schema
}

/// Deserializes `options` with the path of the first bad field. Secrets
/// are never quoted by `Secret`'s own errors; anything else that echoes a
/// secret is redacted by the registry.
fn deserialize_options<T: DeserializeOwned>(options: Value) -> Result<T, BuildError> {
    serde_path_to_error::deserialize(options).map_err(|e| {
        let path = e.path().to_string();
        BuildError::Options(vec![OptionsError {
            path: if path == "." { String::new() } else { path },
            message: e.into_inner().to_string(),
        }])
    })
}

/// Deserializes `options` once, then runs `build` on the typed value. The
/// description and derived secrets are taken before `build` consumes it, and
/// `build`'s errors are redacted with those secrets.
fn build_typed<C: Connector>(
    connector: &C,
    ctx: &BuildContext<'_>,
    options: Value,
    build: impl FnOnce(&C, &BuildContext<'_>, C::Options) -> Result<Endpoints, BuildError>,
) -> Result<Built, BuildError> {
    let options: C::Options = deserialize_options(options)?;
    let secrets = connector.derived_secrets(&options);
    let description = connector.describe(&options);
    let redactor = Redactor::new(secrets.iter().cloned());
    let endpoints = build(connector, ctx, options).map_err(|e| e.redact(&redactor, C::TYPE))?;
    Ok(Built {
        endpoints,
        description,
        secrets,
    })
}

/// `register`'s wrapper: the typed trait behind the dyn one.
struct Typed<C>(C);

impl<C: Connector> DynConnector for Typed<C> {
    fn type_name(&self) -> &str {
        C::TYPE
    }

    fn roles(&self) -> Roles {
        C::ROLES
    }

    fn options_schema(&self, generator: &mut SchemaGenerator) -> Schema {
        C::Options::json_schema(generator)
    }

    fn build(
        &self,
        ctx: &BuildContext<'_>,
        options: Value,
        wants: Roles,
    ) -> Result<Built, BuildError> {
        build_typed(&self.0, ctx, options, |c, ctx, o| c.build(ctx, o, wants))
    }
}

/// `register_state_capable`'s wrapper. Holds the concrete connector and
/// backend, so `pool`, `open` and `build_with_ledger` are called with
/// concrete types.
struct StateTyped<C, B> {
    connector: C,
    backend: B,
}

impl<C, B> DynConnector for StateTyped<C, B>
where
    C: StateCapable,
    B: StateBackend<Pool = C::Pool, Conn = C::Conn>,
{
    fn type_name(&self) -> &str {
        C::TYPE
    }

    fn roles(&self) -> Roles {
        C::ROLES
    }

    fn options_schema(&self, generator: &mut SchemaGenerator) -> Schema {
        C::Options::json_schema(generator)
    }

    /// Plain `build`: no ledger, so the sink must not list `TxnWithState`.
    fn build(
        &self,
        ctx: &BuildContext<'_>,
        options: Value,
        wants: Roles,
    ) -> Result<Built, BuildError> {
        build_typed(&self.connector, ctx, options, |c, ctx, o| {
            c.build(ctx, o, wants)
        })
    }

    fn state_capable(&self) -> bool {
        true
    }

    /// The options are deserialized once; the source half, the sink half
    /// and the state store share the one pool.
    fn build_with_state(
        &self,
        ctx: &BuildContext<'_>,
        options: Value,
        wants: Roles,
    ) -> Result<(Built, Arc<dyn StateStore>), BuildError> {
        let mut store = None;
        let built = build_typed(&self.connector, ctx, options, |c, ctx, o| {
            let pool = c.pool(ctx, &o)?;
            let (opened, ledger) = self.backend.open(pool.clone());
            store = Some(opened);
            c.build_with_ledger(ctx, o, wants, pool, ledger)
        })?;
        Ok((built, store.expect("set by a successful build")))
    }
}
