//! The connector author's API (0004 §2).

use std::borrow::Cow;
use std::fmt;
use std::sync::Arc;

use ferry_core::{FerryError, Sink, Source};
use schemars::JsonSchema;
use serde::Deserialize;
use serde::de::DeserializeOwned;

use crate::ddl::RenderDdl;
use crate::finalize::Finalizer;
use crate::secret::Redactor;

/// A connection type: turns the type-specific fields of a `connections.yaml`
/// entry into a [`Source`], a [`Sink`] or both.
///
/// Built-in connectors register through this trait exactly the way a
/// third-party one does.
pub trait Connector: Send + Sync + 'static {
    /// The `type:` value in `connections.yaml`, e.g. "postgres".
    const TYPE: &'static str;

    /// What this connector can be used as. Checked by `ferry validate`
    /// without building anything: a connection named in `from:` must be a
    /// source, one named in `sink:` must be a sink.
    const ROLES: Roles;

    /// The type-specific fields: the connection entry minus `name` and
    /// `type`. Must use `#[serde(deny_unknown_fields)]`, and must not use a
    /// reserved key ([`RESERVED_KEYS`](crate::RESERVED_KEYS)); registering a
    /// connector that does panics. Doc comments become editor hover text.
    /// Credential-bearing fields must be [`Secret`](crate::Secret)s.
    type Options: DeserializeOwned + JsonSchema;

    /// Builds the endpoints `wants` asks for (always a subset of `ROLES`).
    /// Must not do I/O: `ferry validate` calls it to catch malformed
    /// options, and connecting happens on the first trait call.
    fn build(
        &self,
        ctx: &BuildContext<'_>,
        options: Self::Options,
        wants: Roles,
    ) -> Result<Endpoints, BuildError>;

    /// One line for `ferry plan` and the run report, with no secrets,
    /// e.g. `postgres://app@db.example.com:5432/shop, schema public`.
    fn describe(&self, options: &Self::Options) -> String;

    /// Secret values that are not whole `Secret` fields, such as a password
    /// embedded in a URL ([`url_password`](crate::secret::url_password)).
    /// They are added to the redaction set.
    fn derived_secrets(&self, options: &Self::Options) -> Vec<String> {
        let _ = options;
        Vec::new()
    }
}

/// Which endpoints a connector provides, or a connection is used as.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Roles {
    pub source: bool,
    pub sink: bool,
}

impl Roles {
    /// Only for the state connection when no flow reads or writes it.
    pub const NONE: Roles = Roles {
        source: false,
        sink: false,
    };
    pub const SOURCE: Roles = Roles {
        source: true,
        sink: false,
    };
    pub const SINK: Roles = Roles {
        source: false,
        sink: true,
    };
    pub const BOTH: Roles = Roles {
        source: true,
        sink: true,
    };

    /// Whether every role in `other` is also in `self`.
    pub fn contains(self, other: Roles) -> bool {
        (self.source || !other.source) && (self.sink || !other.sink)
    }

    pub fn union(self, other: Roles) -> Roles {
        Roles {
            source: self.source || other.source,
            sink: self.sink || other.sink,
        }
    }
}

impl fmt::Display for Roles {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match (self.source, self.sink) {
            (false, false) => "nothing",
            (true, false) => "source",
            (false, true) => "sink",
            (true, true) => "source and sink",
        })
    }
}

/// What `build` returns. The registry checks it matches `wants`, and treats
/// a mismatch as a connector bug, not a user error.
#[derive(Default)]
pub struct Endpoints {
    pub source: Option<Arc<dyn Source>>,
    pub sink: Option<Arc<dyn Sink>>,
    /// Restores what the sink's `prepare` loosened for the load (A6). Only
    /// with a sink, and only if `prepare` changes anything a load has to
    /// put back.
    pub finalizer: Option<Arc<dyn Finalizer>>,
    /// Renders the DDL the sink's `prepare` would run, for `ferry plan`.
    /// Only with a sink. Without one, the plan shows the target schema in
    /// its canonical form instead.
    pub ddl: Option<Arc<dyn RenderDdl>>,
}

impl Endpoints {
    pub fn roles(&self) -> Roles {
        Roles {
            source: self.source.is_some(),
            sink: self.sink.is_some(),
        }
    }
}

impl fmt::Debug for Endpoints {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Endpoints")
            .field("source", &self.source.is_some())
            .field("sink", &self.sink.is_some())
            .field("finalizer", &self.finalizer.is_some())
            .field("ddl", &self.ddl.is_some())
            .finish()
    }
}

/// What a connector is told about the connection it builds. Data only,
/// never handles into the runner.
#[non_exhaustive]
#[derive(Debug, Clone, Copy)]
pub struct BuildContext<'a> {
    /// The connection's `name:`, for error messages.
    pub connection: &'a str,
    // Room for runtime hints later (e.g. a pool size derived from A5's
    // `source_max_connections`) without breaking connectors.
}

impl<'a> BuildContext<'a> {
    pub fn new(connection: &'a str) -> Self {
        Self { connection }
    }
}

/// Why a connection could not be built.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum BuildError {
    /// Bad options, each with a path relative to the connection entry
    /// (e.g. `url`, `tls.ca_file`). The CLI maps the path to file:line.
    #[error("{}", display_options(.0))]
    Options(Vec<OptionsError>),
    #[error(transparent)]
    Other(FerryError),
}

fn display_options(errors: &[OptionsError]) -> String {
    errors
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("; ")
}

impl BuildError {
    /// One options error at `path`.
    pub fn option(path: impl Into<String>, message: impl Into<String>) -> Self {
        BuildError::Options(vec![OptionsError {
            path: path.into(),
            message: message.into(),
        }])
    }

    /// The same error with every secret `redactor` knows replaced. A
    /// [`FerryError`] that mentioned one becomes a `Connector` error carrying
    /// the redacted text, since its own fields can't be rewritten generally.
    pub fn redact(self, redactor: &Redactor, connector: &str) -> Self {
        match self {
            BuildError::Options(errors) => BuildError::Options(
                errors
                    .into_iter()
                    .map(|e| OptionsError {
                        path: redactor.redact(&e.path).into_owned(),
                        message: redactor.redact(&e.message).into_owned(),
                    })
                    .collect(),
            ),
            BuildError::Other(FerryError::Connector { connector, message }) => {
                BuildError::Other(FerryError::Connector {
                    connector: redactor.redact(&connector).into_owned(),
                    message: redactor.redact(&message).into_owned(),
                })
            }
            BuildError::Other(e) => {
                let text = e.to_string();
                match redactor.redact(&text) {
                    Cow::Borrowed(_) => BuildError::Other(e),
                    Cow::Owned(message) => BuildError::Other(FerryError::Connector {
                        connector: connector.to_owned(),
                        message,
                    }),
                }
            }
        }
    }
}

/// One bad option.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{}{message}", if path.is_empty() { String::new() } else { format!("`{path}`: ") })]
pub struct OptionsError {
    /// Dot-separated, relative to the connection entry; empty for the entry
    /// itself.
    pub path: String,
    pub message: String,
}

/// Endpoints plus everything the CLI needs to display and redact them.
pub struct Built {
    pub endpoints: Endpoints,
    /// [`Connector::describe`], already redacted.
    pub description: String,
    /// Every secret value of this connection: `x-ferry-secret` fields and
    /// [`Connector::derived_secrets`]. Feed them to the CLI's [`Redactor`].
    pub secrets: Vec<String>,
}

impl fmt::Debug for Built {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Built")
            .field("endpoints", &self.endpoints)
            .field("description", &self.description)
            .field("secrets", &self.secrets.len())
            .finish()
    }
}

/// The registry's view of one `connections.yaml` entry, after `${ENV}`
/// substitution: `name`, `type`, and the type-specific fields left as JSON.
#[derive(Clone, PartialEq, Deserialize)]
pub struct RawConnection {
    pub name: String,
    pub r#type: String,
    #[serde(flatten)]
    pub fields: serde_json::Map<String, serde_json::Value>,
}

/// Field values may be secrets, so only their keys are shown.
impl fmt::Debug for RawConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RawConnection")
            .field("name", &self.name)
            .field("type", &self.r#type)
            .field("fields", &self.fields.keys().collect::<Vec<_>>())
            .finish()
    }
}
