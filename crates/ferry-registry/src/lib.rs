//! The connector author's API (decision 0004): the [`Connector`] trait, the
//! [`ConnectorRegistry`] that maps `connections[].type` to one, [`Secret`]
//! and redaction, the JSON Schema composition for `connections.yaml`, and
//! the state-capable extension ([`state`]) that lets a sink commit its rows
//! and the state store's done-record in one transaction.
//!
//! Depends on `ferry-core` only. Built-in connectors register through this
//! crate exactly the way a third-party connector would.

mod connector;
mod registry;
mod schema;
pub mod secret;
pub mod state;

pub use connector::{
    BuildContext, BuildError, Built, Connector, Endpoints, OptionsError, RawConnection, Roles,
};
pub use registry::{ConnectorRegistry, DynConnector, RegisterError};
pub use schema::{ENV_REFERENCE_PATTERN, RESERVED_KEYS};
pub use secret::{Redactor, Secret};
pub use state::{Ledger, StateBackend, StateCapable, StateStore};
