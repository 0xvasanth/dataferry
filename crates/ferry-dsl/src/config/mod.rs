//! The `ferry/` config directory: loading, the model, and the checks that
//! need no connection (0003).

mod de;
mod env;
mod error;
mod hash;
mod load;
pub mod model;
pub mod schema;
mod span;
mod validate;
mod yaml;

pub use env::{Redactor, Resolved, ResolvedConnection, ResolvedState, mask_urls};
pub use error::{ConfigError, ConfigErrors, ErrorKind};
pub use hash::ConfigHash;
pub use load::{DEFAULT_DIR, FileKind, SourceFile, check_file, file_to_json, load, load_default};
pub use model::{Connection, Flow, Runtime, Version};
pub use span::{FieldPath, Location, Spanned};

use model::Ident;

/// A loaded config: every file merged into one model. Flows and connections
/// are in load order (files sorted by path, then as written); nothing about
/// the model depends on that order.
///
/// Two configs are equal when their models are, wherever they were read
/// from.
#[derive(Clone, Debug)]
pub struct Config {
    pub version: Version,
    pub name: Spanned<Ident>,
    pub connections: Vec<Connection>,
    pub runtime: Runtime,
    pub flows: Vec<Flow>,
    sources: Vec<SourceFile>,
}

impl PartialEq for Config {
    fn eq(&self, other: &Self) -> bool {
        self.version == other.version
            && self.name == other.name
            && self.connections == other.connections
            && self.runtime == other.runtime
            && self.flows == other.flows
    }
}

impl Config {
    /// The files the config was read from, in load order.
    pub fn sources(&self) -> &[SourceFile] {
        &self.sources
    }

    pub fn flow(&self, name: &str) -> Option<&Flow> {
        self.flows.iter().find(|f| f.name.as_str() == name)
    }

    pub fn connection(&self, name: &str) -> Option<&Connection> {
        self.connections.iter().find(|c| c.name.as_str() == name)
    }
}
