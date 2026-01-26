//! How `run` reads a sink's tables back to verify them (A8).
//!
//! The verifier's [`Target`] queries a sink's database after the load, and
//! a connector has no way to hand one out through the registry yet. So the
//! binary keeps one constructor per connection type it can verify, set
//! where its connectors are registered (`builtins`). A run whose sink has
//! none still runs; it is reported unverified.

use std::collections::BTreeMap;
use std::sync::Arc;

use ferry_registry::RawConnection;
use ferry_verify::Target;

/// Builds the verifier's view of a connection from its substituted entry.
/// Must not do I/O, like `Connector::build`.
pub(crate) type TargetFactory = fn(&RawConnection) -> Result<Arc<dyn Target>, String>;

#[derive(Default)]
pub(crate) struct Targets {
    by_type: BTreeMap<&'static str, TargetFactory>,
}

impl Targets {
    pub fn add(&mut self, type_name: &'static str, factory: TargetFactory) {
        self.by_type.insert(type_name, factory);
    }

    pub fn get(&self, type_name: &str) -> Option<TargetFactory> {
        self.by_type.get(type_name).copied()
    }
}

/// The built-in `postgres` target: the sink connection's schema, read back
/// through a Postgres source built for the connection the way the
/// connector builds every source, so `password:`, TLS and the rest of the
/// entry apply as they do to the run. Constraints and aggregates are
/// queried on a pool built from the same entry (connecting lazily), not
/// the run's own.
#[cfg(feature = "postgres")]
pub(crate) fn postgres(conn: &RawConnection) -> Result<Arc<dyn Target>, String> {
    use ferry_connector_postgres::{PostgresConnector, PostgresOptions};
    use ferry_registry::{BuildContext, BuildError, Connector, Roles, StateCapable};

    let options = || -> Result<PostgresOptions, String> {
        serde_json::from_value(serde_json::Value::Object(conn.fields.clone()))
            .map_err(|e| e.to_string())
    };
    let built = |e: BuildError| match e {
        BuildError::Options(errors) => errors
            .iter()
            .map(|e| format!("{}: {}", e.path, e.message))
            .collect::<Vec<_>>()
            .join("; "),
        BuildError::Other(e) => e.to_string(),
    };
    let ctx = BuildContext::new(&conn.name);
    let schema = options()?.schema;
    let pool = PostgresConnector.pool(&ctx, &options()?).map_err(built)?;
    let source = PostgresConnector
        .build(&ctx, options()?, Roles::SOURCE)
        .map_err(built)?
        .source
        .ok_or("the postgres connector built no source")?;
    Ok(Arc::new(ferry_verify::PgTarget::from_source(
        source, schema, pool,
    )))
}
