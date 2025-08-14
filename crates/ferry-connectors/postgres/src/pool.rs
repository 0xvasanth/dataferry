//! The connection pool behind the sink, and behind the state store when it
//! lives on the same connection (0004 §8): one `deadpool_postgres::Pool`,
//! built from a parsed connection string under the TLS policy in
//! [`crate::tls`].

use deadpool_postgres::{Manager, ManagerConfig, Object, Pool, PoolError, RecyclingMethod};
use ferry_core::{FerryError, Result};
use tokio_postgres::{Config, NoTls};

use crate::source::{connector_error, pg_error};
use crate::tls::{LazyTls, Tls};

/// Builds a pool. Does no I/O: connections open when they are first needed.
pub(crate) fn build(config: Config, tls: Tls) -> Result<Pool> {
    let manager_config = ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    };
    let manager = if tls.is_plaintext() {
        Manager::from_config(config, NoTls, manager_config)
    } else {
        Manager::from_config(config, LazyTls::new(tls), manager_config)
    };
    Pool::builder(manager)
        .build()
        .map_err(|e| connector_error(format!("connection pool: {e}")))
}

/// A pooled connection.
pub(crate) async fn get(pool: &Pool) -> Result<Object> {
    pool.get().await.map_err(pool_error)
}

fn pool_error(e: PoolError) -> FerryError {
    match e {
        PoolError::Backend(e) => pg_error(e),
        e => connector_error(format!("connection pool: {e}")),
    }
}
