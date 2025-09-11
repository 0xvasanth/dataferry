//! The built-in connectors, registered through the same public API a
//! third-party binary uses (0004 §5): no private path, no match on type
//! names. Each sits behind a default-on cargo feature, so a slim binary can
//! drop a driver.

use crate::Ferry;

/// `postgres` (source and sink, and the home of the `postgres` state
/// backend) and `mongo` (source), as their features allow.
pub(crate) fn register(ferry: Ferry) -> Ferry {
    #[cfg(feature = "postgres")]
    let ferry = ferry.register_state_capable(
        ferry_connector_postgres::PostgresConnector,
        ferry_state::PgStateBackend,
    );
    #[cfg(feature = "mongo")]
    let ferry = ferry.register(ferry_connector_mongo::MongoConnector);
    ferry
}
