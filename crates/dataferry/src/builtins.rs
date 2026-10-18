//! The built-in connectors, registered through the same public API a
//! third-party binary uses (0004 §5): no private path, no match on type
//! names. Each sits behind a default-on cargo feature, so a slim binary can
//! drop a driver.

use crate::Ferry;

/// `postgres` (source and sink, and the home of the `postgres` state
/// backend) and `mongo` (source), as their features allow. A `postgres`
/// sink is verified after a run by reading it back, and its tables may be
/// named with the connection's schema (`app.public.orders`).
pub(crate) fn register(ferry: Ferry) -> Ferry {
    #[cfg(feature = "postgres")]
    let ferry = {
        let mut ferry = ferry.register_state_capable(
            dataferry_connector_postgres::PostgresConnector,
            dataferry_state::PgStateBackend,
        );
        ferry.targets.add("postgres", crate::targets::postgres);
        ferry
            .namespaces
            .add("postgres", crate::namespaces::postgres);
        ferry
    };
    #[cfg(feature = "mongo")]
    let ferry = ferry.register(dataferry_connector_mongo::MongoConnector);
    ferry
}
