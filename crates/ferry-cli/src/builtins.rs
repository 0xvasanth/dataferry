//! The built-in connectors, registered through the same public API a
//! third-party binary uses (0004 §5): no private path, no match on type
//! names. Each sits behind a default-on cargo feature, so a slim binary can
//! drop a driver.

use crate::Ferry;

/// TODO(C1 follow-up, 0XT-276): register the built-ins once their
/// `Connector` impls land on main from track A (A1/A2/A3/A4):
///
/// ```ignore
/// #[cfg(feature = "postgres")]
/// let ferry = ferry.register_state_capable(
///     ferry_connector_postgres::PostgresConnector,
///     ferry_state::PgStateBackend,
/// );
/// #[cfg(feature = "mongo")]
/// let ferry = ferry.register(ferry_connector_mongo::MongoConnector);
/// ```
///
/// Until then the stock binary has no connection types, and `ferry validate`
/// says so in its "unknown connection type" help.
pub(crate) fn register(ferry: Ferry) -> Ferry {
    ferry
}
