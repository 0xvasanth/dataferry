//! Postgres source and sink for 0xferry, and the `postgres` connector that
//! registers them (and the state store's pool) with the registry.

mod columns;
mod connector;
mod ddl;
mod decode;
mod deferred;
mod encode;
mod json;
mod pg_type;
mod pool;
mod sink;
mod source;
mod tls;

pub use connector::{PostgresConnector, PostgresOptions};
pub use deferred::{ForeignKeys, LoadOptions};
pub use sink::PostgresSink;
pub use source::PostgresSource;
