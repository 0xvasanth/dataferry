//! Postgres source (and, later, sink) for 0xferry.

mod columns;
mod decode;
mod pg_type;
mod source;

pub use source::PostgresSource;
