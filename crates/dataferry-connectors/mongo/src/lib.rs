//! MongoDB source for dataferry, and the `mongo` connector that registers it.

mod connector;
mod convert;
mod decimal;
mod infer;
mod source;
mod tls;

pub use connector::{MongoConnector, MongoOptions};
pub use source::MongoSource;
