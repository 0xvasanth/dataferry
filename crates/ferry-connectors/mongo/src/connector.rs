//! The `mongo` connection type (0004 §2), registered through
//! `ferry-registry` exactly the way a third-party connector would be. It is
//! a source only until the Mongo sink decision (S3) says otherwise.

use std::sync::Arc;

use ferry_core::FerryError;
use ferry_registry::secret::url_password;
use ferry_registry::{BuildContext, BuildError, Connector, Endpoints, Roles, Secret};
use mongodb::options::{HostInfo, ServerAddress};
use schemars::JsonSchema;
use serde::Deserialize;

use crate::source::MongoSource;
use crate::tls;

/// The built-in `mongo` connector: a source.
#[derive(Debug, Clone, Copy, Default)]
pub struct MongoConnector;

/// A MongoDB database.
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MongoOptions {
    /// `mongodb://` or `mongodb+srv://` connection string. TLS settings
    /// (`tls`, `tlsCAFile`) go here.
    pub url: Secret,
    /// The database whose collections are read.
    pub database: String,
}

impl Connector for MongoConnector {
    const TYPE: &'static str = "mongo";
    const ROLES: Roles = Roles::SOURCE;
    type Options = MongoOptions;

    fn build(
        &self,
        _: &BuildContext<'_>,
        options: MongoOptions,
        wants: Roles,
    ) -> Result<Endpoints, BuildError> {
        if options.database.is_empty() {
            return Err(BuildError::option("database", "must not be empty"));
        }
        let source = MongoSource::new(options.url.expose(), options.database).map_err(|e| {
            let message = match e {
                FerryError::Connector { message, .. } => message,
                e => e.to_string(),
            };
            BuildError::option("url", message)
        })?;
        Ok(Endpoints {
            source: wants.source.then(|| Arc::new(source) as _),
            sink: None,
            finalizer: None,
            ddl: None,
        })
    }

    /// `mongodb://db.example.com:27017, database shop`: hosts only, never
    /// the credentials or options.
    fn describe(&self, options: &MongoOptions) -> String {
        let hosts = match tls::parse(options.url.expose()).map(|c| c.host_info) {
            Ok(HostInfo::HostIdentifiers(addresses)) => {
                let hosts: Vec<String> = addresses
                    .iter()
                    .map(|a| match a {
                        ServerAddress::Tcp { host, port } => {
                            format!("{host}:{}", port.unwrap_or(27017))
                        }
                        other => other.to_string(),
                    })
                    .collect();
                format!("mongodb://{}", hosts.join(","))
            }
            Ok(HostInfo::DnsRecord(name)) => format!("mongodb+srv://{name}"),
            _ => "mongo (invalid url)".into(),
        };
        format!("{hosts}, database {}", options.database)
    }

    fn derived_secrets(&self, options: &MongoOptions) -> Vec<String> {
        url_password(options.url.expose())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options(url: &str) -> MongoOptions {
        MongoOptions {
            url: Secret::new(url),
            database: "shop".into(),
        }
    }

    #[test]
    fn describe_names_the_hosts_but_not_the_credentials() {
        let text = MongoConnector.describe(&options(
            "mongodb://app:s3cret@db1.example.com,db2.example.com:27018/admin?replicaSet=rs0",
        ));
        assert_eq!(
            text,
            "mongodb://db1.example.com:27017,db2.example.com:27018, database shop"
        );
    }

    #[test]
    fn srv_urls_describe_their_record_name() {
        // No DNS lookup: `mongodb+srv` resolves on connect.
        let text = MongoConnector.describe(&options("mongodb+srv://app:pw@cluster.example.com/"));
        assert_eq!(text, "mongodb+srv://cluster.example.com, database shop");
    }
}
