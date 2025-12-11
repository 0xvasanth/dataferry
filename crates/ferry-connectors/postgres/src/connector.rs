//! The `postgres` connection type (0004 §2), registered through
//! `ferry-registry` exactly the way a third-party connector would be.
//!
//! It is state-capable (0004 §8): the state store can live in the same
//! database, on the sink's pool, so a partition's rows and its done-record
//! commit in one transaction. The driver handles a state backend works with
//! are `deadpool_postgres::Pool` (deadpool-postgres 0.14, Tokio) and
//! `tokio_postgres::Client`.

use std::sync::Arc;

use deadpool_postgres::Pool;
use ferry_core::FerryError;
use ferry_registry::secret::url_password;
use ferry_registry::{
    BuildContext, BuildError, Connector, Endpoints, Ledger, Roles, Secret, StateCapable,
};
use schemars::JsonSchema;
use serde::Deserialize;
use tokio_postgres::Client;
use tokio_postgres::config::{Config, Host};

use crate::deferred::{ForeignKeys, LoadOptions};
use crate::pool;
use crate::sink::PostgresSink;
use crate::source::PostgresSource;
use crate::tls::{self, Tls};

/// The built-in `postgres` connector: a source and a sink.
#[derive(Debug, Clone, Copy, Default)]
pub struct PostgresConnector;

/// A Postgres database.
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PostgresOptions {
    /// libpq URL (`postgres://user@host:5432/db?sslmode=require`) or
    /// key=value string. TLS settings (`sslmode`, `sslrootcert`) go here.
    pub url: Secret,
    /// Overrides a password in `url`.
    #[serde(default)]
    pub password: Option<Secret>,
    /// The schema read from as a source and written to as a sink.
    #[serde(default = "default_schema")]
    pub schema: String,
    /// As a sink: drop the secondary indexes of target tables that already
    /// exist (not unique, not backing a constraint) before a load, and
    /// recreate them after it. Faster loads into indexed tables.
    #[serde(default)]
    pub drop_indexes: bool,
    /// As a sink: `defer` (the default) drops the foreign keys of target
    /// tables that already exist for the load, then recreates and validates
    /// them, reporting rows that break one. `enforce` keeps them during the
    /// load.
    #[serde(default)]
    pub foreign_keys: ForeignKeys,
}

fn default_schema() -> String {
    "public".into()
}

impl PostgresConnector {
    /// The parsed connection string, with `password` applied.
    fn parse(options: &PostgresOptions) -> Result<(Config, Tls), BuildError> {
        let (mut config, tls) = tls::parse(options.url.expose()).map_err(|e| {
            let message = match e {
                FerryError::Connector { message, .. } => message,
                e => e.to_string(),
            };
            BuildError::option("url", message)
        })?;
        if let Some(password) = &options.password {
            config.password(password.expose());
        }
        if options.schema.is_empty() {
            return Err(BuildError::option("schema", "must not be empty"));
        }
        Ok((config, tls))
    }

    fn endpoints(
        options: &PostgresOptions,
        wants: Roles,
        parsed: (Config, Tls),
        sink: impl FnOnce() -> Result<PostgresSink, BuildError>,
    ) -> Result<Endpoints, BuildError> {
        let (config, tls) = parsed;
        // One sink is all three: finalize restores what its prepare
        // dropped, and it renders what its prepare would create.
        let sink = match wants.sink {
            true => Some(Arc::new(sink()?.with_load(LoadOptions {
                drop_indexes: options.drop_indexes,
                foreign_keys: options.foreign_keys,
            }))),
            false => None,
        };
        Ok(Endpoints {
            source: wants
                .source
                .then(|| Arc::new(PostgresSource::from_parts(config, tls, &options.schema)) as _),
            finalizer: sink.clone().map(|s| s as _),
            ddl: sink.clone().map(|s| s as _),
            sink: sink.map(|s| s as _),
        })
    }
}

impl Connector for PostgresConnector {
    const TYPE: &'static str = "postgres";
    const ROLES: Roles = Roles::BOTH;
    type Options = PostgresOptions;

    fn build(
        &self,
        _: &BuildContext<'_>,
        options: PostgresOptions,
        wants: Roles,
    ) -> Result<Endpoints, BuildError> {
        let parsed = Self::parse(&options)?;
        let (config, tls) = parsed.clone();
        Self::endpoints(&options, wants, parsed, || {
            let pool = pool::build(config, tls).map_err(BuildError::Other)?;
            Ok(PostgresSink::from_pool(pool, &options.schema, None))
        })
    }

    /// `postgres://app@db.example.com:5432/shop, schema public`: user,
    /// hosts, ports and database, never the password.
    fn describe(&self, options: &PostgresOptions) -> String {
        match Self::parse(options) {
            Ok((config, _)) => format!("{}, schema {}", location(&config), options.schema),
            Err(_) => format!("postgres (invalid url), schema {}", options.schema),
        }
    }

    /// The password in `url`, whether written as a URL's userinfo or as a
    /// key=value `password=`.
    fn derived_secrets(&self, options: &PostgresOptions) -> Vec<String> {
        let mut secrets = url_password(options.url.expose());
        if let Ok((config, _)) = tls::parse(options.url.expose())
            && let Some(password) = config.get_password()
        {
            let password = String::from_utf8_lossy(password).into_owned();
            if !password.is_empty() && !secrets.contains(&password) {
                secrets.push(password);
            }
        }
        secrets
    }
}

impl StateCapable for PostgresConnector {
    type Pool = Pool;
    type Conn = Client;

    fn pool(&self, _: &BuildContext<'_>, options: &PostgresOptions) -> Result<Pool, BuildError> {
        let (config, tls) = Self::parse(options)?;
        pool::build(config, tls).map_err(BuildError::Other)
    }

    fn build_with_ledger(
        &self,
        _: &BuildContext<'_>,
        options: PostgresOptions,
        wants: Roles,
        pool: Pool,
        ledger: Arc<dyn Ledger<Client>>,
    ) -> Result<Endpoints, BuildError> {
        let parsed = Self::parse(&options)?;
        Self::endpoints(&options, wants, parsed, || {
            Ok(PostgresSink::from_pool(pool, &options.schema, Some(ledger)))
        })
    }
}

/// `postgres://user@host:port/db` from a parsed config.
fn location(config: &Config) -> String {
    let ports = config.get_ports();
    let hosts: Vec<String> = config
        .get_hosts()
        .iter()
        .enumerate()
        .map(|(i, host)| {
            let port = ports.get(i).or(ports.first()).copied().unwrap_or(5432);
            match host {
                Host::Tcp(name) => format!("{name}:{port}"),
                Host::Unix(path) => format!("{}:{port}", path.display()),
            }
        })
        .collect();
    let user = config
        .get_user()
        .map(|u| format!("{u}@"))
        .unwrap_or_default();
    let db = config.get_dbname().unwrap_or_default();
    format!("postgres://{user}{}/{db}", hosts.join(","))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options(url: &str) -> PostgresOptions {
        PostgresOptions {
            url: Secret::new(url),
            password: None,
            schema: "public".into(),
            drop_indexes: false,
            foreign_keys: ForeignKeys::Defer,
        }
    }

    #[test]
    fn describe_names_the_server_but_not_the_password() {
        let o = options("postgres://app:s3cret@db.example.com:6432/shop?sslmode=require");
        let text = PostgresConnector.describe(&o);
        assert_eq!(
            text,
            "postgres://app@db.example.com:6432/shop, schema public"
        );
    }

    #[test]
    fn passwords_in_either_url_form_are_derived_secrets() {
        let url = PostgresConnector.derived_secrets(&options("postgres://app:p%40ss@db/shop"));
        assert!(url.contains(&"p%40ss".to_owned()), "{url:?}");
        assert!(url.contains(&"p@ss".to_owned()), "{url:?}");
        let kv = PostgresConnector.derived_secrets(&options("host=db user=app password=kv-secret"));
        assert_eq!(kv, ["kv-secret"]);
    }

    #[test]
    fn a_bad_url_is_an_options_error_at_url() {
        let Err(BuildError::Options(errors)) =
            PostgresConnector::parse(&options("postgres://db/shop?sslmode=bogus"))
        else {
            panic!("expected an options error");
        };
        assert_eq!(errors[0].path, "url");
        assert!(errors[0].message.contains("sslmode"), "{errors:?}");
    }
}
