//! Connectors for a third-party ferry binary, written against the public
//! API only (ferry-core, ferry-registry, and `ferry_cli::Ferry`).
//!
//! `postgres` and `mongo` mirror the built-in types' options (0004 §2), so
//! the repository's example configs validate against them. Every endpoint
//! they build panics the moment anything tries to use it: `ferry validate`
//! passing with these registered proves it never connects. `fake` is a type
//! the stock binary doesn't have.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use ferry_cli::Ferry;
use ferry_core::{
    BatchStream, Entity, FerryError, IdempotencyStrategy, Lease, Partition, PartitionHint,
    PartitionId, Result, Sink, SinkCaps, SinkTxn, Source, SourceCaps, TargetSchema, WriteMode,
    WriteSummary,
};
use ferry_registry::secret::url_password;
use ferry_registry::state::{NewRun, PartitionCounts, RunId, RunRecord};
use ferry_registry::{
    BuildContext, BuildError, Connector, Endpoints, Ledger, Roles, Secret, StateBackend,
    StateCapable, StateStore,
};
use schemars::JsonSchema;
use serde::Deserialize;

/// The binary's command line: what `main` runs, and what tests build on.
pub fn cli() -> Ferry {
    Ferry::new()
        .register_state_capable(Postgres, Backend)
        .register(Mongo)
        .register(Fake)
        .name("acme-ferry")
}

const NEVER: &str = "ferry validate must never connect";

/// A source and sink that panic on first use, standing in for a driver
/// that would open a connection.
struct Unreachable {
    connection: String,
}

#[async_trait]
impl Source for Unreachable {
    async fn discover(&self) -> Result<Vec<Entity>> {
        panic!("{NEVER}: discover on `{}`", self.connection)
    }

    async fn plan_partitions(&self, _: &Entity, _: &PartitionHint) -> Result<Vec<Partition>> {
        panic!("{NEVER}: plan_partitions on `{}`", self.connection)
    }

    async fn read(&self, _: &Entity, _: &Partition) -> Result<BatchStream> {
        panic!("{NEVER}: read on `{}`", self.connection)
    }

    async fn capabilities(&self) -> Result<SourceCaps> {
        panic!("{NEVER}: source capabilities on `{}`", self.connection)
    }
}

#[async_trait]
impl Sink for Unreachable {
    async fn prepare(&self, _: &TargetSchema, _: WriteMode) -> Result<()> {
        panic!("{NEVER}: prepare on `{}`", self.connection)
    }

    async fn begin(
        &self,
        _: &Partition,
        _: &Lease,
        _: IdempotencyStrategy,
    ) -> Result<Box<dyn SinkTxn>> {
        panic!("{NEVER}: begin on `{}`", self.connection)
    }

    async fn capabilities(&self) -> Result<SinkCaps> {
        panic!("{NEVER}: sink capabilities on `{}`", self.connection)
    }
}

fn endpoints(ctx: &BuildContext<'_>, wants: Roles) -> Endpoints {
    let endpoint = Arc::new(Unreachable {
        connection: ctx.connection.to_owned(),
    });
    Endpoints {
        source: wants.source.then(|| endpoint.clone() as Arc<dyn Source>),
        sink: wants.sink.then_some(endpoint as Arc<dyn Sink>),
    }
}

/// Like libpq: the URL must at least have a scheme.
fn check_url(url: &Secret, schemes: &[&str]) -> std::result::Result<(), BuildError> {
    match url.expose().split_once("://") {
        Some((scheme, _)) if schemes.contains(&scheme) => Ok(()),
        _ => Err(BuildError::option(
            "url",
            format!("expected a {} URL", schemes.join(" or ")),
        )),
    }
}

// ---- postgres: source and sink, can hold the state store ----

pub struct Postgres;

/// A Postgres database.
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PostgresOptions {
    /// libpq URL; `sslmode` goes here.
    url: Secret,
    /// Overrides a password in `url`.
    #[serde(default)]
    password: Option<Secret>,
    /// Read from as a source, written to as a sink.
    #[serde(default = "public")]
    schema: String,
}

fn public() -> String {
    "public".into()
}

impl Connector for Postgres {
    const TYPE: &'static str = "postgres";
    const ROLES: Roles = Roles::BOTH;
    type Options = PostgresOptions;

    fn build(
        &self,
        ctx: &BuildContext<'_>,
        options: PostgresOptions,
        wants: Roles,
    ) -> std::result::Result<Endpoints, BuildError> {
        check_url(&options.url, &["postgres", "postgresql"])?;
        Ok(endpoints(ctx, wants))
    }

    fn describe(&self, options: &PostgresOptions) -> String {
        let host = options
            .url
            .expose()
            .rsplit_once('@')
            .map_or("?", |(_, host)| host);
        let password = match options.password {
            Some(_) => ", password from `password`",
            None => "",
        };
        format!("postgres at {host}, schema {}{password}", options.schema)
    }

    fn derived_secrets(&self, options: &PostgresOptions) -> Vec<String> {
        url_password(options.url.expose())
    }
}

/// The driver's pool, connecting lazily: there is nothing to connect to.
#[derive(Clone)]
pub struct Pool;

impl StateCapable for Postgres {
    type Pool = Pool;
    type Conn = ();

    fn pool(
        &self,
        _: &BuildContext<'_>,
        _: &PostgresOptions,
    ) -> std::result::Result<Pool, BuildError> {
        Ok(Pool)
    }

    fn build_with_ledger(
        &self,
        ctx: &BuildContext<'_>,
        options: PostgresOptions,
        wants: Roles,
        _: Pool,
        _: Arc<dyn Ledger<()>>,
    ) -> std::result::Result<Endpoints, BuildError> {
        self.build(ctx, options, wants)
    }
}

/// A state store that panics on first use, like the endpoints.
pub struct Backend;

impl StateBackend for Backend {
    type Pool = Pool;
    type Conn = ();

    fn open(&self, _: Pool) -> (Arc<dyn StateStore>, Arc<dyn Ledger<()>>) {
        (Arc::new(Store), Arc::new(Store))
    }
}

struct Store;

#[async_trait]
impl StateStore for Store {
    async fn create_run(&self, _: NewRun) -> Result<RunId> {
        panic!("{NEVER}: create_run")
    }

    async fn load_run(&self, _: &RunId) -> Result<Option<RunRecord>> {
        panic!("{NEVER}: load_run")
    }

    async fn store_partitions(&self, _: &RunId, _: &str, _: &[Partition]) -> Result<()> {
        panic!("{NEVER}: store_partitions")
    }

    async fn load_partitions(&self, _: &RunId, _: &str) -> Result<Vec<Partition>> {
        panic!("{NEVER}: load_partitions")
    }

    async fn lease_next(&self, _: &RunId, _: &str, _: Duration) -> Result<Option<Lease>> {
        panic!("{NEVER}: lease_next")
    }

    async fn renew(&self, _: &RunId, _: &Lease, _: Duration) -> Result<()> {
        panic!("{NEVER}: renew")
    }

    async fn mark_done(&self, _: &RunId, _: &Lease, _: &WriteSummary) -> Result<()> {
        panic!("{NEVER}: mark_done")
    }

    async fn mark_failed(&self, _: &RunId, _: &Lease, _: &str) -> Result<()> {
        panic!("{NEVER}: mark_failed")
    }

    async fn is_done(&self, _: &RunId, _: &PartitionId) -> Result<bool> {
        panic!("{NEVER}: is_done")
    }

    async fn counts(&self, _: &RunId) -> Result<PartitionCounts> {
        panic!("{NEVER}: counts")
    }
}

#[async_trait]
impl Ledger<()> for Store {
    async fn record_done(&self, _: &mut (), _: &Lease, _: &WriteSummary) -> Result<()> {
        panic!("{NEVER}: record_done")
    }
}

// ---- mongo: a source only ----

pub struct Mongo;

/// A MongoDB database.
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MongoOptions {
    /// `mongodb://` or `mongodb+srv://`.
    url: Secret,
    database: String,
}

impl Connector for Mongo {
    const TYPE: &'static str = "mongo";
    const ROLES: Roles = Roles::SOURCE;
    type Options = MongoOptions;

    fn build(
        &self,
        ctx: &BuildContext<'_>,
        options: MongoOptions,
        wants: Roles,
    ) -> std::result::Result<Endpoints, BuildError> {
        check_url(&options.url, &["mongodb", "mongodb+srv"])?;
        Ok(endpoints(ctx, wants))
    }

    fn describe(&self, options: &MongoOptions) -> String {
        format!("mongo database {}", options.database)
    }
}

// ---- fake: a type only this binary has ----

pub struct Fake;

/// An inventory service.
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FakeOptions {
    url: Secret,
    /// Makes `build` fail with an error that quotes the URL, the way a
    /// careless driver does. The CLI's redaction pass must still hide it.
    #[serde(default)]
    leak_url_in_error: bool,
}

impl Connector for Fake {
    const TYPE: &'static str = "fake";
    const ROLES: Roles = Roles::BOTH;
    type Options = FakeOptions;

    fn build(
        &self,
        ctx: &BuildContext<'_>,
        options: FakeOptions,
        wants: Roles,
    ) -> std::result::Result<Endpoints, BuildError> {
        if options.leak_url_in_error {
            return Err(BuildError::Other(FerryError::Connector {
                connector: "fake".into(),
                message: format!("could not parse {}", options.url.expose()),
            }));
        }
        Ok(endpoints(ctx, wants))
    }

    fn describe(&self, _: &FakeOptions) -> String {
        "fake inventory".into()
    }
}
