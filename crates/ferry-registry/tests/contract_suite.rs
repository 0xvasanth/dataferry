//! The connector contract suite is only useful if it catches broken
//! connectors. Each test runs it against a fake that keeps the contract
//! (which must pass) or one that breaks exactly one guarantee (which must be
//! caught, for the right reason).

mod common;

use std::sync::Arc;

use common::{FakeBackend, FakeProbe, FakeSql, Faults, URL, sink_fixture};
use ferry_core::IdempotencyStrategy;
use ferry_core::testkit::{MemSink, MemSource};
use ferry_registry::testkit::{ConnectorFixture, check_connector, check_state_capable};
use ferry_registry::{BuildContext, BuildError, Connector, Endpoints, Roles, Secret};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;

const SECRET: &str = "contract-s3cret";

fn fixture() -> ConnectorFixture {
    ConnectorFixture {
        options: json!({ "url": format!("svc://app:{SECRET}@host"), "dataset": "orders" }),
        secret: SECRET.into(),
    }
}

/// Builds a source-and-sink connector; `$options` and the methods' bodies
/// vary per broken case.
macro_rules! connector {
    ($name:ident, $options:ty, roles: $roles:expr, describe: |$o:ident| $describe:expr, build: |$wants:ident| $build:expr) => {
        struct $name;
        impl Connector for $name {
            const TYPE: &'static str = "svc";
            const ROLES: Roles = $roles;
            type Options = $options;
            #[allow(unused_variables)]
            fn build(
                &self,
                _: &BuildContext<'_>,
                $o: $options,
                $wants: Roles,
            ) -> Result<Endpoints, BuildError> {
                $build
            }
            #[allow(unused_variables)]
            fn describe(&self, $o: &$options) -> String {
                $describe
            }
        }
    };
}

fn endpoints(wants: Roles) -> Endpoints {
    Endpoints {
        source: wants.source.then(|| Arc::new(MemSource::new()) as _),
        sink: wants
            .sink
            .then(|| Arc::new(MemSink::new(vec![IdempotencyStrategy::UpsertByKey])) as _),
        finalizer: None,
        ddl: None,
    }
}

/// A service.
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct Good {
    url: Secret,
    dataset: String,
}

/// No `deny_unknown_fields`.
#[derive(Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Open {
    url: Secret,
    dataset: String,
}

/// The credential isn't a `Secret`.
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[allow(dead_code)]
struct Unmarked {
    url: String,
    dataset: String,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[allow(dead_code)]
struct Reserved {
    url: Secret,
    dataset: String,
    description: Option<String>,
}

fn assert_caught(violations: Vec<String>, reason: &str) {
    assert!(
        violations.iter().any(|v| v.contains(reason)),
        "expected a violation containing {reason:?}, got {violations:?}"
    );
}

#[test]
fn a_well_behaved_connector_passes() {
    connector!(Svc, Good, roles: Roles::BOTH, describe: |o| format!("svc, dataset {}", o.dataset), build: |wants| Ok(endpoints(wants)));
    assert_eq!(check_connector(Svc, &fixture()), Vec::<String>::new());

    connector!(ReadOnly, Good, roles: Roles::SOURCE, describe: |o| o.dataset.clone(), build: |wants| Ok(endpoints(wants)));
    assert_eq!(check_connector(ReadOnly, &fixture()), Vec::<String>::new());
}

#[test]
fn the_fakesql_connector_passes() {
    let fixture = ConnectorFixture {
        options: json!({ "url": URL }),
        secret: "s3cr3t-pw".into(),
    };
    assert_eq!(
        check_connector(FakeSql::default(), &fixture),
        Vec::<String>::new()
    );
}

#[test]
fn open_options_are_caught() {
    connector!(Svc, Open, roles: Roles::BOTH, describe: |o| String::new(), build: |wants| Ok(endpoints(wants)));
    let violations = check_connector(Svc, &fixture());
    assert_caught(violations.clone(), "options schema is open");
    assert_caught(violations, "was accepted");
}

#[test]
fn a_describe_that_leaks_the_secret_is_caught() {
    // The registry would redact this; the suite checks the connector itself.
    connector!(Svc, Good, roles: Roles::BOTH, describe: |o| format!("svc at {}", o.url.expose()), build: |wants| Ok(endpoints(wants)));
    assert_caught(
        check_connector(Svc, &fixture()),
        "the secret appears in describe()",
    );
}

#[test]
fn an_unmarked_credential_is_caught() {
    connector!(Svc, Unmarked, roles: Roles::BOTH, describe: |o| String::new(), build: |wants| Ok(endpoints(wants)));
    assert_caught(
        check_connector(Svc, &fixture()),
        "missing from the source build's redaction set",
    );
}

#[test]
fn returning_unasked_roles_is_caught() {
    connector!(Svc, Good, roles: Roles::BOTH, describe: |o| String::new(), build: |wants| Ok(endpoints(Roles::BOTH)));
    assert_caught(
        check_connector(Svc, &fixture()),
        "building as source failed: svc: connector bug",
    );
}

#[test]
fn a_reserved_key_is_caught() {
    connector!(Svc, Reserved, roles: Roles::BOTH, describe: |o| String::new(), build: |wants| Ok(endpoints(wants)));
    assert_caught(check_connector(Svc, &fixture()), "does not register");
}

#[test]
fn a_build_error_that_leaks_the_secret_is_still_clean_after_redaction() {
    // The registry's backstop redacts build errors, so the suite sees them
    // clean; it reports the failed build itself.
    connector!(Svc, Good, roles: Roles::BOTH, describe: |o| String::new(), build: |wants| Err(BuildError::option("url", format!("bad {}", o.url.expose()))));
    let violations = check_connector(Svc, &fixture());
    assert_caught(
        violations.clone(),
        "building as source failed: `url`: bad ***",
    );
    assert!(
        !violations.iter().any(|v| v.contains(SECRET)),
        "{violations:?}"
    );
}

// ---- state-capable ----

async fn check_fakesql(faults: Faults) -> Vec<String> {
    let connector = FakeSql::default();
    *connector.server.faults.lock().unwrap() = faults;
    let probe = FakeProbe {
        server: Arc::clone(&connector.server),
    };
    let fixture = ConnectorFixture {
        options: json!({ "url": URL }),
        secret: "s3cr3t-pw".into(),
    };
    check_state_capable(connector, FakeBackend, &fixture, &sink_fixture(), &probe).await
}

#[tokio::test]
async fn the_fakesql_state_backend_passes() {
    assert_eq!(check_fakesql(Faults::default()).await, Vec::<String>::new());
}

#[tokio::test]
async fn claiming_txn_with_state_without_a_ledger_is_caught() {
    assert_caught(
        check_fakesql(Faults {
            txn_with_state_without_ledger: true,
            ..Default::default()
        })
        .await,
        "a sink built without the state store lists TxnWithState",
    );
}

#[tokio::test]
async fn rows_written_outside_the_transaction_are_caught() {
    let violations = check_fakesql(Faults {
        rows_outside_txn: true,
        ..Default::default()
    })
    .await;
    assert_caught(
        violations.clone(),
        "after a crash BeforeRecord, `orders` has 3 rows instead of 0",
    );
    assert_caught(
        violations,
        "after a crash BeforeRecord, `order_items` has 5 rows",
    );
}

#[tokio::test]
async fn a_done_record_written_outside_the_transaction_is_caught() {
    let violations = check_fakesql(Faults {
        done_outside_txn: true,
        ..Default::default()
    })
    .await;
    assert_caught(
        violations.clone(),
        "after a crash AfterRecord, the partition is recorded done",
    );
    // Rows still roll back; only the record escaped.
    assert!(
        !violations.iter().any(|v| v.contains("rows instead of 0")),
        "{violations:?}"
    );
}
