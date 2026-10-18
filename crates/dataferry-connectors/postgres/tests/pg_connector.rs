//! The `postgres` connector through the registry, against a real Postgres:
//! the registry's contract suites, and endpoints built from a
//! `connections.yaml` entry that really connect.

mod common;

use common::{Db, db, lease, orders, partition, sink_fixture};
use dataferry_connector_postgres::PostgresConnector;
use dataferry_core::{IdempotencyStrategy, WriteMode};
use dataferry_registry::testkit::{ConnectorFixture, check_connector, check_state_capable};
use dataferry_registry::{BuildError, ConnectorRegistry, RawConnection, Roles};
use dataferry_state::PgStateBackend;
use serde_json::{Value, json};

const PASSWORD: &str = "pg-s3cret-pw";

/// A login role whose password appears nowhere else in its URL, so a leak
/// check can't be fooled by the user or database name.
async fn app_role(db: &Db) -> String {
    db.client
        .batch_execute(&format!(
            "CREATE ROLE ferry_app LOGIN PASSWORD '{PASSWORD}';
             GRANT ALL ON DATABASE postgres TO ferry_app;
             GRANT ALL ON SCHEMA public TO ferry_app;"
        ))
        .await
        .unwrap();
    format!(
        "postgres://ferry_app:{PASSWORD}@127.0.0.1:{}/postgres",
        db.port
    )
}

fn connection(fields: Value) -> RawConnection {
    let mut entry = json!({"name": "warehouse", "type": "postgres"});
    entry
        .as_object_mut()
        .unwrap()
        .extend(fields.as_object().unwrap().clone());
    serde_json::from_value(entry).unwrap()
}

fn registry() -> ConnectorRegistry {
    let mut registry = ConnectorRegistry::new();
    registry.register(PostgresConnector);
    registry
}

#[tokio::test]
async fn passes_the_connector_contract() {
    let db = db().await;
    let url = app_role(&db).await;
    for options in [
        json!({"url": url}),
        // The secret only in the separate password field.
        json!({"url": url.replace(&format!(":{PASSWORD}"), ""), "password": PASSWORD}),
        // key=value form.
        json!({"url": format!(
            "host=127.0.0.1 port={} user=ferry_app password={PASSWORD} dbname=postgres",
            db.port
        )}),
    ] {
        let fixture = ConnectorFixture {
            options: options.clone(),
            secret: PASSWORD.into(),
        };
        let violations = check_connector(PostgresConnector, &fixture);
        assert!(violations.is_empty(), "{options}: {violations:#?}");
    }
}

/// The registry's state-capable suite with dataferry-state's real backend: a
/// crash before or after `record_done` commits neither the rows nor the
/// done-record, and a clean commit makes both durable. Under both write
/// modes, each into its own schema with its own run.
#[tokio::test]
async fn passes_the_state_capable_contract_with_the_real_state_backend() {
    let db = db().await;
    for mode in [WriteMode::Insert, WriteMode::Upsert] {
        let schema = format!("contract_{mode:?}").to_lowercase();
        let fixture = ConnectorFixture {
            options: json!({"url": db.url, "schema": schema}),
            secret: "postgres".into(),
        };
        let violations = check_state_capable(
            PostgresConnector,
            PgStateBackend,
            &fixture,
            &sink_fixture(mode),
            &db.probe(&schema),
        )
        .await;
        assert!(violations.is_empty(), "{mode:?}: {violations:#?}");
    }
}

#[tokio::test]
async fn endpoints_built_from_a_connection_entry_read_and_write() {
    let db = db().await;
    let url = app_role(&db).await;
    let built = registry()
        .build(&connection(json!({"url": url})), Roles::BOTH)
        .unwrap();
    assert_eq!(
        built.description,
        format!(
            "postgres://ferry_app@127.0.0.1:{}/postgres, schema public",
            db.port
        )
    );
    assert!(built.secrets.iter().any(|s| s == PASSWORD));

    let sink = built.endpoints.sink.unwrap();
    sink.prepare(&common::order_schema(), WriteMode::Insert)
        .await
        .unwrap();
    let mut txn = sink
        .begin(
            &partition("p"),
            &lease("p", 1),
            IdempotencyStrategy::DeletePartitionThenInsert,
        )
        .await
        .unwrap();
    txn.write("orders", orders(&[("o1", "new")])).await.unwrap();
    txn.commit().await.unwrap();

    let entities = built.endpoints.source.unwrap().discover().await.unwrap();
    let names: Vec<&str> = entities.iter().map(|e| e.name()).collect();
    assert_eq!(names, ["item_options", "order_items", "orders"]);
}

#[tokio::test]
async fn the_password_field_overrides_the_url_and_schema_defaults_to_public() {
    let db = db().await;
    let url = app_role(&db).await;
    let wrong = url.replace(PASSWORD, "wrong-password");
    let built = registry()
        .build(
            &connection(json!({"url": wrong, "password": PASSWORD})),
            Roles::SOURCE,
        )
        .unwrap();
    built.endpoints.source.unwrap().discover().await.unwrap();

    let built = registry()
        .build(&connection(json!({"url": wrong})), Roles::SOURCE)
        .unwrap();
    let err = built
        .endpoints
        .source
        .unwrap()
        .discover()
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("password authentication failed"), "{err}");
    assert!(!err.contains("wrong-password"), "{err}");
}

#[tokio::test]
async fn bad_options_are_reported_at_their_field_without_connecting() {
    let cases = [
        (json!({"url": "postgres://h/db?sslmode=sometimes"}), "url"),
        (json!({"url": "postgres://h/db", "schema": ""}), "schema"),
        (
            json!({"url": "postgres://h/db", "sslmode": "require"}),
            "sslmode",
        ),
        (json!({"url": 5432}), "url"),
    ];
    for (fields, path) in cases {
        match registry().build(&connection(fields.clone()), Roles::SINK) {
            Err(BuildError::Options(errors)) => {
                assert!(
                    errors.iter().any(|e| e.path == path),
                    "{fields}: {errors:?}"
                );
            }
            other => panic!("{fields}: expected an options error, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn a_bad_ca_file_fails_the_first_connect_not_the_build() {
    let db = db().await;
    let url = format!(
        "postgres://postgres@127.0.0.1:{}/postgres?sslmode=verify-full&sslrootcert=/nonexistent/ca.pem",
        db.port
    );
    let built = registry()
        .build(&connection(json!({ "url": url })), Roles::SINK)
        .expect("build reads no files");
    let err = built
        .endpoints
        .sink
        .unwrap()
        .prepare(&common::order_schema(), WriteMode::Insert)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("/nonexistent/ca.pem"), "{err}");
}
