//! The `mongo` connector through the registry, against a real MongoDB.

use dataferry_connector_mongo::MongoConnector;
use dataferry_registry::testkit::{ConnectorFixture, check_connector};
use dataferry_registry::{BuildError, ConnectorRegistry, RawConnection, Roles};
use mongodb::bson::{Document, doc};
use serde_json::{Value, json};
use testcontainers_modules::mongo::Mongo;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

fn connection(fields: Value) -> RawConnection {
    let mut entry = json!({"name": "shop", "type": "mongo"});
    entry
        .as_object_mut()
        .unwrap()
        .extend(fields.as_object().unwrap().clone());
    serde_json::from_value(entry).unwrap()
}

fn registry() -> ConnectorRegistry {
    let mut registry = ConnectorRegistry::new();
    registry.register(MongoConnector);
    registry
}

#[test]
fn passes_the_connector_contract() {
    // Building does no I/O, so the credentials need not be valid anywhere.
    for url in [
        "mongodb://app:mongo-s3cret@127.0.0.1:27017/?authSource=admin",
        "mongodb://app:mongo%2Ds3cret@db.example.com/shop?replicaSet=rs0",
        "mongodb+srv://app:mongo-s3cret@cluster.example.com/",
    ] {
        let fixture = ConnectorFixture {
            options: json!({"url": url, "database": "shop"}),
            secret: "mongo-s3cret".into(),
        };
        let violations = check_connector(MongoConnector, &fixture);
        assert!(violations.is_empty(), "{url}: {violations:#?}");
    }
}

#[test]
fn it_is_a_source_only() {
    let err = registry()
        .build(
            &connection(json!({"url": "mongodb://127.0.0.1", "database": "shop"})),
            Roles::SINK,
        )
        .unwrap_err();
    assert!(err.to_string().contains("sink"), "{err}");
}

#[test]
fn bad_options_are_reported_at_their_field() {
    let cases = [
        // A missing field is reported on the entry itself.
        (json!({"url": "mongodb://127.0.0.1"}), ""),
        (
            json!({"url": "mongodb://127.0.0.1", "database": ""}),
            "database",
        ),
        (
            json!({"url": "http://127.0.0.1", "database": "shop"}),
            "url",
        ),
        (
            json!({"url": "mongodb://h/?tlsInsecure=true", "database": "shop"}),
            "url",
        ),
        (
            json!({"url": "mongodb://h", "database": "shop", "uri": "x"}),
            "uri",
        ),
    ];
    for (fields, path) in cases {
        match registry().build(&connection(fields.clone()), Roles::SOURCE) {
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
async fn a_source_built_from_a_connection_entry_reads_the_database() {
    let container = Mongo::default().start().await.expect("start mongo");
    let port = container.get_host_port_ipv4(27017).await.unwrap();
    // The whole `url` is a secret, so a description that repeated it
    // verbatim would be redacted; a path keeps the two apart.
    let url = format!("mongodb://127.0.0.1:{port}/admin");
    let client = mongodb::Client::with_uri_str(&url).await.unwrap();
    client
        .database("shop")
        .collection::<Document>("orders")
        .insert_many([
            doc! {"_id": 1, "status": "new"},
            doc! {"_id": 2, "status": "paid"},
        ])
        .await
        .unwrap();

    let built = registry()
        .build(
            &connection(json!({"url": url, "database": "shop"})),
            Roles::SOURCE,
        )
        .unwrap();
    assert_eq!(
        built.description,
        format!("mongodb://127.0.0.1:{port}, database shop")
    );
    let entities = built.endpoints.source.unwrap().discover().await.unwrap();
    let names: Vec<&str> = entities.iter().map(|e| e.name()).collect();
    assert_eq!(names, ["orders"]);
}
