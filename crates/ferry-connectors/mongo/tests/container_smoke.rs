//! Proves the test environment can start a real MongoDB. Every connector
//! test that talks to a database depends on this working.

use mongodb::bson::{Document, doc};
use testcontainers_modules::mongo::Mongo;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

#[tokio::test]
async fn mongo_container_round_trips_a_document() {
    let container = Mongo::default().start().await.expect("start mongo");
    let port = container
        .get_host_port_ipv4(27017)
        .await
        .expect("mapped port");
    let client = mongodb::Client::with_uri_str(format!("mongodb://127.0.0.1:{port}"))
        .await
        .expect("connect");

    let coll = client.database("smoke").collection::<Document>("orders");
    coll.insert_one(doc! { "_id": 1, "status": "PAID" })
        .await
        .expect("insert");

    let found = coll
        .find_one(doc! { "_id": 1 })
        .await
        .expect("find")
        .expect("document exists");
    assert_eq!(found.get_str("status").unwrap(), "PAID");
}
