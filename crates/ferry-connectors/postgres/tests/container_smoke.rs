//! Proves the test environment can start a real Postgres. Every connector
//! test that talks to a database depends on this working.

use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

#[tokio::test]
async fn postgres_container_accepts_queries() {
    let container = Postgres::default().start().await.expect("start postgres");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("mapped port");
    let url = format!("host=127.0.0.1 port={port} user=postgres password=postgres dbname=postgres");

    let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(connection);

    let row = client.query_one("SELECT 40 + 2", &[]).await.expect("query");
    assert_eq!(row.get::<_, i32>(0), 42);
}
