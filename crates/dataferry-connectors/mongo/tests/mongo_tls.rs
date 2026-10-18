//! The TLS policy against a real MongoDB that only accepts TLS, with a
//! certificate from a throwaway CA.

use std::path::PathBuf;

use dataferry_connector_mongo::MongoSource;
use dataferry_core::Source;
use mongodb::bson::{Document, doc};
use rcgen::{BasicConstraints, CertificateParams, CertifiedIssuer, IsCa, KeyPair};
use testcontainers_modules::mongo::Mongo;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};

struct Server {
    _container: ContainerAsync<Mongo>,
    port: u16,
    ca_file: PathBuf,
    _dir: TempDir,
}

/// A mongod that refuses plaintext, with a certificate naming only
/// `localhost`, and one collection to discover.
async fn tls_server() -> Server {
    let ca_key = KeyPair::generate().unwrap();
    let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca = CertifiedIssuer::self_signed(ca_params, ca_key).unwrap();
    let server_key = KeyPair::generate().unwrap();
    let server_cert = CertificateParams::new(vec!["localhost".to_owned()])
        .unwrap()
        .signed_by(&server_key, &ca)
        .unwrap();

    let dir = TempDir::new();
    let ca_file = dir.0.join("ca.pem");
    std::fs::write(&ca_file, ca.pem()).unwrap();

    let container = Mongo::default()
        .with_copy_to(
            "/certs/server.pem",
            format!("{}{}", server_cert.pem(), server_key.serialize_pem()).into_bytes(),
        )
        .with_copy_to("/certs/ca.pem", ca.pem().into_bytes())
        .with_cmd([
            "--tlsMode",
            "requireTLS",
            "--tlsCertificateKeyFile",
            "/certs/server.pem",
            "--tlsCAFile",
            "/certs/ca.pem",
            "--tlsAllowConnectionsWithoutCertificates",
        ])
        .start()
        .await
        .expect("start mongo with TLS");
    let port = container.get_host_port_ipv4(27017).await.unwrap();
    let server = Server {
        _container: container,
        port,
        ca_file,
        _dir: dir,
    };

    let client =
        mongodb::Client::with_uri_str(server.url("localhost", "tls=true&tlsCAFile=", true))
            .await
            .unwrap();
    client
        .database("shop")
        .collection::<Document>("orders")
        .insert_one(doc! { "_id": 1_i32 })
        .await
        .expect("insert over TLS");
    server
}

impl Server {
    /// `options` ending in `tlsCAFile=` gets the CA path appended when
    /// `with_ca` is set.
    fn url(&self, host: &str, options: &str, with_ca: bool) -> String {
        let mut url = format!(
            "mongodb://{host}:{}/?serverSelectionTimeoutMS=3000&{options}",
            self.port
        );
        if with_ca {
            url += &self.ca_file.display().to_string();
        }
        url
    }
}

async fn discover(url: &str) -> Result<Vec<String>, String> {
    let source = MongoSource::new(url, "shop").map_err(|e| e.to_string())?;
    source
        .discover()
        .await
        .map(|entities| entities.iter().map(|e| e.name().to_owned()).collect())
        .map_err(|e| e.to_string())
}

fn assert_refused(result: Result<Vec<String>, String>, expected: &str, case: &str) {
    match result {
        Ok(found) => panic!("{case}: connected (found {found:?}) but should have been refused"),
        Err(e) => assert!(
            e.contains(expected),
            "{case}: expected `{expected}` in: {e}"
        ),
    }
}

#[tokio::test]
async fn tls_with_the_ca_connects_and_is_verified() {
    let server = tls_server().await;
    assert_eq!(
        discover(&server.url("localhost", "tls=true&tlsCAFile=", true)).await,
        Ok(vec!["orders".to_owned()])
    );
    // tlsCAFile alone implies TLS.
    assert_eq!(
        discover(&server.url("localhost", "tlsCAFile=", true)).await,
        Ok(vec!["orders".to_owned()])
    );
}

#[tokio::test]
async fn an_unknown_ca_or_a_wrong_hostname_is_refused() {
    let server = tls_server().await;
    assert_refused(
        discover(&server.url("localhost", "tls=true", false)).await,
        "UnknownIssuer",
        "no CA",
    );
    assert_refused(
        discover(&server.url("127.0.0.1", "tls=true&tlsCAFile=", true)).await,
        "certificate not valid for name",
        "hostname",
    );
}

#[tokio::test]
async fn verification_can_not_be_switched_off() {
    let server = tls_server().await;
    for options in [
        "tls=true&tlsInsecure=true",
        "tlsAllowInvalidCertificates=true",
    ] {
        assert_refused(
            discover(&server.url("localhost", options, false)).await,
            "can't be turned off",
            options,
        );
    }
}

/// A directory removed on drop.
struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "ferry-mongo-tls-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
