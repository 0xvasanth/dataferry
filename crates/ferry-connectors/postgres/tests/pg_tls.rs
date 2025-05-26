//! The TLS policy against a real Postgres: one that only accepts TLS, with a
//! certificate from a throwaway CA, and one that has no TLS at all.
//!
//! Remote hosts are simulated with `host=<name> hostaddr=127.0.0.1`: the
//! connection goes to the container, but the policy and the certificate
//! check both see the remote name.

use std::path::PathBuf;

use ferry_connector_postgres::PostgresSource;
use ferry_core::Source;
use rcgen::{BasicConstraints, CertificateParams, CertifiedIssuer, IsCa, KeyPair};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::core::{CmdWaitFor, ExecCommand};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};

/// Names on the server certificate. `127.0.0.1` is deliberately not one.
const CERT_NAMES: [&str; 2] = ["localhost", "db.ferry.test"];

/// Rejects every TCP connection that isn't TLS, so a successful query
/// proves the session was encrypted.
const TLS_ONLY_HBA: &str = "local all all trust\n\
                            hostssl all all all md5\n\
                            hostnossl all all all reject\n";

struct Server {
    _container: ContainerAsync<Postgres>,
    port: u16,
    /// The CA that signed the server certificate, as a PEM file.
    ca_file: PathBuf,
    _dir: TempDir,
}

/// A Postgres that only accepts TLS, with a table to discover.
async fn tls_server() -> Server {
    let ca_key = KeyPair::generate().unwrap();
    let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca = CertifiedIssuer::self_signed(ca_params, ca_key).unwrap();
    let server_key = KeyPair::generate().unwrap();
    let server_cert = CertificateParams::new(CERT_NAMES.map(String::from).to_vec())
        .unwrap()
        .signed_by(&server_key, &ca)
        .unwrap();

    let dir = TempDir::new();
    let ca_file = dir.0.join("ca.pem");
    std::fs::write(&ca_file, ca.pem()).unwrap();

    // Postgres refuses a key file other users can read, and files copied in
    // are owned by root, so they are moved into place before it starts.
    let script = "mkdir /tmp/tls && cp /certs/* /tmp/tls/ && chown -R postgres /tmp/tls \
                  && chmod 600 /tmp/tls/server.key \
                  && exec docker-entrypoint.sh postgres -c fsync=off -c ssl=on \
                  -c ssl_cert_file=/tmp/tls/server.crt -c ssl_key_file=/tmp/tls/server.key \
                  -c hba_file=/tmp/tls/pg_hba.conf";
    let container = Postgres::default()
        .with_copy_to("/certs/server.crt", server_cert.pem().into_bytes())
        .with_copy_to("/certs/server.key", server_key.serialize_pem().into_bytes())
        .with_copy_to("/certs/pg_hba.conf", TLS_ONLY_HBA.as_bytes().to_vec())
        .with_cmd(["sh", "-c", script])
        .start()
        .await
        .expect("start postgres with TLS");
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let server = Server {
        _container: container,
        port,
        ca_file,
        _dir: dir,
    };
    // Created over the container's Unix socket: TCP would need TLS.
    server
        ._container
        .exec(
            ExecCommand::new([
                "psql",
                "-U",
                "postgres",
                "-c",
                "CREATE TABLE accounts (id int PRIMARY KEY)",
            ])
            .with_cmd_ready_condition(CmdWaitFor::exit_code(0)),
        )
        .await
        .expect("create table");
    server
}

impl Server {
    fn url(&self, host: &str, sslmode: &str, with_ca: bool) -> String {
        let mut url = format!(
            "postgres://postgres:postgres@{host}:{}/postgres?sslmode={sslmode}",
            self.port
        );
        if with_ca {
            url += &format!("&sslrootcert={}", self.ca_file.display());
        }
        url
    }

    /// A key=value string naming `host` but connecting to the container.
    fn remote(&self, host: &str, sslmode: &str, with_ca: bool) -> String {
        let mut conn = format!(
            "host={host} hostaddr=127.0.0.1 port={} user=postgres password=postgres \
             dbname=postgres sslmode={sslmode}",
            self.port
        );
        if with_ca {
            conn += &format!(" sslrootcert='{}'", self.ca_file.display());
        }
        conn
    }
}

/// What discovering through `conn` gives: the table names, or the error.
async fn discover(conn: &str) -> Result<Vec<String>, String> {
    let source = PostgresSource::new(conn, "public").map_err(|e| e.to_string())?;
    source
        .discover()
        .await
        .map(|entities| entities.iter().map(|e| e.name().to_owned()).collect())
        .map_err(|e| e.to_string())
}

fn assert_refused(result: Result<Vec<String>, String>, expected: &str, case: &str) {
    match result {
        Ok(tables) => panic!("{case}: connected (found {tables:?}) but should have been refused"),
        Err(e) => assert!(
            e.contains(expected),
            "{case}: expected `{expected}` in: {e}"
        ),
    }
}

#[tokio::test]
async fn verify_full_with_the_ca_connects_over_tls() {
    let server = tls_server().await;
    assert_eq!(
        discover(&server.url("localhost", "verify-full", true)).await,
        Ok(vec!["accounts".to_owned()])
    );
    // A remote name on the certificate verifies the same way, and `prefer`
    // to it means verified TLS.
    for mode in ["verify-full", "prefer"] {
        assert_eq!(
            discover(&server.remote("db.ferry.test", mode, true)).await,
            Ok(vec!["accounts".to_owned()]),
            "{mode}"
        );
    }
}

#[tokio::test]
async fn a_server_certificate_from_an_unknown_ca_is_refused_in_every_tls_mode() {
    let server = tls_server().await;
    // Without `sslrootcert` only the Mozilla roots are trusted. `require`
    // and `prefer` verify too, unlike libpq's.
    for mode in ["verify-full", "verify-ca", "require", "prefer"] {
        assert_refused(
            discover(&server.url("localhost", mode, false)).await,
            "invalid peer certificate: UnknownIssuer",
            mode,
        );
    }
}

#[tokio::test]
async fn a_host_the_certificate_does_not_name_is_refused() {
    let server = tls_server().await;
    for (case, conn) in [
        ("ip", server.url("127.0.0.1", "verify-full", true)),
        (
            "other name",
            server.remote("evil.ferry.test", "verify-full", true),
        ),
        // verify-ca checks the hostname too.
        (
            "verify-ca",
            server.remote("evil.ferry.test", "verify-ca", true),
        ),
    ] {
        assert_refused(
            discover(&conn).await,
            "certificate not valid for name",
            case,
        );
    }
}

/// A Postgres without TLS.
async fn plaintext_server() -> Server {
    let container = Postgres::default().start().await.expect("start postgres");
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let (client, connection) = tokio_postgres::connect(
        &format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres"),
        tokio_postgres::NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(connection);
    client
        .batch_execute("CREATE TABLE accounts (id int PRIMARY KEY)")
        .await
        .unwrap();
    let dir = TempDir::new();
    Server {
        _container: container,
        port,
        ca_file: dir.0.join("unused.pem"),
        _dir: dir,
    }
}

#[tokio::test]
async fn a_remote_host_never_falls_back_to_plaintext() {
    let server = plaintext_server().await;
    for mode in ["prefer", "allow"] {
        assert_refused(
            discover(&server.remote("db.ferry.test", mode, false)).await,
            "server does not support TLS",
            mode,
        );
    }
}

#[tokio::test]
async fn plaintext_needs_a_local_host_or_an_explicit_disable() {
    let server = plaintext_server().await;
    let accounts = Ok(vec!["accounts".to_owned()]);
    assert_eq!(
        discover(&server.url("localhost", "prefer", false)).await,
        accounts,
        "local prefer"
    );
    assert_eq!(
        discover(&server.remote("db.ferry.test", "disable", false)).await,
        accounts,
        "remote disable"
    );
}

/// A directory removed on drop.
struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "ferry-pg-tls-{}-{}",
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
