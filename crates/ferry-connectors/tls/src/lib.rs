//! The TLS policy every 0xferry connector applies, so that each database
//! driver's own defaults can't weaken it:
//!
//! - Plaintext only when the connection string asks for it explicitly
//!   (`sslmode=disable`, `tls=false`).
//! - Otherwise TLS, verifying the server's certificate chain *and* hostname.
//!   There is no "encrypt but don't verify" mode.
//! - Falling back to plaintext when the server offers no TLS is only allowed
//!   when every host is local. For a remote host the fallback is upgraded to
//!   a hard requirement, so a man-in-the-middle can't strip TLS by claiming
//!   the server lacks it.
//!
//! Drivers differ in how they take TLS settings, so each connector maps its
//! own options onto [`Requested`], applies [`transport`], and, if its driver
//! accepts a rustls config, builds it with [`client_config`].

use std::fmt;
use std::net::IpAddr;
use std::path::Path;
use std::sync::Arc;

use rustls::{ClientConfig, RootCertStore};
use rustls_pki_types::CertificateDer;
use rustls_pki_types::pem::PemObject;

/// What a connection string asks for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Requested {
    /// Plaintext, chosen explicitly.
    Disable,
    /// TLS if the server offers it. The default when nothing is said.
    Prefer,
    /// TLS or no connection.
    Require,
}

/// What a connector must actually do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transport {
    Plaintext,
    /// Try TLS, fall back to plaintext if the server has none. Only ever
    /// returned when every host is local.
    PreferTls,
    RequireTls,
}

/// Applies the policy to a request for connecting to `hosts`. An empty host
/// list means the driver's local default (a Unix socket or localhost).
pub fn transport<'a>(requested: Requested, hosts: impl IntoIterator<Item = &'a str>) -> Transport {
    match requested {
        Requested::Disable => Transport::Plaintext,
        Requested::Require => Transport::RequireTls,
        Requested::Prefer if hosts.into_iter().all(is_local) => Transport::PreferTls,
        Requested::Prefer => Transport::RequireTls,
    }
}

/// Whether `host` can only be reached on this machine: `localhost`, a
/// loopback address, or a Unix socket path. Anything else, including a
/// private-network address, could be on a path an attacker controls.
pub fn is_local(host: &str) -> bool {
    if host.starts_with('/') {
        return true;
    }
    let host = host.trim_end_matches('.');
    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    let bare = host
        .strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(host);
    bare.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback())
}

/// A rustls client config that verifies certificates and hostnames.
///
/// Trusts the Mozilla root store, or, when `ca_file` is set, only the PEM
/// certificates in that file (like libpq's `sslrootcert`), which is how a
/// private CA is used.
pub fn client_config(ca_file: Option<&Path>) -> Result<Arc<ClientConfig>, TlsError> {
    let roots = root_store(ca_file)?;
    // The provider is chosen explicitly so the config can't depend on which
    // rustls crypto features other crates in the build happen to enable.
    let config =
        ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .map_err(|e| TlsError(e.to_string()))?
            .with_root_certificates(roots)
            .with_no_client_auth();
    Ok(Arc::new(config))
}

fn root_store(ca_file: Option<&Path>) -> Result<RootCertStore, TlsError> {
    let mut roots = RootCertStore::empty();
    let Some(path) = ca_file else {
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        return Ok(roots);
    };
    let certs = CertificateDer::pem_file_iter(path)
        .and_then(|certs| certs.collect::<Result<Vec<_>, _>>())
        .map_err(|e| TlsError(format!("reading CA file {}: {e}", path.display())))?;
    if certs.is_empty() {
        return Err(TlsError(format!(
            "CA file {} holds no PEM certificates",
            path.display()
        )));
    }
    for cert in certs {
        roots
            .add(cert)
            .map_err(|e| TlsError(format!("CA file {}: bad certificate: {e}", path.display())))?;
    }
    Ok(roots)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsError(String);

impl fmt::Display for TlsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for TlsError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_hosts() {
        for host in [
            "localhost",
            "LOCALHOST",
            "localhost.",
            "127.0.0.1",
            "127.1.2.3",
            "::1",
            "[::1]",
            "/var/run/postgresql",
        ] {
            assert!(is_local(host), "{host}");
        }
    }

    #[test]
    fn remote_hosts_include_private_networks_and_lookalikes() {
        for host in [
            "db.example.com",
            "10.0.0.5",
            "192.168.1.2",
            "0.0.0.0",
            "::",
            "localhost.example.com",
            "evil-localhost",
            "@abstract-socket",
            "",
        ] {
            assert!(!is_local(host), "{host:?}");
        }
    }

    #[test]
    fn prefer_falls_back_only_when_every_host_is_local() {
        assert_eq!(
            transport(Requested::Prefer, ["localhost", "127.0.0.1"]),
            Transport::PreferTls
        );
        assert_eq!(transport(Requested::Prefer, []), Transport::PreferTls);
        assert_eq!(
            transport(Requested::Prefer, ["db.example.com"]),
            Transport::RequireTls
        );
        assert_eq!(
            transport(Requested::Prefer, ["localhost", "db.example.com"]),
            Transport::RequireTls
        );
    }

    #[test]
    fn explicit_choices_are_kept_whatever_the_hosts() {
        assert_eq!(
            transport(Requested::Disable, ["db.example.com"]),
            Transport::Plaintext
        );
        assert_eq!(
            transport(Requested::Require, ["localhost"]),
            Transport::RequireTls
        );
    }

    #[test]
    fn the_default_trust_store_is_mozillas() {
        let roots = root_store(None).unwrap();
        assert_eq!(roots.len(), webpki_roots::TLS_SERVER_ROOTS.len());
        assert!(roots.len() > 100, "{} roots", roots.len());
        assert!(client_config(None).is_ok());
    }

    #[test]
    fn a_ca_file_that_is_missing_or_has_no_certificates_is_an_error() {
        let missing = client_config(Some(Path::new("/nonexistent/ca.pem"))).unwrap_err();
        assert!(
            missing.to_string().contains("/nonexistent/ca.pem"),
            "{missing}"
        );

        let dir = std::env::temp_dir().join(format!("ferry-tls-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let empty = dir.join("empty.pem");
        std::fs::write(&empty, "not a certificate\n").unwrap();
        let err = client_config(Some(&empty)).unwrap_err();
        assert!(err.to_string().contains("no PEM certificates"), "{err}");
        std::fs::remove_dir_all(dir).unwrap();
    }
}
