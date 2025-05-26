//! The shared TLS policy from [`ferry_connector_tls`], applied to Mongo
//! connection strings.
//!
//! - `tls=false` connects in plaintext: an explicit choice.
//! - `tls=true` (or any TLS option such as `tlsCAFile`) uses TLS, and the
//!   driver verifies the certificate chain and hostname against the Mozilla
//!   roots or the CA in `tlsCAFile`. Options that turn verification off
//!   (`tlsInsecure`, `tlsAllowInvalidCertificates`) are refused.
//! - With no TLS setting, remote hosts get TLS. The driver can't try TLS and
//!   fall back, so a single local host gets its default instead: plaintext,
//!   which the policy only allows locally. That connection is made direct,
//!   because a replica-set member could otherwise hand the driver remote
//!   members to reach in plaintext; a local replica set (several seeds, or
//!   `replicaSet=`) therefore gets TLS too. `mongodb+srv://` always uses TLS.

use ferry_connector_tls::{Requested, Transport, transport};
use ferry_core::Result;
use mongodb::options::{ConnectionString, HostInfo, ServerAddress, Tls, TlsOptions};

use crate::source::connector_error;

/// Parses `url` and applies the policy to it. Does no I/O: `mongodb+srv`
/// names are resolved on connect.
pub(crate) fn parse(url: &str) -> Result<ConnectionString> {
    let url = url.trim();
    let mut conn = ConnectionString::parse(url).map_err(|e| {
        // The driver's message can quote a mis-split host, which with an
        // unencoded `?` or `/` in the password is part of the password.
        connector_error(match has_credentials(url) {
            true => "invalid connection string (details hidden because it holds credentials; \
                     check that special characters in the username and password are \
                     percent-encoded)"
                .to_owned(),
            false => format!("invalid connection string: {}", e.kind),
        })
    })?;
    secure(&mut conn)?;
    Ok(conn)
}

fn secure(conn: &mut ConnectionString) -> Result<()> {
    let requested = match &conn.tls {
        None => Requested::Prefer,
        Some(Tls::Disabled) => Requested::Disable,
        Some(Tls::Enabled(options)) => {
            // With rustls the driver has no way to skip the hostname check;
            // `tlsInsecure` sets this one.
            if options.allow_invalid_certificates == Some(true) {
                return Err(connector_error(
                    "TLS certificate and hostname checks can't be turned off; to trust a \
                     private CA, set tlsCAFile",
                ));
            }
            Requested::Require
        }
    };
    let hosts: Vec<&str> = match &conn.host_info {
        HostInfo::HostIdentifiers(addresses) => addresses
            .iter()
            .map(|a| match a {
                ServerAddress::Tcp { host, .. } => host.as_str(),
                // Unix sockets are local by definition.
                _ => "/",
            })
            .collect(),
        HostInfo::DnsRecord(name) => vec![name.as_str()],
        _ => Vec::new(),
    };
    let single_seed = hosts.len() <= 1
        && conn.replica_set.is_none()
        && conn.direct_connection != Some(false)
        && conn.load_balanced != Some(true);
    match transport(requested, hosts) {
        Transport::Plaintext => conn.tls = Some(Tls::Disabled),
        Transport::PreferTls if single_seed => conn.direct_connection = Some(true),
        Transport::PreferTls | Transport::RequireTls => {
            if conn.tls.is_none() {
                conn.tls = Some(Tls::Enabled(TlsOptions::default()));
            }
        }
    }
    Ok(())
}

/// Whether `url` has a `user@` or `user:password@` part.
fn has_credentials(url: &str) -> bool {
    url.split_once("://")
        .is_some_and(|(_, rest)| rest.contains('@'))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tls(url: &str) -> Option<Tls> {
        parse(url).unwrap().tls
    }

    fn enabled(tls: Option<Tls>) -> bool {
        matches!(tls, Some(Tls::Enabled(_)))
    }

    #[test]
    fn remote_hosts_get_tls_unless_it_is_disabled() {
        for url in [
            "mongodb://db.example.com/shop",
            "mongodb://u:p@10.0.0.5:27017/shop",
            "mongodb://localhost,db.example.com/shop?replicaSet=rs0",
            "mongodb+srv://cluster.example.com/shop",
        ] {
            assert!(enabled(tls(url)), "{url}");
        }
    }

    #[test]
    fn a_single_local_host_without_a_tls_setting_is_plaintext_and_direct() {
        for url in [
            "mongodb://localhost/shop",
            "mongodb://127.0.0.1:27017/shop",
            "mongodb://[::1]:27017/shop",
            "mongodb://%2Ftmp%2Fmongodb-27017.sock/shop",
        ] {
            let conn = parse(url).unwrap();
            assert_eq!(conn.tls, None, "{url}");
            // Discovery could otherwise lead to remote members in plaintext.
            assert_eq!(conn.direct_connection, Some(true), "{url}");
        }
    }

    #[test]
    fn a_local_replica_set_gets_tls_because_its_members_may_not_be_local() {
        for url in [
            "mongodb://localhost:27017,localhost:27018/shop",
            "mongodb://localhost/shop?replicaSet=rs0",
            "mongodb://localhost/shop?directConnection=false",
        ] {
            assert!(enabled(tls(url)), "{url}");
        }
        assert_eq!(
            tls("mongodb://localhost/shop?replicaSet=rs0&tls=false"),
            Some(Tls::Disabled)
        );
    }

    #[test]
    fn explicit_settings_are_kept() {
        assert_eq!(
            tls("mongodb://db.example.com/shop?tls=false"),
            Some(Tls::Disabled)
        );
        assert!(enabled(tls("mongodb://localhost/shop?tls=true")));
        let Some(Tls::Enabled(options)) = tls("mongodb://localhost/shop?tlsCAFile=/etc/ca.pem")
        else {
            panic!("tlsCAFile implies TLS");
        };
        assert_eq!(
            options.ca_file_path.as_deref(),
            Some(std::path::Path::new("/etc/ca.pem"))
        );
    }

    #[test]
    fn verification_can_not_be_turned_off() {
        for url in [
            "mongodb://db.example.com/shop?tls=true&tlsInsecure=true",
            "mongodb://localhost/shop?tlsAllowInvalidCertificates=true",
        ] {
            let err = parse(url).unwrap_err().to_string();
            assert!(err.contains("can't be turned off"), "{url}: {err}");
        }
    }

    #[test]
    fn invalid_urls_fail_without_echoing_secrets() {
        for url in [
            "mongodb://u:hunter2@localhost:notaport/shop",
            "mongo://u:hunter2@localhost/shop",
            "mongodb://u:hunter2@localhost/shop?tls=maybe",
            // An unencoded `?` splits the URL inside the password.
            "mongodb://admin:hunter2?x@db.example.com/shop",
            "mongodb://admin:hun/ter2@db.example.com/shop",
        ] {
            let err = parse(url).unwrap_err().to_string();
            assert!(
                !err.contains("hunter2") && !err.contains("hun") && !err.contains("admin"),
                "{url}: {err}"
            );
        }
    }
}
