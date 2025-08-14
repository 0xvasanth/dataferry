//! How connections are secured: the shared policy from
//! [`ferry_connector_tls`], applied to libpq-style connection strings.
//!
//! - `sslmode=disable` connects in plaintext: an explicit choice.
//! - Every other mode uses TLS and verifies the server certificate *and*
//!   hostname, against the Mozilla roots or the CA in `sslrootcert`. That
//!   makes `require` and `verify-ca` as strict as `verify-full`: a migration
//!   tool has no use for encryption to an unauthenticated server.
//! - `prefer` (the default) and `allow` may fall back to plaintext only when
//!   every host is local. For remote hosts they are upgraded to `require`, so
//!   a man-in-the-middle can't strip TLS by claiming the server lacks it.
//!
//! tokio-postgres only understands `disable`, `prefer` and `require`, and no
//! `sslrootcert`, so those settings are taken out of the connection string
//! here before the rest is handed to it.

use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use ferry_connector_tls::{Requested, Transport, client_config, transport};
use ferry_core::Result;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_postgres::Config;
use tokio_postgres::config::{Host, SslMode};
use tokio_postgres::tls::MakeTlsConnect;
use tokio_postgres_rustls::MakeRustlsConnect;

use crate::source::{connector_error, with_causes};

/// The TLS half of a parsed connection string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Tls {
    transport: Transport,
    root_cert: Option<PathBuf>,
}

impl Tls {
    /// The connector to pass to `Config::connect`, or `None` for plaintext.
    /// Reads `sslrootcert`, so it runs on connect rather than on parse.
    pub(crate) fn connector(&self) -> Result<Option<MakeRustlsConnect>> {
        if self.transport == Transport::Plaintext {
            return Ok(None);
        }
        let config = client_config(self.root_cert.as_deref())
            .map_err(|e| connector_error(format!("TLS: {e}")))?;
        Ok(Some(MakeRustlsConnect::new((*config).clone())))
    }

    pub(crate) fn is_plaintext(&self) -> bool {
        self.transport == Transport::Plaintext
    }
}

/// A TLS connector for a pool that builds itself on the first connect, so
/// creating the pool does no I/O (reading `sslrootcert` included), and a bad
/// CA file fails the connect that needs it, like the source's.
#[derive(Clone)]
pub(crate) struct LazyTls {
    tls: Tls,
    built: Arc<Mutex<Option<MakeRustlsConnect>>>,
}

impl LazyTls {
    pub(crate) fn new(tls: Tls) -> Self {
        Self {
            tls,
            built: Arc::default(),
        }
    }
}

impl<S> MakeTlsConnect<S> for LazyTls
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Stream = <MakeRustlsConnect as MakeTlsConnect<S>>::Stream;
    type TlsConnect = <MakeRustlsConnect as MakeTlsConnect<S>>::TlsConnect;
    type Error = io::Error;

    fn make_tls_connect(&mut self, hostname: &str) -> io::Result<Self::TlsConnect> {
        let mut built = self.built.lock().unwrap_or_else(|e| e.into_inner());
        if built.is_none() {
            *built = self
                .tls
                .connector()
                .map_err(|e| io::Error::other(e.to_string()))?;
        }
        let connector = built
            .as_mut()
            .ok_or_else(|| io::Error::other("TLS was requested on a plaintext connection"))?;
        match MakeTlsConnect::<S>::make_tls_connect(connector, hostname) {
            Ok(connect) => Ok(connect),
            Err(never) => match never {},
        }
    }
}

/// Parses a libpq URL or key=value string and applies the TLS policy to it.
/// Errors never echo the string, which may hold a password.
pub(crate) fn parse(conn: &str) -> Result<(Config, Tls)> {
    let (rest, settings) = take_tls_settings(conn.trim())?;
    let mut config: Config = rest.parse().map_err(|e: tokio_postgres::Error| {
        // The causes name an option or a position, never a value.
        connector_error(with_causes(&e))
    })?;
    let requested = match settings.mode.as_deref() {
        None | Some("prefer" | "allow") => Requested::Prefer,
        Some("disable") => Requested::Disable,
        Some("require" | "verify-ca" | "verify-full") => Requested::Require,
        Some(_) => {
            return Err(connector_error(
                "invalid sslmode: expected disable, allow, prefer, require, verify-ca or \
                 verify-full",
            ));
        }
    };
    let transport = if is_unix_socket_only(&config) {
        // Like libpq: a Unix socket never leaves the machine, and Postgres
        // doesn't offer TLS on one.
        Transport::Plaintext
    } else {
        let hosts: Vec<&str> = config
            .get_hosts()
            .iter()
            .map(|h| match h {
                Host::Tcp(name) => name.as_str(),
                Host::Unix(_) => "/",
            })
            .collect();
        // `hostaddr` is where the connection actually goes, and `host` is the
        // name the certificate is checked against; both must be local to
        // skip TLS.
        let addrs: Vec<String> = config
            .get_hostaddrs()
            .iter()
            .map(ToString::to_string)
            .collect();
        transport(
            requested,
            hosts.into_iter().chain(addrs.iter().map(String::as_str)),
        )
    };
    config.ssl_mode(match transport {
        Transport::Plaintext => SslMode::Disable,
        Transport::PreferTls => SslMode::Prefer,
        Transport::RequireTls => SslMode::Require,
    });
    Ok((
        config,
        Tls {
            transport,
            root_cert: settings.root_cert.map(PathBuf::from),
        },
    ))
}

fn is_unix_socket_only(config: &Config) -> bool {
    !config.get_hosts().is_empty()
        && config.get_hostaddrs().is_empty()
        && config
            .get_hosts()
            .iter()
            .all(|h| matches!(h, Host::Unix(_)))
}

#[derive(Debug, Default, PartialEq, Eq)]
struct TlsSettings {
    mode: Option<String>,
    root_cert: Option<String>,
}

impl TlsSettings {
    /// Records `key` if it is a TLS setting; `false` if it is not one.
    fn take(&mut self, key: &str, value: impl FnOnce() -> Result<String>) -> Result<bool> {
        match key {
            "sslmode" => self.mode = Some(value()?),
            "sslrootcert" => {
                let value = value()?;
                // libpq's "system" means the platform's store; the Mozilla
                // roots are the portable equivalent.
                self.root_cert = (value != "system").then_some(value);
            }
            "sslcert" | "sslkey" | "sslpassword" => {
                return Err(connector_error(format!(
                    "`{key}`: client certificates are not supported yet"
                )));
            }
            _ => return Ok(false),
        }
        Ok(true)
    }
}

/// Splits the TLS settings out of a connection string, returning the rest
/// in a form tokio-postgres parses.
fn take_tls_settings(conn: &str) -> Result<(String, TlsSettings)> {
    let mut settings = TlsSettings::default();
    if let Some(after_scheme) = conn
        .strip_prefix("postgres://")
        .or_else(|| conn.strip_prefix("postgresql://"))
    {
        // As in libpq, credentials run to the first `@`, and only after it
        // can a `?` start the query, so a `?` in a password can't hide one.
        let authority_start =
            conn.len() - after_scheme.len() + after_scheme.find('@').map_or(0, |at| at + 1);
        let Some(q) = conn[authority_start..].find('?') else {
            return Ok((conn.to_owned(), settings));
        };
        let (base, query) = conn.split_at(authority_start + q);
        let mut kept = Vec::new();
        for pair in query[1..].split('&') {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            // Other keys and their values go to tokio-postgres untouched.
            let key = percent_decode(key).unwrap_or_default();
            if !settings.take(&key, || percent_decode(value))? {
                kept.push(pair);
            }
        }
        let rest = match kept.is_empty() {
            true => base.to_owned(),
            false => format!("{base}?{}", kept.join("&")),
        };
        return Ok((rest, settings));
    }

    let mut kept = Vec::new();
    for (key, value) in key_values(conn)? {
        if !settings.take(&key, || Ok(value.clone()))? {
            kept.push(format!(
                "{key}='{}'",
                value.replace('\\', "\\\\").replace('\'', "\\'")
            ));
        }
    }
    Ok((kept.join(" "), settings))
}

/// The pairs of a libpq key=value string. Values may be single-quoted, and
/// a backslash escapes the next character. Errors give a position rather
/// than a token, which could be part of a password.
fn key_values(conn: &str) -> Result<Vec<(String, String)>> {
    let invalid = |reason: &str, at: usize| {
        connector_error(format!(
            "invalid connection string: {reason} at character {at}"
        ))
    };
    let mut pairs = Vec::new();
    let mut chars = conn.chars().enumerate().peekable();
    let skip_whitespace = |chars: &mut std::iter::Peekable<_>| {
        while chars
            .next_if(|(_, c): &(usize, char)| c.is_whitespace())
            .is_some()
        {}
    };
    loop {
        skip_whitespace(&mut chars);
        let Some(&(key_at, _)) = chars.peek() else {
            return Ok(pairs);
        };
        let mut key = String::new();
        while let Some((_, c)) = chars.next_if(|(_, c)| *c != '=' && !c.is_whitespace()) {
            key.push(c);
        }
        skip_whitespace(&mut chars);
        if chars.next().map(|(_, c)| c) != Some('=') {
            return Err(invalid("expected `key=value`", key_at));
        }
        skip_whitespace(&mut chars);
        let mut value = String::new();
        if let Some((quote_at, _)) = chars.next_if(|(_, c)| *c == '\'') {
            loop {
                match chars.next().map(|(_, c)| c) {
                    Some('\'') => break,
                    Some('\\') => value.extend(chars.next().map(|(_, c)| c)),
                    Some(c) => value.push(c),
                    None => return Err(invalid("unterminated quote", quote_at)),
                }
            }
        } else {
            while let Some((_, c)) = chars.next_if(|(_, c)| !c.is_whitespace()) {
                if c == '\\' {
                    value.extend(chars.next().map(|(_, c)| c));
                } else {
                    value.push(c);
                }
            }
        }
        pairs.push((key, value));
    }
}

fn percent_decode(s: &str) -> Result<String> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let byte = s
                .get(i + 1..i + 3)
                .and_then(|hex| u8::from_str_radix(hex, 16).ok())
                .ok_or_else(|| connector_error("invalid percent-encoding in connection URL"))?;
            out.push(byte);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).map_err(|_| connector_error("connection URL is not valid UTF-8"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parsed(conn: &str) -> (Config, Tls) {
        parse(conn).unwrap()
    }

    #[test]
    fn remote_hosts_never_fall_back_to_plaintext() {
        for conn in [
            "postgres://u@db.example.com/app",
            "postgres://u@db.example.com/app?sslmode=prefer",
            "postgres://u@db.example.com/app?sslmode=allow",
            "host=10.0.0.5 user=u",
            // The name the certificate is checked against is remote, even if
            // the address isn't.
            "host=db.example.com hostaddr=127.0.0.1 user=u",
            "host=localhost hostaddr=10.0.0.5 user=u",
        ] {
            let (c, tls) = parsed(conn);
            assert_eq!(tls.transport, Transport::RequireTls, "{conn}");
            assert_eq!(c.get_ssl_mode(), SslMode::Require, "{conn}");
            assert!(tls.connector().unwrap().is_some(), "{conn}");
        }
    }

    #[test]
    fn local_connections_may_fall_back_when_the_server_has_no_tls() {
        for conn in [
            "postgres://u@localhost/app",
            "postgres://u@127.0.0.1:5433/app",
            "host=::1 user=u",
        ] {
            let (c, tls) = parsed(conn);
            assert_eq!(c.get_ssl_mode(), SslMode::Prefer, "{conn}");
            assert!(
                tls.connector().unwrap().is_some(),
                "TLS is still attempted: {conn}"
            );
        }
    }

    #[test]
    fn a_mix_of_local_and_remote_hosts_counts_as_remote() {
        let (c, _) = parsed("host=localhost,db.example.com user=u");
        assert_eq!(c.get_ssl_mode(), SslMode::Require);
    }

    #[test]
    fn every_verifying_mode_requires_tls_even_locally() {
        for mode in ["require", "verify-ca", "verify-full"] {
            let (c, tls) = parsed(&format!("postgres://u@localhost/app?sslmode={mode}"));
            assert_eq!(c.get_ssl_mode(), SslMode::Require, "{mode}");
            assert_eq!(tls.transport, Transport::RequireTls, "{mode}");
        }
    }

    #[test]
    fn explicit_disable_is_plaintext_even_for_remote_hosts() {
        let (c, tls) = parsed("postgres://u@db.example.com/app?sslmode=disable");
        assert_eq!(c.get_ssl_mode(), SslMode::Disable);
        assert!(tls.connector().unwrap().is_none());
    }

    #[test]
    fn unknown_sslmodes_are_rejected() {
        let err = parse("postgres://u@localhost/app?sslmode=maybe").unwrap_err();
        assert!(err.to_string().contains("invalid sslmode"), "{err}");
    }

    #[test]
    fn client_certificates_are_rejected_rather_than_ignored() {
        let err = parse("host=localhost sslcert=/c.pem").unwrap_err();
        assert!(err.to_string().contains("client certificates"), "{err}");
    }

    #[test]
    fn tls_settings_are_taken_out_and_everything_else_is_kept() {
        let (c, tls) = parsed(
            "postgres://u:p%40ss@db.example.com:6432/app?application_name=ferry\
             &sslrootcert=%2Fetc%2Fca.pem&sslmode=verify-full&connect_timeout=5",
        );
        assert_eq!(tls.root_cert, Some(PathBuf::from("/etc/ca.pem")));
        assert_eq!(c.get_password(), Some(&b"p@ss"[..]));
        assert_eq!(c.get_ports(), [6432]);
        assert_eq!(c.get_application_name(), Some("ferry"));
        assert_eq!(
            c.get_connect_timeout(),
            Some(&std::time::Duration::from_secs(5))
        );

        let (c, tls) = parsed(
            r"host=db.example.com sslmode = verify-full sslrootcert='/my certs/ca.pem' password='it\'s \\ secret'",
        );
        assert_eq!(tls.root_cert, Some(PathBuf::from("/my certs/ca.pem")));
        assert_eq!(c.get_password(), Some(&br"it's \ secret"[..]));
        assert_eq!(c.get_ssl_mode(), SslMode::Require);
    }

    #[test]
    fn sslrootcert_system_means_the_default_roots() {
        let (_, tls) = parsed("postgres://u@db.example.com/app?sslrootcert=system");
        assert_eq!(tls.root_cert, None);
    }

    #[test]
    fn a_missing_ca_file_fails_on_connect_not_on_parse() {
        let (_, tls) = parsed("postgres://u@db.example.com/app?sslrootcert=/nonexistent/ca.pem");
        let Err(err) = tls.connector() else {
            panic!("a missing CA file must fail");
        };
        assert!(err.to_string().contains("/nonexistent/ca.pem"), "{err}");
    }

    #[test]
    fn a_question_mark_in_the_password_does_not_hide_the_query() {
        let (c, tls) = parsed("postgres://u:pa?ss@localhost/app?sslmode=require");
        assert_eq!(tls.transport, Transport::RequireTls);
        assert_eq!(c.get_ssl_mode(), SslMode::Require);
        assert_eq!(c.get_password(), Some(&b"pa?ss"[..]));
    }

    #[test]
    fn unix_sockets_are_plaintext_whatever_the_sslmode() {
        // Postgres has no TLS on a socket; libpq ignores sslmode there.
        for conn in [
            "host=/var/run/postgresql user=u sslmode=require",
            "postgres://u@%2Fvar%2Frun%2Fpostgresql/app",
        ] {
            let (c, tls) = parsed(conn);
            assert_eq!(tls.transport, Transport::Plaintext, "{conn}");
            assert_eq!(c.get_ssl_mode(), SslMode::Disable, "{conn}");
        }
    }

    #[test]
    fn other_values_reach_tokio_postgres_undecoded() {
        let (c, _) = parsed("postgres://u@localhost/app?application_name=100%");
        assert_eq!(c.get_application_name(), Some("100%"));
    }

    #[test]
    fn surrounding_whitespace_is_ignored() {
        let (c, _) = parsed("  postgres://u:secret@db.example.com/app?sslmode=disable\n");
        assert_eq!(c.get_ssl_mode(), SslMode::Disable);
        assert_eq!(c.get_password(), Some(&b"secret"[..]));
    }

    #[test]
    fn parse_errors_keep_their_reason() {
        let err = parse("postgres://u@localhost/app?no_such_option=1").unwrap_err();
        assert!(err.to_string().contains("unknown option"), "{err}");
        let err = parse("postgres://u@localhost:notaport/app").unwrap_err();
        assert!(err.to_string().contains("port"), "{err}");
    }

    #[test]
    fn malformed_strings_fail_without_echoing_secrets() {
        for conn in [
            "host=localhost password='hunter2",
            "host=db password=hunter2 is my password",
            "postgress://u:hunter2@localhost/app",
            "postgres://u:hunter2@localhost/app?sslmode=%zz",
            "postgres://u:hunter2@localhost:notaport/app",
            "postgres://u@localhost/app?sslmode=hunter2",
        ] {
            let err = parse(conn).unwrap_err().to_string();
            assert!(
                !err.contains("hunter2") && !err.contains("password"),
                "{conn}: {err}"
            );
        }
    }
}
