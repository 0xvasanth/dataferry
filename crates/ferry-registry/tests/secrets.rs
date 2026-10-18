//! A planted secret never shows up in `Debug`, `Display`, descriptions or
//! error output (0004 §4).

use std::sync::Arc;

use ferry_core::FerryError;
use ferry_core::testkit::MemSource;
use ferry_registry::secret::url_password;
use ferry_registry::{
    BuildContext, BuildError, Built, Connector, ConnectorRegistry, DynConnector, Endpoints,
    RawConnection, Redactor, Roles, Secret,
};
use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::Deserialize;
use serde_json::{Value, json};

const PASSWORD: &str = "pl4nted-Pa55";
const TOKEN: &str = "tok-9f8e7d6c5b4a";

fn url() -> String {
    format!("pg://app:{PASSWORD}@db.internal:5432/shop")
}

/// A connector that does everything a careless connector might: describes
/// itself with its URL, and fails with errors that echo it, as some drivers
/// do.
struct Leaky;

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct LeakyOptions {
    url: Secret,
    token: Option<Secret>,
    #[serde(default)]
    replicas: Vec<Replica>,
    #[serde(default)]
    fail: Option<String>,
    #[serde(default)]
    port: u16,
}

impl LeakyOptions {
    fn endpoint(&self) -> String {
        format!("{}:{}", self.url.expose(), self.port)
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct Replica {
    url: Secret,
}

impl Connector for Leaky {
    const TYPE: &'static str = "leaky";
    const ROLES: Roles = Roles::SOURCE;
    type Options = LeakyOptions;

    fn build(
        &self,
        _: &BuildContext<'_>,
        o: LeakyOptions,
        _: Roles,
    ) -> Result<Endpoints, BuildError> {
        let everything = format!(
            "{} token={} replicas={}",
            o.endpoint(),
            o.token.as_ref().map_or("", Secret::expose),
            o.replicas
                .iter()
                .map(|r| r.url.expose())
                .collect::<Vec<_>>()
                .join(",")
        );
        match o.fail.as_deref() {
            Some("driver") => Err(BuildError::Other(FerryError::Connector {
                connector: "leaky".into(),
                message: format!("could not parse {everything}"),
            })),
            Some("entity") => Err(BuildError::Other(FerryError::InvalidEntity {
                entity: "orders".into(),
                reason: format!("not found at {everything}"),
            })),
            Some("options") => Err(BuildError::option("url", format!("bad URL {everything}"))),
            // Only the password, percent-decoded, as a driver would print it.
            Some("password") => Err(BuildError::Other(FerryError::Connector {
                connector: "leaky".into(),
                message: "authentication failed for password p@ss w0rd".into(),
            })),
            _ => Ok(Endpoints {
                source: Some(Arc::new(MemSource::new())),
                sink: None,
                finalizer: None,
                ddl: None,
            }),
        }
    }

    fn describe(&self, o: &LeakyOptions) -> String {
        format!("leaky at {}", o.url.expose())
    }

    fn derived_secrets(&self, o: &LeakyOptions) -> Vec<String> {
        url_password(o.url.expose())
    }
}

fn registry() -> ConnectorRegistry {
    let mut registry = ConnectorRegistry::new();
    registry.register(Leaky);
    registry
}

fn raw(fields: Value) -> RawConnection {
    let mut entry = json!({ "name": "leak", "type": "leaky" });
    entry
        .as_object_mut()
        .unwrap()
        .extend(fields.as_object().unwrap().clone());
    serde_json::from_value(entry).unwrap()
}

fn assert_clean(text: &str) {
    for secret in [PASSWORD, TOKEN, "replica-pw", "p@ss w0rd", "p%40ss%20w0rd"] {
        assert!(!text.contains(secret), "`{secret}` leaked into: {text}");
    }
}

fn assert_error_clean(err: &BuildError) {
    assert_clean(&err.to_string());
    assert_clean(&format!("{err:?}"));
}

#[test]
fn secret_formats_as_stars() {
    let secret = Secret::new(PASSWORD);
    assert_eq!(secret.to_string(), "***");
    assert_eq!(format!("{secret:?}"), "***");
    assert_eq!(secret.expose(), PASSWORD);

    let options: LeakyOptions = serde_json::from_value(json!({
        "url": url(),
        "token": TOKEN,
        "replicas": [{ "url": "pg://r:replica-pw@r1" }],
    }))
    .unwrap();
    assert_clean(&format!("{options:?}"));
    assert_clean(&format!("{options:#?}"));
}

#[test]
fn raw_connections_show_field_names_only() {
    let raw = raw(json!({ "url": url(), "token": TOKEN }));
    let debug = format!("{raw:?}");
    assert_clean(&debug);
    assert!(
        debug.contains("\"url\"") && debug.contains("\"token\""),
        "{debug}"
    );
}

#[test]
fn a_successful_build_redacts_the_description_and_reports_every_secret() {
    let built = registry()
        .build(
            &raw(json!({
                "url": url(),
                "token": TOKEN,
                "replicas": [{ "url": "pg://r:replica-pw@r1" }],
            })),
            Roles::SOURCE,
        )
        .unwrap();
    assert_eq!(built.description, "leaky at ***");
    assert_clean(&format!("{built:?}"));

    // Every secret field, nested ones included, plus the derived password.
    let mut secrets = built.secrets.clone();
    secrets.sort();
    let mut expected = vec![
        url(),
        TOKEN.to_owned(),
        "pg://r:replica-pw@r1".into(),
        PASSWORD.into(),
    ];
    expected.sort();
    assert_eq!(secrets, expected);

    // The CLI's renderer, fed those secrets, cleans anything a driver says.
    let redactor = Redactor::new(built.secrets);
    let log = format!("connect {} failed; retrying with token {TOKEN}", url());
    assert_eq!(
        redactor.redact(&log),
        "connect *** failed; retrying with token ***"
    );
}

#[test]
fn build_errors_that_echo_secrets_are_redacted() {
    let registry = registry();
    for fail in ["driver", "options", "entity"] {
        let err = registry
            .build(
                &raw(json!({
                    "url": url(),
                    "token": TOKEN,
                    "replicas": [{ "url": "pg://r:replica-pw@r1" }],
                    "fail": fail,
                })),
                Roles::SOURCE,
            )
            .unwrap_err();
        assert_error_clean(&err);
        assert!(err.to_string().contains("***"), "{err}");
    }

    // Paths reach into nested options.
    let err = registry
        .build(
            &raw(json!({ "url": url(), "replicas": [{ "url": 5 }] })),
            Roles::SOURCE,
        )
        .unwrap_err();
    assert!(
        matches!(&err, BuildError::Options(e) if e[0].path == "replicas[0].url"),
        "{err:?}"
    );
}

#[test]
fn a_percent_encoded_password_is_redacted_in_both_forms() {
    let encoded = "pg://app:p%40ss%20w0rd@db/shop";
    assert_eq!(url_password(encoded), ["p%40ss%20w0rd", "p@ss w0rd"]);

    let err = registry()
        .build(
            &raw(json!({ "url": encoded, "fail": "password" })),
            Roles::SOURCE,
        )
        .unwrap_err();
    assert_error_clean(&err);
    assert_eq!(
        err,
        BuildError::Other(FerryError::Connector {
            connector: "leaky".into(),
            message: "authentication failed for password ***".into(),
        })
    );
}

#[test]
fn deserialization_errors_never_quote_a_secret() {
    let registry = registry();
    // A secret written unquoted in YAML arrives as a number. serde would
    // normally quote the value it rejected.
    let err = registry
        .build(
            &raw(json!({ "url": url(), "token": 98765432 })),
            Roles::SOURCE,
        )
        .unwrap_err();
    assert!(!err.to_string().contains("98765432"), "{err}");
    let BuildError::Options(errors) = &err else {
        panic!("{err:?}")
    };
    assert_eq!(errors[0].path, "token");

    // A secret that deserialized fine, and a later field that didn't: the
    // error names the bad field and doesn't carry the secret.
    for fields in [
        json!({ "url": url(), "token": TOKEN, "port": TOKEN.to_string() }),
        json!({ "port": TOKEN, "url": url(), "token": TOKEN }),
    ] {
        let err = registry.build(&raw(fields), Roles::SOURCE).unwrap_err();
        assert_error_clean(&err);
        let BuildError::Options(errors) = &err else {
            panic!("{err:?}")
        };
        assert_eq!(errors[0].path, "port");
    }
}

/// A dyn connector (e.g. a future subprocess connector) gets the same
/// backstop: the registry finds secrets through the schema, not through the
/// connector's Rust types.
#[test]
fn errors_from_a_dyn_connector_are_redacted_through_its_schema() {
    struct Opaque;
    impl DynConnector for Opaque {
        fn type_name(&self) -> &str {
            "opaque"
        }
        fn roles(&self) -> Roles {
            Roles::SOURCE
        }
        fn options_schema(&self, _: &mut SchemaGenerator) -> Schema {
            json_schema!({
                "type": "object",
                "properties": { "key": { "type": "string", "x-ferry-secret": true } },
            })
        }
        fn build(
            &self,
            _: &BuildContext<'_>,
            options: Value,
            _: Roles,
        ) -> Result<Built, BuildError> {
            Err(BuildError::Other(FerryError::Connector {
                connector: "opaque".into(),
                message: format!("rejected key {}", options["key"]),
            }))
        }
    }
    let mut registry = ConnectorRegistry::new();
    registry.register_dyn(Box::new(Opaque));
    let raw: RawConnection =
        serde_json::from_value(json!({ "name": "o", "type": "opaque", "key": TOKEN })).unwrap();
    let err = registry.build(&raw, Roles::SOURCE).unwrap_err();
    assert_error_clean(&err);
    assert_eq!(err.to_string(), "opaque: rejected key \"***\"");
}

#[test]
fn redactor_replaces_longer_secrets_first_and_ignores_empty_ones() {
    let redactor = Redactor::new(["pw", "", "user:pw@host"]);
    assert_eq!(redactor.redact("dsn user:pw@host, pw"), "dsn ***, ***");
    assert_eq!(redactor.redact("nothing here"), "nothing here");
    assert!(!format!("{redactor:?}").contains("pw"));
    assert!(Redactor::new([""]).is_empty());
}

#[test]
fn url_password_finds_the_userinfo_password_only() {
    assert_eq!(
        url_password("postgres://u:pw@h:5432/db?sslmode=require"),
        ["pw"]
    );
    assert_eq!(url_password("mongodb+srv://u:pw@cluster/db?x=a@b"), ["pw"]);
    // `@` in the password must be encoded; the last `@` ends the userinfo.
    assert_eq!(url_password("postgres://u:p@ss@h/db"), ["p@ss"]);
    // libpq ends the userinfo at the last `@` before the first `/`, so `?`
    // and `#` may appear raw in a password.
    assert_eq!(url_password("postgres://u:pa?ss@h/db"), ["pa?ss"]);
    assert_eq!(url_password("postgres://u:pa#ss@h"), ["pa#ss"]);
    for none in [
        "postgres://u@h/db",
        "postgres://h/db",
        "postgres://u:@h/db",
        "host=h password=pw",
        "postgres://h/db?user=u:x@y",
    ] {
        assert!(url_password(none).is_empty(), "{none}");
    }
}

#[test]
fn escaped_forms_of_a_secret_are_redacted_too() {
    let secret = "zq\"ss\\w0rd\u{7}";
    let redactor = Redactor::new([secret]);
    for rendered in [
        secret.to_owned(),
        format!("{secret:?}"),
        serde_json::to_string(secret).unwrap(),
        format!("{:?}", BuildError::option("url", format!("bad {secret:?}"))),
    ] {
        let out = redactor.redact(&rendered);
        assert!(!out.contains("zq"), "{rendered} became {out}");
    }

    // Through the registry: a dyn connector that quotes the value as JSON,
    // and an error whose Debug output escapes it again.
    struct Quoting;
    impl DynConnector for Quoting {
        fn type_name(&self) -> &str {
            "quoting"
        }
        fn roles(&self) -> Roles {
            Roles::SOURCE
        }
        fn options_schema(&self, _: &mut SchemaGenerator) -> Schema {
            json_schema!({
                "type": "object",
                "properties": { "key": { "type": "string", "x-ferry-secret": true } },
            })
        }
        fn build(
            &self,
            _: &BuildContext<'_>,
            options: Value,
            _: Roles,
        ) -> Result<Built, BuildError> {
            Err(BuildError::option(
                "key",
                format!("rejected {}", options["key"]),
            ))
        }
    }
    let mut registry = ConnectorRegistry::new();
    registry.register_dyn(Box::new(Quoting));
    let raw: RawConnection =
        serde_json::from_value(json!({ "name": "q", "type": "quoting", "key": secret })).unwrap();
    let err = registry.build(&raw, Roles::SOURCE).unwrap_err();
    for text in [err.to_string(), format!("{err:?}")] {
        assert!(!text.contains("zq"), "{text}");
    }
}
