//! Secret connection values and the redaction pass (0004 §4).
//!
//! Connectors mark every credential-bearing option as [`Secret`]. Its schema
//! carries `x-ferry-secret`, which is how the registry finds secret values in
//! a connection entry without knowing the connector's types. Every value
//! found that way, plus the connector's
//! [`derived_secrets`](crate::Connector::derived_secrets), goes into a
//! [`Redactor`] that the CLI runs over everything it prints.

use std::borrow::Cow;
use std::fmt;

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::Deserialize;
use serde::de::{self, Deserializer, Visitor};

/// What a secret value is replaced with in any output.
pub const REDACTED: &str = "***";

/// The schema keyword that marks a field as secret.
pub const SECRET_KEYWORD: &str = "x-ferry-secret";

/// A credential: a password, a token, or a URL that may embed one.
///
/// `Debug` and `Display` print [`REDACTED`], and it has no `Serialize`
/// impl, so the value can't leak through formatting or a serialized model.
/// The only way to read it is [`expose`](Secret::expose).
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// The secret value. Pass it to the driver, never to a formatter.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(REDACTED)
    }
}

impl fmt::Display for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(REDACTED)
    }
}

impl<'de> Deserialize<'de> for Secret {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(SecretVisitor)
    }
}

/// Accepts only strings. serde's default "invalid type" errors quote the
/// value they got, so every other kind is rejected without it: a password
/// written unquoted as `password: 123456` must not end up in a diagnostic.
struct SecretVisitor;

impl SecretVisitor {
    fn not_a_string<E: de::Error>(kind: &str) -> E {
        E::custom(format_args!(
            "expected a string, got {kind} (secret values are never shown); quote the value"
        ))
    }
}

impl Visitor<'_> for SecretVisitor {
    type Value = Secret;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a string")
    }

    fn visit_str<E: de::Error>(self, v: &str) -> Result<Secret, E> {
        Ok(Secret::new(v))
    }

    fn visit_string<E: de::Error>(self, v: String) -> Result<Secret, E> {
        Ok(Secret(v))
    }

    fn visit_bool<E: de::Error>(self, _: bool) -> Result<Secret, E> {
        Err(Self::not_a_string("a boolean"))
    }

    fn visit_i64<E: de::Error>(self, _: i64) -> Result<Secret, E> {
        Err(Self::not_a_string("a number"))
    }

    fn visit_i128<E: de::Error>(self, _: i128) -> Result<Secret, E> {
        Err(Self::not_a_string("a number"))
    }

    fn visit_u64<E: de::Error>(self, _: u64) -> Result<Secret, E> {
        Err(Self::not_a_string("a number"))
    }

    fn visit_u128<E: de::Error>(self, _: u128) -> Result<Secret, E> {
        Err(Self::not_a_string("a number"))
    }

    fn visit_f64<E: de::Error>(self, _: f64) -> Result<Secret, E> {
        Err(Self::not_a_string("a number"))
    }

    fn visit_char<E: de::Error>(self, v: char) -> Result<Secret, E> {
        Ok(Secret(v.to_string()))
    }

    fn visit_bytes<E: de::Error>(self, _: &[u8]) -> Result<Secret, E> {
        Err(Self::not_a_string("bytes"))
    }
}

impl JsonSchema for Secret {
    fn inline_schema() -> bool {
        true
    }

    fn schema_name() -> Cow<'static, str> {
        "Secret".into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "string",
            "writeOnly": true,
            SECRET_KEYWORD: true,
        })
    }
}

/// Replaces every known secret value with [`REDACTED`].
///
/// Longer secrets are replaced first, so a password that is also part of a
/// URL secret can't leave the rest of the URL behind. Empty strings are
/// ignored: they would match everywhere.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct Redactor {
    secrets: Vec<String>,
}

impl Redactor {
    pub fn new<I, S>(secrets: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut redactor = Self::default();
        redactor.extend(secrets);
        redactor
    }

    pub fn add(&mut self, secret: impl Into<String>) {
        self.extend([secret]);
    }

    pub fn extend<I, S>(&mut self, secrets: I)
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.secrets.extend(
            secrets
                .into_iter()
                .map(Into::into)
                .filter(|s| !s.is_empty()),
        );
        self.secrets
            .sort_by(|a, b| b.len().cmp(&a.len()).then_with(|| a.cmp(b)));
        self.secrets.dedup();
    }

    pub fn is_empty(&self) -> bool {
        self.secrets.is_empty()
    }

    /// `text` with every secret replaced. Borrows when nothing matched.
    pub fn redact<'a>(&self, text: &'a str) -> Cow<'a, str> {
        let mut out = Cow::Borrowed(text);
        for secret in &self.secrets {
            if out.contains(secret.as_str()) {
                out = Cow::Owned(out.replace(secret.as_str(), REDACTED));
            }
        }
        out
    }
}

/// Lists how many secrets it holds, never the secrets.
impl fmt::Debug for Redactor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Redactor")
            .field("secrets", &self.secrets.len())
            .finish()
    }
}

/// The password embedded in a URL's userinfo (`scheme://user:password@host`),
/// for a connector's [`derived_secrets`](crate::Connector::derived_secrets).
///
/// Returns the password as written and, if it is percent-encoded, decoded as
/// well, because a driver error may echo either form. Empty when the URL has
/// no password.
pub fn url_password(url: &str) -> Vec<String> {
    let Some((_, rest)) = url.split_once("://") else {
        return Vec::new();
    };
    // As libpq parses it: the userinfo ends at the last `@` before the first
    // `/`, so a password may hold a raw `?` or `#`.
    let authority = rest.split('/').next().unwrap_or_default();
    let Some((userinfo, _)) = authority.rsplit_once('@') else {
        return Vec::new();
    };
    let Some((_, password)) = userinfo.split_once(':') else {
        return Vec::new();
    };
    if password.is_empty() {
        return Vec::new();
    }
    let mut out = vec![password.to_owned()];
    if let Some(decoded) = percent_decode(password)
        && decoded != password
    {
        out.push(decoded);
    }
    out
}

/// `None` if the escapes don't decode to UTF-8.
fn percent_decode(s: &str) -> Option<String> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && let Some(hex) = s.get(i + 1..i + 3)
            && let Ok(b) = u8::from_str_radix(hex, 16)
        {
            out.push(b);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}
