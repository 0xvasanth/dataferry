//! `${NAME}` substitution (0003 §9) and keeping substituted values out of
//! everything ferry prints.
//!
//! `${NAME}` is allowed only in connection entries and in `runtime.state`.
//! The loader rejects it anywhere in a flow (the config hash is taken
//! before substitution, so an environment-driven flow could change between
//! a run and its resume). Substitution works on the parsed model, string by
//! string: an environment value can never add YAML structure, and is never
//! expanded again.
//!
//! Still open in 0003: how to write a literal `${`, and whether a variable
//! set to the empty string counts as missing. Until that is decided, every
//! `${` must start a reference, and an empty value is a value.

use std::collections::BTreeMap;
use std::fmt;

use serde_json::Value;

use super::Config;
use super::error::{ConfigError, ConfigErrors, ErrorKind};
use super::model::{Connection, OrderedMap, RawKind, RawValue};
use super::span::{FieldPath, Location, Spanned};

/// What a `${...}` string is made of.
#[derive(Debug, PartialEq)]
enum Piece<'a> {
    Text(&'a str),
    Var(&'a str),
}

/// Splits `text` into literal text and variable references. Anything after a
/// `${` that isn't `NAME}` is an error.
fn pieces(text: &str) -> Result<Vec<Piece<'_>>, String> {
    let mut out = Vec::new();
    let mut rest = text;
    while let Some(start) = rest.find("${") {
        if start > 0 {
            out.push(Piece::Text(&rest[..start]));
        }
        let after = &rest[start + 2..];
        let Some(end) = after.find('}') else {
            return Err("`${` without a closing `}`".into());
        };
        let name = &after[..end];
        if !is_var_name(name) {
            return Err(format!(
                "`${{{name}}}` is not supported: the only form is `${{NAME}}`, a variable from the environment (0003 §9)"
            ));
        }
        out.push(Piece::Var(name));
        rest = &after[end + 1..];
    }
    if !rest.is_empty() {
        out.push(Piece::Text(rest));
    }
    Ok(out)
}

fn is_var_name(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// One string `${...}` may appear in.
struct Slot<'a> {
    text: &'a str,
    location: &'a Location,
    path: FieldPath,
}

/// Every string where `${...}` is allowed: connection fields and
/// `runtime.state`.
fn slots(config: &Config) -> Vec<Slot<'_>> {
    let mut out = Vec::new();
    for connection in &config.connections {
        let path = FieldPath::root()
            .key("connections")
            .item(0, Some(connection.name.as_str()));
        for (key, value) in connection.fields.iter() {
            raw_slots(value, path.key(key.as_str()), &mut out);
        }
    }
    if let Some(state) = &config.runtime.state {
        let path = FieldPath::root().key("runtime").key("state");
        for (key, value) in [
            ("backend", Some(&state.backend)),
            ("path", state.path.as_ref()),
            ("connection", state.connection.as_ref()),
        ] {
            if let Some(value) = value {
                out.push(Slot {
                    text: value.as_str(),
                    location: value.location(),
                    path: path.key(key),
                });
            }
        }
    }
    out
}

fn raw_slots<'a>(value: &'a RawValue, path: FieldPath, out: &mut Vec<Slot<'a>>) {
    match value.value() {
        RawKind::Str(text) => out.push(Slot {
            text,
            location: value.location(),
            path,
        }),
        RawKind::Seq(items) => {
            for (i, item) in items.iter().enumerate() {
                raw_slots(item, path.item(i, None), out);
            }
        }
        RawKind::Map(map) => {
            for (key, item) in map.iter() {
                raw_slots(item, path.key(key.as_str()), out);
            }
        }
        _ => {}
    }
}

/// Malformed references, found at load time whatever the environment holds.
pub(crate) fn check_syntax(config: &Config) -> Vec<ConfigError> {
    slots(config)
        .into_iter()
        .filter_map(|slot| {
            let message = pieces(slot.text).err()?;
            Some(
                ConfigError::new(ErrorKind::Env, message)
                    .at(slot.location)
                    .path(&slot.path),
            )
        })
        .collect()
}

/// A config with every `${NAME}` replaced, ready for the registry (0004).
pub struct Resolved {
    pub connections: Vec<ResolvedConnection>,
    pub state: Option<ResolvedState>,
    redactor: Redactor,
}

impl Resolved {
    /// Masks every substituted value in text about to be printed.
    pub fn redactor(&self) -> &Redactor {
        &self.redactor
    }
}

impl fmt::Debug for Resolved {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Resolved")
            .field("connections", &self.connections)
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}

/// A connection entry after substitution: `name`, `type` and the
/// type-specific fields as JSON, which is what the registry deserializes
/// into a connector's `Options` (0004 §1).
pub struct ResolvedConnection {
    pub name: String,
    pub r#type: String,
    /// A JSON object of the type-specific fields, substituted.
    pub fields: Value,
    /// Where each field was written, by dotted path (`url`, `tls.ca[0]`), so
    /// a registry error about a field can point at the original line.
    pub locations: BTreeMap<String, Location>,
    /// Fields whose value came from the environment.
    substituted: Vec<String>,
}

impl ResolvedConnection {
    pub fn location(&self, field: &str) -> Option<&Location> {
        self.locations.get(field)
    }
}

impl fmt::Debug for ResolvedConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut fields = self.fields.clone();
        for path in &self.substituted {
            if let Some(value) = pointer_mut(&mut fields, path) {
                *value = Value::String(Redactor::MASK.into());
            }
        }
        f.debug_struct("ResolvedConnection")
            .field("name", &self.name)
            .field("type", &self.r#type)
            .field("fields", &mask_urls(&fields.to_string()))
            .finish()
    }
}

/// `runtime.state` after substitution.
#[derive(Clone, PartialEq, Eq)]
pub struct ResolvedState {
    pub backend: String,
    pub path: Option<String>,
    pub connection: Option<String>,
    /// Whether `path` came (partly) from the environment.
    path_substituted: bool,
}

impl fmt::Debug for ResolvedState {
    // The path is masked when it came from the environment, like any other
    // substituted value. Backend and connection are names.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let path = match (&self.path, self.path_substituted) {
            (Some(_), true) => Some(Redactor::MASK),
            (path, _) => path.as_deref(),
        };
        f.debug_struct("ResolvedState")
            .field("backend", &self.backend)
            .field("path", &path)
            .field("connection", &self.connection)
            .finish()
    }
}

/// Replaces substituted values, and passwords inside URLs, with `***`.
#[derive(Clone, Default)]
pub struct Redactor {
    /// Longest first, so a value containing another is masked whole.
    values: Vec<String>,
}

impl Redactor {
    pub const MASK: &'static str = "***";

    /// Adds a substituted value, and the password of any URL inside it, so
    /// the password is masked even where it is printed on its own.
    fn add(&mut self, value: &str) {
        for secret in std::iter::once(value).chain(url_passwords(value)) {
            if !secret.is_empty() && !self.values.iter().any(|v| v == secret) {
                self.values.push(secret.to_owned());
            }
        }
        self.values
            .sort_by(|a, b| b.len().cmp(&a.len()).then_with(|| a.cmp(b)));
    }

    pub fn redact(&self, text: &str) -> String {
        let mut out = text.to_owned();
        for value in &self.values {
            out = out.replace(value.as_str(), Self::MASK);
        }
        mask_urls(&out)
    }
}

impl fmt::Debug for Redactor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Redactor({} values)", self.values.len())
    }
}

/// The URLs in `text`, each split as (text before the authority,
/// authority, password span within the authority if it has one).
fn urls(text: &str) -> impl Iterator<Item = (&str, &str, Option<(usize, usize)>)> {
    let mut rest = text;
    std::iter::from_fn(move || {
        let i = rest.find("://")?;
        let (head, tail) = rest.split_at(i + 3);
        let end = tail
            .find(|c: char| matches!(c, '/' | '?' | '#' | '"' | '\'') || c.is_whitespace())
            .unwrap_or(tail.len());
        let authority = &tail[..end];
        let password = authority.rfind('@').and_then(|at| {
            let colon = authority[..at].find(':')?;
            Some((colon + 1, at))
        });
        rest = &tail[end..];
        Some((head, authority, password))
    })
}

fn url_passwords(text: &str) -> impl Iterator<Item = &str> {
    urls(text).filter_map(|(_, authority, password)| password.map(|(s, e)| &authority[s..e]))
}

/// `scheme://user:password@host` → `scheme://user:***@host`, for every URL
/// in `text`.
pub fn mask_urls(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut consumed = 0;
    for (head, authority, password) in urls(text) {
        out.push_str(head);
        match password {
            Some((start, end)) => {
                out.push_str(&authority[..start]);
                out.push_str(Redactor::MASK);
                out.push_str(&authority[end..]);
            }
            None => out.push_str(authority),
        }
        consumed += head.len() + authority.len();
    }
    out.push_str(&text[consumed..]);
    out
}

fn pointer_mut<'a>(value: &'a mut Value, path: &str) -> Option<&'a mut Value> {
    let mut current = value;
    for part in path.split('.') {
        let (key, indexes) = match part.find('[') {
            Some(i) => (&part[..i], &part[i..]),
            None => (part, ""),
        };
        current = current.get_mut(key)?;
        for index in indexes.split(['[', ']']).filter(|s| !s.is_empty()) {
            current = current.get_mut(index.parse::<usize>().ok()?)?;
        }
    }
    Some(current)
}

impl Config {
    /// Substitutes `${NAME}` from the process environment.
    pub fn resolve(&self) -> Result<Resolved, ConfigErrors> {
        self.resolve_with(|name| std::env::var(name).ok())
    }

    /// Substitutes `${NAME}` using `env` to look names up. Every missing
    /// variable is reported in one error, by name and location; no value is
    /// ever printed.
    pub fn resolve_with(
        &self,
        env: impl Fn(&str) -> Option<String>,
    ) -> Result<Resolved, ConfigErrors> {
        let mut resolver = Resolver {
            env: &env,
            missing: Vec::new(),
            redactor: Redactor::default(),
        };
        let connections = self
            .connections
            .iter()
            .map(|c| resolver.connection(c))
            .collect();
        let path = FieldPath::root().key("runtime").key("state");
        let state = self.runtime.state.as_ref().map(|s| {
            let state_path = s
                .path
                .as_ref()
                .map(|p| resolver.string(p, path.key("path")));
            ResolvedState {
                backend: resolver.string(&s.backend, path.key("backend")).0,
                path_substituted: state_path.as_ref().is_some_and(|(_, sub)| *sub),
                path: state_path.map(|(p, _)| p),
                connection: s
                    .connection
                    .as_ref()
                    .map(|c| resolver.string(c, path.key("connection")).0),
            }
        });
        if !resolver.missing.is_empty() {
            return Err(resolver.missing_error().into());
        }
        Ok(Resolved {
            connections,
            state,
            redactor: resolver.redactor,
        })
    }
}

struct Resolver<'e> {
    env: &'e dyn Fn(&str) -> Option<String>,
    missing: Vec<(String, Location, FieldPath)>,
    redactor: Redactor,
}

impl Resolver<'_> {
    /// Substitutes one string. Malformed references were rejected at load.
    fn substitute(&mut self, text: &str, location: &Location, path: &FieldPath) -> (String, bool) {
        let Ok(pieces) = pieces(text) else {
            return (text.to_owned(), false);
        };
        let mut out = String::new();
        let mut substituted = false;
        for piece in pieces {
            match piece {
                Piece::Text(t) => out.push_str(t),
                Piece::Var(name) => match (self.env)(name) {
                    Some(value) => {
                        self.redactor.add(&value);
                        out.push_str(&value);
                        substituted = true;
                    }
                    None => self
                        .missing
                        .push((name.to_owned(), location.clone(), path.clone())),
                },
            }
        }
        (out, substituted)
    }

    /// The substituted string, and whether anything was substituted.
    fn string(&mut self, value: &Spanned<String>, path: FieldPath) -> (String, bool) {
        self.substitute(value.as_str(), value.location(), &path)
    }

    fn connection(&mut self, connection: &Connection) -> ResolvedConnection {
        let path = FieldPath::root()
            .key("connections")
            .item(0, Some(connection.name.as_str()));
        let mut resolved = ResolvedConnection {
            name: connection.name.as_str().to_owned(),
            r#type: connection.r#type.as_str().to_owned(),
            fields: Value::Null,
            locations: BTreeMap::new(),
            substituted: Vec::new(),
        };
        resolved.fields = self.map(&connection.fields, "", &path, &mut resolved);
        resolved
    }

    fn map(
        &mut self,
        map: &OrderedMap<RawValue>,
        prefix: &str,
        path: &FieldPath,
        out: &mut ResolvedConnection,
    ) -> Value {
        Value::Object(
            map.iter()
                .map(|(key, value)| {
                    let field = match prefix {
                        "" => key.as_str().to_owned(),
                        _ => format!("{prefix}.{}", key.as_str()),
                    };
                    let json = self.value(value, &field, &path.key(key.as_str()), out);
                    (key.as_str().to_owned(), json)
                })
                .collect(),
        )
    }

    fn value(
        &mut self,
        value: &RawValue,
        field: &str,
        path: &FieldPath,
        out: &mut ResolvedConnection,
    ) -> Value {
        out.locations
            .insert(field.to_owned(), value.location().clone());
        match value.value() {
            RawKind::Str(text) => {
                let (text, substituted) = self.substitute(text, value.location(), path);
                if substituted {
                    out.substituted.push(field.to_owned());
                }
                Value::String(text)
            }
            RawKind::Seq(items) => Value::Array(
                items
                    .iter()
                    .enumerate()
                    .map(|(i, item)| {
                        self.value(item, &format!("{field}[{i}]"), &path.item(i, None), out)
                    })
                    .collect(),
            ),
            RawKind::Map(map) => self.map(map, field, path, out),
            other => other.to_json(),
        }
    }

    fn missing_error(&self) -> ConfigError {
        let mut by_name: Vec<(&str, Vec<&Location>)> = Vec::new();
        for (name, location, _) in &self.missing {
            match by_name.iter_mut().find(|(n, _)| n == name) {
                Some((_, locations)) => locations.push(location),
                None => by_name.push((name, vec![location])),
            }
        }
        let list = by_name
            .iter()
            .map(|(name, locations)| {
                let at: Vec<String> = locations.iter().map(|l| l.to_string()).collect();
                format!("{name} ({})", at.join(", "))
            })
            .collect::<Vec<_>>()
            .join(", ");
        let (_, location, path) = &self.missing[0];
        let (noun, them) = match by_name.len() {
            1 => ("variable", "it"),
            _ => ("variables", "them"),
        };
        ConfigError::new(
            ErrorKind::Env,
            format!("environment {noun} not set: {list}"),
        )
        .at(location)
        .path(path)
        .help(format!(
            "export {them} before running ferry; values are read only from the environment (0003 §9)"
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pieces_split_text_and_references() {
        assert_eq!(
            pieces("postgres://u:${PW}@h/${DB}").unwrap(),
            [
                Piece::Text("postgres://u:"),
                Piece::Var("PW"),
                Piece::Text("@h/"),
                Piece::Var("DB"),
            ]
        );
        assert_eq!(
            pieces("no refs $HOME").unwrap(),
            [Piece::Text("no refs $HOME")]
        );
        assert_eq!(pieces("${_A1}").unwrap(), [Piece::Var("_A1")]);
    }

    #[test]
    fn malformed_references_are_errors() {
        for text in ["${file:/run/secrets/pg}", "${}", "${1A}", "${A", "a ${B-C}"] {
            assert!(pieces(text).is_err(), "{text}");
        }
    }

    #[test]
    fn url_passwords_are_masked() {
        assert_eq!(
            mask_urls("connect to postgres://app:hunter2@db:5432/shop failed"),
            "connect to postgres://app:***@db:5432/shop failed"
        );
        assert_eq!(
            mask_urls("mongodb+srv://u:p@x.net/?tls=true and http://plain.host/a"),
            "mongodb+srv://u:***@x.net/?tls=true and http://plain.host/a"
        );
        assert_eq!(
            mask_urls("postgres://app@db/shop"),
            "postgres://app@db/shop"
        );
    }

    #[test]
    fn the_redactor_masks_the_longest_value_first() {
        let mut redactor = Redactor::default();
        redactor.add("abc");
        redactor.add("abcdef");
        redactor.add("");
        assert_eq!(redactor.redact("x abcdef y abc"), "x *** y ***");
    }
}
