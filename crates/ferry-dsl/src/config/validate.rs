//! Checks across the merged model, after every file is loaded.

use super::Config;
use super::error::{ConfigError, ErrorKind};
use super::span::{FieldPath, Location};

pub(crate) fn check(config: &Config) -> Vec<ConfigError> {
    let mut errors = Vec::new();
    duplicates(
        "connection",
        "connections",
        config
            .connections
            .iter()
            .map(|c| (c.name.as_str(), c.name.location())),
        &mut errors,
    );
    duplicates(
        "flow",
        "flows",
        config
            .flows
            .iter()
            .map(|f| (f.name.as_str(), f.name.location())),
        &mut errors,
    );
    errors
}

/// A name defined twice, in one file or in two, is one error at the second
/// definition naming the first (0003 §2).
fn duplicates<'a>(
    what: &str,
    list: &str,
    names: impl Iterator<Item = (&'a str, &'a Location)>,
    errors: &mut Vec<ConfigError>,
) {
    let mut seen: Vec<(&str, &Location)> = Vec::new();
    for (name, location) in names {
        match seen.iter().find(|(n, _)| *n == name) {
            Some((_, first)) => errors.push(
                ConfigError::new(
                    ErrorKind::Duplicate,
                    format!("{what} `{name}` is defined twice: first at {first}, again here"),
                )
                .at(location)
                .path(&FieldPath::root().key(list).item(0, Some(name)).key("name")),
            ),
            None => seen.push((name, location)),
        }
    }
}
