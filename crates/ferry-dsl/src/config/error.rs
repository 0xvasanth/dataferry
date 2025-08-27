//! Config errors. Every error that can point at the config does: the
//! original file and line (after includes and merging) and the field path.

use std::fmt;

use super::span::{FieldPath, Location};

/// What kind of check failed. Stable enough for tests and for the CLI to map
/// onto diagnostic codes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ErrorKind {
    /// A file or directory couldn't be read.
    Io,
    /// Not valid YAML, or YAML the loader doesn't accept (several documents,
    /// tags, anchors, duplicate keys).
    Yaml,
    /// A file is in the wrong place or holds keys that belong elsewhere.
    Layout,
    /// An `include:` entry is wrong.
    Include,
    /// A value doesn't fit the model: unknown key, missing key, wrong type.
    Model,
    /// The same flow or connection name is defined twice.
    Duplicate,
    /// A name refers to something that doesn't exist, or a rule that spans
    /// several parts of a flow is broken.
    Invalid,
    /// `${ENV}` misuse or a missing variable.
    Env,
    /// `depends_on:` names an unknown flow, the flow itself, or forms a cycle.
    DependsOn,
    /// An expression doesn't parse, or names something that isn't in scope
    /// (B2).
    Expr,
    /// An expression's type doesn't fit where it is used (B3).
    Type,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConfigError {
    pub kind: ErrorKind,
    pub message: String,
    pub location: Option<Location>,
    /// Rendered field path; empty when the error is about a whole file.
    pub path: String,
    pub help: Option<String>,
}

impl ConfigError {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            location: None,
            path: String::new(),
            help: None,
        }
    }

    pub fn at(mut self, location: &Location) -> Self {
        self.location = Some(location.clone());
        self
    }

    pub fn maybe_at(mut self, location: Option<&Location>) -> Self {
        self.location = location.cloned();
        self
    }

    pub fn path(mut self, path: &FieldPath) -> Self {
        self.path = path.to_string();
        self
    }

    pub fn help(mut self, help: impl Into<String>) -> Self {
        self.help = Some(help.into());
        self
    }

    pub fn maybe_help(mut self, help: Option<String>) -> Self {
        self.help = help;
        self
    }
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(location) = &self.location {
            write!(f, "{location} ")?;
        }
        if !self.path.is_empty() {
            write!(f, "{}: ", self.path)?;
        }
        f.write_str(&self.message)?;
        if let Some(help) = &self.help {
            write!(f, "\n  help: {help}")?;
        }
        Ok(())
    }
}

impl std::error::Error for ConfigError {}

/// Everything wrong with a config. The loader keeps going after an error
/// where it can, so one run reports as much as possible.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConfigErrors(pub Vec<ConfigError>);

impl ConfigErrors {
    pub fn iter(&self) -> std::slice::Iter<'_, ConfigError> {
        self.0.iter()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl From<ConfigError> for ConfigErrors {
    fn from(error: ConfigError) -> Self {
        Self(vec![error])
    }
}

impl IntoIterator for ConfigErrors {
    type Item = ConfigError;
    type IntoIter = std::vec::IntoIter<ConfigError>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl fmt::Display for ConfigErrors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, error) in self.0.iter().enumerate() {
            if i > 0 {
                f.write_str("\n")?;
            }
            write!(f, "error: {error}")?;
        }
        Ok(())
    }
}

impl std::error::Error for ConfigErrors {}

/// The closest candidate within a small edit distance, for "did you mean".
pub(crate) fn suggest<'a>(
    word: &str,
    candidates: impl IntoIterator<Item = &'a str>,
) -> Option<&'a str> {
    let limit = (word.chars().count() / 3).max(1);
    candidates
        .into_iter()
        .map(|c| (edit_distance(word, c), c))
        .filter(|(d, _)| *d <= limit)
        .min()
        .map(|(_, c)| c)
}

/// Optimal string alignment distance: Levenshtein plus adjacent
/// transpositions, so `mdoe` is one edit from `mode`.
fn edit_distance(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let mut d = vec![vec![0; b.len() + 1]; a.len() + 1];
    for (i, row) in d.iter_mut().enumerate() {
        row[0] = i;
    }
    d[0] = (0..=b.len()).collect();
    for i in 1..=a.len() {
        for j in 1..=b.len() {
            let cost = usize::from(a[i - 1] != b[j - 1]);
            d[i][j] = (d[i - 1][j] + 1)
                .min(d[i][j - 1] + 1)
                .min(d[i - 1][j - 1] + cost);
            if i > 1 && j > 1 && a[i - 1] == b[j - 2] && a[i - 2] == b[j - 1] {
                d[i][j] = d[i][j].min(d[i - 2][j - 2] + 1);
            }
        }
    }
    d[a.len()][b.len()]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn suggests_close_names_only() {
        assert_eq!(
            suggest("wrokers", ["workers", "memory_budget"]),
            Some("workers")
        );
        assert_eq!(
            suggest("warehous", ["shop", "warehouse"]),
            Some("warehouse")
        );
        assert_eq!(suggest("customer", ["orders"]), None);
        assert_eq!(suggest("ab", ["xy"]), None);
        assert_eq!(suggest("mdoe", ["mode", "sink"]), Some("mode"));
    }
}
