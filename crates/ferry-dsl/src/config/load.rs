//! Finds and reads a `ferry/` directory (0003 §2, §8).

use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use super::Config;
use super::de::{DeError, from_node};
use super::error::{ConfigError, ConfigErrors, ErrorKind, suggest};
use super::model::{
    Connection, ConnectionsFile, FerryFile, Flow, FlowsFile, IncludedFile, Runtime, RuntimeFile,
};
use super::span::{FieldPath, Location, Spanned};
use super::validate;
use super::yaml::{self, Node, NodeKind};

/// The directory used when no `--config` is given.
pub const DEFAULT_DIR: &str = "ferry";

/// What a file is, which decides the top-level keys it may hold.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FileKind {
    Ferry,
    Connections,
    Runtime,
    Flows,
    Included,
}

impl FileKind {
    /// The kind of a file by its path relative to `ferry/`: a well-known
    /// name, something under `flows/`, or else an included file.
    pub fn for_path(path: &str) -> FileKind {
        match path {
            "ferry.yaml" | "ferry.yml" => FileKind::Ferry,
            "connections.yaml" | "connections.yml" => FileKind::Connections,
            "runtime.yaml" | "runtime.yml" => FileKind::Runtime,
            _ if path.starts_with("flows/") => FileKind::Flows,
            _ => FileKind::Included,
        }
    }

    fn allowed(self) -> &'static [&'static str] {
        match self {
            FileKind::Ferry => &["version", "name", "include"],
            FileKind::Connections => &["connections"],
            FileKind::Runtime => &["runtime"],
            FileKind::Flows => &["flows"],
            FileKind::Included => &["connections", "flows"],
        }
    }

    fn describe(self) -> &'static str {
        match self {
            FileKind::Ferry => "ferry.yaml",
            FileKind::Connections => "connections.yaml",
            FileKind::Runtime => "runtime.yaml",
            FileKind::Flows => "a flow file",
            FileKind::Included => "an included file",
        }
    }
}

/// A file the config was read from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceFile {
    /// Relative to the `ferry/` directory, `/`-separated. What errors show.
    pub path: Arc<str>,
    /// Where it was read from.
    pub absolute: PathBuf,
    pub kind: FileKind,
    pub text: String,
}

/// Loads `./ferry`.
pub fn load_default() -> Result<Config, ConfigErrors> {
    load(DEFAULT_DIR)
}

/// Loads the `ferry/` directory at `dir`: every file, merged, checked.
pub fn load(dir: impl AsRef<Path>) -> Result<Config, ConfigErrors> {
    Loader::default().run(dir.as_ref())
}

#[derive(Default)]
struct Loader {
    errors: Vec<ConfigError>,
    sources: Vec<SourceFile>,
}

struct Pending {
    display: Arc<str>,
    absolute: PathBuf,
    kind: FileKind,
}

impl Loader {
    fn run(mut self, dir: &Path) -> Result<Config, ConfigErrors> {
        if !dir.is_dir() {
            return Err(ConfigError::new(
                ErrorKind::Io,
                format!("config directory `{}` not found", dir.display()),
            )
            .into());
        }
        let Some(ferry_path) = self.well_known(dir, "ferry") else {
            return Err(self.errors.pop().map(Into::into).unwrap_or_else(|| {
                let expected = absolute(&dir.join("ferry.yaml"));
                ConfigError::new(
                    ErrorKind::Layout,
                    format!(
                        "no ferry.yaml in `{}`: expected `{}`",
                        dir.display(),
                        expected.display()
                    ),
                )
                .help("a ferry config directory is marked by its ferry.yaml (0003 §2)")
                .into()
            }));
        };

        // ferry.yaml first: it says what else to include.
        let ferry_file = self
            .read(
                Pending {
                    display: file_name(&ferry_path),
                    absolute: ferry_path.clone(),
                    kind: FileKind::Ferry,
                },
                parse_as::<FerryFile>,
            )
            .flatten();

        let mut pending = Vec::new();
        let connections_path = self.well_known(dir, "connections");
        if let Some(path) = &connections_path {
            pending.push(Pending {
                display: file_name(path),
                absolute: path.clone(),
                kind: FileKind::Connections,
            });
        }
        if let Some(path) = self.well_known(dir, "runtime") {
            pending.push(Pending {
                display: file_name(&path),
                absolute: path,
                kind: FileKind::Runtime,
            });
        }
        self.flow_files(dir, &dir.join("flows"), &mut pending);

        // Includes come after the convention, so a file reached both ways
        // keeps its conventional kind.
        let mut seen: HashMap<PathBuf, ()> = HashMap::new();
        seen.insert(canonical(&ferry_path), ());
        for file in &pending {
            seen.insert(canonical(&file.absolute), ());
        }
        if let Some(ferry) = &ferry_file {
            for (i, pattern) in ferry.include.iter().enumerate() {
                for path in self.resolve_include(dir, pattern, i) {
                    if seen.insert(canonical(&path), ()).is_none() {
                        pending.push(Pending {
                            display: display_path(dir, &path),
                            absolute: path,
                            kind: FileKind::Included,
                        });
                    }
                }
            }
        }

        // Byte-wise on the relative path: never readdir order or locale
        // collation (0003 §2).
        pending.sort_by(|a, b| a.display.as_bytes().cmp(b.display.as_bytes()));

        let mut connections: Vec<Connection> = Vec::new();
        let mut runtime = Runtime::default();
        let mut flows: Vec<Flow> = Vec::new();
        let mut has_flow_files = false;
        for file in pending {
            match file.kind {
                FileKind::Connections => {
                    if let Some(Some(f)) = self.read(file, parse_as::<ConnectionsFile>) {
                        connections.extend(f.connections);
                    }
                }
                FileKind::Runtime => {
                    if let Some(Some(f)) = self.read(file, parse_as::<RuntimeFile>) {
                        runtime = f.runtime;
                    }
                }
                FileKind::Flows => {
                    has_flow_files = true;
                    if let Some(Some(f)) = self.read(file, parse_as::<FlowsFile>) {
                        flows.extend(f.flows);
                    }
                }
                FileKind::Included => {
                    if let Some(Some(f)) = self.read(file, parse_as::<IncludedFile>) {
                        has_flow_files |= f.flows.is_some();
                        connections.extend(f.connections.unwrap_or_default());
                        flows.extend(f.flows.unwrap_or_default());
                    }
                }
                FileKind::Ferry => unreachable!("ferry.yaml is read first"),
            }
        }

        let Some(ferry) = ferry_file else {
            return Err(ConfigErrors(self.errors));
        };
        if self.errors.is_empty() {
            if !has_flow_files || flows.is_empty() {
                self.errors.push(
                    ConfigError::new(ErrorKind::Layout, "the config has no flows")
                        .help("add a flow file under flows/ (0003 §2)"),
                );
            } else if connections_path.is_none() {
                self.errors.push(
                    ConfigError::new(ErrorKind::Layout, "connections.yaml is missing")
                        .help("a config with flows needs connections.yaml (0003 §2)"),
                );
            }
        }
        let config = Config {
            version: ferry.version,
            name: ferry.name,
            connections,
            runtime,
            flows,
            sources: self.sources,
        };
        // Checks across files only make sense on a complete model: with a
        // file missing they would report names it defines as undefined.
        if self.errors.is_empty() {
            self.errors.extend(validate::check(&config));
        }
        if self.errors.is_empty() {
            Ok(config)
        } else {
            Err(ConfigErrors(self.errors))
        }
    }

    /// `<dir>/<stem>.yaml` or `.yml`. Both at once is an error.
    fn well_known(&mut self, dir: &Path, stem: &str) -> Option<PathBuf> {
        let yaml = dir.join(format!("{stem}.yaml"));
        let yml = dir.join(format!("{stem}.yml"));
        match (yaml.is_file(), yml.is_file()) {
            (true, true) => {
                self.errors.push(ConfigError::new(
                    ErrorKind::Layout,
                    format!("both {stem}.yaml and {stem}.yml exist: keep one"),
                ));
                None
            }
            (true, false) => Some(yaml),
            (false, true) => Some(yml),
            (false, false) => None,
        }
    }

    /// Every `*.yaml` / `*.yml` under `flows/`, at any depth, skipping
    /// anything whose name starts with `.`.
    fn flow_files(&mut self, root: &Path, dir: &Path, out: &mut Vec<Pending>) {
        let entries = match std::fs::read_dir(dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound && dir == root.join("flows") => {
                return;
            }
            Err(e) => {
                self.errors.push(ConfigError::new(
                    ErrorKind::Io,
                    format!("can't read `{}`: {e}", display_path(root, dir)),
                ));
                return;
            }
        };
        for entry in entries {
            let path = match entry {
                Ok(entry) => entry.path(),
                Err(e) => {
                    self.errors.push(ConfigError::new(
                        ErrorKind::Io,
                        format!("can't read `{}`: {e}", display_path(root, dir)),
                    ));
                    continue;
                }
            };
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                self.errors.push(ConfigError::new(
                    ErrorKind::Layout,
                    format!("`{}` is not a UTF-8 file name", path.display()),
                ));
                continue;
            };
            if name.starts_with('.') {
                continue;
            }
            if path.is_dir() {
                self.flow_files(root, &path, out);
            } else if is_yaml(&path) {
                out.push(Pending {
                    display: display_path(root, &path),
                    absolute: path,
                    kind: FileKind::Flows,
                });
            }
        }
    }

    /// The files one `include:` entry names (0003 §8).
    fn resolve_include(
        &mut self,
        dir: &Path,
        pattern: &Spanned<String>,
        index: usize,
    ) -> Vec<PathBuf> {
        let path = FieldPath::root().key("include").item(index, None);
        let error = |message: String| {
            ConfigError::new(ErrorKind::Include, message)
                .at(pattern.location())
                .path(&path)
        };
        if !pattern.contains(['*', '?', '[']) {
            let file = dir.join(pattern.as_str());
            return if !file.is_file() {
                self.errors.push(error(format!(
                    "included file `{}` does not exist",
                    pattern.as_str()
                )));
                Vec::new()
            } else if !is_yaml(&file) {
                self.errors.push(error(format!(
                    "included file `{}` is not a .yaml or .yml file",
                    pattern.as_str()
                )));
                Vec::new()
            } else {
                vec![file]
            };
        }
        let full = format!(
            "{}/{}",
            glob::Pattern::escape(&dir.to_string_lossy()),
            pattern.as_str()
        );
        let options = glob::MatchOptions {
            case_sensitive: true,
            require_literal_separator: true,
            require_literal_leading_dot: true,
        };
        let paths = match glob::glob_with(&full, options) {
            Ok(paths) => paths,
            Err(e) => {
                self.errors.push(error(format!(
                    "`{}` is not a valid glob: {}",
                    pattern.as_str(),
                    e.msg
                )));
                return Vec::new();
            }
        };
        let mut files: Vec<PathBuf> = paths
            .filter_map(Result::ok)
            .filter(|p| p.is_file() && is_yaml(p))
            .collect();
        files.sort();
        if files.is_empty() {
            self.errors.push(error(format!(
                "`{}` matches no .yaml or .yml file",
                pattern.as_str()
            )));
        }
        files
    }

    /// Reads, parses and deserializes one file. `None` when the file
    /// couldn't be read or parsed, `Some(None)` when it didn't fit its model.
    fn read<T>(
        &mut self,
        file: Pending,
        parse: impl FnOnce(&Node, FileKind) -> Result<T, ConfigError>,
    ) -> Option<Option<T>> {
        let text = match std::fs::read_to_string(&file.absolute) {
            Ok(text) => text,
            Err(e) => {
                self.errors.push(ConfigError::new(
                    ErrorKind::Io,
                    format!("can't read `{}`: {e}", file.display),
                ));
                return None;
            }
        };
        let result = parse_file(&file.display, &text, file.kind, parse, true);
        self.sources.push(SourceFile {
            path: file.display,
            absolute: file.absolute,
            kind: file.kind,
            text,
        });
        match result {
            Ok(value) => Some(Some(value)),
            Err(errors) => {
                self.errors.extend(errors);
                Some(None)
            }
        }
    }
}

/// Checks one file on its own against the model for `kind`: YAML, keys and
/// value types, but nothing that needs other files (names defined
/// elsewhere, includes, `depends_on`). This is exactly what the file's JSON
/// Schema checks too. `path` is what errors show.
pub fn check_file(kind: FileKind, path: &str, text: &str) -> Result<(), ConfigErrors> {
    let display: Arc<str> = path.into();
    let result = match kind {
        FileKind::Ferry => parse_file(&display, text, kind, parse_as::<FerryFile>, false).map(drop),
        FileKind::Connections => {
            parse_file(&display, text, kind, parse_as::<ConnectionsFile>, false).map(drop)
        }
        FileKind::Runtime => {
            parse_file(&display, text, kind, parse_as::<RuntimeFile>, false).map(drop)
        }
        FileKind::Flows => parse_file(&display, text, kind, parse_as::<FlowsFile>, false).map(drop),
        FileKind::Included => {
            parse_file(&display, text, kind, parse_as::<IncludedFile>, false).map(drop)
        }
    };
    result.map_err(ConfigErrors)
}

/// A file as JSON, the form its JSON Schema validates. An empty file is
/// `null`.
pub fn file_to_json(path: &str, text: &str) -> Result<serde_json::Value, ConfigError> {
    let root = yaml::parse(&path.into(), text)?;
    Ok(root.map_or(serde_json::Value::Null, |node| node.to_json()))
}

/// Parses `text` as a file of `kind`, on its own: YAML, top-level keys and
/// the serde model, but nothing that needs other files.
///
/// With `env_check`, `${` anywhere in a flow is also an error (0003 §9).
/// That rule is the loader's: JSON Schema can't express it, so single-file
/// checks leave it out to stay in step with the schema.
pub(crate) fn parse_file<T>(
    display: &Arc<str>,
    text: &str,
    kind: FileKind,
    parse: impl FnOnce(&Node, FileKind) -> Result<T, ConfigError>,
    env_check: bool,
) -> Result<T, Vec<ConfigError>> {
    let root = yaml::parse(display, text).map_err(|e| vec![e])?;
    let Some(root) = root else {
        let mut error =
            ConfigError::new(ErrorKind::Layout, format!("{} is empty", kind.describe()))
                .at(&Location::new(display.clone(), 1, 1));
        error = match kind {
            FileKind::Flows => error
                .help("a flow file must contain `flows:`; write `flows: []` to keep a placeholder"),
            _ => error.help(format!("it must contain {}", expected_keys(kind))),
        };
        return Err(vec![error]);
    };
    let mut errors = Vec::new();
    if env_check && matches!(kind, FileKind::Flows | FileKind::Included) {
        if let Some(flows) = root.get("flows") {
            errors.extend(env_in_flows(flows, &FieldPath::root().key("flows")));
        }
    }
    match parse(&root, kind) {
        Ok(value) if errors.is_empty() => Ok(value),
        Ok(_) => Err(errors),
        Err(error) => {
            errors.insert(0, error);
            Err(errors)
        }
    }
}

/// `${` anywhere in a flow, value or key, is an error whatever the
/// environment holds (0003 §9).
fn env_in_flows(node: &Node, path: &FieldPath) -> Vec<ConfigError> {
    let mut errors = Vec::new();
    node.walk(path, &mut |node, path| {
        if node.as_str().is_some_and(|s| s.contains("${")) {
            errors.push(
                ConfigError::new(
                    ErrorKind::Env,
                    "`${` in a flow: environment variables are substituted only in connections and runtime.state",
                )
                .at(&node.location)
                .path(path)
                .help("write the value into the flow, so it is part of the config hash (0003 §9)"),
            );
        }
    });
    errors
}

fn expected_keys(kind: FileKind) -> String {
    kind.allowed()
        .iter()
        .map(|k| format!("`{k}:`"))
        .collect::<Vec<_>>()
        .join(" or ")
}

/// Deserializes a file's root as `T`, turning serde errors into config
/// errors with hints.
pub(crate) fn parse_as<T: serde::de::DeserializeOwned>(
    root: &Node,
    kind: FileKind,
) -> Result<T, ConfigError> {
    if !matches!(root.kind, NodeKind::Map(_)) {
        return Err(ConfigError::new(
            ErrorKind::Model,
            format!(
                "{} must be a map with {}, found {}",
                kind.describe(),
                expected_keys(kind),
                root.describe()
            ),
        )
        .at(&root.location));
    }
    from_node(root, FieldPath::root()).map_err(|e| model_error(e, kind))
}

fn model_error(error: DeError, kind: FileKind) -> ConfigError {
    let path = error.path.unwrap_or_default();
    let help = error
        .unknown_field
        .as_ref()
        .and_then(|(field, expected)| unknown_key_help(kind, &path, field, expected));
    ConfigError::new(ErrorKind::Model, error.message)
        .maybe_help(help)
        .path(&path)
        .maybe_at(error.location.as_ref())
}

/// Explains an unknown key: where it belongs if it's a known key in the
/// wrong place, otherwise the nearest valid key.
fn unknown_key_help(
    kind: FileKind,
    path: &FieldPath,
    field: &str,
    expected: &[&str],
) -> Option<String> {
    let parent = path.shape();
    let parent = parent
        .strip_suffix(field)
        .unwrap_or(&parent)
        .trim_end_matches('.');
    let top = parent.is_empty();
    match (top, field) {
        (_, "include") if !(top && kind == FileKind::Ferry) => {
            return Some("`include:` is only allowed in ferry.yaml (0003 §8)".into());
        }
        (true, "flows") => {
            return Some(
                "flows go in files under flows/, or in a file listed in `include:` (0003 §2)"
                    .into(),
            );
        }
        (true, "connections") => {
            return Some(
                "connections go in connections.yaml, or in a file listed in `include:` (0003 §2)"
                    .into(),
            );
        }
        (true, "runtime") => {
            return Some("runtime settings go in runtime.yaml, and only there (0003 §8)".into());
        }
        (true, "version" | "name") => {
            return Some(format!("`{field}:` goes in ferry.yaml"));
        }
        _ => {}
    }
    if parent == "flows[].runtime" {
        let reason = match field {
            "state" => Some("there is one state store per run"),
            "workers" | "memory_budget" => Some("it is shared by every flow in the process"),
            "source_max_connections" | "source_rows_per_sec" => {
                Some("it limits a pool shared across flows")
            }
            _ => None,
        };
        if let Some(reason) = reason {
            return Some(format!(
                "`{field}` can't be set per flow: {reason}. Set it in runtime.yaml (0003 §6)"
            ));
        }
    }
    suggest(field, expected.iter().copied()).map(|s| format!("did you mean `{s}`?"))
}

fn is_yaml(path: &Path) -> bool {
    matches!(
        path.extension().and_then(|e| e.to_str()),
        Some("yaml" | "yml")
    )
}

fn file_name(path: &Path) -> Arc<str> {
    path.file_name()
        .map(|n| n.to_string_lossy())
        .unwrap_or_default()
        .into()
}

fn absolute(path: &Path) -> PathBuf {
    std::path::absolute(path).unwrap_or_else(|_| path.to_path_buf())
}

fn canonical(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| absolute(path))
}

/// `path` relative to `dir`, `/`-separated; through `..` when it lies
/// outside.
fn display_path(dir: &Path, path: &Path) -> Arc<str> {
    let parts = |p: &Path| -> Vec<String> {
        p.components()
            .filter(|c| !matches!(c, Component::CurDir))
            .map(|c| c.as_os_str().to_string_lossy().into_owned())
            .collect()
    };
    let relative = match path.strip_prefix(dir) {
        Ok(rest) => parts(rest),
        Err(_) => {
            let base = parts(&canonical(dir));
            let target = parts(&canonical(path));
            let common = base.iter().zip(&target).take_while(|(a, b)| a == b).count();
            std::iter::repeat_n("..".to_owned(), base.len() - common)
                .chain(target[common..].iter().cloned())
                .collect()
        }
    };
    relative.join("/").into()
}

#[cfg(test)]
mod tests {
    use serde::Serialize;
    use serde::de::DeserializeOwned;

    use super::*;

    fn example(path: &str) -> String {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples");
        std::fs::read_to_string(root.join(path)).unwrap()
    }

    /// Parse, serialize to JSON, read the JSON back through the same
    /// deserializer: the model must come back unchanged, defaults included.
    fn round_trip<T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug>(
        path: &str,
        kind: FileKind,
    ) {
        let text = example(path);
        let display: Arc<str> = path.into();
        let parsed: T = parse_file(&display, &text, kind, parse_as::<T>, true).unwrap();
        let json = serde_json::to_value(&parsed).unwrap();
        let node = Node::from_json(&json, &Location::new("json", 1, 1));
        let again: T = parse_as(&node, kind).unwrap_or_else(|e| panic!("{path}: {e}\n{json:#}"));
        assert_eq!(parsed, again, "{path}");
        // And serializing again gives the same JSON.
        assert_eq!(serde_json::to_value(&again).unwrap(), json, "{path}");
    }

    #[test]
    fn example_models_survive_serialize_and_parse() {
        for dir in ["golden/ferry", "pg-merge/ferry", "edge/layout/ferry"] {
            round_trip::<FerryFile>(&format!("{dir}/ferry.yaml"), FileKind::Ferry);
            round_trip::<ConnectionsFile>(
                &format!("{dir}/connections.yaml"),
                FileKind::Connections,
            );
            round_trip::<RuntimeFile>(&format!("{dir}/runtime.yaml"), FileKind::Runtime);
        }
        round_trip::<FlowsFile>("golden/ferry/flows/orders.yaml", FileKind::Flows);
        round_trip::<FlowsFile>("pg-merge/ferry/flows/order_summaries.yaml", FileKind::Flows);
        round_trip::<IncludedFile>("pg-merge/ferry/legacy/customers.yaml", FileKind::Included);
    }
}
