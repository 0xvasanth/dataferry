//! Checks across the merged model, after every file is loaded: names
//! defined twice, references to connections, and the flow rules of 0003 §5
//! that need more than one value to check.

use std::collections::{BTreeMap, HashSet};

use super::Config;
use super::error::{ConfigError, ErrorKind, suggest};
use super::model::{Cardinality, Columns, FILE_BACKEND, Flow, From, Mode};
use super::span::{FieldPath, Location, Spanned};

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
    let connections: Vec<&str> = config.connections.iter().map(|c| c.name.as_str()).collect();
    for flow in &config.flows {
        check_flow(flow, &connections, &mut errors);
    }
    one_writer_per_table(config, &mut errors);
    check_state(config, &connections, &mut errors);
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

fn flow_path(flow: &Flow) -> FieldPath {
    FieldPath::root()
        .key("flows")
        .item(0, Some(flow.name.as_str()))
}

fn invalid<T>(at: &Spanned<T>, path: &FieldPath, message: String) -> ConfigError {
    ConfigError::new(ErrorKind::Invalid, message)
        .at(at.location())
        .path(path)
}

/// `name` must be a connection.
fn connection_ref(
    name: &str,
    at: &Location,
    path: &FieldPath,
    connections: &[&str],
    errors: &mut Vec<ConfigError>,
) {
    if !connections.contains(&name) {
        let help = suggest(name, connections.iter().copied())
            .map(|s| format!("did you mean `{s}`?"))
            .unwrap_or_else(|| format!("defined connections: {}", connections.join(", ")));
        errors.push(
            ConfigError::new(ErrorKind::Invalid, format!("no connection named `{name}`"))
                .at(at)
                .path(path)
                .help(help),
        );
    }
}

fn check_flow(flow: &Flow, connections: &[&str], errors: &mut Vec<ConfigError>) {
    let path = flow_path(flow);
    connection_ref(
        flow.sink.as_str(),
        flow.sink.location(),
        &path.key("sink"),
        connections,
        errors,
    );
    let single_entity = match flow.from.value() {
        From::Entity(entity) => {
            connection_ref(
                entity.connection.as_str(),
                flow.from.location(),
                &path.key("from"),
                connections,
                errors,
            );
            true
        }
        From::Merge(entries) => {
            check_merge(flow, entries, &path.key("from"), connections, errors);
            false
        }
    };

    if flow.to.is_empty() {
        errors.push(invalid(
            &flow.name,
            &path.key("to"),
            "a flow needs at least one target under `to:`".into(),
        ));
    }
    let targets: Vec<&str> = flow.to.keys().map(|k| k.as_str()).collect();
    for (table, target) in flow.to.iter() {
        let target_path = path.key("to").key(table.as_str());
        if table.contains('.') {
            errors.push(invalid(
                table,
                &target_path,
                format!(
                    "target `{}` contains a dot: targets are plain table names, and the schema comes from the sink connection (0003 §3)",
                    table.as_str()
                ),
            ));
        }
        if let Some(parent) = &target.parent {
            if parent.as_str() == table.as_str() {
                errors.push(invalid(
                    parent,
                    &target_path.key("parent"),
                    format!("target `{}` is its own parent", table.as_str()),
                ));
            } else if !targets.contains(&parent.as_str()) {
                errors.push(
                    invalid(
                        parent,
                        &target_path.key("parent"),
                        format!(
                            "`parent: {}` is not a target of flow `{}`",
                            parent.as_str(),
                            flow.name.as_str()
                        ),
                    )
                    .maybe_help(
                        suggest(parent, targets.iter().copied())
                            .map(|s| format!("did you mean `{s}`?")),
                    ),
                );
            }
        }
        if matches!(target.columns.value(), Columns::All)
            && (!single_entity || target.parent.is_some() || target.unnest.is_some())
        {
            errors.push(invalid(
                &target.columns,
                &target_path.key("columns"),
                "`columns: \"*\"` copies one entity unchanged: it can't be used with `parent:`, `unnest:` or a merge (0003 §5.4)".into(),
            ));
        }
        if flow.mode == Mode::Upsert && target.key.is_none() {
            errors.push(
                invalid(
                    table,
                    &target_path.key("key"),
                    format!(
                        "flow `{}` is `mode: upsert`, so target `{}` needs `key:`",
                        flow.name.as_str(),
                        table.as_str()
                    ),
                )
                .help("every target of an upsert flow names its key columns (0003 §5.5)"),
            );
        }
    }
    parent_cycles(flow, &path, errors);
}

/// The first entry is the driver and has no `join:`; every other entry
/// joins on keys of an earlier entry that isn't a `many` (0003 §5.2).
fn check_merge(
    flow: &Flow,
    entries: &[super::model::FromEntry],
    path: &FieldPath,
    connections: &[&str],
    errors: &mut Vec<ConfigError>,
) {
    if entries.is_empty() {
        errors.push(invalid(
            &flow.from,
            path,
            "a merge `from:` needs at least the driver entry".into(),
        ));
        return;
    }
    let mut aliases: Vec<(String, Option<Cardinality>)> = Vec::new();
    for (i, entry) in entries.iter().enumerate() {
        let entry_path = path.item(i, None);
        connection_ref(
            entry.entity.connection.as_str(),
            entry.entity.location(),
            &entry_path.key("entity"),
            connections,
            errors,
        );
        let alias = entry
            .alias
            .as_ref()
            .map(|a| a.as_str().to_owned())
            .unwrap_or_else(|| entry.entity.default_alias().to_owned());
        if aliases.iter().any(|(a, _)| *a == alias) {
            let at = entry
                .alias
                .as_ref()
                .map(|a| a.location())
                .unwrap_or(entry.entity.location());
            errors.push(
                ConfigError::new(
                    ErrorKind::Invalid,
                    format!("alias `{alias}` is used twice in this merge"),
                )
                .at(at)
                .path(&entry_path.key("as"))
                .help("give one of the entries a different `as:`"),
            );
        }
        match (i, &entry.join) {
            (0, Some(join)) => errors.push(invalid(
                &join.cardinality,
                &entry_path.key("join"),
                "the first `from:` entry is the driver and has no `join:` (0003 §5.2)".into(),
            )),
            (0, None) => {}
            (_, None) => errors.push(invalid(
                &entry.entity,
                &entry_path.key("join"),
                "every `from:` entry after the driver needs `join:` with `cardinality` and `keys`"
                    .into(),
            )),
            (_, Some(join)) => {
                if join.keys.is_empty() {
                    errors.push(invalid(
                        &join.cardinality,
                        &entry_path.key("join").key("keys"),
                        "a join needs at least one key pair".into(),
                    ));
                }
                for (column, target) in join.keys.iter() {
                    let key_path = entry_path.key("join").key("keys").key(column.as_str());
                    let referenced = target.split_once('.').map(|(a, _)| a);
                    match referenced.and_then(|a| aliases.iter().find(|(b, _)| b == a)) {
                        None => errors.push(
                            invalid(
                                target,
                                &key_path,
                                format!(
                                    "`{}` must be `<alias>.<column>` of an earlier `from:` entry",
                                    target.as_str()
                                ),
                            )
                            .help(format!(
                                "earlier aliases: {}",
                                aliases
                                    .iter()
                                    .map(|(a, _)| a.as_str())
                                    .collect::<Vec<_>>()
                                    .join(", ")
                            )),
                        ),
                        Some((a, Some(Cardinality::Many))) => errors.push(invalid(
                            target,
                            &key_path,
                            format!("a join can't reference `{a}`, a `many` entry (0003 §5.2)"),
                        )),
                        Some(_) => {}
                    }
                }
            }
        }
        aliases.push((alias, entry.join.as_ref().map(|j| *j.cardinality.value())));
    }
}

/// `parent:` links must form a tree. Each cycle is reported once, at the
/// `parent:` of its first target in flow order.
fn parent_cycles(flow: &Flow, path: &FieldPath, errors: &mut Vec<ConfigError>) {
    let parents: BTreeMap<&str, &Spanned<String>> = flow
        .to
        .iter()
        .filter_map(|(t, target)| Some((t.as_str(), target.parent.as_ref()?)))
        .collect();
    let mut reported: HashSet<Vec<&str>> = HashSet::new();
    for table in flow.to.keys() {
        let start = table.as_str();
        let mut chain = vec![start];
        let mut current = start;
        while let Some(parent) = parents.get(current).map(|p| p.as_str()) {
            if parent == current || chain[1..].contains(&parent) {
                break; // its own parent (reported above), or a cycle not through `start`
            }
            if parent == start {
                let mut members = chain.clone();
                members.sort();
                if reported.insert(members) {
                    chain.push(start);
                    errors.push(invalid(
                        parents[start],
                        &path.key("to").key(start).key("parent"),
                        format!("`parent:` links form a cycle: {}", chain.join(" → ")),
                    ));
                }
                break;
            }
            chain.push(parent);
            current = parent;
        }
    }
}

/// A `(sink, table)` pair may be written by only one flow (0003 §3).
fn one_writer_per_table(config: &Config, errors: &mut Vec<ConfigError>) {
    let mut writers: BTreeMap<(&str, &str), &Flow> = BTreeMap::new();
    for flow in &config.flows {
        for table in flow.to.keys() {
            let key = (flow.sink.as_str(), table.as_str());
            match writers.get(&key) {
                Some(first) if first.name.as_str() != flow.name.as_str() => errors.push(invalid(
                    table,
                    &flow_path(flow).key("to").key(table.as_str()),
                    format!(
                        "flow `{}` also writes `{}` on `{}` ({}): a table has one writer",
                        first.name.as_str(),
                        table.as_str(),
                        flow.sink.as_str(),
                        first.name.location()
                    ),
                )),
                Some(_) => {}
                None => {
                    writers.insert(key, flow);
                }
            }
        }
    }
}

/// `runtime.state` (0003 §4). Whether the backend is a state-capable type
/// and the connection is of that type is the registry's check (0004 §8).
fn check_state(config: &Config, connections: &[&str], errors: &mut Vec<ConfigError>) {
    let Some(state) = &config.runtime.state else {
        return;
    };
    let path = FieldPath::root().key("runtime").key("state");
    if state.backend.as_str() == FILE_BACKEND {
        if let Some(connection) = &state.connection {
            errors.push(invalid(
                connection,
                &path.key("connection"),
                "`backend: file` keeps state in a directory and takes `path:`, not `connection:`"
                    .into(),
            ));
        }
    } else {
        if let Some(dir) = &state.path {
            errors.push(invalid(
                dir,
                &path.key("path"),
                format!(
                    "`backend: {}` keeps state in a connection: `path:` only applies to `backend: file`",
                    state.backend.as_str()
                ),
            ));
        }
        match &state.connection {
            Some(connection) => connection_ref(
                connection.as_str(),
                connection.location(),
                &path.key("connection"),
                connections,
                errors,
            ),
            None => errors.push(invalid(
                &state.backend,
                &path.key("connection"),
                format!(
                    "`backend: {}` needs `connection:` naming the connection that holds the state",
                    state.backend.as_str()
                ),
            )),
        }
    }
}
