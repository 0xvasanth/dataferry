//! `depends_on:` between flows (0003 §7): checked at load, so `dataferry
//! validate` catches it without connecting, and handed to the planner (B7)
//! as a graph.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use super::Config;
use super::error::{ConfigError, ErrorKind, suggest};
use super::model::Flow;
use super::span::FieldPath;

/// Which flows each flow waits for. Built from names only, so it doesn't
/// depend on which file a flow is in.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DependencyGraph {
    edges: BTreeMap<String, BTreeSet<String>>,
}

impl DependencyGraph {
    /// Every flow, by name.
    pub fn flows(&self) -> impl Iterator<Item = &str> {
        self.edges.keys().map(String::as_str)
    }

    /// The flows `flow` waits for.
    pub fn dependencies(&self, flow: &str) -> impl Iterator<Item = &str> {
        self.edges
            .get(flow)
            .into_iter()
            .flatten()
            .map(String::as_str)
    }

    /// Every `(flow, dependency)` edge, sorted.
    pub fn edges(&self) -> impl Iterator<Item = (&str, &str)> {
        self.edges
            .iter()
            .flat_map(|(flow, deps)| deps.iter().map(move |d| (flow.as_str(), d.as_str())))
    }
}

impl Config {
    /// The `depends_on:` graph. The loader has already rejected unknown
    /// names, self-references and cycles.
    pub fn dependency_graph(&self) -> DependencyGraph {
        DependencyGraph {
            edges: self
                .flows
                .iter()
                .map(|f| {
                    let deps = f.depends_on.iter().map(|d| d.as_str().to_owned()).collect();
                    (f.name.as_str().to_owned(), deps)
                })
                .collect(),
        }
    }
}

fn entry_path(flow: &Flow, index: usize) -> FieldPath {
    FieldPath::root()
        .key("flows")
        .item(0, Some(flow.name.as_str()))
        .key("depends_on")
        .item(index, None)
}

pub(crate) fn check(config: &Config) -> Vec<ConfigError> {
    let mut errors = Vec::new();
    let names: Vec<&str> = config.flows.iter().map(|f| f.name.as_str()).collect();
    for flow in &config.flows {
        for (i, dep) in flow.depends_on.iter().enumerate() {
            let error = |message: String| {
                ConfigError::new(ErrorKind::DependsOn, message)
                    .at(dep.location())
                    .path(&entry_path(flow, i))
            };
            if dep.as_str() == flow.name.as_str() {
                errors.push(error(format!(
                    "flow `{}` depends on itself",
                    flow.name.as_str()
                )));
            } else if !names.contains(&dep.as_str()) {
                let help = suggest(dep.as_str(), names.iter().copied())
                    .map(|s| format!("did you mean `{s}`?"))
                    .unwrap_or_else(|| format!("defined flows: {}", names.join(", ")));
                errors.push(error(format!("no flow named `{}`", dep.as_str())).help(help));
            } else if flow.depends_on[..i]
                .iter()
                .any(|d| d.as_str() == dep.as_str())
            {
                errors.push(error(format!("`{}` is listed twice", dep.as_str())));
            }
        }
    }
    if errors.is_empty() {
        errors.extend(cycles(config));
    }
    errors
}

/// One error per cycle: each strongly connected component of more than one
/// flow, reported as the shortest cycle through its smallest flow name.
fn cycles(config: &Config) -> Vec<ConfigError> {
    let flows: BTreeMap<&str, &Flow> = config.flows.iter().map(|f| (f.name.as_str(), f)).collect();
    let edges = |name: &str| -> Vec<&str> {
        let mut deps: Vec<&str> = flows[name].depends_on.iter().map(|d| d.as_str()).collect();
        deps.sort();
        deps
    };
    let mut errors = Vec::new();
    for component in components(&flows.keys().copied().collect::<Vec<_>>(), &edges) {
        if component.len() < 2 {
            continue;
        }
        let start = component[0];
        let cycle = shortest_cycle(start, &component, &edges);
        let first = flows[start];
        let next = cycle[1];
        let index = first
            .depends_on
            .iter()
            .position(|d| d.as_str() == next)
            .expect("the cycle follows a depends_on edge");
        let described: Vec<String> = cycle
            .iter()
            .take(cycle.len() - 1)
            .map(|name| format!("{name} ({})", flows[name].name.location()))
            .chain([start.to_owned()])
            .collect();
        errors.push(
            ConfigError::new(
                ErrorKind::DependsOn,
                format!("`depends_on` forms a cycle: {}", described.join(" → ")),
            )
            .at(first.depends_on[index].location())
            .path(&entry_path(first, index))
            .help("remove one of these edges; flows in a cycle could never start (0003 §7)"),
        );
    }
    errors
}

/// Strongly connected components (Tarjan), each sorted, in order of their
/// smallest member.
fn components<'a>(nodes: &[&'a str], edges: &dyn Fn(&str) -> Vec<&'a str>) -> Vec<Vec<&'a str>> {
    struct State<'a> {
        index: BTreeMap<&'a str, usize>,
        low: BTreeMap<&'a str, usize>,
        stack: Vec<&'a str>,
        on_stack: BTreeSet<&'a str>,
        out: Vec<Vec<&'a str>>,
    }
    fn visit<'a>(node: &'a str, edges: &dyn Fn(&str) -> Vec<&'a str>, s: &mut State<'a>) {
        let i = s.index.len();
        s.index.insert(node, i);
        s.low.insert(node, i);
        s.stack.push(node);
        s.on_stack.insert(node);
        for next in edges(node) {
            if !s.index.contains_key(next) {
                visit(next, edges, s);
                let low = s.low[node].min(s.low[next]);
                s.low.insert(node, low);
            } else if s.on_stack.contains(next) {
                let low = s.low[node].min(s.index[next]);
                s.low.insert(node, low);
            }
        }
        if s.low[node] == s.index[node] {
            let mut component = Vec::new();
            while let Some(top) = s.stack.pop() {
                s.on_stack.remove(top);
                component.push(top);
                if top == node {
                    break;
                }
            }
            component.sort();
            s.out.push(component);
        }
    }
    let mut state = State {
        index: BTreeMap::new(),
        low: BTreeMap::new(),
        stack: Vec::new(),
        on_stack: BTreeSet::new(),
        out: Vec::new(),
    };
    for node in nodes {
        if !state.index.contains_key(node) {
            visit(node, edges, &mut state);
        }
    }
    state.out.sort();
    state.out
}

/// The shortest path from `start` back to itself inside `component`, as
/// `[start, ..., start]`. Ties go to the smaller name.
fn shortest_cycle<'a>(
    start: &'a str,
    component: &[&'a str],
    edges: &dyn Fn(&str) -> Vec<&'a str>,
) -> Vec<&'a str> {
    let mut previous: BTreeMap<&str, &str> = BTreeMap::new();
    let mut queue = VecDeque::from([start]);
    while let Some(node) = queue.pop_front() {
        for next in edges(node) {
            if !component.contains(&next) {
                continue;
            }
            if next == start {
                let mut path = vec![node];
                let mut current = node;
                while current != start {
                    current = previous[current];
                    path.push(current);
                }
                path.reverse();
                path.push(start);
                return path;
            }
            if !previous.contains_key(next) && next != start {
                previous.insert(next, node);
                queue.push_back(next);
            }
        }
    }
    unreachable!("a strongly connected component of two or more has a cycle through each member")
}
