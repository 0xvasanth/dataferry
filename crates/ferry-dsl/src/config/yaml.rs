//! Reads one YAML file into a tree whose every node knows its location.
//!
//! Built on `saphyr-parser`, the maintained YAML 1.2 successor of
//! `yaml-rust` (`serde_yaml` is archived). We keep our own tree instead of a
//! serde deserializer over the event stream so that duplicate keys, several
//! documents, tags and anchors are rejected up front with exact positions,
//! and so later stages can map any value back to its file and line.

use std::sync::Arc;

use saphyr_parser::{Event, Parser, ScalarStyle, Span};

use super::error::{ConfigError, ErrorKind};
use super::span::{FieldPath, Location};

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Node {
    pub kind: NodeKind,
    pub location: Location,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum NodeKind {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    Seq(Vec<Node>),
    /// Keys are always `Str`, in the order written.
    Map(Vec<(Node, Node)>),
}

impl Node {
    pub fn as_str(&self) -> Option<&str> {
        match &self.kind {
            NodeKind::Str(s) => Some(s),
            _ => None,
        }
    }

    pub fn get(&self, key: &str) -> Option<&Node> {
        match &self.kind {
            NodeKind::Map(entries) => entries
                .iter()
                .find(|(k, _)| k.as_str() == Some(key))
                .map(|(_, v)| v),
            _ => None,
        }
    }

    /// The `name:` of a list item, used to name it in field paths.
    pub fn item_name(&self) -> Option<&str> {
        self.get("name").and_then(Node::as_str)
    }

    /// A short description of what this node is, for "expected X, found Y".
    pub fn describe(&self) -> String {
        match &self.kind {
            NodeKind::Null => "nothing (null)".into(),
            NodeKind::Bool(b) => format!("the boolean `{b}`"),
            NodeKind::Int(i) => format!("the number `{i}`"),
            NodeKind::Float(x) => format!("the number `{x}`"),
            NodeKind::Str(_) => "a string".into(),
            NodeKind::Seq(_) => "a list".into(),
            NodeKind::Map(_) => "a map".into(),
        }
    }
}

enum Frame {
    Seq(Location, Vec<Node>),
    Map(Location, Vec<(Node, Node)>, Option<Node>),
}

/// Parses `text`, the contents of `file`. An empty file (or one holding only
/// comments) gives `None`.
pub(crate) fn parse(file: &Arc<str>, text: &str) -> Result<Option<Node>, ConfigError> {
    let location = |span: &Span| {
        Location::new(
            file.clone(),
            span.start.line() as u32,
            span.start.col() as u32 + 1,
        )
    };
    let yaml_error =
        |message: String, at: Location| ConfigError::new(ErrorKind::Yaml, message).at(&at);

    let mut stack: Vec<Frame> = Vec::new();
    let mut root: Option<Node> = None;
    let mut documents = 0;

    for event in Parser::new_from_str(text) {
        let (event, span) = event.map_err(|e| {
            let m = e.marker();
            yaml_error(
                format!("invalid YAML: {}", e.info()),
                Location::new(file.clone(), m.line() as u32, m.col() as u32 + 1),
            )
        })?;
        let at = location(&span);
        let node = match event {
            Event::StreamStart | Event::StreamEnd | Event::DocumentEnd | Event::Nothing => {
                continue;
            }
            Event::DocumentStart(_) => {
                documents += 1;
                if documents > 1 {
                    return Err(yaml_error(
                        "a second YAML document (`---`): each file holds exactly one".into(),
                        at,
                    ));
                }
                continue;
            }
            Event::Alias(_) => {
                return Err(yaml_error(
                    "YAML aliases (`*name`) are not supported: write the value out".into(),
                    at,
                ));
            }
            Event::Scalar(value, style, anchor, tag) => {
                if anchor != 0 {
                    return Err(yaml_error(
                        "YAML anchors (`&name`) are not supported".into(),
                        at,
                    ));
                }
                if let Some(tag) = tag {
                    return Err(yaml_error(
                        format!(
                            "YAML tags (`{}{}`) are not supported",
                            tag.handle, tag.suffix
                        ),
                        at,
                    ));
                }
                let is_key = matches!(stack.last(), Some(Frame::Map(_, _, None)));
                let kind = if is_key || style != ScalarStyle::Plain {
                    NodeKind::Str(value.into_owned())
                } else {
                    resolve_plain(&value)
                };
                Node { kind, location: at }
            }
            Event::SequenceStart(anchor, tag) => {
                check_collection_start(&stack, anchor, tag.is_some(), &at)?;
                stack.push(Frame::Seq(at, Vec::new()));
                continue;
            }
            Event::MappingStart(anchor, tag) => {
                check_collection_start(&stack, anchor, tag.is_some(), &at)?;
                stack.push(Frame::Map(at, Vec::new(), None));
                continue;
            }
            Event::SequenceEnd => match stack.pop() {
                Some(Frame::Seq(at, items)) => Node {
                    kind: NodeKind::Seq(items),
                    location: at,
                },
                _ => unreachable!("parser emitted an unbalanced sequence end"),
            },
            Event::MappingEnd => match stack.pop() {
                Some(Frame::Map(at, entries, _)) => Node {
                    kind: NodeKind::Map(entries),
                    location: at,
                },
                _ => unreachable!("parser emitted an unbalanced mapping end"),
            },
        };
        match stack.last_mut() {
            None => root = Some(node),
            Some(Frame::Seq(_, items)) => items.push(node),
            Some(Frame::Map(_, _, key @ None)) => *key = Some(node),
            Some(Frame::Map(_, entries, key)) => entries.push((key.take().unwrap(), node)),
        }
    }

    if let Some(root) = &root {
        check_duplicate_keys(root, &FieldPath::root())?;
    }
    Ok(root)
}

fn check_collection_start(
    stack: &[Frame],
    anchor: usize,
    tagged: bool,
    at: &Location,
) -> Result<(), ConfigError> {
    if anchor != 0 || tagged {
        return Err(
            ConfigError::new(ErrorKind::Yaml, "YAML anchors and tags are not supported").at(at),
        );
    }
    if matches!(stack.last(), Some(Frame::Map(_, _, None))) {
        return Err(ConfigError::new(
            ErrorKind::Yaml,
            "a list or map as a key is not supported: keys are plain names",
        )
        .at(at));
    }
    Ok(())
}

/// Resolves a plain (unquoted) scalar under the YAML 1.2 core schema, so
/// `on`, `yes` and `no` stay strings (0003 §2).
fn resolve_plain(text: &str) -> NodeKind {
    match text {
        "" | "~" | "null" | "Null" | "NULL" => return NodeKind::Null,
        "true" | "True" | "TRUE" => return NodeKind::Bool(true),
        "false" | "False" | "FALSE" => return NodeKind::Bool(false),
        ".inf" | ".Inf" | ".INF" | "+.inf" | "+.Inf" | "+.INF" => {
            return NodeKind::Float(f64::INFINITY);
        }
        "-.inf" | "-.Inf" | "-.INF" => return NodeKind::Float(f64::NEG_INFINITY),
        ".nan" | ".NaN" | ".NAN" => return NodeKind::Float(f64::NAN),
        _ => {}
    }
    let int = |digits: &str, radix: u32| i64::from_str_radix(digits, radix).ok();
    let parsed = if let Some(octal) = text.strip_prefix("0o") {
        int(octal, 8)
    } else if let Some(hex) = text.strip_prefix("0x") {
        int(hex, 16)
    } else if is_decimal_int(text) {
        text.parse().ok()
    } else {
        None
    };
    if let Some(i) = parsed {
        return NodeKind::Int(i);
    }
    if is_float(text) {
        if let Ok(x) = text.parse() {
            return NodeKind::Float(x);
        }
    }
    NodeKind::Str(text.to_owned())
}

// [-+]?[0-9]+
fn is_decimal_int(text: &str) -> bool {
    let digits = text.strip_prefix(['-', '+']).unwrap_or(text);
    !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit())
}

// [-+]?(\.[0-9]+|[0-9]+(\.[0-9]*)?)([eE][-+]?[0-9]+)?
fn is_float(text: &str) -> bool {
    let body = text.strip_prefix(['-', '+']).unwrap_or(text);
    let (mantissa, exponent) = match body.find(['e', 'E']) {
        Some(i) => (&body[..i], Some(&body[i + 1..])),
        None => (body, None),
    };
    let digits = |s: &str| s.bytes().all(|b| b.is_ascii_digit());
    let mantissa_ok = match mantissa.split_once('.') {
        Some((int, frac)) => (!int.is_empty() || !frac.is_empty()) && digits(int) && digits(frac),
        None => !mantissa.is_empty() && digits(mantissa),
    };
    let exponent_ok = exponent.is_none_or(is_decimal_int);
    mantissa_ok && exponent_ok
}

/// A key written twice in one mapping is an error at the second one, never
/// last-wins.
fn check_duplicate_keys(node: &Node, path: &FieldPath) -> Result<(), ConfigError> {
    match &node.kind {
        NodeKind::Seq(items) => {
            for (i, item) in items.iter().enumerate() {
                check_duplicate_keys(item, &path.item(i, item.item_name()))?;
            }
        }
        NodeKind::Map(entries) => {
            for (i, (key, value)) in entries.iter().enumerate() {
                let name = key.as_str().unwrap_or_default();
                let child = path.key(name);
                if let Some((first, _)) =
                    entries[..i].iter().find(|(k, _)| k.as_str() == Some(name))
                {
                    return Err(ConfigError::new(
                        ErrorKind::Yaml,
                        format!(
                            "duplicate key `{name}`, first written at line {}",
                            first.location.line
                        ),
                    )
                    .at(&key.location)
                    .path(&child));
                }
                check_duplicate_keys(value, &child)?;
            }
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read(text: &str) -> Result<Option<Node>, ConfigError> {
        parse(&Arc::from("t.yaml"), text)
    }

    fn kind(text: &str) -> NodeKind {
        read(&format!("v: {text}"))
            .unwrap()
            .unwrap()
            .get("v")
            .unwrap()
            .kind
            .clone()
    }

    #[test]
    fn plain_scalars_follow_the_yaml_1_2_core_schema() {
        assert_eq!(kind("on"), NodeKind::Str("on".into()));
        assert_eq!(kind("yes"), NodeKind::Str("yes".into()));
        assert_eq!(kind("no"), NodeKind::Str("no".into()));
        assert_eq!(kind("true"), NodeKind::Bool(true));
        assert_eq!(kind("~"), NodeKind::Null);
        assert_eq!(kind(""), NodeKind::Null);
        assert_eq!(kind("-12"), NodeKind::Int(-12));
        assert_eq!(kind("0x1f"), NodeKind::Int(31));
        assert_eq!(kind("0o17"), NodeKind::Int(15));
        assert_eq!(kind("1.5e3"), NodeKind::Float(1500.0));
        assert_eq!(kind("1_000"), NodeKind::Str("1_000".into()));
        assert_eq!(kind("012"), NodeKind::Int(12));
        assert_eq!(kind("'5'"), NodeKind::Str("5".into()));
        assert_eq!(kind("\"null\""), NodeKind::Str("null".into()));
        assert_eq!(kind("256MiB"), NodeKind::Str("256MiB".into()));
    }

    #[test]
    fn locations_are_one_based_lines_and_columns() {
        let root = read("# c\na:\n  b: [1, 2]\n").unwrap().unwrap();
        let a = root.get("a").unwrap();
        assert_eq!((a.location.line, a.location.column), (3, 3));
        let b = a.get("b").unwrap();
        assert_eq!((b.location.line, b.location.column), (3, 6));
        match &b.kind {
            NodeKind::Seq(items) => assert_eq!(items[1].location.column, 10),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn keys_stay_strings_whatever_they_look_like() {
        let root = read("1: a\ntrue: b\nnull: c\n").unwrap().unwrap();
        for key in ["1", "true", "null"] {
            assert!(root.get(key).is_some(), "{key}");
        }
    }

    #[test]
    fn duplicate_keys_point_at_the_second_occurrence() {
        let err = read("flows:\n  - name: a\n    mode: insert\n    mode: upsert\n").unwrap_err();
        assert_eq!(err.kind, ErrorKind::Yaml);
        assert_eq!(err.location.as_ref().unwrap().line, 4);
        assert_eq!(err.path, "flows[a].mode");
        assert!(err.message.contains("first written at line 3"), "{err}");
    }

    #[test]
    fn several_documents_anchors_aliases_and_tags_are_rejected() {
        for (text, line) in [
            ("a: 1\n---\nb: 2\n", 2),
            ("a: &x 1\nb: *x\n", 1),
            ("a: !!str 1\n", 1),
            ("a: !custom {b: 1}\n", 1),
        ] {
            let err = read(text).unwrap_err();
            assert_eq!(err.kind, ErrorKind::Yaml, "{text}");
            assert_eq!(err.location.unwrap().line, line, "{text}");
        }
    }

    #[test]
    fn complex_keys_are_rejected() {
        assert!(read("? [a, b]\n: 1\n").is_err());
    }

    #[test]
    fn empty_and_comment_only_files_have_no_root() {
        assert_eq!(read("").unwrap(), None);
        assert_eq!(read("# only a comment\n").unwrap(), None);
    }

    #[test]
    fn syntax_errors_carry_a_line() {
        let err = read("a:\n  b: 1\n c: 2\n").unwrap_err();
        assert_eq!(err.kind, ErrorKind::Yaml);
        assert_eq!(err.location.unwrap().line, 3);
    }
}
