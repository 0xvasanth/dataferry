//! A serde deserializer over the located YAML tree. It tracks the field path
//! as it descends, fills in [`Spanned`] values, and stamps every error with
//! the location and path of the node it happened at.
//!
//! [`Spanned`]: super::span::Spanned

use std::fmt;

use serde::de::value::{BorrowedStrDeserializer, U32Deserializer};
use serde::de::{self, DeserializeSeed, IntoDeserializer, Visitor};

use super::span::{
    FieldPath, Location, SPANNED, SPANNED_COLUMN, SPANNED_FILE, SPANNED_LINE, SPANNED_VALUE,
};
use super::yaml::{Node, NodeKind};

#[derive(Debug)]
pub(crate) struct DeError {
    pub message: String,
    pub location: Option<Location>,
    pub path: Option<FieldPath>,
    /// Set for unknown keys, so the loader can add a hint.
    pub unknown_field: Option<(String, &'static [&'static str])>,
}

impl DeError {
    fn new(message: String) -> Self {
        Self {
            message,
            location: None,
            path: None,
            unknown_field: None,
        }
    }

    /// Stamps the error with where it happened, unless something deeper
    /// already did.
    fn stamp(mut self, node: &Node, path: &FieldPath) -> Self {
        if self.location.is_none() {
            self.location = Some(node.location.clone());
        }
        if self.path.is_none() {
            self.path = Some(path.clone());
        }
        self
    }
}

impl fmt::Display for DeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for DeError {}

impl de::Error for DeError {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Self::new(msg.to_string())
    }

    fn invalid_type(unexp: de::Unexpected<'_>, exp: &dyn de::Expected) -> Self {
        Self::new(format!("expected {}, found {unexp}", plain(exp)))
    }

    fn invalid_value(unexp: de::Unexpected<'_>, exp: &dyn de::Expected) -> Self {
        Self::new(format!("expected {}, found {unexp}", plain(exp)))
    }

    fn unknown_field(field: &str, expected: &'static [&'static str]) -> Self {
        let mut error = Self::new(format!("unknown key `{field}`"));
        error.unknown_field = Some((field.to_owned(), expected));
        error
    }

    fn unknown_variant(variant: &str, expected: &'static [&'static str]) -> Self {
        let options = expected
            .iter()
            .map(|v| format!("`{v}`"))
            .collect::<Vec<_>>()
            .join(", ");
        Self::new(format!(
            "unknown value `{variant}`, expected one of {options}"
        ))
    }

    fn missing_field(field: &'static str) -> Self {
        let mut error = Self::new(format!("missing key `{field}`"));
        // The path names the missing key; the location stays the map's.
        error.path = Some(FieldPath::root().key(field));
        error
    }
}

/// Serde names number types the Rust way (`u64`); config errors say what
/// to write instead.
fn plain(expected: &dyn de::Expected) -> String {
    let expected = expected.to_string();
    match expected.as_str() {
        "u8" | "u16" | "u32" | "u64" | "u128" | "usize" => "a whole number, 0 or more".into(),
        "i8" | "i16" | "i32" | "i64" | "i128" | "isize" => "a whole number".into(),
        "f32" | "f64" => "a number".into(),
        _ if expected.starts_with("a nonzero u") => "a whole number, 1 or more".into(),
        _ => expected,
    }
}

/// Deserializes `node`, whose field path is `path`.
pub(crate) fn from_node<'de, T: de::Deserialize<'de>>(
    node: &'de Node,
    path: FieldPath,
) -> Result<T, DeError> {
    T::deserialize(NodeDe { node, path })
}

pub(crate) struct NodeDe<'a> {
    node: &'a Node,
    path: FieldPath,
}

impl<'a> NodeDe<'a> {
    fn stamp<T>(&self, result: Result<T, DeError>) -> Result<T, DeError> {
        result.map_err(|e| self.fix_missing(e).stamp(self.node, &self.path))
    }

    // `missing_field` only knows the key's name: put it under this path.
    fn fix_missing(&self, mut error: DeError) -> DeError {
        if error.location.is_none()
            && error.message.starts_with("missing key")
            && let Some(key) = error
                .path
                .as_ref()
                .and_then(|p| p.last_key().map(str::to_owned))
        {
            error.path = Some(self.path.key(key));
        }
        error
    }

    fn invalid(&self, expected: &str) -> DeError {
        let mut message = format!("expected {expected}, found {}", self.node.describe());
        if expected.contains("string")
            && let Some(text) = scalar_text(&self.node.kind)
        {
            message = format!(
                "expected {expected}, found {}: quote it (\"{text}\") to make it a string",
                self.node.describe()
            );
        }
        DeError::new(message).stamp(self.node, &self.path)
    }
}

fn scalar_text(kind: &NodeKind) -> Option<String> {
    match kind {
        NodeKind::Null => Some("null".into()),
        NodeKind::Bool(b) => Some(b.to_string()),
        NodeKind::Int(i) => Some(i.to_string()),
        NodeKind::Float(x) => Some(x.to_string()),
        _ => None,
    }
}

macro_rules! forward_to_any {
    ($($method:ident)*) => {
        $(
            fn $method<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, DeError> {
                self.deserialize_any(visitor)
            }
        )*
    };
}

impl<'de> de::Deserializer<'de> for NodeDe<'de> {
    type Error = DeError;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, DeError> {
        let result = match &self.node.kind {
            NodeKind::Null => visitor.visit_unit(),
            NodeKind::Bool(b) => visitor.visit_bool(*b),
            NodeKind::Int(i) => visitor.visit_i64(*i),
            NodeKind::Float(x) => visitor.visit_f64(*x),
            NodeKind::Str(s) => visitor.visit_borrowed_str(s),
            NodeKind::Seq(items) => visitor.visit_seq(SeqAccess {
                items: items.iter().enumerate(),
                path: &self.path,
            }),
            NodeKind::Map(entries) => visitor.visit_map(MapAccess {
                entries: entries.iter(),
                value: None,
                path: &self.path,
            }),
        };
        self.stamp(result)
    }

    fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, DeError> {
        match &self.node.kind {
            NodeKind::Str(s) => {
                let result = visitor.visit_borrowed_str(s);
                self.stamp(result)
            }
            _ => Err(self.invalid("a string")),
        }
    }

    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, DeError> {
        self.deserialize_str(visitor)
    }

    fn deserialize_identifier<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, DeError> {
        self.deserialize_str(visitor)
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, DeError> {
        match self.node.kind {
            NodeKind::Null => visitor.visit_none(),
            _ => visitor.visit_some(self),
        }
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, DeError> {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, DeError> {
        match &self.node.kind {
            NodeKind::Seq(_) => self.deserialize_any(visitor),
            _ => Err(self.invalid("a list")),
        }
    }

    fn deserialize_tuple<V: Visitor<'de>>(
        self,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value, DeError> {
        self.deserialize_seq(visitor)
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value, DeError> {
        self.deserialize_seq(visitor)
    }

    fn deserialize_map<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, DeError> {
        match &self.node.kind {
            NodeKind::Map(_) => self.deserialize_any(visitor),
            _ => Err(self.invalid("a map")),
        }
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, DeError> {
        if name == SPANNED {
            let result = visitor.visit_map(SpannedAccess {
                node: self.node,
                path: &self.path,
                step: 0,
            });
            return self.stamp(result);
        }
        self.deserialize_map(visitor)
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, DeError> {
        // Only unit variants are used in the config: `mode: upsert`.
        match &self.node.kind {
            NodeKind::Str(s) => {
                let result = visitor.visit_enum(s.as_str().into_deserializer());
                self.stamp(result)
            }
            _ => Err(self.invalid("a string")),
        }
    }

    fn deserialize_unit<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, DeError> {
        match self.node.kind {
            NodeKind::Null => visitor.visit_unit(),
            _ => Err(self.invalid("nothing (null)")),
        }
    }

    fn deserialize_unit_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, DeError> {
        self.deserialize_unit(visitor)
    }

    fn deserialize_ignored_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, DeError> {
        visitor.visit_unit()
    }

    forward_to_any! {
        deserialize_bool deserialize_i8 deserialize_i16 deserialize_i32 deserialize_i64
        deserialize_i128 deserialize_u8 deserialize_u16 deserialize_u32 deserialize_u64
        deserialize_u128 deserialize_f32 deserialize_f64 deserialize_char
        deserialize_bytes deserialize_byte_buf
    }
}

struct SeqAccess<'a, I> {
    items: I,
    path: &'a FieldPath,
}

impl<'de, I: Iterator<Item = (usize, &'de Node)>> de::SeqAccess<'de> for SeqAccess<'_, I> {
    type Error = DeError;

    fn next_element_seed<T: DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>, DeError> {
        match self.items.next() {
            Some((index, node)) => {
                let path = self.path.item(index, node.item_name());
                // Stamped here as well as inside, for errors a `Deserialize`
                // impl raises after the deserializer has returned (an invalid
                // name, an unsupported version).
                seed.deserialize(NodeDe {
                    node,
                    path: path.clone(),
                })
                .map(Some)
                .map_err(|e| e.stamp(node, &path))
            }
            None => Ok(None),
        }
    }
}

struct MapAccess<'p, 'de, I> {
    entries: I,
    value: Option<(&'de Node, FieldPath)>,
    path: &'p FieldPath,
}

impl<'de, I: Iterator<Item = &'de (Node, Node)>> de::MapAccess<'de> for MapAccess<'_, 'de, I> {
    type Error = DeError;

    fn next_key_seed<K: DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> Result<Option<K::Value>, DeError> {
        match self.entries.next() {
            Some((key, value)) => {
                let path = self.path.key(key.as_str().unwrap_or_default());
                self.value = Some((value, path.clone()));
                seed.deserialize(NodeDe {
                    node: key,
                    path: path.clone(),
                })
                .map(Some)
                .map_err(|e| e.stamp(key, &path))
            }
            None => Ok(None),
        }
    }

    fn next_value_seed<V: DeserializeSeed<'de>>(&mut self, seed: V) -> Result<V::Value, DeError> {
        let (node, path) = self.value.take().expect("next_value before next_key");
        seed.deserialize(NodeDe {
            node,
            path: path.clone(),
        })
        .map_err(|e| e.stamp(node, &path))
    }
}

/// Feeds a `Spanned<T>` visitor the node's file, line and column, then the
/// node itself.
struct SpannedAccess<'p, 'de> {
    node: &'de Node,
    path: &'p FieldPath,
    step: u8,
}

impl<'de> de::MapAccess<'de> for SpannedAccess<'_, 'de> {
    type Error = DeError;

    fn next_key_seed<K: DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> Result<Option<K::Value>, DeError> {
        let key = match self.step {
            0 => SPANNED_FILE,
            1 => SPANNED_LINE,
            2 => SPANNED_COLUMN,
            3 => SPANNED_VALUE,
            _ => return Ok(None),
        };
        seed.deserialize(BorrowedStrDeserializer::new(key))
            .map(Some)
    }

    fn next_value_seed<V: DeserializeSeed<'de>>(&mut self, seed: V) -> Result<V::Value, DeError> {
        let location = &self.node.location;
        self.step += 1;
        match self.step {
            1 => seed.deserialize(BorrowedStrDeserializer::new(&location.file)),
            2 => seed.deserialize(U32Deserializer::new(location.line)),
            3 => seed.deserialize(U32Deserializer::new(location.column)),
            _ => seed.deserialize(NodeDe {
                node: self.node,
                path: self.path.clone(),
            }),
        }
    }
}
