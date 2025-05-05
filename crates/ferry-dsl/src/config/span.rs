//! Source locations: every value the loader produces remembers the file,
//! line and column it was written at, and the field path that leads to it.

use std::fmt;
use std::ops::Deref;
use std::sync::Arc;

use schemars::{JsonSchema, Schema, SchemaGenerator};
use serde::de::{self, Deserialize, Deserializer, MapAccess, Visitor};
use serde::{Serialize, Serializer};

/// A position in a config file. `file` is the path relative to the `ferry/`
/// directory, `/`-separated (`flows/orders.yaml`, `../shared/c.yaml`).
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Location {
    pub file: Arc<str>,
    /// 1-based.
    pub line: u32,
    /// 1-based.
    pub column: u32,
}

impl Location {
    pub fn new(file: impl Into<Arc<str>>, line: u32, column: u32) -> Self {
        Self {
            file: file.into(),
            line,
            column,
        }
    }
}

impl fmt::Display for Location {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}:{}", self.file, self.line, self.column)
    }
}

/// A value and the place it was written.
///
/// Equality ignores the location, so two models that say the same thing in
/// different files compare equal. Serializes as the bare value. It can only be
/// deserialized by the config loader, which is the only thing that knows
/// locations.
#[derive(Clone)]
pub struct Spanned<T> {
    value: T,
    location: Location,
}

impl<T> Spanned<T> {
    pub fn new(value: T, location: Location) -> Self {
        Self { value, location }
    }

    pub fn value(&self) -> &T {
        &self.value
    }

    pub fn location(&self) -> &Location {
        &self.location
    }

    pub fn into_inner(self) -> T {
        self.value
    }
}

impl<T> Deref for Spanned<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.value
    }
}

impl<T: fmt::Debug> fmt::Debug for Spanned<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?} @ {}", self.value, self.location)
    }
}

impl<T: PartialEq> PartialEq for Spanned<T> {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl<T: Eq> Eq for Spanned<T> {}

impl<T: Serialize> Serialize for Spanned<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.value.serialize(serializer)
    }
}

impl<T: JsonSchema> JsonSchema for Spanned<T> {
    fn inline_schema() -> bool {
        T::inline_schema()
    }

    fn schema_name() -> std::borrow::Cow<'static, str> {
        T::schema_name()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        T::schema_id()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        T::json_schema(generator)
    }
}

// The loader's deserializer recognizes this struct name and hands the
// visitor the node's location followed by the node itself (the same trick
// `toml::Spanned` uses).
pub(crate) const SPANNED: &str = "$__ferry_spanned";
pub(crate) const SPANNED_FILE: &str = "$__ferry_file";
pub(crate) const SPANNED_LINE: &str = "$__ferry_line";
pub(crate) const SPANNED_COLUMN: &str = "$__ferry_column";
pub(crate) const SPANNED_VALUE: &str = "$__ferry_value";
pub(crate) const SPANNED_FIELDS: &[&str] =
    &[SPANNED_FILE, SPANNED_LINE, SPANNED_COLUMN, SPANNED_VALUE];

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Spanned<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct SpannedVisitor<T>(std::marker::PhantomData<T>);

        impl<'de, T: Deserialize<'de>> Visitor<'de> for SpannedVisitor<T> {
            type Value = Spanned<T>;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a value read by the ferry config loader")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Spanned<T>, A::Error> {
                fn expect<'de, A: MapAccess<'de>>(map: &mut A, name: &str) -> Result<(), A::Error> {
                    match map.next_key::<String>()? {
                        Some(key) if key == name => Ok(()),
                        _ => Err(de::Error::custom(
                            "Spanned can only be read by the ferry config loader",
                        )),
                    }
                }
                expect(&mut map, SPANNED_FILE)?;
                let file: String = map.next_value()?;
                expect(&mut map, SPANNED_LINE)?;
                let line: u32 = map.next_value()?;
                expect(&mut map, SPANNED_COLUMN)?;
                let column: u32 = map.next_value()?;
                expect(&mut map, SPANNED_VALUE)?;
                let value: T = map.next_value()?;
                Ok(Spanned::new(value, Location::new(file, line, column)))
            }
        }

        deserializer.deserialize_struct(
            SPANNED,
            SPANNED_FIELDS,
            SpannedVisitor(std::marker::PhantomData),
        )
    }
}

/// The path from a file's root to a value: `flows[orders].to.order_items.key`.
/// List items are named by their `name:` field when they have one.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FieldPath(Vec<Segment>);

#[derive(Clone, Debug, PartialEq, Eq)]
enum Segment {
    Key(String),
    Item { index: usize, name: Option<String> },
}

impl FieldPath {
    pub fn root() -> Self {
        Self::default()
    }

    pub fn is_root(&self) -> bool {
        self.0.is_empty()
    }

    pub fn key(&self, key: impl Into<String>) -> Self {
        let mut path = self.clone();
        path.0.push(Segment::Key(key.into()));
        path
    }

    pub fn item(&self, index: usize, name: Option<&str>) -> Self {
        let mut path = self.clone();
        path.0.push(Segment::Item {
            index,
            name: name.map(str::to_owned),
        });
        path
    }

    /// The last key, if the path ends in one.
    pub fn last_key(&self) -> Option<&str> {
        match self.0.last() {
            Some(Segment::Key(key)) => Some(key),
            _ => None,
        }
    }

    /// The path rendered with every list index replaced by `[]`, for
    /// matching against a pattern such as `flows[].runtime`.
    pub(crate) fn shape(&self) -> String {
        let mut out = String::new();
        for segment in &self.0 {
            match segment {
                Segment::Key(key) => {
                    if !out.is_empty() {
                        out.push('.');
                    }
                    out.push_str(key);
                }
                Segment::Item { .. } => out.push_str("[]"),
            }
        }
        out
    }
}

impl fmt::Display for FieldPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, segment) in self.0.iter().enumerate() {
            match segment {
                Segment::Key(key) if i == 0 => f.write_str(key)?,
                Segment::Key(key) => write!(f, ".{key}")?,
                Segment::Item {
                    name: Some(name), ..
                } => write!(f, "[{name}]")?,
                Segment::Item { index, name: None } => write!(f, "[{index}]")?,
            }
        }
        Ok(())
    }
}
