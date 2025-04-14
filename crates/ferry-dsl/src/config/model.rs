//! The config model: one struct per file kind and the merged [`Config`],
//! exactly as 0003 §1 defines the grammar.
//!
//! Unknown keys are errors everywhere (`deny_unknown_fields`). Expressions
//! are kept as raw strings with their location ([`Expr`]); B2 parses them.
//! Values that can take two shapes (`from:`, `where:`, `columns:`, a column
//! spec) are wrapped in [`Spanned`] where they're used, so the location of
//! the scalar form survives.

use std::fmt;
use std::num::{NonZeroU32, NonZeroU64};

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::de::{self, Deserializer, MapAccess, SeqAccess, Visitor};
use serde::ser::{SerializeMap, SerializeSeq};
use serde::{Deserialize, Serialize, Serializer};

use super::span::Spanned;

/// An expression (B2), as written.
pub type Expr = Spanned<String>;

/// The only grammar version this build reads.
pub const VERSION: u32 = 1;

// ---------------------------------------------------------------- files

/// `ferry.yaml`.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(title = "0xferry ferry.yaml")]
pub struct FerryFile {
    /// Version of the config grammar. Only `1` exists.
    pub version: Version,
    /// Names the migration in state and reports.
    pub name: Spanned<Ident>,
    /// Extra files to load, as paths or globs relative to the `ferry/`
    /// directory (0003 §8). Each holds `flows:`, `connections:` or both.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub include: Vec<Spanned<String>>,
}

/// `connections.yaml`.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(title = "0xferry connections.yaml")]
pub struct ConnectionsFile {
    pub connections: Vec<Connection>,
}

/// `runtime.yaml`.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(title = "0xferry runtime.yaml")]
pub struct RuntimeFile {
    pub runtime: Runtime,
}

/// A file under `flows/`.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(title = "0xferry flow file")]
pub struct FlowsFile {
    pub flows: Vec<Flow>,
}

/// A file reached through `include:`.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct IncludedFile {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connections: Option<Vec<Connection>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flows: Option<Vec<Flow>>,
}

// ---------------------------------------------------------------- scalars

/// `version:`. Anything but [`VERSION`] is an error naming the supported one.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct Version(u32);

impl Version {
    pub fn get(self) -> u32 {
        self.0
    }
}

impl<'de> Deserialize<'de> for Version {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let version = i64::deserialize(deserializer)?;
        if version == i64::from(VERSION) {
            Ok(Version(VERSION))
        } else {
            Err(de::Error::custom(format!(
                "unsupported version {version}: this ferry reads version {VERSION}"
            )))
        }
    }
}

impl JsonSchema for Version {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "Version".into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "description": "Version of the config grammar. Only 1 exists.",
            "type": "integer",
            "const": VERSION,
        })
    }
}

/// A connection or flow name: `[a-z][a-z0-9_]*` (0003 §2).
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
#[serde(transparent)]
pub struct Ident(String);

const IDENT_PATTERN: &str = "^[a-z][a-z0-9_]*$";

impl Ident {
    pub fn parse(name: &str) -> Result<Self, String> {
        let mut chars = name.chars();
        let valid = chars.next().is_some_and(|c| c.is_ascii_lowercase())
            && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
        if valid {
            Ok(Ident(name.to_owned()))
        } else {
            Err(format!(
                "`{name}` is not a valid name: names are lowercase letters, digits and `_`, starting with a letter"
            ))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Ident {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for Ident {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let name = String::deserialize(deserializer)?;
        Ident::parse(&name).map_err(de::Error::custom)
    }
}

impl JsonSchema for Ident {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "Ident".into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "description": "Lowercase letters, digits and `_`, starting with a letter.",
            "type": "string",
            "pattern": IDENT_PATTERN,
        })
    }
}

/// `<conn>.<entity>`, split at the first dot (0003 §3): `app.public.orders`
/// is entity `public.orders` on connection `app`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct EntityRef {
    pub connection: Ident,
    pub entity: String,
}

impl EntityRef {
    pub fn parse(text: &str) -> Result<Self, String> {
        let invalid = || {
            format!(
                "`{text}` is not an entity reference: write `<connection>.<entity>`, e.g. `shop.orders`"
            )
        };
        let (connection, entity) = text.split_once('.').ok_or_else(invalid)?;
        if entity.is_empty() {
            return Err(invalid());
        }
        Ok(EntityRef {
            connection: Ident::parse(connection).map_err(|_| invalid())?,
            entity: entity.to_owned(),
        })
    }

    /// The default `as:` alias: the last dot-separated part of the entity.
    pub fn default_alias(&self) -> &str {
        self.entity.rsplit('.').next().unwrap_or(&self.entity)
    }
}

impl fmt::Display for EntityRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.connection, self.entity)
    }
}

impl Serialize for EntityRef {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for EntityRef {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        EntityRef::parse(&text).map_err(de::Error::custom)
    }
}

impl JsonSchema for EntityRef {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "EntityRef".into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "description": "`<connection>.<entity>`, split at the first dot.",
            "type": "string",
            "pattern": "^[a-z][a-z0-9_]*\\..+$",
        })
    }
}

/// A map whose order means something (columns, named predicates,
/// assertions), kept as written.
#[derive(Clone, Debug, PartialEq)]
pub struct OrderedMap<V>(Vec<(Spanned<String>, V)>);

impl<V> OrderedMap<V> {
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (&Spanned<String>, &V)> {
        self.0.iter().map(|(k, v)| (k, v))
    }

    pub fn keys(&self) -> impl ExactSizeIterator<Item = &Spanned<String>> {
        self.0.iter().map(|(k, _)| k)
    }

    pub fn get(&self, key: &str) -> Option<&V> {
        self.0
            .iter()
            .find(|(k, _)| k.as_str() == key)
            .map(|(_, v)| v)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl<V> Default for OrderedMap<V> {
    fn default() -> Self {
        Self(Vec::new())
    }
}

impl<V: Serialize> Serialize for OrderedMap<V> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (k, v) in &self.0 {
            map.serialize_entry(k.as_str(), v)?;
        }
        map.end()
    }
}

impl<'de, V: Deserialize<'de>> Deserialize<'de> for OrderedMap<V> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct MapVisitor<V>(std::marker::PhantomData<V>);

        impl<'de, V: Deserialize<'de>> Visitor<'de> for MapVisitor<V> {
            type Value = OrderedMap<V>;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a map")
            }

            fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<Self::Value, A::Error> {
                read_ordered_map(map)
            }
        }

        deserializer.deserialize_map(MapVisitor(std::marker::PhantomData))
    }
}

fn read_ordered_map<'de, A: MapAccess<'de>, V: Deserialize<'de>>(
    mut map: A,
) -> Result<OrderedMap<V>, A::Error> {
    let mut entries = Vec::new();
    while let Some(key) = map.next_key::<Spanned<String>>()? {
        entries.push((key, map.next_value()?));
    }
    Ok(OrderedMap(entries))
}

impl<V: JsonSchema> JsonSchema for OrderedMap<V> {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        format!("Map_of_{}", V::schema_name()).into()
    }

    fn inline_schema() -> bool {
        true
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "object",
            "additionalProperties": generator.subschema_for::<V>(),
        })
    }
}

macro_rules! keyword_enum {
    ($(#[$meta:meta])* $name:ident { $($(#[$vmeta:meta])* $variant:ident => $text:literal),* $(,)? } default $default:ident) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Deserialize, Serialize, JsonSchema)]
        #[serde(rename_all = "snake_case")]
        pub enum $name {
            $(
                $(#[$vmeta])*
                #[serde(rename = $text)]
                $variant,
            )*
        }

        impl $name {
            pub fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $text,)*
                }
            }

            pub fn is_default(&self) -> bool {
                *self == Self::$default
            }
        }
    };
}

keyword_enum! {
    /// `mode:` per flow (0003 §5.5).
    Mode {
        #[default]
        Insert => "insert",
        Upsert => "upsert",
    } default Insert
}

keyword_enum! {
    /// `on_error:` (0003 §4).
    OnError {
        #[default]
        Fail => "fail",
        DeadLetter => "dead_letter",
    } default Fail
}

keyword_enum! {
    /// `join.strategy:` (0003 §5.2).
    JoinStrategy {
        #[default]
        Auto => "auto",
        CoPartition => "co_partition",
        Stage => "stage",
    } default Auto
}

/// `join.cardinality:`. Required, no default (0003 §5.2).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Cardinality {
    One,
    Many,
}

impl Cardinality {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::One => "one",
            Self::Many => "many",
        }
    }
}

// ---------------------------------------------------------------- connections

/// One entry of `connections:`. `name` and `type` are 0003's; every other
/// key is the connector's own (0004 §2), kept as written for the registry to
/// check. `description` is reserved and rejected until 0003 defines it.
#[derive(Clone, Debug, PartialEq)]
pub struct Connection {
    pub name: Spanned<Ident>,
    pub r#type: Spanned<String>,
    /// The type-specific fields, in the order written.
    pub fields: OrderedMap<RawValue>,
}

/// Keys every connection entry reserves (0003 §3).
pub const RESERVED_CONNECTION_KEYS: &[&str] = &["name", "type", "description"];

impl<'de> Deserialize<'de> for Connection {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ConnectionVisitor;

        impl<'de> Visitor<'de> for ConnectionVisitor {
            type Value = Connection;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a connection: a map with `name`, `type` and the type's own keys")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Connection, A::Error> {
                let mut name = None;
                let mut r#type = None;
                let mut fields = Vec::new();
                while let Some(key) = map.next_key::<ConnectionKey>()? {
                    match key {
                        ConnectionKey::Name => name = Some(map.next_value()?),
                        ConnectionKey::Type => r#type = Some(map.next_value()?),
                        ConnectionKey::Field(key) => fields.push((key, map.next_value()?)),
                    }
                }
                Ok(Connection {
                    name: name.ok_or_else(|| de::Error::missing_field("name"))?,
                    r#type: r#type.ok_or_else(|| de::Error::missing_field("type"))?,
                    fields: OrderedMap(fields),
                })
            }
        }

        deserializer.deserialize_map(ConnectionVisitor)
    }
}

/// Rejects `description` while the key itself is read, so the error points
/// at its line.
enum ConnectionKey {
    Name,
    Type,
    Field(Spanned<String>),
}

impl<'de> Deserialize<'de> for ConnectionKey {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let key = Spanned::<String>::deserialize(deserializer)?;
        match key.as_str() {
            "name" => Ok(ConnectionKey::Name),
            "type" => Ok(ConnectionKey::Type),
            "description" => Err(de::Error::custom(
                "`description` is reserved for connection entries and has no meaning in v0.1 (0003 §3)",
            )),
            _ => Ok(ConnectionKey::Field(key)),
        }
    }
}

impl Serialize for Connection {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.fields.len() + 2))?;
        map.serialize_entry("name", &self.name)?;
        map.serialize_entry("type", &self.r#type)?;
        for (k, v) in self.fields.iter() {
            map.serialize_entry(k.as_str(), v)?;
        }
        map.end()
    }
}

impl JsonSchema for Connection {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "Connection".into()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        // Left open: the registry fills in each type's fields (0004 §3).
        json_schema!({
            "description": "A connection. `name` and `type` are fixed; the other keys belong to the connector type (0004).",
            "type": "object",
            "properties": {
                "name": generator.subschema_for::<Ident>(),
                "type": { "type": "string", "description": "A connector type registered with this ferry binary." },
            },
            "required": ["name", "type"],
            "not": { "required": ["description"] },
        })
    }
}

/// A connection field as written, with the location of every value. The
/// registry gets it as JSON ([`RawValue::to_json`]).
pub type RawValue = Spanned<RawKind>;

#[derive(Clone, Debug, PartialEq)]
pub enum RawKind {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    Seq(Vec<RawValue>),
    Map(OrderedMap<RawValue>),
}

impl RawKind {
    pub fn to_json(&self) -> serde_json::Value {
        use serde_json::Value;
        match self {
            RawKind::Null => Value::Null,
            RawKind::Bool(b) => Value::Bool(*b),
            RawKind::Int(i) => Value::from(*i),
            RawKind::Float(x) => serde_json::Number::from_f64(*x)
                .map(Value::Number)
                .unwrap_or_else(|| Value::String(x.to_string())),
            RawKind::Str(s) => Value::String(s.clone()),
            RawKind::Seq(items) => Value::Array(items.iter().map(|v| v.to_json()).collect()),
            RawKind::Map(map) => Value::Object(
                map.iter()
                    .map(|(k, v)| (k.as_str().to_owned(), v.to_json()))
                    .collect(),
            ),
        }
    }
}

impl Serialize for RawKind {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            RawKind::Null => serializer.serialize_unit(),
            RawKind::Bool(b) => serializer.serialize_bool(*b),
            RawKind::Int(i) => serializer.serialize_i64(*i),
            RawKind::Float(x) => serializer.serialize_f64(*x),
            RawKind::Str(s) => serializer.serialize_str(s),
            RawKind::Seq(items) => {
                let mut seq = serializer.serialize_seq(Some(items.len()))?;
                for item in items {
                    seq.serialize_element(item)?;
                }
                seq.end()
            }
            RawKind::Map(map) => map.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for RawKind {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct RawVisitor;

        impl<'de> Visitor<'de> for RawVisitor {
            type Value = RawKind;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("any value")
            }

            fn visit_unit<E>(self) -> Result<RawKind, E> {
                Ok(RawKind::Null)
            }

            fn visit_bool<E>(self, v: bool) -> Result<RawKind, E> {
                Ok(RawKind::Bool(v))
            }

            fn visit_i64<E>(self, v: i64) -> Result<RawKind, E> {
                Ok(RawKind::Int(v))
            }

            fn visit_u64<E: de::Error>(self, v: u64) -> Result<RawKind, E> {
                i64::try_from(v)
                    .map(RawKind::Int)
                    .map_err(|_| E::custom("integer too large"))
            }

            fn visit_f64<E>(self, v: f64) -> Result<RawKind, E> {
                Ok(RawKind::Float(v))
            }

            fn visit_str<E>(self, v: &str) -> Result<RawKind, E> {
                Ok(RawKind::Str(v.to_owned()))
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<RawKind, A::Error> {
                let mut items = Vec::new();
                while let Some(item) = seq.next_element()? {
                    items.push(item);
                }
                Ok(RawKind::Seq(items))
            }

            fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<RawKind, A::Error> {
                read_ordered_map(map).map(RawKind::Map)
            }
        }

        deserializer.deserialize_any(RawVisitor)
    }
}

// ---------------------------------------------------------------- runtime

/// `runtime:` (0003 §1, §4). Every key is optional.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Runtime {
    /// Where run state lives. Default: `{backend: file}`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<StateConfig>,
    /// Worker count. Default `auto`: one per core.
    #[serde(default)]
    pub workers: Workers,
    /// Memory the runner may use, e.g. `512MiB`. Default `1GiB`.
    #[serde(default)]
    pub memory_budget: ByteSize,
    /// Target rows per partition (`PartitionHint::target_rows`). Default 100000.
    #[serde(default = "default_partition_rows")]
    pub partition_rows: NonZeroU64,
    /// `fail` stops at the first error row, `dead_letter` writes it to
    /// `_ferry_dlq` and carries on. Default `fail`.
    #[serde(default)]
    pub on_error: OnError,
    /// How many records may be dead-lettered before the run fails. Default
    /// unbounded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_errors: Option<u64>,
    /// Connection cap across every source (A5).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_max_connections: Option<NonZeroU32>,
    /// Read rate cap across every source (A5).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_rows_per_sec: Option<NonZeroU64>,
}

impl Default for Runtime {
    fn default() -> Self {
        Self {
            state: None,
            workers: Workers::default(),
            memory_budget: ByteSize::default(),
            partition_rows: default_partition_rows(),
            on_error: OnError::default(),
            max_errors: None,
            source_max_connections: None,
            source_rows_per_sec: None,
        }
    }
}

pub const DEFAULT_PARTITION_ROWS: u64 = 100_000;

fn default_partition_rows() -> NonZeroU64 {
    NonZeroU64::new(DEFAULT_PARTITION_ROWS).unwrap()
}

/// `runtime.state:` (0003 §4). `backend: file` keeps state in `path`; any
/// other backend is a state-capable connector type and `connection:` names
/// a connection of that type (checked by the registry, 0004 §8).
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StateConfig {
    pub backend: Spanned<String>,
    /// For `backend: file`: the state directory. Default chosen by A4.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<Spanned<String>>,
    /// For any other backend: the connection holding the state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection: Option<Spanned<String>>,
}

pub const FILE_BACKEND: &str = "file";

/// `workers: auto | <int>`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Workers {
    #[default]
    Auto,
    Count(NonZeroU32),
}

impl Serialize for Workers {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Workers::Auto => serializer.serialize_str("auto"),
            Workers::Count(n) => serializer.serialize_u32(n.get()),
        }
    }
}

impl<'de> Deserialize<'de> for Workers {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct WorkersVisitor;

        impl Visitor<'_> for WorkersVisitor {
            type Value = Workers;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("`auto` or a positive number")
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<Workers, E> {
                match v {
                    "auto" => Ok(Workers::Auto),
                    _ => Err(E::invalid_value(de::Unexpected::Str(v), &self)),
                }
            }

            fn visit_i64<E: de::Error>(self, v: i64) -> Result<Workers, E> {
                u32::try_from(v)
                    .ok()
                    .and_then(NonZeroU32::new)
                    .map(Workers::Count)
                    .ok_or_else(|| E::invalid_value(de::Unexpected::Signed(v), &self))
            }
        }

        deserializer.deserialize_any(WorkersVisitor)
    }
}

impl JsonSchema for Workers {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "Workers".into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "description": "`auto` (one per core) or a positive number.",
            "oneOf": [
                { "const": "auto" },
                { "type": "integer", "minimum": 1, "maximum": u32::MAX },
            ],
        })
    }
}

/// A size in bytes, written `512MiB` or as a plain number of bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ByteSize(pub NonZeroU64);

impl Default for ByteSize {
    fn default() -> Self {
        ByteSize(NonZeroU64::new(1 << 30).unwrap())
    }
}

const BYTE_UNITS: &[(&str, u32)] = &[("TiB", 40), ("GiB", 30), ("MiB", 20), ("KiB", 10), ("B", 0)];

impl ByteSize {
    pub fn parse(text: &str) -> Option<Self> {
        let (digits, shift) = BYTE_UNITS
            .iter()
            .find_map(|(unit, shift)| text.strip_suffix(unit).map(|d| (d, *shift)))?;
        if digits.is_empty()
            || !digits.bytes().all(|b| b.is_ascii_digit())
            || digits.starts_with('0')
        {
            return None;
        }
        let n: u64 = digits.parse().ok()?;
        n.checked_mul(1 << shift)
            .and_then(NonZeroU64::new)
            .map(ByteSize)
    }
}

impl Serialize for ByteSize {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u64(self.0.get())
    }
}

impl<'de> Deserialize<'de> for ByteSize {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct SizeVisitor;

        impl Visitor<'_> for SizeVisitor {
            type Value = ByteSize;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(
                    "a size such as `512MiB` (units B, KiB, MiB, GiB, TiB) or a number of bytes",
                )
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<ByteSize, E> {
                ByteSize::parse(v).ok_or_else(|| E::invalid_value(de::Unexpected::Str(v), &self))
            }

            fn visit_i64<E: de::Error>(self, v: i64) -> Result<ByteSize, E> {
                u64::try_from(v)
                    .ok()
                    .and_then(NonZeroU64::new)
                    .map(ByteSize)
                    .ok_or_else(|| E::invalid_value(de::Unexpected::Signed(v), &self))
            }

            fn visit_u64<E: de::Error>(self, v: u64) -> Result<ByteSize, E> {
                NonZeroU64::new(v)
                    .map(ByteSize)
                    .ok_or_else(|| E::invalid_value(de::Unexpected::Unsigned(v), &self))
            }
        }

        deserializer.deserialize_any(SizeVisitor)
    }
}

impl JsonSchema for ByteSize {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "ByteSize".into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "description": "A size such as `512MiB` (units B, KiB, MiB, GiB, TiB) or a number of bytes.",
            "oneOf": [
                { "type": "string", "pattern": "^[1-9][0-9]*(B|KiB|MiB|GiB|TiB)$" },
                { "type": "integer", "minimum": 1 },
            ],
        })
    }
}

// ---------------------------------------------------------------- flows

/// One flow (0003 §5).
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Flow {
    /// Unique across every file.
    pub name: Spanned<Ident>,
    /// One entity (`shop.orders`) or a merge: a driver plus joined entries.
    pub from: Spanned<From>,
    /// The one connection this flow writes to.
    pub sink: Spanned<Ident>,
    #[serde(default, skip_serializing_if = "Mode::is_default")]
    pub mode: Mode,
    /// Filter on the driver record.
    #[serde(default, rename = "where", skip_serializing_if = "Option::is_none")]
    pub filter: Option<Spanned<Where>>,
    /// Flows that must finish before this one starts (0003 §7).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub depends_on: Vec<Spanned<Ident>>,
    /// Per-flow overrides of the settings that only affect this flow (0003 §6).
    #[serde(default, skip_serializing_if = "FlowRuntime::is_empty")]
    pub runtime: FlowRuntime,
    /// Target tables, each with its columns.
    pub to: OrderedMap<Target>,
    /// Named assertions checked after the run (0003 §5.9).
    #[serde(default, skip_serializing_if = "OrderedMap::is_empty")]
    pub expect: OrderedMap<Expr>,
}

/// `from:`.
#[derive(Clone, Debug, PartialEq)]
pub enum From {
    Entity(EntityRef),
    Merge(Vec<FromEntry>),
}

impl Serialize for From {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            From::Entity(entity) => entity.serialize(serializer),
            From::Merge(entries) => entries.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for From {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct FromVisitor;

        impl<'de> Visitor<'de> for FromVisitor {
            type Value = From;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("an entity (`<connection>.<entity>`) or a list of merge entries")
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<From, E> {
                EntityRef::parse(v).map(From::Entity).map_err(E::custom)
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<From, A::Error> {
                let mut entries = Vec::new();
                while let Some(entry) = seq.next_element()? {
                    entries.push(entry);
                }
                Ok(From::Merge(entries))
            }
        }

        deserializer.deserialize_any(FromVisitor)
    }
}

impl JsonSchema for From {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "From".into()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "description": "One entity, or a merge: the driver first (no `join`), then joined entries.",
            "oneOf": [
                generator.subschema_for::<EntityRef>(),
                { "type": "array", "items": generator.subschema_for::<FromEntry>() },
            ],
        })
    }
}

/// One entry of a merge `from:` list (0003 §5.2).
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FromEntry {
    pub entity: Spanned<EntityRef>,
    /// The alias expressions use. Default: the last part of the entity name.
    #[serde(default, rename = "as", skip_serializing_if = "Option::is_none")]
    pub alias: Option<Spanned<Ident>>,
    /// Absent on the driver (the first entry), required on every other.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub join: Option<Join>,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Join {
    pub cardinality: Spanned<Cardinality>,
    /// `{<column of this entry>: <alias>.<column>}`, each pair an equality.
    pub keys: OrderedMap<Expr>,
    #[serde(default, skip_serializing_if = "JoinStrategy::is_default")]
    pub strategy: JoinStrategy,
}

/// `where:`: one expression, or named predicates that must all hold, in
/// order (0003 §5.6).
#[derive(Clone, Debug, PartialEq)]
pub enum Where {
    Expr(String),
    Named(OrderedMap<Expr>),
}

impl Serialize for Where {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Where::Expr(expr) => serializer.serialize_str(expr),
            Where::Named(map) => map.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for Where {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct WhereVisitor;

        impl<'de> Visitor<'de> for WhereVisitor {
            type Value = Where;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("an expression or a map of named predicates")
            }

            fn visit_str<E>(self, v: &str) -> Result<Where, E> {
                Ok(Where::Expr(v.to_owned()))
            }

            fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<Where, A::Error> {
                read_ordered_map(map).map(Where::Named)
            }
        }

        deserializer.deserialize_any(WhereVisitor)
    }
}

impl JsonSchema for Where {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "Where".into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "description": "An expression, or named predicates that must all hold.",
            "oneOf": [
                { "type": "string" },
                { "type": "object", "additionalProperties": { "type": "string" } },
            ],
        })
    }
}

/// A flow's `runtime:`. Only the settings whose effect is limited to the
/// flow can be overridden (0003 §6).
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FlowRuntime {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_error: Option<OnError>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_errors: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition_rows: Option<NonZeroU64>,
}

impl FlowRuntime {
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }
}

/// A target table under `to:` (0003 §5.3).
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Target {
    /// Another target of this flow that this one is a child of.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<Spanned<String>>,
    /// A path or a `many` alias: one row per element.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unnest: Option<Spanned<String>>,
    /// Target columns forming the primary key. Required when `mode: upsert`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<Spanned<Vec<Spanned<String>>>>,
    /// Filter on this target's rows.
    #[serde(default, rename = "where", skip_serializing_if = "Option::is_none")]
    pub filter: Option<Spanned<Where>>,
    pub columns: Spanned<Columns>,
}

/// `columns:`: `"*"` (copy every field) or column specs in DDL order.
#[derive(Clone, Debug, PartialEq)]
pub enum Columns {
    All,
    Map(OrderedMap<Spanned<ColumnSpec>>),
}

impl Serialize for Columns {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Columns::All => serializer.serialize_str("*"),
            Columns::Map(map) => map.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for Columns {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ColumnsVisitor;

        impl<'de> Visitor<'de> for ColumnsVisitor {
            type Value = Columns;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("\"*\" or a map of columns")
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<Columns, E> {
                match v {
                    "*" => Ok(Columns::All),
                    _ => Err(E::invalid_value(de::Unexpected::Str(v), &self)),
                }
            }

            fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<Columns, A::Error> {
                read_ordered_map(map).map(Columns::Map)
            }
        }

        deserializer.deserialize_any(ColumnsVisitor)
    }
}

impl JsonSchema for Columns {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "Columns".into()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "description": "\"*\" copies every field unchanged; otherwise target column → spec, in DDL order.",
            "oneOf": [
                { "const": "*" },
                { "type": "object", "additionalProperties": generator.subschema_for::<ColumnSpec>() },
            ],
        })
    }
}

/// One column (0003 §5.4).
#[derive(Clone, Debug, PartialEq)]
pub enum ColumnSpec {
    /// `status: lower(status)`
    Expr(String),
    /// `{expr, type, nullable, allow_lossy}`
    Typed(TypedColumn),
    /// `{nest, order_by, where, columns}`: an array built from a `many` join.
    Nest(NestColumn),
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct TypedColumn {
    pub expr: Expr,
    /// A canonical type name (B3 owns the list).
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub r#type: Option<Spanned<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nullable: Option<bool>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub allow_lossy: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct NestColumn {
    /// A `many` alias.
    pub nest: Spanned<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub order_by: Vec<Expr>,
    #[serde(rename = "where", skip_serializing_if = "Option::is_none")]
    pub filter: Option<Spanned<Where>>,
    pub columns: OrderedMap<Spanned<ColumnSpec>>,
}

/// Every key a column object may have; which ones decide its shape.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ColumnObject {
    expr: Option<Expr>,
    r#type: Option<Spanned<String>>,
    nullable: Option<bool>,
    allow_lossy: Option<bool>,
    nest: Option<Spanned<String>>,
    order_by: Option<Vec<Expr>>,
    #[serde(rename = "where")]
    filter: Option<Spanned<Where>>,
    columns: Option<OrderedMap<Spanned<ColumnSpec>>>,
}

impl ColumnObject {
    fn into_spec<E: de::Error>(self) -> Result<ColumnSpec, E> {
        let present = |set: bool, key: &str| set.then(|| format!("`{key}`"));
        match self.nest {
            Some(nest) => {
                let typed: Vec<_> = [
                    present(self.expr.is_some(), "expr"),
                    present(self.r#type.is_some(), "type"),
                    present(self.nullable.is_some(), "nullable"),
                    present(self.allow_lossy.is_some(), "allow_lossy"),
                ]
                .into_iter()
                .flatten()
                .collect();
                if !typed.is_empty() {
                    return Err(E::custom(format!(
                        "a nest column can't have {}: its columns are typed one by one",
                        typed.join(", ")
                    )));
                }
                Ok(ColumnSpec::Nest(NestColumn {
                    nest,
                    order_by: self.order_by.unwrap_or_default(),
                    filter: self.filter,
                    columns: self
                        .columns
                        .ok_or_else(|| E::custom("a nest column needs `columns`"))?,
                }))
            }
            None => {
                let nested: Vec<_> = [
                    present(self.order_by.is_some(), "order_by"),
                    present(self.filter.is_some(), "where"),
                    present(self.columns.is_some(), "columns"),
                ]
                .into_iter()
                .flatten()
                .collect();
                if !nested.is_empty() {
                    return Err(E::custom(format!(
                        "{} only apply to a nest column (one with `nest:`)",
                        nested.join(", ")
                    )));
                }
                Ok(ColumnSpec::Typed(TypedColumn {
                    expr: self
                        .expr
                        .ok_or_else(|| E::custom("a column object needs `expr` (or `nest`)"))?,
                    r#type: self.r#type,
                    nullable: self.nullable,
                    allow_lossy: self.allow_lossy.unwrap_or(false),
                }))
            }
        }
    }
}

impl Serialize for ColumnSpec {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            ColumnSpec::Expr(expr) => serializer.serialize_str(expr),
            ColumnSpec::Typed(typed) => typed.serialize(serializer),
            ColumnSpec::Nest(nest) => nest.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for ColumnSpec {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct SpecVisitor;

        impl<'de> Visitor<'de> for SpecVisitor {
            type Value = ColumnSpec;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(
                    "an expression (a string), `{expr, type, ...}` or `{nest, columns, ...}`",
                )
            }

            fn visit_str<E>(self, v: &str) -> Result<ColumnSpec, E> {
                Ok(ColumnSpec::Expr(v.to_owned()))
            }

            fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<ColumnSpec, A::Error> {
                let object = ColumnObject::deserialize(de::value::MapAccessDeserializer::new(map))?;
                object.into_spec()
            }
        }

        deserializer.deserialize_any(SpecVisitor)
    }
}

impl JsonSchema for ColumnSpec {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "ColumnSpec".into()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        let column_map = json_schema!({
            "type": "object",
            "additionalProperties": generator.subschema_for::<ColumnSpec>(),
        });
        json_schema!({
            "description": "An expression, a typed column, or a nest column.",
            "oneOf": [
                { "type": "string", "description": "An expression (B2)." },
                {
                    "type": "object",
                    "properties": {
                        "expr": { "type": "string" },
                        "type": { "type": "string", "description": "A canonical type name, e.g. `int64`, `decimal(12,2)`, `timestamp(UTC)`." },
                        "nullable": { "type": "boolean" },
                        "allow_lossy": { "type": "boolean" },
                    },
                    "required": ["expr"],
                    "additionalProperties": false,
                },
                {
                    "type": "object",
                    "properties": {
                        "nest": { "type": "string", "description": "A `many` alias." },
                        "order_by": { "type": "array", "items": { "type": "string" } },
                        "where": generator.subschema_for::<Where>(),
                        "columns": column_map,
                    },
                    "required": ["nest", "columns"],
                    "additionalProperties": false,
                },
            ],
        })
    }
}
