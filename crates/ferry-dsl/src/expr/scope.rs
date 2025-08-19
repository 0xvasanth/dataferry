//! What names an expression can see (0003 §5.4) and the batch it reads.
//!
//! A scope has one *root* that bare names resolve to (the driver record, the
//! current array element or the current joined row), plus the aliases of
//! joined `one` entries and the sigils the target allows. The batch an
//! expression is evaluated on has one column per thing in scope, laid out by
//! [`Scope::input_schema`]:
//!
//! | Column | Holds |
//! | --- | --- |
//! | the root's fields, by name | the record or element |
//! | `_ferry_missing` | paths absent from the root, relative to it (ferry-core `missing`) |
//! | `_ferry_join_<alias>` | a joined `one` row, as a struct; null when nothing matched |
//! | `_ferry_parent` | the mapped `parent:` row, as a struct |
//! | `_ferry_index` | `$index`, int64 |
//! | `_ferry_item` | `$item`, the current element |
//! | `_ferry_key` | `$key`, uuid |
//!
//! The `_ferry_` prefix is reserved by ferry-core, so none of these can
//! collide with a field.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};
use ferry_core::entity::Entity;
use ferry_core::missing::{MISSING_PATHS_COLUMN, missing_paths_field};
use ferry_core::types::{CanonicalField, CanonicalType};

pub const PARENT_COLUMN: &str = "_ferry_parent";
pub const INDEX_COLUMN: &str = "_ferry_index";
pub const KEY_COLUMN: &str = "_ferry_key";
pub const ITEM_COLUMN: &str = "_ferry_item";
pub const JOIN_PREFIX: &str = "_ferry_join_";

/// The engine-provided values (0003 §5.4).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Sigil {
    Parent,
    Index,
    Item,
    Key,
}

impl Sigil {
    pub const ALL: [Sigil; 4] = [Sigil::Parent, Sigil::Index, Sigil::Item, Sigil::Key];

    /// The bare word, which is reserved: `index` is an error, `$index` the
    /// sigil and `"index"` a field.
    pub fn word(self) -> &'static str {
        match self {
            Sigil::Parent => "parent",
            Sigil::Index => "index",
            Sigil::Item => "item",
            Sigil::Key => "key",
        }
    }

    pub fn from_word(word: &str) -> Option<Sigil> {
        Sigil::ALL.into_iter().find(|s| s.word() == word)
    }

    pub(crate) fn where_valid(self) -> &'static str {
        match self {
            Sigil::Parent => "targets with `parent:`",
            Sigil::Index | Sigil::Key => "`unnest:` targets",
            Sigil::Item => "targets that unnest a path",
        }
    }
}

/// Everything an expression can name. Build one per target (or per
/// `where:`), then compile the target's expressions against it.
#[derive(Clone, Debug)]
pub struct Scope {
    /// What bare names are, for messages: "entity `shop.orders`".
    describe: String,
    fields: Vec<CanonicalField>,
    tracks_missing: bool,
    driver_alias: Option<String>,
    joins: Vec<(String, Vec<CanonicalField>)>,
    many: Vec<String>,
    parent: Option<Vec<CanonicalField>>,
    unnest: bool,
    item: Option<CanonicalType>,
}

impl Scope {
    /// Bare names are `fields`.
    pub fn record(describe: impl Into<String>, fields: Vec<CanonicalField>) -> Self {
        Self {
            describe: describe.into(),
            fields,
            tracks_missing: false,
            driver_alias: None,
            joins: Vec::new(),
            many: Vec::new(),
            parent: None,
            unnest: false,
            item: None,
        }
    }

    /// Bare names are the entity's fields; `missing()` reads its
    /// missing-paths column if it has one.
    pub fn entity(describe: impl Into<String>, entity: &Entity) -> Self {
        let mut scope = Self::record(describe, entity.fields().to_vec());
        scope.tracks_missing = entity.tracks_missing();
        scope
    }

    /// One row per element of an array of `element` (`unnest: <path>`).
    /// Bare names are the element's fields when it is a struct; `$item` is
    /// the element itself, and `$index` and `$key` are available.
    pub fn element(describe: impl Into<String>, element: CanonicalType) -> Self {
        let fields = match &element {
            CanonicalType::Struct(fields) => fields.clone(),
            _ => Vec::new(),
        };
        let mut scope = Self::record(describe, fields);
        scope.unnest = true;
        scope.item = Some(element);
        scope
    }

    /// One row per joined row of a `many` join (`unnest: <alias>`): bare
    /// names are its fields, with `$index` and `$key`.
    pub fn joined_rows(describe: impl Into<String>, fields: Vec<CanonicalField>) -> Self {
        let mut scope = Self::record(describe, fields);
        scope.unnest = true;
        scope
    }

    pub fn tracking_missing(mut self, tracks: bool) -> Self {
        self.tracks_missing = tracks;
        self
    }

    /// The driver's own alias in a merge: `o.status` means `status`.
    pub fn with_driver_alias(mut self, alias: impl Into<String>) -> Self {
        self.driver_alias = Some(alias.into());
        self
    }

    /// A joined `one` entry, reachable as `<alias>.<field>`.
    pub fn with_join(mut self, alias: impl Into<String>, fields: Vec<CanonicalField>) -> Self {
        self.joins.push((alias.into(), fields));
        self
    }

    /// A `many` alias. It isn't a value here (it would fan out the driver),
    /// but naming it gets a better error than "unknown field".
    pub fn with_many(mut self, alias: impl Into<String>) -> Self {
        self.many.push(alias.into());
        self
    }

    /// `$parent`: the mapped row of the `parent:` target.
    pub fn with_parent(mut self, columns: Vec<CanonicalField>) -> Self {
        self.parent = Some(columns);
        self
    }

    pub fn describe(&self) -> &str {
        &self.describe
    }

    pub fn fields(&self) -> &[CanonicalField] {
        &self.fields
    }

    pub fn tracks_missing(&self) -> bool {
        self.tracks_missing
    }

    pub(crate) fn driver_alias(&self) -> Option<&str> {
        self.driver_alias.as_deref()
    }

    pub(crate) fn join(&self, alias: &str) -> Option<&[CanonicalField]> {
        self.joins
            .iter()
            .find(|(a, _)| a == alias)
            .map(|(_, f)| f.as_slice())
    }

    pub(crate) fn is_many(&self, alias: &str) -> bool {
        self.many.iter().any(|a| a == alias)
    }

    pub(crate) fn aliases(&self) -> impl Iterator<Item = &str> {
        self.driver_alias
            .as_deref()
            .into_iter()
            .chain(self.joins.iter().map(|(a, _)| a.as_str()))
    }

    pub(crate) fn sigil_field(&self, sigil: Sigil) -> Option<Field> {
        let field = match sigil {
            Sigil::Parent => Field::new(PARENT_COLUMN, struct_type(self.parent.as_ref()?), false),
            Sigil::Index if self.unnest => Field::new(INDEX_COLUMN, DataType::Int64, false),
            Sigil::Key if self.unnest => {
                CanonicalField::new(KEY_COLUMN, CanonicalType::Uuid, false).to_arrow()
            }
            Sigil::Item => CanonicalField::new(ITEM_COLUMN, self.item.clone()?, true).to_arrow(),
            _ => return None,
        };
        Some(field)
    }

    /// The schema of the batches expressions in this scope are evaluated on
    /// (see the module docs).
    pub fn input_schema(&self) -> SchemaRef {
        let mut fields: Vec<Field> = self.fields.iter().map(CanonicalField::to_arrow).collect();
        if self.tracks_missing {
            fields.push(missing_paths_field());
        }
        for (alias, joined) in &self.joins {
            fields.push(Field::new(
                format!("{JOIN_PREFIX}{alias}"),
                struct_type(joined),
                true,
            ));
        }
        fields.extend(Sigil::ALL.into_iter().filter_map(|s| self.sigil_field(s)));
        Arc::new(Schema::new(fields))
    }

    pub(crate) fn missing_column(&self) -> Option<&'static str> {
        self.tracks_missing.then_some(MISSING_PATHS_COLUMN)
    }
}

fn struct_type(fields: &[CanonicalField]) -> DataType {
    DataType::Struct(Fields::from(
        fields
            .iter()
            .map(CanonicalField::to_arrow)
            .collect::<Vec<_>>(),
    ))
}
