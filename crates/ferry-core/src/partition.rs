//! Partitions are units of work any worker can lease, process and checkpoint.
//!
//! A run plans its partitions once and stores them; resume reads them back
//! rather than re-planning, so a source that changed between attempts can't
//! shift partition boundaries under a half-finished run.

use std::cmp::Ordering;

use serde::{Deserialize, Serialize};

use crate::error::{FerryError, Result};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PartitionId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Partition {
    pub id: PartitionId,
    pub entity: String,
    pub scope: PartitionScope,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PartitionScope {
    /// The whole entity in one unit of work.
    Whole,
    KeyRange(KeyRange),
}

/// A value of a partitioning key column.
///
/// The derived `Ord` (kinds first, then values) exists so keys can be sorted
/// and used in maps. Range semantics use [`KeyValue::compare`], under which
/// values of different kinds are incomparable.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum KeyValue {
    Int(i64),
    Str(String),
    /// Raw bytes compared lexicographically, e.g. a Mongo ObjectId.
    Bytes(Vec<u8>),
}

impl KeyValue {
    /// Values of different kinds are not comparable.
    pub fn compare(&self, other: &KeyValue) -> Option<Ordering> {
        match (self, other) {
            (KeyValue::Int(a), KeyValue::Int(b)) => Some(a.cmp(b)),
            (KeyValue::Str(a), KeyValue::Str(b)) => Some(a.cmp(b)),
            (KeyValue::Bytes(a), KeyValue::Bytes(b)) => Some(a.cmp(b)),
            _ => None,
        }
    }
}

/// The half-open range `[lower, upper)` over one key column. A missing bound
/// is unbounded on that side.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyRange {
    column: String,
    lower: Option<KeyValue>,
    upper: Option<KeyValue>,
}

impl KeyRange {
    pub fn new(
        column: impl Into<String>,
        lower: Option<KeyValue>,
        upper: Option<KeyValue>,
    ) -> Result<Self> {
        let column = column.into();
        if let (Some(lo), Some(hi)) = (&lower, &upper) {
            let reason = match lo.compare(hi) {
                None => Some("bounds are of different kinds"),
                Some(Ordering::Equal | Ordering::Greater) => {
                    Some("lower bound must be below upper")
                }
                Some(Ordering::Less) => None,
            };
            if let Some(reason) = reason {
                return Err(FerryError::InvalidKeyRange { column, reason });
            }
        }
        Ok(Self {
            column,
            lower,
            upper,
        })
    }

    pub fn column(&self) -> &str {
        &self.column
    }

    pub fn lower(&self) -> Option<&KeyValue> {
        self.lower.as_ref()
    }

    pub fn upper(&self) -> Option<&KeyValue> {
        self.upper.as_ref()
    }

    /// Whether `value` falls in this range. A value of a different kind than
    /// the bounds is never contained.
    pub fn contains(&self, value: &KeyValue) -> bool {
        let above_lower = match &self.lower {
            None => true,
            Some(lo) => matches!(value.compare(lo), Some(Ordering::Equal | Ordering::Greater)),
        };
        let below_upper = match &self.upper {
            None => true,
            Some(hi) => value.compare(hi) == Some(Ordering::Less),
        };
        above_lower && below_upper
    }
}
