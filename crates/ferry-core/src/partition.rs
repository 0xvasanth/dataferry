//! Partitions are units of work any worker can lease, process and checkpoint.
//!
//! A run plans its partitions once and stores them; resume reads them back
//! rather than re-planning, so a source that changed between attempts can't
//! shift partition boundaries under a half-finished run.

use std::cmp::Ordering;

use arrow_array::{
    Array, BinaryArray, FixedSizeBinaryArray, Int8Array, Int16Array, Int32Array, Int64Array,
    StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
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

/// Guidance from the planner on how to split an entity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionHint {
    /// Approximate rows per partition. Bounds the length of each partition's
    /// sink transaction.
    pub target_rows: u64,
    /// When set, the source must split on exactly these boundaries instead
    /// of choosing its own, or fail with
    /// [`FerryError::CoPartitionUnsupported`].
    pub co_partition: Option<CoPartition>,
}

impl PartitionHint {
    pub fn rows(target_rows: u64) -> Self {
        Self {
            target_rows,
            co_partition: None,
        }
    }
}

/// Key boundaries shared by two entities so that rows with equal keys land
/// in partitions with the same index, which lets a merge stream partition by
/// partition instead of staging one side. See `docs/decisions/0002`.
///
/// `n` boundaries `b0 < b1 < … < bn-1` define `n + 1` ranges:
/// `(-∞, b0)`, `[b0, b1)`, …, `[bn-1, +∞)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoPartition {
    column: String,
    boundaries: Vec<KeyValue>,
}

impl CoPartition {
    pub fn new(column: impl Into<String>, boundaries: Vec<KeyValue>) -> Result<Self> {
        let column = column.into();
        if let Some(w) = boundaries
            .windows(2)
            .find(|w| w[0].compare(&w[1]) != Some(Ordering::Less))
        {
            return Err(FerryError::CoPartitionUnsupported {
                column,
                reason: format!(
                    "boundaries must be strictly increasing values of one kind, got {:?} then {:?}",
                    w[0], w[1]
                ),
            });
        }
        Ok(Self { column, boundaries })
    }

    /// Boundaries of an already-planned driver entity, so another entity can
    /// be split the same way on `column`. The driver's partitions must be
    /// contiguous key ranges on one column, in order, covering every key.
    pub fn from_partitions(column: impl Into<String>, driver: &[Partition]) -> Result<Self> {
        let column = column.into();
        let unsupported = |reason: String| FerryError::CoPartitionUnsupported {
            column: column.clone(),
            reason,
        };
        let ranges = driver
            .iter()
            .map(|p| match &p.scope {
                PartitionScope::KeyRange(r) => Ok(r),
                PartitionScope::Whole => Err(unsupported(format!(
                    "driver partition {:?} is not a key range",
                    p.id
                ))),
            })
            .collect::<Result<Vec<_>>>()?;
        let (Some(first), Some(last)) = (ranges.first(), ranges.last()) else {
            return Err(unsupported("the driver has no partitions".into()));
        };
        if first.lower().is_some() || last.upper().is_some() {
            return Err(unsupported(
                "driver partitions must be unbounded at both ends to cover every key".into(),
            ));
        }
        for w in ranges.windows(2) {
            if w[0].column() != w[1].column() || w[0].upper() != w[1].lower() {
                return Err(unsupported(format!(
                    "driver partitions are not contiguous: {:?} is followed by {:?}",
                    w[0], w[1]
                )));
            }
        }
        let boundaries = ranges[1..]
            .iter()
            .map(|r| r.lower().cloned().expect("checked contiguous"))
            .collect();
        Self::new(column, boundaries)
    }

    pub fn column(&self) -> &str {
        &self.column
    }

    pub fn boundaries(&self) -> &[KeyValue] {
        &self.boundaries
    }

    /// The ranges, in order, that a co-partitioned source must produce.
    pub fn ranges(&self) -> Vec<KeyRange> {
        let lowers = std::iter::once(None).chain(self.boundaries.iter().cloned().map(Some));
        let uppers = self
            .boundaries
            .iter()
            .cloned()
            .map(Some)
            .chain(std::iter::once(None));
        lowers
            .zip(uppers)
            .map(|(lower, upper)| KeyRange {
                column: self.column.clone(),
                lower,
                upper,
            })
            .collect()
    }
}

/// Pairs each driver partition with the co-partitioned partition covering
/// the same keys. Fails unless both sides have the same number of key-range
/// partitions with identical bounds, in order.
pub fn align(driver: &[Partition], other: &[Partition]) -> Result<Vec<(Partition, Partition)>> {
    let mismatch = |reason: String| FerryError::CoPartitionUnsupported {
        column: other
            .first()
            .and_then(|p| match &p.scope {
                PartitionScope::KeyRange(r) => Some(r.column().to_owned()),
                PartitionScope::Whole => None,
            })
            .unwrap_or_default(),
        reason,
    };
    if driver.len() != other.len() {
        return Err(mismatch(format!(
            "{} driver partitions but {} co-partitioned ones",
            driver.len(),
            other.len()
        )));
    }
    driver
        .iter()
        .zip(other)
        .map(|(d, o)| match (&d.scope, &o.scope) {
            (PartitionScope::KeyRange(a), PartitionScope::KeyRange(b))
                if a.lower() == b.lower() && a.upper() == b.upper() =>
            {
                Ok((d.clone(), o.clone()))
            }
            _ => Err(mismatch(format!(
                "partition {:?} does not cover the same keys as {:?}",
                o.id, d.id
            ))),
        })
        .collect()
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
    /// Reads the key value at `row` of a key column. Fails for nulls (keys
    /// are never null) and for column types that can't be partition keys.
    pub fn from_array(array: &dyn Array, row: usize) -> Result<KeyValue> {
        let invalid = |reason: String| FerryError::InvalidKey { reason };
        if array.is_null(row) {
            return Err(invalid(format!("row {row} has a null key")));
        }
        let any = array.as_any();
        macro_rules! int {
            ($ty:ty) => {
                if let Some(a) = any.downcast_ref::<$ty>() {
                    let v = a.value(row);
                    return i64::try_from(v).map(KeyValue::Int).map_err(|_| {
                        invalid(format!("key {v} does not fit in a signed 64-bit integer"))
                    });
                }
            };
        }
        int!(Int8Array);
        int!(Int16Array);
        int!(Int32Array);
        int!(Int64Array);
        int!(UInt8Array);
        int!(UInt16Array);
        int!(UInt32Array);
        int!(UInt64Array);
        if let Some(a) = any.downcast_ref::<StringArray>() {
            return Ok(KeyValue::Str(a.value(row).to_owned()));
        }
        if let Some(a) = any.downcast_ref::<BinaryArray>() {
            return Ok(KeyValue::Bytes(a.value(row).to_vec()));
        }
        if let Some(a) = any.downcast_ref::<FixedSizeBinaryArray>() {
            return Ok(KeyValue::Bytes(a.value(row).to_vec()));
        }
        Err(invalid(format!(
            "{} columns can't be used as keys",
            array.data_type()
        )))
    }

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
