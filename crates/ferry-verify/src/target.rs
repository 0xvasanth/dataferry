//! The target as the verifier sees it after the load: rows to read back,
//! constraints to check, aggregates for assertions.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt;

use async_trait::async_trait;
use ferry_core::{BatchStream, ForeignKey, Result, TargetTable};
use serde::Serialize;

/// A sink's database, queried after the run. [`PgTarget`](crate::PgTarget)
/// is the Postgres one.
#[async_trait]
pub trait Target: Send + Sync {
    /// One line naming what is verified, with no secrets, for the report.
    fn describe(&self) -> String;

    /// Fractional-second digits the target keeps for each timestamp column
    /// of `table` that keeps fewer than 6. Write-time checksums round to
    /// these, so they match what the target stored.
    async fn timestamp_digits(&self, table: &TargetTable) -> Result<BTreeMap<String, u8>>;

    /// Every row of `table`, with its mapped columns.
    async fn read_back(&self, table: &TargetTable) -> Result<BatchStream>;

    /// Rows of `table` whose `column` is null.
    async fn null_count(&self, table: &str, column: &str) -> Result<u64>;

    /// Distinct values of `columns` that more than one row of `table` has.
    async fn duplicate_keys(&self, table: &str, columns: &[String]) -> Result<u64>;

    /// Rows of `table` whose foreign key has no row in the table it
    /// references. A key with a null column references nothing and is not
    /// an orphan (SQL's `MATCH SIMPLE`).
    async fn orphans(&self, table: &str, foreign_key: &ForeignKey) -> Result<u64>;

    /// `aggregate` over `table.column`, or over the table's rows for
    /// `count` without a column.
    async fn aggregate(
        &self,
        table: &str,
        aggregate: Aggregate,
        column: Option<&str>,
    ) -> Result<Scalar>;
}

/// The target-side aggregates `expect:` assertions use (0003 §5.9).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Aggregate {
    Count,
    Sum,
    Min,
    Max,
    CountDistinct,
}

/// An aggregate's value.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
pub enum Scalar {
    Null,
    /// An exact number: `unscaled / 10^scale`.
    Number {
        #[serde(serialize_with = "as_string")]
        unscaled: i128,
        scale: u32,
    },
    Float(f64),
    Text(String),
}

fn as_string<S: serde::Serializer>(v: &i128, s: S) -> std::result::Result<S::Ok, S::Error> {
    s.serialize_str(&v.to_string())
}

impl Scalar {
    pub fn int(value: i128) -> Self {
        Scalar::Number {
            unscaled: value,
            scale: 0,
        }
    }

    /// An exact number from its decimal spelling (`-12.50`), a float from
    /// any other number (`1.5e+20`, `NaN`), and text otherwise.
    pub fn parse(text: &str) -> Self {
        let digits = text.strip_prefix('-').unwrap_or(text);
        let (whole, fraction) = digits.split_once('.').unwrap_or((digits, ""));
        let exact = !whole.is_empty()
            && whole.bytes().all(|b| b.is_ascii_digit())
            && fraction.bytes().all(|b| b.is_ascii_digit());
        if exact && let Ok(unscaled) = format!("{whole}{fraction}").parse::<i128>() {
            let unscaled = if text.starts_with('-') {
                -unscaled
            } else {
                unscaled
            };
            return Scalar::Number {
                unscaled,
                scale: fraction.len() as u32,
            };
        }
        match text.parse::<f64>() {
            Ok(v) => Scalar::Float(v),
            Err(_) => Scalar::Text(text.to_owned()),
        }
    }

    /// Numbers compare by value whatever their scale or kind, text with
    /// text. `None` for null, or for a number against text.
    pub fn compare(&self, other: &Scalar) -> Option<Ordering> {
        use Scalar::*;
        match (self, other) {
            (
                Number {
                    unscaled: a,
                    scale: sa,
                },
                Number {
                    unscaled: b,
                    scale: sb,
                },
            ) => {
                let scale = (*sa).max(*sb);
                let up = |v: i128, s: u32| 10_i128.checked_pow(scale - s)?.checked_mul(v);
                match (up(*a, *sa), up(*b, *sb)) {
                    (Some(a), Some(b)) => Some(a.cmp(&b)),
                    _ => self.as_f64()?.partial_cmp(&other.as_f64()?),
                }
            }
            (Text(a), Text(b)) => Some(a.cmp(b)),
            (Number { .. } | Float(_), Number { .. } | Float(_)) => {
                self.as_f64()?.partial_cmp(&other.as_f64()?)
            }
            _ => None,
        }
    }

    fn as_f64(&self) -> Option<f64> {
        match self {
            Scalar::Number { unscaled, scale } => {
                Some(*unscaled as f64 / 10_f64.powi(*scale as i32))
            }
            Scalar::Float(v) => Some(*v),
            _ => None,
        }
    }
}

impl fmt::Display for Scalar {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Scalar::Null => f.write_str("null"),
            Scalar::Number { unscaled, scale: 0 } => write!(f, "{unscaled}"),
            Scalar::Number { unscaled, scale } => {
                let digits = unscaled.unsigned_abs().to_string();
                let scale = *scale as usize;
                let digits = format!("{digits:0>width$}", width = scale + 1);
                let (whole, fraction) = digits.split_at(digits.len() - scale);
                let sign = if *unscaled < 0 { "-" } else { "" };
                write!(f, "{sign}{whole}.{fraction}")
            }
            Scalar::Float(v) => write!(f, "{v}"),
            Scalar::Text(t) => write!(f, "{t:?}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_compares_numbers_by_value() {
        assert_eq!(
            Scalar::parse("-12.50"),
            Scalar::Number {
                unscaled: -1250,
                scale: 2
            }
        );
        assert_eq!(Scalar::parse("42"), Scalar::int(42));
        assert_eq!(Scalar::parse("1.5e+20"), Scalar::Float(1.5e20));
        assert_eq!(
            Scalar::parse("2024-01-01 00:00:00+00"),
            Scalar::Text("2024-01-01 00:00:00+00".into())
        );
        assert_eq!(
            Scalar::parse("12.5").compare(&Scalar::parse("12.500")),
            Some(Ordering::Equal)
        );
        assert_eq!(
            Scalar::int(3).compare(&Scalar::Float(2.5)),
            Some(Ordering::Greater)
        );
        assert_eq!(Scalar::Null.compare(&Scalar::Null), None);
        assert_eq!(Scalar::parse("-0.05").to_string(), "-0.05");
        assert_eq!(Scalar::parse("1234.50").to_string(), "1234.50");
    }
}
