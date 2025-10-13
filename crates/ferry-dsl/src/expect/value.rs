//! The values an assertion computes with: exact numbers, floats and text,
//! and the per-partition accumulators of source-side aggregates.
//!
//! Arithmetic on exact numbers stays exact (`unscaled / 10^scale`, as the
//! verifier's [`Scalar`] keeps them), so `sum(total_cents) ==
//! source.sum(total * 100)` compares money without float rounding. Null
//! follows SQL: any arithmetic with null is null, and an aggregate skips
//! nulls (a `sum` over no values is null, a `count` 0).

use std::cmp::Ordering;

use arrow_array::cast::AsArray;
use arrow_array::types::{Decimal128Type, Float64Type};
use arrow_array::{Array, ArrayRef};
use arrow_schema::DataType;
use ferry_verify::Scalar;
use serde::{Deserialize, Serialize};

/// One value: an aggregate's result, a literal, or one row's value of a
/// source-side expression.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Value {
    Null,
    /// `unscaled / 10^scale`.
    Number {
        #[serde(with = "as_string")]
        unscaled: i128,
        scale: u32,
    },
    Float {
        value: f64,
    },
    Text {
        value: String,
    },
}

mod as_string {
    use serde::{Deserialize, Deserializer, Serializer, de};

    pub fn serialize<S: Serializer>(v: &i128, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&v.to_string())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<i128, D::Error> {
        String::deserialize(d)?.parse().map_err(de::Error::custom)
    }
}

/// Scales past this can't be represented by the verifier's scalars without
/// overflowing on comparison.
const MAX_SCALE: u32 = 38;

impl Value {
    pub fn int(value: i128) -> Self {
        Value::Number {
            unscaled: value,
            scale: 0,
        }
    }

    /// A literal as written: an exact number for `12`, `-0.05` or `1.50`,
    /// a float for `1e3`.
    pub fn number(text: &str) -> Option<Self> {
        match Scalar::parse(text) {
            Scalar::Number { unscaled, scale } => Some(Value::Number { unscaled, scale }),
            Scalar::Float(value) => Some(Value::Float { value }),
            _ => None,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn to_scalar(&self) -> Scalar {
        match self {
            Value::Null => Scalar::Null,
            Value::Number { unscaled, scale } => Scalar::Number {
                unscaled: *unscaled,
                scale: *scale,
            },
            Value::Float { value } => Scalar::Float(*value),
            Value::Text { value } => Scalar::Text(value.clone()),
        }
    }

    pub fn from_scalar(scalar: Scalar) -> Self {
        match scalar {
            Scalar::Null => Value::Null,
            Scalar::Number { unscaled, scale } => Value::Number { unscaled, scale },
            Scalar::Float(value) => Value::Float { value },
            Scalar::Text(value) => Value::Text { value },
        }
    }

    /// Numbers by value whatever their scale, text with text; `None` for
    /// null or a number against text.
    pub fn compare(&self, other: &Value) -> Option<Ordering> {
        self.to_scalar().compare(&other.to_scalar())
    }

    fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Number { unscaled, scale } => {
                Some(*unscaled as f64 / 10_f64.powi(*scale as i32))
            }
            Value::Float { value } => Some(*value),
            _ => None,
        }
    }

    pub fn add(&self, other: &Value) -> Result<Value, String> {
        self.exact_or_float(other, "+", |a, b| a.checked_add(b), |a, b| a + b)
    }

    pub fn sub(&self, other: &Value) -> Result<Value, String> {
        self.exact_or_float(other, "-", |a, b| a.checked_sub(b), |a, b| a - b)
    }

    pub fn mul(&self, other: &Value) -> Result<Value, String> {
        match (self, other) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (
                Value::Number {
                    unscaled: a,
                    scale: sa,
                },
                Value::Number {
                    unscaled: b,
                    scale: sb,
                },
            ) => {
                let scale = sa + sb;
                match a.checked_mul(*b) {
                    Some(unscaled) if scale <= MAX_SCALE => Ok(Value::Number { unscaled, scale }),
                    _ => Err(overflow(self, "*", other)),
                }
            }
            _ => float(self, other, "*", |a, b| a * b),
        }
    }

    /// Division is a float: an exact quotient often has no finite scale.
    pub fn div(&self, other: &Value) -> Result<Value, String> {
        match (self, other) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            _ if other.as_f64() == Some(0.0) => Err(format!("{self} / {other}: division by zero")),
            _ => float(self, other, "/", |a, b| a / b),
        }
    }

    pub fn neg(&self) -> Result<Value, String> {
        Value::int(0).sub(self)
    }

    pub fn abs(&self) -> Result<Value, String> {
        match self.compare(&Value::int(0)) {
            Some(Ordering::Less) => self.neg(),
            _ => Ok(self.clone()),
        }
    }

    /// Rounds to `digits` fractional digits, half away from zero (as the
    /// expression language's `round` does on decimals).
    pub fn round(&self, digits: i32) -> Result<Value, String> {
        match self {
            Value::Number { unscaled, scale } => {
                let drop = i64::from(*scale) - i64::from(digits);
                if drop <= 0 {
                    return Ok(self.clone());
                }
                let Some(unit) = u32::try_from(drop)
                    .ok()
                    .and_then(|d| 10_i128.checked_pow(d))
                else {
                    return Ok(Value::int(0));
                };
                let (q, r) = (unscaled / unit, unscaled % unit);
                let q = if r.abs() * 2 >= unit {
                    q + unscaled.signum()
                } else {
                    q
                };
                // Back to the asked-for digits: `round(1234.5, -2)` is 1200.
                if digits >= 0 {
                    Ok(Value::Number {
                        unscaled: q,
                        scale: digits as u32,
                    })
                } else {
                    let up = 10_i128
                        .checked_pow(digits.unsigned_abs())
                        .and_then(|p| q.checked_mul(p))
                        .ok_or_else(|| format!("round({self}, {digits}) overflows"))?;
                    Ok(Value::int(up))
                }
            }
            Value::Float { value } => {
                let p = 10_f64.powi(digits);
                Ok(Value::Float {
                    value: (value * p).round() / p,
                })
            }
            Value::Null => Ok(Value::Null),
            Value::Text { .. } => Err(format!("round({self}): not a number")),
        }
    }

    fn exact_or_float(
        &self,
        other: &Value,
        op: &str,
        exact: impl Fn(i128, i128) -> Option<i128>,
        approx: impl Fn(f64, f64) -> f64,
    ) -> Result<Value, String> {
        match (self, other) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (
                Value::Number {
                    unscaled: a,
                    scale: sa,
                },
                Value::Number {
                    unscaled: b,
                    scale: sb,
                },
            ) => {
                let scale = (*sa).max(*sb);
                let up = |v: i128, s: u32| 10_i128.checked_pow(scale - s)?.checked_mul(v);
                up(*a, *sa)
                    .zip(up(*b, *sb))
                    .and_then(|(a, b)| exact(a, b))
                    .map(|unscaled| Value::Number { unscaled, scale })
                    .ok_or_else(|| overflow(self, op, other))
            }
            _ => float(self, other, op, approx),
        }
    }
}

fn float(a: &Value, b: &Value, op: &str, f: impl Fn(f64, f64) -> f64) -> Result<Value, String> {
    match (a.as_f64(), b.as_f64()) {
        (Some(x), Some(y)) => Ok(Value::Float { value: f(x, y) }),
        _ => Err(format!("{a} {op} {b}: not numbers")),
    }
}

fn overflow(a: &Value, op: &str, b: &Value) -> String {
    format!("{a} {op} {b} overflows a 128-bit decimal")
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.to_scalar().fmt(f)
    }
}

/// Every row of `array` as a value. Integers and decimals are exact,
/// floats are floats, text is text; any other type is an error (the type
/// checker only lets these reach an aggregate that needs the values).
pub fn values(array: &ArrayRef) -> Result<Vec<Value>, String> {
    let n = array.len();
    let nulls = |f: &dyn Fn(usize) -> Value| -> Vec<Value> {
        (0..n)
            .map(|i| if array.is_valid(i) { f(i) } else { Value::Null })
            .collect()
    };
    let cast = |to: &DataType| {
        arrow_cast::cast(array, to).map_err(|e| format!("reading {}: {e}", array.data_type()))
    };
    Ok(match array.data_type() {
        dt if dt.is_integer() => {
            let a = cast(&DataType::Decimal128(38, 0))?;
            let a = a.as_primitive::<Decimal128Type>();
            nulls(&|i| Value::int(a.value(i)))
        }
        DataType::Decimal128(_, s) | DataType::Decimal256(_, s) if *s >= 0 => {
            let a = cast(&DataType::Decimal128(38, *s))?;
            let a = a.as_primitive::<Decimal128Type>();
            nulls(&|i| Value::Number {
                unscaled: a.value(i),
                scale: *s as u32,
            })
        }
        dt if dt.is_floating() => {
            let a = cast(&DataType::Float64)?;
            let a = a.as_primitive::<Float64Type>();
            nulls(&|i| Value::Float { value: a.value(i) })
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            let a = cast(&DataType::Utf8)?;
            let a = a.as_string::<i32>();
            nulls(&|i| Value::Text {
                value: a.value(i).to_owned(),
            })
        }
        other => return Err(format!("{other} values can't be aggregated")),
    })
}

/// A source-side aggregate over the records one partition attempt wrote.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "agg", rename_all = "snake_case")]
pub enum Accumulated {
    /// Values that weren't null.
    Count {
        count: u64,
    },
    /// Null until a value that isn't null is added.
    Sum {
        value: Value,
    },
    Min {
        value: Value,
    },
    Max {
        value: Value,
    },
    /// The expression failed on a record the run wrote, or the records
    /// read and written couldn't be matched: the aggregate is unknown.
    Failed {
        error: String,
    },
}

impl Accumulated {
    /// Nothing accumulated yet.
    pub fn empty(agg: super::Agg) -> Self {
        use super::Agg;
        match agg {
            Agg::Count => Accumulated::Count { count: 0 },
            Agg::Sum => Accumulated::Sum { value: Value::Null },
            Agg::Min => Accumulated::Min { value: Value::Null },
            Agg::Max => Accumulated::Max { value: Value::Null },
            Agg::CountDistinct => Accumulated::Failed {
                error: "source.count_distinct isn't supported".into(),
            },
        }
    }

    /// Adds one written record's value, or the error its expression gave.
    pub fn add(&mut self, value: Result<&Value, &str>) {
        let value = match value {
            Ok(v) => v,
            Err(error) => return self.fail(error.to_owned()),
        };
        if value.is_null() {
            return;
        }
        let next = match self {
            Accumulated::Failed { .. } => return,
            Accumulated::Count { count } => {
                *count += 1;
                return;
            }
            Accumulated::Sum { value: sum } if sum.is_null() => Ok(value.clone()),
            Accumulated::Sum { value: sum } => sum.add(value),
            Accumulated::Min { value: min } => Ok(pick(min, value, Ordering::Less)),
            Accumulated::Max { value: max } => Ok(pick(max, value, Ordering::Greater)),
        };
        match (next, self) {
            (Err(e), this) => this.fail(e),
            (
                Ok(v),
                Accumulated::Sum { value }
                | Accumulated::Min { value }
                | Accumulated::Max { value },
            ) => *value = v,
            _ => unreachable!("count returned above"),
        }
    }

    /// Folds another partition's aggregate into this one.
    pub fn merge(&mut self, other: &Accumulated) {
        let same = std::mem::discriminant(&*self) == std::mem::discriminant(other);
        match other {
            _ if matches!(self, Accumulated::Failed { .. }) => {}
            Accumulated::Failed { error } => self.fail(error.clone()),
            Accumulated::Count { count: more } if same => {
                if let Accumulated::Count { count } = self {
                    *count += more;
                }
            }
            Accumulated::Sum { value }
            | Accumulated::Min { value }
            | Accumulated::Max { value }
                if same =>
            {
                self.add(Ok(value))
            }
            _ => {
                let error = format!("can't merge {other:?} into {self:?}");
                self.fail(error)
            }
        }
    }

    fn fail(&mut self, error: String) {
        if !matches!(self, Accumulated::Failed { .. }) {
            *self = Accumulated::Failed { error };
        }
    }

    /// The aggregate's value, or why it is unknown.
    pub fn value(&self) -> Result<Value, String> {
        match self {
            Accumulated::Count { count } => Ok(Value::int(i128::from(*count))),
            Accumulated::Sum { value }
            | Accumulated::Min { value }
            | Accumulated::Max { value } => Ok(value.clone()),
            Accumulated::Failed { error } => Err(error.clone()),
        }
    }
}

/// `current` or `candidate`, whichever is `wanted` of the other. Null is
/// never kept over a value.
fn pick(current: &Value, candidate: &Value, wanted: Ordering) -> Value {
    if current.is_null() || candidate.compare(current) == Some(wanted) {
        candidate.clone()
    } else {
        current.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Decimal128Array, Float64Array, Int32Array, StringArray, UInt64Array};

    use super::*;

    fn n(text: &str) -> Value {
        Value::number(text).unwrap()
    }

    #[test]
    fn arithmetic_on_exact_numbers_stays_exact() {
        assert_eq!(n("0.1").add(&n("0.2")).unwrap(), n("0.3"));
        assert_eq!(n("1.005").mul(&n("100")).unwrap().to_string(), "100.500");
        assert_eq!(
            n("12.50").sub(&n("12.5")).unwrap().compare(&n("0")),
            Some(Ordering::Equal)
        );
        assert_eq!(n("-3").abs().unwrap(), n("3"));
        assert_eq!(n("7").neg().unwrap(), n("-7"));
        // Division is a float.
        assert_eq!(n("1").div(&n("4")).unwrap(), Value::Float { value: 0.25 });
        assert!(
            n("1")
                .div(&n("0.0"))
                .unwrap_err()
                .contains("division by zero")
        );
        // Null in, null out.
        assert_eq!(Value::Null.add(&n("1")).unwrap(), Value::Null);
        assert_eq!(n("2").mul(&Value::Null).unwrap(), Value::Null);
        // Overflow is an error, never a wrapped number.
        let big = Value::int(i128::MAX);
        assert!(big.add(&n("1")).unwrap_err().contains("overflows"));
        assert!(big.mul(&n("2")).is_err());
        // A float on either side makes a float.
        assert_eq!(
            n("1.5").add(&n("1e1")).unwrap(),
            Value::Float { value: 11.5 }
        );
    }

    #[test]
    fn round_is_half_away_from_zero() {
        assert_eq!(n("100.500").round(0).unwrap(), n("101"));
        assert_eq!(n("-100.500").round(0).unwrap(), n("-101"));
        assert_eq!(n("100.499").round(0).unwrap(), n("100"));
        assert_eq!(n("1.2345").round(2).unwrap(), n("1.23"));
        assert_eq!(n("1234.5").round(-2).unwrap(), n("1200"));
        assert_eq!(n("7").round(2).unwrap(), n("7"));
        assert_eq!(
            Value::Float { value: 2.5 }.round(0).unwrap(),
            Value::Float { value: 3.0 }
        );
    }

    #[test]
    fn reads_integers_and_decimals_exactly_and_keeps_nulls() {
        let ints: ArrayRef = Arc::new(Int32Array::from(vec![Some(3), None, Some(-4)]));
        assert_eq!(values(&ints).unwrap(), [n("3"), Value::Null, n("-4")]);
        let big: ArrayRef = Arc::new(UInt64Array::from(vec![u64::MAX]));
        assert_eq!(values(&big).unwrap(), [Value::int(u64::MAX.into())]);
        let decimals: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(1250), None])
                .with_precision_and_scale(12, 2)
                .unwrap(),
        );
        assert_eq!(values(&decimals).unwrap(), [n("12.50"), Value::Null]);
        let floats: ArrayRef = Arc::new(Float64Array::from(vec![0.5]));
        assert_eq!(values(&floats).unwrap(), [Value::Float { value: 0.5 }]);
        let text: ArrayRef = Arc::new(StringArray::from(vec!["b"]));
        assert_eq!(values(&text).unwrap(), [Value::Text { value: "b".into() }]);
        let bools: ArrayRef = Arc::new(arrow_array::BooleanArray::from(vec![true]));
        assert!(values(&bools).is_err());
    }

    #[test]
    fn accumulators_skip_nulls_and_merge_across_partitions() {
        let mut sum = Accumulated::Sum { value: Value::Null };
        sum.add(Ok(&Value::Null));
        assert_eq!(
            sum.value().unwrap(),
            Value::Null,
            "a sum of no values is null"
        );
        sum.add(Ok(&n("1.25")));
        sum.add(Ok(&n("2")));
        let mut other = Accumulated::Sum { value: n("10") };
        other.merge(&sum);
        assert_eq!(other.value().unwrap(), n("13.25"));

        let mut count = Accumulated::Count { count: 0 };
        for v in [n("1"), Value::Null, n("2")] {
            count.add(Ok(&v));
        }
        assert_eq!(count.value().unwrap(), n("2"));

        let mut min = Accumulated::Min { value: Value::Null };
        let mut max = Accumulated::Max { value: Value::Null };
        for v in [n("3"), n("-1.5"), Value::Null, n("2")] {
            min.add(Ok(&v));
            max.add(Ok(&v));
        }
        assert_eq!(min.value().unwrap(), n("-1.5"));
        assert_eq!(max.value().unwrap(), n("3"));

        // A failure sticks, through adds and merges.
        sum.add(Err("int('three') failed"));
        sum.add(Ok(&n("1")));
        assert_eq!(sum.value().unwrap_err(), "int('three') failed");
        let mut merged = Accumulated::Sum { value: n("1") };
        merged.merge(&sum);
        assert!(merged.value().is_err());
        let mut mismatched = Accumulated::Count { count: 1 };
        mismatched.merge(&Accumulated::Sum { value: n("1") });
        assert!(mismatched.value().is_err());
    }

    #[test]
    fn records_round_trip_through_json() {
        let acc = Accumulated::Sum {
            value: Value::Number {
                unscaled: i128::MAX,
                scale: 2,
            },
        };
        let text = serde_json::to_string(&acc).unwrap();
        assert_eq!(serde_json::from_str::<Accumulated>(&text).unwrap(), acc);
    }
}
