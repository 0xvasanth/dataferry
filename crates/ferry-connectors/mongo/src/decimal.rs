//! Exact conversion of BSON Decimal128 values to scaled integers.
//!
//! Decimal128 is a floating decimal: each value carries its own exponent.
//! Arrow decimals have one scale per column, so values are rescaled to it,
//! which is exact as long as no non-zero digit is lost. Nothing goes through
//! a float.

use mongodb::bson::Decimal128;

/// A finite Decimal128 as `coefficient × 10^-scale`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Finite {
    negative: bool,
    /// Decimal digits without leading zeros (`""` for zero).
    digits: String,
    /// Digits after the decimal point. Negative for values like `1E+3`.
    scale: i32,
}

impl Finite {
    /// `None` for NaN and infinities, which no Arrow decimal can hold.
    pub(crate) fn parse(value: &Decimal128) -> Option<Self> {
        let text = value.to_string();
        let (negative, unsigned) = match text.strip_prefix('-') {
            Some(rest) => (true, rest),
            None => (false, text.as_str()),
        };
        let (mantissa, exponent) = match unsigned.split_once(['E', 'e']) {
            Some((m, e)) => (m, e.parse::<i32>().ok()?),
            None => (unsigned, 0),
        };
        let (int_part, frac_part) = mantissa.split_once('.').unwrap_or((mantissa, ""));
        if int_part.is_empty() && frac_part.is_empty()
            || !int_part
                .bytes()
                .chain(frac_part.bytes())
                .all(|b| b.is_ascii_digit())
        {
            return None;
        }
        let digits = format!("{int_part}{frac_part}")
            .trim_start_matches('0')
            .to_owned();
        Some(Self {
            negative,
            digits,
            scale: frac_part.len() as i32 - exponent,
        })
    }

    /// The scale the value was written with (`25.50` has 2), never below 0.
    pub(crate) fn scale(&self) -> u8 {
        self.scale.clamp(0, i32::from(u8::MAX)) as u8
    }

    /// Digits before the decimal point.
    pub(crate) fn integer_digits(&self) -> u8 {
        let n = self.digits.len() as i32 - self.scale;
        n.clamp(0, i32::from(u8::MAX)) as u8
    }

    /// The value scaled to `scale` digits after the point, if that loses no
    /// non-zero digit and fits `precision` digits.
    pub(crate) fn to_scaled(&self, precision: u8, scale: i8) -> Result<i128, String> {
        let shift = i32::from(scale) - self.scale;
        let digits = if shift >= 0 {
            format!("{}{}", self.digits, "0".repeat(shift as usize))
        } else {
            let keep = self
                .digits
                .len()
                .saturating_sub(shift.unsigned_abs() as usize);
            let (kept, dropped) = self.digits.split_at(keep);
            if dropped.bytes().any(|b| b != b'0') {
                return Err(format!(
                    "{} has more than {scale} digits after the decimal point",
                    self.display()
                ));
            }
            kept.to_owned()
        };
        let digits = digits.trim_start_matches('0');
        if digits.len() > usize::from(precision) {
            return Err(format!(
                "{} does not fit decimal({precision}, {scale})",
                self.display()
            ));
        }
        let magnitude = if digits.is_empty() {
            0
        } else {
            digits
                .parse::<i128>()
                .map_err(|_| format!("{} does not fit 128 bits", self.display()))?
        };
        Ok(if self.negative { -magnitude } else { magnitude })
    }

    fn display(&self) -> String {
        format!(
            "{}{}E{}",
            if self.negative { "-" } else { "" },
            if self.digits.is_empty() {
                "0"
            } else {
                &self.digits
            },
            -self.scale
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn finite(s: &str) -> Finite {
        Finite::parse(&s.parse::<Decimal128>().unwrap()).unwrap()
    }

    #[test]
    fn the_scale_is_the_one_the_value_was_written_with() {
        assert_eq!(finite("25.50").scale(), 2);
        assert_eq!(finite("1.005").scale(), 3);
        assert_eq!(finite("12").scale(), 0);
        assert_eq!(finite("1E+3").scale(), 0);
        assert_eq!(finite("1.23E-7").scale(), 9);
        assert_eq!(finite("123.45").integer_digits(), 3);
        assert_eq!(finite("0.005").integer_digits(), 0);
    }

    #[test]
    fn values_are_rescaled_exactly() {
        assert_eq!(finite("1.005").to_scaled(38, 3), Ok(1005));
        assert_eq!(finite("25.50").to_scaled(38, 3), Ok(25500));
        assert_eq!(finite("-7.25").to_scaled(38, 2), Ok(-725));
        assert_eq!(finite("1E+3").to_scaled(38, 2), Ok(100000));
        assert_eq!(finite("0.00").to_scaled(38, 2), Ok(0));
        assert_eq!(finite("-0").to_scaled(38, 0), Ok(0));
        // Trailing zeros can be dropped without losing anything.
        assert_eq!(finite("1.5000").to_scaled(38, 1), Ok(15));
    }

    #[test]
    fn dropping_a_non_zero_digit_is_an_error_not_a_rounding() {
        let err = finite("1.005").to_scaled(38, 2).unwrap_err();
        assert!(err.contains("more than 2 digits"), "{err}");
    }

    #[test]
    fn values_too_wide_for_the_precision_are_errors() {
        let max = "9".repeat(34);
        assert_eq!(
            finite(&max).to_scaled(38, 4),
            Ok(max.parse::<i128>().unwrap() * 10_000)
        );
        let err = finite(&max).to_scaled(38, 5).unwrap_err();
        assert!(err.contains("does not fit decimal(38, 5)"), "{err}");
    }

    #[test]
    fn nan_and_infinity_are_not_finite() {
        for s in ["NaN", "Infinity", "-Infinity"] {
            assert_eq!(
                Finite::parse(&s.parse::<Decimal128>().unwrap()),
                None,
                "{s}"
            );
        }
    }
}
