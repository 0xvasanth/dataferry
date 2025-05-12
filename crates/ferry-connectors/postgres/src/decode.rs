//! Decoding Postgres binary wire values that need more than a byte copy.

use arrow_buffer::i256;

/// Microseconds between the Unix epoch and Postgres's epoch (2000-01-01).
const PG_EPOCH_MICROS: i64 = 946_684_800_000_000;
/// Days between the Unix epoch and Postgres's epoch.
const PG_EPOCH_DAYS: i32 = 10_957;

/// A binary `numeric` as the unscaled integer of a decimal with `scale`
/// fractional digits, e.g. `12.34` at scale 3 is `12340`. Fails for NaN and
/// infinities, and if the value has more fractional digits than `scale`.
pub(crate) fn numeric_unscaled(buf: &[u8], scale: i8) -> Result<i256, String> {
    let word = |i: usize| -> Result<u16, String> {
        buf.get(i..i + 2)
            .map(|b| u16::from_be_bytes([b[0], b[1]]))
            .ok_or_else(|| "numeric value is truncated".to_string())
    };
    let ndigits = usize::from(word(0)?);
    let weight = i32::from(word(2)? as i16);
    let sign = word(4)?;
    if buf.len() != 8 + 2 * ndigits {
        return Err("numeric value is truncated".into());
    }
    let negative = match sign {
        0x0000 => false,
        0x4000 => true,
        0xC000 => return Err("NaN cannot be stored as a decimal".into()),
        _ => return Err("infinity cannot be stored as a decimal".into()),
    };
    if ndigits == 0 {
        return Ok(i256::ZERO);
    }

    let overflow = || "numeric value does not fit in 76 digits".to_string();
    let mut acc = i256::ZERO;
    for i in 0..ndigits {
        let group = i256::from_i128(i128::from(word(8 + 2 * i)?));
        acc = acc
            .checked_mul(i256::from_i128(10_000))
            .and_then(|a| a.checked_add(group))
            .ok_or_else(overflow)?;
    }
    // `acc` now holds the value times 10^implied_scale.
    let implied_scale = 4 * (ndigits as i32 - 1 - weight);
    let shift = i32::from(scale) - implied_scale;
    let ten = i256::from_i128(10);
    if shift >= 0 {
        let factor = ten.checked_pow(shift as u32).ok_or_else(overflow)?;
        acc = acc.checked_mul(factor).ok_or_else(overflow)?;
    } else {
        let divisor = ten.checked_pow((-shift) as u32).ok_or_else(overflow)?;
        if acc.checked_rem(divisor) != Some(i256::ZERO) {
            return Err(format!(
                "numeric value has more fractional digits than scale {scale}"
            ));
        }
        acc = acc.checked_div(divisor).ok_or_else(overflow)?;
    }
    Ok(if negative { acc.wrapping_neg() } else { acc })
}

/// A binary `timestamp`/`timestamptz` as microseconds since the Unix epoch.
pub(crate) fn timestamp_micros(pg_micros: i64) -> Result<i64, String> {
    if pg_micros == i64::MAX || pg_micros == i64::MIN {
        return Err("infinite timestamps cannot be migrated".into());
    }
    pg_micros
        .checked_add(PG_EPOCH_MICROS)
        .ok_or_else(|| "timestamp is out of range".into())
}

/// A binary `date` as days since the Unix epoch.
pub(crate) fn date_days(pg_days: i32) -> Result<i32, String> {
    if pg_days == i32::MAX || pg_days == i32::MIN {
        return Err("infinite dates cannot be migrated".into());
    }
    pg_days
        .checked_add(PG_EPOCH_DAYS)
        .ok_or_else(|| "date is out of range".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    const POSITIVE: u16 = 0x0000;
    const NEGATIVE: u16 = 0x4000;
    const NAN: u16 = 0xC000;
    const POS_INFINITY: u16 = 0xD000;

    /// The binary wire form of a numeric: base-10000 digit groups where the
    /// first group is multiplied by 10000^weight.
    fn wire(weight: i16, sign: u16, dscale: u16, digits: &[i16]) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend((digits.len() as i16).to_be_bytes());
        b.extend(weight.to_be_bytes());
        b.extend(sign.to_be_bytes());
        b.extend(dscale.to_be_bytes());
        for d in digits {
            b.extend(d.to_be_bytes());
        }
        b
    }

    fn unscaled(buf: &[u8], scale: i8) -> i128 {
        numeric_unscaled(buf, scale).unwrap().as_i128()
    }

    #[test]
    fn zero_has_no_digit_groups() {
        assert_eq!(unscaled(&wire(0, POSITIVE, 0, &[]), 2), 0);
    }

    #[test]
    fn digits_are_scaled_to_the_column_scale() {
        // 12345.6789
        let v = wire(1, POSITIVE, 4, &[1, 2345, 6789]);
        assert_eq!(unscaled(&v, 4), 123_456_789);
        assert_eq!(unscaled(&v, 6), 12_345_678_900);
    }

    #[test]
    fn a_short_fraction_is_padded_to_the_scale() {
        // 1.5 is sent as groups [1, 5000]; at scale 2 it is 150.
        assert_eq!(unscaled(&wire(0, POSITIVE, 1, &[1, 5000]), 2), 150);
    }

    #[test]
    fn trailing_zero_groups_are_implied_by_the_weight() {
        // 10000 is sent as the single group [1] with weight 1.
        assert_eq!(unscaled(&wire(1, POSITIVE, 0, &[1]), 0), 10_000);
        // 200000000 is [2] with weight 2.
        assert_eq!(unscaled(&wire(2, POSITIVE, 0, &[2]), 2), 20_000_000_000);
    }

    #[test]
    fn negative_and_purely_fractional_values() {
        // -0.0001
        assert_eq!(unscaled(&wire(-1, NEGATIVE, 4, &[1]), 4), -1);
        // -0.00000123 has a weight of -2.
        assert_eq!(unscaled(&wire(-2, NEGATIVE, 8, &[123]), 8), -123);
    }

    #[test]
    fn thirty_eight_nines_fit_decimal128_exactly() {
        let mut groups = vec![99];
        groups.extend([9999; 9]);
        let max = 10i128.pow(38) - 1;
        assert_eq!(unscaled(&wire(9, POSITIVE, 0, &groups), 0), max);
    }

    #[test]
    fn values_wider_than_decimal128_use_the_full_256_bits() {
        // 10^50, scale 0: group [1] with weight 12 and two leading zeros.
        let v = numeric_unscaled(&wire(12, POSITIVE, 0, &[100]), 0).unwrap();
        let expected =
            i256::from_i128(10i128.pow(25)).wrapping_mul(i256::from_i128(10i128.pow(25)));
        assert_eq!(v, expected);
    }

    #[test]
    fn more_fractional_digits_than_the_scale_is_an_error_not_a_truncation() {
        // 1.25 at scale 1 would lose the 5.
        let err = numeric_unscaled(&wire(0, POSITIVE, 2, &[1, 2500]), 1).unwrap_err();
        assert!(err.contains("scale"), "{err}");
    }

    #[test]
    fn nan_and_infinity_are_errors() {
        assert!(numeric_unscaled(&wire(0, NAN, 0, &[]), 2).is_err());
        assert!(numeric_unscaled(&wire(0, POS_INFINITY, 0, &[]), 2).is_err());
    }

    #[test]
    fn truncated_buffers_are_errors() {
        let mut v = wire(1, POSITIVE, 4, &[1, 2345, 6789]);
        v.truncate(v.len() - 1);
        assert!(numeric_unscaled(&v, 4).is_err());
        assert!(numeric_unscaled(&[0, 1], 0).is_err());
    }

    #[test]
    fn timestamps_shift_from_the_2000_epoch_to_unix() {
        assert_eq!(timestamp_micros(0), Ok(946_684_800_000_000));
        assert_eq!(timestamp_micros(-946_684_800_000_000), Ok(0));
    }

    #[test]
    fn infinite_timestamps_and_dates_are_errors() {
        assert!(timestamp_micros(i64::MAX).is_err());
        assert!(timestamp_micros(i64::MIN).is_err());
        assert!(date_days(i32::MAX).is_err());
        assert!(date_days(i32::MIN).is_err());
    }

    #[test]
    fn dates_shift_from_the_2000_epoch_to_unix() {
        assert_eq!(date_days(0), Ok(10_957));
        assert_eq!(date_days(-10_957), Ok(0));
    }
}
