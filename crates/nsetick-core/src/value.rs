//! Scalar conversions for the fixed-width field types.
//!
//! Kept separate from both the decoder and the filter engine because both need them and
//! they must agree exactly: a predicate that compares `txn_time` against a jiffies value has
//! to use the same clock arithmetic the decoder uses to build the timestamp column.

use anyhow::{bail, Context, Result};
use chrono::{Datelike, NaiveDate};

use crate::layout::unpad_left;

/// 65536 jiffies = 1 second, counted from 1980-01-01.
pub const JIFFIES_PER_SECOND: u64 = 65_536;

/// Seconds between the Unix epoch and the NSE jiffies epoch (1970-01-01 to 1980-01-01:
/// 3652 days, including the 1972 and 1976 leap days).
pub const JIFFIES_EPOCH_UNIX_SECONDS: i64 = 315_532_800;

const UNIX_EPOCH_DAY: i32 = 719_163; // days from 0001-01-01 to 1970-01-01

/// Parse a zero- or pad-filled integer field.
///
/// Returns `None` when the field is entirely padding (a genuine null) and `None` when it
/// contains a non-digit, which the caller distinguishes by checking the slice itself. The
/// decoder treats the latter as a hard error rather than a silent null, which is the
/// difference between noticing a misaligned file and writing 500 million NULLs.
#[inline]
pub fn parse_u64(raw: &[u8]) -> Option<u64> {
    let s = unpad_left(raw);
    if s.is_empty() {
        return None;
    }
    let mut n: u64 = 0;
    for &b in s {
        let d = b.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        n = n.wrapping_mul(10).wrapping_add(d as u64);
    }
    Some(n)
}

/// True when the field holds nothing but padding, i.e. a real null rather than bad data.
#[inline]
pub fn is_blank(raw: &[u8]) -> bool {
    unpad_left(raw).is_empty()
}

/// Convert jiffies to microseconds since the Unix epoch.
///
/// Computed as seconds and remainder rather than `j * 1_000_000 / 65536`, because a 14-digit
/// jiffies value times a million overflows u64.
#[inline]
pub fn jiffies_to_unix_micros(j: u64) -> i64 {
    let secs = j / JIFFIES_PER_SECOND;
    let rem = j % JIFFIES_PER_SECOND;
    let micros_in_epoch = secs * 1_000_000 + (rem * 1_000_000) / JIFFIES_PER_SECOND;
    micros_in_epoch as i64 + JIFFIES_EPOCH_UNIX_SECONDS * 1_000_000
}

/// Convert a `HH:MM:SS` or `HH:MM:SS.fff` wall-clock time on `date` to a jiffies value.
///
/// NSE jiffies decode directly to IST wall-clock time; no timezone conversion is applied
/// anywhere in nsetick, which is why a pre-open record reads 09:00 and not 03:30.
pub fn time_of_day_to_jiffies(hms: &str, date: NaiveDate) -> Result<u64> {
    let (clock, frac) = match hms.split_once('.') {
        Some((c, f)) => (c, Some(f)),
        None => (hms, None),
    };

    let parts: Vec<&str> = clock.split(':').collect();
    if parts.len() != 3 {
        bail!("expected HH:MM:SS, got {hms:?}");
    }
    let h: u64 = parts[0].parse().context("hour")?;
    let m: u64 = parts[1].parse().context("minute")?;
    let s: u64 = parts[2].parse().context("second")?;
    if h > 23 || m > 59 || s > 59 {
        bail!("{hms:?} is not a valid time of day");
    }

    let epoch = NaiveDate::from_ymd_opt(1980, 1, 1).expect("valid constant");
    let days = (date - epoch).num_days();
    if days < 0 {
        bail!("{date} precedes the NSE jiffies epoch of 1980-01-01");
    }

    let seconds = days as u64 * 86_400 + h * 3_600 + m * 60 + s;
    let mut jiffies = seconds * JIFFIES_PER_SECOND;

    if let Some(f) = frac {
        if f.is_empty() || !f.bytes().all(|b| b.is_ascii_digit()) {
            bail!("fractional seconds in {hms:?} must be digits");
        }
        // Interpret as a decimal fraction of a second, to whatever precision was written.
        let digits = f.len() as u32;
        let numer: u64 = f.parse().context("fractional seconds")?;
        jiffies += numer * JIFFIES_PER_SECOND / 10u64.pow(digits);
    }

    Ok(jiffies)
}

/// Days since the Unix epoch, for Arrow's Date32.
#[inline]
pub fn date_to_days(d: NaiveDate) -> i32 {
    d.num_days_from_ce() - UNIX_EPOCH_DAY
}

const MONTHS: [&[u8; 3]; 12] = [
    b"JAN", b"FEB", b"MAR", b"APR", b"MAY", b"JUN", b"JUL", b"AUG", b"SEP", b"OCT", b"NOV", b"DEC",
];

/// Parse the `ddMMMyyyy` expiry format, e.g. `28JUN2012`.
pub fn parse_date_dmmmy(raw: &[u8]) -> Option<NaiveDate> {
    if raw.len() != 9 {
        return None;
    }
    let day = parse_u64(&raw[0..2])? as u32;
    let mut mon = [0u8; 3];
    for i in 0..3 {
        mon[i] = raw[2 + i].to_ascii_uppercase();
    }
    let month = MONTHS.iter().position(|m| **m == mon)? as u32 + 1;
    let year = parse_u64(&raw[5..9])? as i32;
    NaiveDate::from_ymd_opt(year, month, day)
}

/// Parse the `YYYYMMDD` date format used by the index feed.
pub fn parse_date_ymd(raw: &[u8]) -> Option<NaiveDate> {
    if raw.len() != 8 {
        return None;
    }
    let year = parse_u64(&raw[0..4])? as i32;
    let month = parse_u64(&raw[4..6])? as u32;
    let day = parse_u64(&raw[6..8])? as u32;
    NaiveDate::from_ymd_opt(year, month, day)
}

/// Parse `HH:MM:SS` into seconds past midnight, for Arrow's Time32.
pub fn parse_time_hms(raw: &[u8]) -> Option<i32> {
    if raw.len() != 8 || raw[2] != b':' || raw[5] != b':' {
        return None;
    }
    let h = parse_u64(&raw[0..2])?;
    let m = parse_u64(&raw[3..5])?;
    let s = parse_u64(&raw[6..8])?;
    if h > 23 || m > 59 || s > 59 {
        return None;
    }
    Some((h * 3_600 + m * 60 + s) as i32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integers_parse_past_leading_zeros_and_pad() {
        assert_eq!(parse_u64(b"00000076"), Some(76));
        assert_eq!(parse_u64(b"00230000"), Some(230_000));
        assert_eq!(parse_u64(b"bbbbbb12"), Some(12));
        assert_eq!(parse_u64(b"      12"), Some(12));
        assert_eq!(parse_u64(b"00000000"), Some(0));
        assert_eq!(parse_u64(b"bbbbbbbb"), None); // all padding: a real null
        assert_eq!(parse_u64(b"0000X076"), None); // not a number
    }

    #[test]
    fn blank_detection_separates_null_from_malformed() {
        assert!(is_blank(b"bbbbbbbb"));
        assert!(is_blank(b"        "));
        assert!(!is_blank(b"0000X076"));
        assert!(!is_blank(b"00000000"));
    }

    #[test]
    fn jiffies_do_not_overflow_and_land_on_the_right_instant() {
        // Observed in CASH_Orders_27012022.DAT.gz: the first pre-open order.
        let micros = jiffies_to_unix_micros(87_014_847_291_575);
        let dt = chrono::DateTime::from_timestamp_micros(micros).unwrap();
        assert_eq!(
            dt.naive_utc().format("%Y-%m-%d %H:%M:%S%.6f").to_string(),
            "2022-01-27 09:00:00.127792"
        );
    }

    #[test]
    fn jiffies_arithmetic_survives_the_largest_plausible_value() {
        // 14 digits of 9s times a million overflows u64 if done naively.
        let j = 99_999_999_999_999u64;
        let micros = jiffies_to_unix_micros(j);
        assert!(micros > 0, "overflowed to {micros}");
    }

    #[test]
    fn time_literals_round_trip_against_the_decoder() {
        let d = NaiveDate::from_ymd_opt(2022, 1, 27).unwrap();
        let j = time_of_day_to_jiffies("09:00:00", d).unwrap();
        let micros = jiffies_to_unix_micros(j);
        let dt = chrono::DateTime::from_timestamp_micros(micros).unwrap();
        assert_eq!(
            dt.naive_utc().format("%Y-%m-%d %H:%M:%S").to_string(),
            "2022-01-27 09:00:00"
        );
        // And the real record sits just after 09:00:00 but before 09:15:00.
        assert!(87_014_847_291_575u64 > j);
        assert!(87_014_847_291_575u64 < time_of_day_to_jiffies("09:15:00", d).unwrap());
    }

    #[test]
    fn fractional_seconds_are_honoured() {
        let d = NaiveDate::from_ymd_opt(2022, 1, 27).unwrap();
        let a = time_of_day_to_jiffies("09:00:00", d).unwrap();
        let b = time_of_day_to_jiffies("09:00:00.5", d).unwrap();
        assert_eq!(b - a, JIFFIES_PER_SECOND / 2);
    }

    #[test]
    fn expiry_dates_parse() {
        assert_eq!(
            parse_date_dmmmy(b"28JUN2012"),
            NaiveDate::from_ymd_opt(2012, 6, 28)
        );
        assert_eq!(
            parse_date_dmmmy(b"27JAN2022"),
            NaiveDate::from_ymd_opt(2022, 1, 27)
        );
        assert_eq!(parse_date_dmmmy(b"28XXX2012"), None);
        assert_eq!(parse_date_dmmmy(b"short"), None);
    }

    #[test]
    fn index_dates_and_times_parse() {
        assert_eq!(
            parse_date_ymd(b"20220127"),
            NaiveDate::from_ymd_opt(2022, 1, 27)
        );
        assert_eq!(parse_time_hms(b"09:15:00"), Some(33_300));
        assert_eq!(parse_time_hms(b"25:00:00"), None);
        assert_eq!(parse_time_hms(b"09-15-00"), None);
    }

    #[test]
    fn date32_matches_the_unix_epoch() {
        assert_eq!(
            date_to_days(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()),
            0
        );
        assert_eq!(
            date_to_days(NaiveDate::from_ymd_opt(1970, 1, 2).unwrap()),
            1
        );
        assert_eq!(
            date_to_days(NaiveDate::from_ymd_opt(2022, 1, 27).unwrap()),
            19_019
        );
    }
}
