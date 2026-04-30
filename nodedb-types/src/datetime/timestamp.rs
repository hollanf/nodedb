//! Microseconds-precision UTC timestamp type.

use serde::{Deserialize, Serialize};

use super::duration::NdbDuration;
use super::error::NdbDateTimeError;

/// Microseconds-precision UTC timestamp.
///
/// Stores microseconds since Unix epoch as i64. Supports dates from
/// ~292,000 years BCE to ~292,000 years CE.
///
/// String format: ISO 8601 `"2024-03-15T10:30:00.000000Z"`.
///
/// `#[non_exhaustive]` — a timezone offset field may be added when
/// named-timezone support is introduced.
#[non_exhaustive]
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct NdbDateTime {
    /// Microseconds since Unix epoch (1970-01-01T00:00:00Z).
    pub micros: i64,
}

impl NdbDateTime {
    /// Create from microseconds since epoch.
    pub fn from_micros(micros: i64) -> Self {
        Self { micros }
    }

    /// Create from milliseconds since epoch.
    ///
    /// Returns `Err` if `millis * 1_000` overflows `i64`.
    pub fn from_millis(millis: i64) -> Result<Self, NdbDateTimeError> {
        let micros = millis
            .checked_mul(1_000)
            .ok_or(NdbDateTimeError::Overflow {
                input: millis,
                unit: "millis",
            })?;
        Ok(Self { micros })
    }

    /// Create from seconds since epoch.
    ///
    /// Returns `Err` if `secs * 1_000_000` overflows `i64`.
    pub fn from_secs(secs: i64) -> Result<Self, NdbDateTimeError> {
        let micros = secs
            .checked_mul(1_000_000)
            .ok_or(NdbDateTimeError::Overflow {
                input: secs,
                unit: "secs",
            })?;
        Ok(Self { micros })
    }

    /// Current UTC time.
    ///
    /// Converts `SystemTime` microseconds (`u128`) to `i64`. Saturates at
    /// `i64::MAX` (year ~292,277 CE) rather than wrapping — clocks that far
    /// in the future simply report the maximum representable timestamp.
    pub fn now() -> Self {
        let dur = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_else(|_| {
                use std::sync::atomic::{AtomicBool, Ordering};
                static LOGGED: AtomicBool = AtomicBool::new(false);
                if !LOGGED.swap(true, Ordering::Relaxed) {
                    tracing::error!(
                        module = module_path!(),
                        "system clock is before UNIX_EPOCH; using 0 (epoch) \
                         — check NTP/RTC configuration"
                    );
                }
                std::time::Duration::ZERO
            });
        Self {
            micros: i64::try_from(dur.as_micros()).unwrap_or(i64::MAX),
        }
    }

    /// Extract year, month, day, hour, minute, second components.
    pub fn components(&self) -> DateTimeComponents {
        let total_secs = self.micros / 1_000_000;
        let micros_rem = (self.micros % 1_000_000).unsigned_abs();

        // Civil date from Unix timestamp (algorithm from Howard Hinnant).
        let mut days = total_secs.div_euclid(86400) as i32;
        let day_secs = total_secs.rem_euclid(86400) as u32;

        days += 719_468; // shift epoch from 1970-01-01 to 0000-03-01
        let era = if days >= 0 { days } else { days - 146_096 } / 146_097;
        let doe = (days - era * 146_097) as u32;
        let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
        let y = yoe as i32 + era * 400;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let d = doy - (153 * mp + 2) / 5 + 1;
        let m = if mp < 10 { mp + 3 } else { mp - 9 };
        let year = if m <= 2 { y + 1 } else { y };

        DateTimeComponents {
            year,
            month: m as u8,
            day: d as u8,
            hour: (day_secs / 3600) as u8,
            minute: ((day_secs % 3600) / 60) as u8,
            second: (day_secs % 60) as u8,
            microsecond: micros_rem as u32,
        }
    }

    /// Format as ISO 8601 string: `"2024-03-15T10:30:00.000000Z"`.
    pub fn to_iso8601(&self) -> String {
        let c = self.components();
        format!(
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:06}Z",
            c.year, c.month, c.day, c.hour, c.minute, c.second, c.microsecond
        )
    }

    /// Parse from ISO 8601 string (basic subset).
    ///
    /// Supports: `"2024-03-15T10:30:00Z"`, `"2024-03-15T10:30:00.123456Z"`,
    /// `"2024-03-15"` (midnight UTC).
    pub fn parse(s: &str) -> Option<Self> {
        let s = s.trim().trim_end_matches('Z').trim_end_matches('z');

        if s.len() == 10 {
            // Date only: "2024-03-15" → midnight UTC.
            let parts: Vec<&str> = s.split('-').collect();
            if parts.len() != 3 {
                return None;
            }
            let year: i32 = parts[0].parse().ok()?;
            let month: u32 = parts[1].parse().ok()?;
            let day: u32 = parts[2].parse().ok()?;
            return Self::from_civil(year, month, day, 0, 0, 0, 0);
        }

        // Full: "2024-03-15T10:30:00" or "2024-03-15T10:30:00.123456"
        let (date_part, time_part) = s.split_once('T').or_else(|| s.split_once(' '))?;
        let date_parts: Vec<&str> = date_part.split('-').collect();
        if date_parts.len() != 3 {
            return None;
        }
        let year: i32 = date_parts[0].parse().ok()?;
        let month: u32 = date_parts[1].parse().ok()?;
        let day: u32 = date_parts[2].parse().ok()?;

        let (time_main, frac) = if let Some((t, f)) = time_part.split_once('.') {
            (t, f)
        } else {
            (time_part, "0")
        };
        let time_parts: Vec<&str> = time_main.split(':').collect();
        if time_parts.len() < 2 {
            return None;
        }
        let hour: u32 = time_parts[0].parse().ok()?;
        let minute: u32 = time_parts[1].parse().ok()?;
        let second: u32 = time_parts.get(2).and_then(|s| s.parse().ok()).unwrap_or(0);

        // Parse fractional seconds (up to microseconds).
        let frac_padded = format!("{frac:0<6}");
        let micros: u32 = frac_padded[..6].parse().unwrap_or(0);

        Self::from_civil(year, month, day, hour, minute, second, micros)
    }

    /// Build from civil date components.
    ///
    /// Returns `None` if any intermediate multiplication overflows `i64`.
    fn from_civil(
        year: i32,
        month: u32,
        day: u32,
        hour: u32,
        minute: u32,
        second: u32,
        micros: u32,
    ) -> Option<Self> {
        // Inverse of the Hinnant algorithm.
        let y = if month <= 2 { year - 1 } else { year };
        let m = if month <= 2 { month + 9 } else { month - 3 };
        let era = if y >= 0 { y } else { y - 399 } / 400;
        let yoe = (y - era * 400) as u32;
        let doy = (153 * m + 2) / 5 + day - 1;
        let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        let days = (era as i64)
            .checked_mul(146_097)?
            .checked_add(doe as i64)?
            .checked_sub(719_468)?;
        let total_secs = days
            .checked_mul(86400)?
            .checked_add(hour as i64 * 3600)?
            .checked_add(minute as i64 * 60)?
            .checked_add(second as i64)?;
        let result_micros = total_secs
            .checked_mul(1_000_000)?
            .checked_add(micros as i64)?;
        Some(Self {
            micros: result_micros,
        })
    }

    /// Add a duration.
    ///
    /// Returns `Err` if the result overflows `i64`.
    pub fn add_duration(&self, d: NdbDuration) -> Result<Self, NdbDateTimeError> {
        let micros = self
            .micros
            .checked_add(d.micros)
            .ok_or(NdbDateTimeError::AddOverflow)?;
        Ok(Self { micros })
    }

    /// Subtract a duration.
    ///
    /// Returns `Err` if the result overflows `i64`.
    pub fn sub_duration(&self, d: NdbDuration) -> Result<Self, NdbDateTimeError> {
        let micros = self
            .micros
            .checked_sub(d.micros)
            .ok_or(NdbDateTimeError::SubOverflow)?;
        Ok(Self { micros })
    }

    /// Duration between two timestamps (self - other).
    ///
    /// Returns `Err` if the result overflows `i64`.
    pub fn duration_since(&self, other: &NdbDateTime) -> Result<NdbDuration, NdbDateTimeError> {
        let micros = self
            .micros
            .checked_sub(other.micros)
            .ok_or(NdbDateTimeError::SubOverflow)?;
        Ok(NdbDuration { micros })
    }

    /// Unix epoch seconds.
    pub fn unix_secs(&self) -> i64 {
        self.micros / 1_000_000
    }

    /// Unix epoch milliseconds.
    pub fn unix_millis(&self) -> i64 {
        self.micros / 1_000
    }
}

impl std::fmt::Display for NdbDateTime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_iso8601())
    }
}

/// Components of a civil date-time.
#[derive(Debug, Clone, Copy)]
pub struct DateTimeComponents {
    pub year: i32,
    pub month: u8,
    pub day: u8,
    pub hour: u8,
    pub minute: u8,
    pub second: u8,
    pub microsecond: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn datetime_now_roundtrip() {
        let dt = NdbDateTime::now();
        let iso = dt.to_iso8601();
        let parsed = NdbDateTime::parse(&iso).unwrap();
        // Allow 1 microsecond rounding difference.
        assert!(
            (dt.micros - parsed.micros).abs() <= 1,
            "dt={}, parsed={}",
            dt.micros,
            parsed.micros
        );
    }

    #[test]
    fn datetime_epoch() {
        let dt = NdbDateTime::from_micros(0);
        assert_eq!(dt.to_iso8601(), "1970-01-01T00:00:00.000000Z");
    }

    #[test]
    fn datetime_known_date() {
        let dt = NdbDateTime::parse("2024-03-15T10:30:00Z").unwrap();
        let c = dt.components();
        assert_eq!(c.year, 2024);
        assert_eq!(c.month, 3);
        assert_eq!(c.day, 15);
        assert_eq!(c.hour, 10);
        assert_eq!(c.minute, 30);
        assert_eq!(c.second, 0);
    }

    #[test]
    fn datetime_fractional_seconds() {
        let dt = NdbDateTime::parse("2024-01-01T00:00:00.123456Z").unwrap();
        let c = dt.components();
        assert_eq!(c.microsecond, 123456);
    }

    #[test]
    fn datetime_date_only() {
        let dt = NdbDateTime::parse("2024-03-15").unwrap();
        let c = dt.components();
        assert_eq!(c.year, 2024);
        assert_eq!(c.month, 3);
        assert_eq!(c.day, 15);
        assert_eq!(c.hour, 0);
    }

    #[test]
    fn datetime_arithmetic() {
        let dt = NdbDateTime::parse("2024-01-01T00:00:00Z").unwrap();
        let later = dt
            .add_duration(NdbDuration::from_hours(24).expect("24 hours in range"))
            .expect("add_duration in range");
        let c = later.components();
        assert_eq!(c.day, 2);
    }

    #[test]
    fn datetime_ordering() {
        let a = NdbDateTime::parse("2024-01-01T00:00:00Z").unwrap();
        let b = NdbDateTime::parse("2024-01-02T00:00:00Z").unwrap();
        assert!(a < b);
    }

    #[test]
    fn unix_accessors() {
        let dt = NdbDateTime::from_secs(1_700_000_000).expect("known unix timestamp in range");
        assert_eq!(dt.unix_secs(), 1_700_000_000);
        assert_eq!(dt.unix_millis(), 1_700_000_000_000);
    }

    #[test]
    fn datetime_from_millis_overflow() {
        assert!(NdbDateTime::from_millis(i64::MAX).is_err());
        assert_eq!(
            NdbDateTime::from_millis(i64::MAX),
            Err(NdbDateTimeError::Overflow {
                input: i64::MAX,
                unit: "millis"
            })
        );
    }

    #[test]
    fn datetime_from_secs_overflow() {
        assert!(NdbDateTime::from_secs(i64::MAX).is_err());
        assert_eq!(
            NdbDateTime::from_secs(i64::MAX),
            Err(NdbDateTimeError::Overflow {
                input: i64::MAX,
                unit: "secs"
            })
        );
    }

    #[test]
    fn add_duration_overflow() {
        let dt = NdbDateTime::from_micros(i64::MAX);
        let one_us = NdbDuration::from_micros(1);
        assert_eq!(dt.add_duration(one_us), Err(NdbDateTimeError::AddOverflow));
    }

    #[test]
    fn sub_duration_overflow() {
        let dt = NdbDateTime::from_micros(i64::MIN);
        let one_us = NdbDuration::from_micros(1);
        assert_eq!(dt.sub_duration(one_us), Err(NdbDateTimeError::SubOverflow));
    }

    #[test]
    fn duration_since_overflow() {
        let a = NdbDateTime::from_micros(i64::MIN);
        let b = NdbDateTime::from_micros(i64::MAX);
        // i64::MIN - i64::MAX overflows
        assert_eq!(a.duration_since(&b), Err(NdbDateTimeError::SubOverflow));
    }

    #[test]
    fn now_returns_positive() {
        // Sanity: current time is after epoch.
        let dt = NdbDateTime::now();
        assert!(dt.micros > 0, "now() returned non-positive: {}", dt.micros);
    }
}
