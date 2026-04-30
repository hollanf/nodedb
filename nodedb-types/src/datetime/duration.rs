//! Microseconds-precision signed duration type.

use serde::{Deserialize, Serialize};

use super::error::NdbDateTimeError;

/// Microseconds-precision duration (signed).
///
/// String format: human-readable `"1h30m15s"` or `"500ms"`.
///
/// `#[non_exhaustive]` — a `months` field for calendar-interval semantics
/// may be added alongside the microsecond component.
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
pub struct NdbDuration {
    /// Microseconds (signed: negative = past).
    pub micros: i64,
}

impl NdbDuration {
    pub fn from_micros(micros: i64) -> Self {
        Self { micros }
    }

    /// Create from milliseconds.
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

    /// Create from seconds.
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

    /// Create from minutes.
    ///
    /// Returns `Err` if `mins * 60_000_000` overflows `i64`.
    pub fn from_minutes(mins: i64) -> Result<Self, NdbDateTimeError> {
        let micros = mins
            .checked_mul(60_000_000)
            .ok_or(NdbDateTimeError::Overflow {
                input: mins,
                unit: "minutes",
            })?;
        Ok(Self { micros })
    }

    /// Create from hours.
    ///
    /// Returns `Err` if `hours * 3_600_000_000` overflows `i64`.
    pub fn from_hours(hours: i64) -> Result<Self, NdbDateTimeError> {
        let micros = hours
            .checked_mul(3_600_000_000)
            .ok_or(NdbDateTimeError::Overflow {
                input: hours,
                unit: "hours",
            })?;
        Ok(Self { micros })
    }

    /// Create from days.
    ///
    /// Returns `Err` if `days * 86_400_000_000` overflows `i64`.
    pub fn from_days(days: i64) -> Result<Self, NdbDateTimeError> {
        let micros = days
            .checked_mul(86_400_000_000)
            .ok_or(NdbDateTimeError::Overflow {
                input: days,
                unit: "days",
            })?;
        Ok(Self { micros })
    }

    pub fn as_secs_f64(&self) -> f64 {
        self.micros as f64 / 1_000_000.0
    }

    pub fn as_millis(&self) -> i64 {
        self.micros / 1_000
    }

    /// Format as human-readable string.
    pub fn to_human(&self) -> String {
        let abs = self.micros.unsigned_abs();
        let sign = if self.micros < 0 { "-" } else { "" };

        if abs < 1_000 {
            return format!("{sign}{abs}us");
        }
        if abs < 1_000_000 {
            return format!("{sign}{}ms", abs / 1_000);
        }

        let total_secs = abs / 1_000_000;
        let hours = total_secs / 3600;
        let mins = (total_secs % 3600) / 60;
        let secs = total_secs % 60;

        if hours > 0 {
            if mins > 0 || secs > 0 {
                format!("{sign}{hours}h{mins}m{secs}s")
            } else {
                format!("{sign}{hours}h")
            }
        } else if mins > 0 {
            if secs > 0 {
                format!("{sign}{mins}m{secs}s")
            } else {
                format!("{sign}{mins}m")
            }
        } else {
            format!("{sign}{secs}s")
        }
    }

    /// Parse from human-readable string: "1h30m", "500ms", "30s", "2d".
    ///
    /// Returns `None` if the string is malformed or any multiplication overflows.
    pub fn parse(s: &str) -> Option<Self> {
        let s = s.trim();
        if s.is_empty() {
            return None;
        }

        let (neg, s) = if let Some(rest) = s.strip_prefix('-') {
            (true, rest)
        } else {
            (false, s)
        };

        // Simple suffix parsing.
        if let Some(n) = s.strip_suffix("us") {
            let v: i64 = n.trim().parse().ok()?;
            return Some(Self::from_micros(if neg { -v } else { v }));
        }
        if let Some(n) = s.strip_suffix("ms") {
            let v: i64 = n.trim().parse().ok()?;
            let d = Self::from_millis(if neg { -v } else { v }).ok()?;
            return Some(d);
        }
        if let Some(n) = s.strip_suffix('d') {
            let v: i64 = n.trim().parse().ok()?;
            let d = Self::from_days(if neg { -v } else { v }).ok()?;
            return Some(d);
        }

        // Compound: "1h30m15s"
        let mut total_micros: i64 = 0;
        let mut num_buf = String::new();
        for c in s.chars() {
            if c.is_ascii_digit() {
                num_buf.push(c);
            } else {
                let n: i64 = num_buf.parse().ok()?;
                num_buf.clear();
                let part = match c {
                    'h' => n.checked_mul(3_600_000_000)?,
                    'm' => n.checked_mul(60_000_000)?,
                    's' => n.checked_mul(1_000_000)?,
                    _ => return None,
                };
                total_micros = total_micros.checked_add(part)?;
            }
        }
        // Trailing number without suffix = seconds.
        if !num_buf.is_empty() {
            let n: i64 = num_buf.parse().ok()?;
            let part = n.checked_mul(1_000_000)?;
            total_micros = total_micros.checked_add(part)?;
        }

        if total_micros == 0 {
            return None;
        }

        Some(Self::from_micros(if neg {
            -total_micros
        } else {
            total_micros
        }))
    }
}

impl std::fmt::Display for NdbDuration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_human())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duration_human_format() {
        assert_eq!(
            NdbDuration::from_secs(90).expect("90s in range").to_human(),
            "1m30s"
        );
        assert_eq!(
            NdbDuration::from_hours(2).expect("2h in range").to_human(),
            "2h"
        );
        assert_eq!(
            NdbDuration::from_millis(500)
                .expect("500ms in range")
                .to_human(),
            "500ms"
        );
        assert_eq!(NdbDuration::from_micros(42).to_human(), "42us");
        assert_eq!(
            NdbDuration::from_secs(3661)
                .expect("3661s in range")
                .to_human(),
            "1h1m1s"
        );
    }

    #[test]
    fn duration_parse() {
        assert_eq!(NdbDuration::parse("30s").unwrap().micros, 30_000_000);
        assert_eq!(NdbDuration::parse("1h30m").unwrap().micros, 5_400_000_000);
        assert_eq!(NdbDuration::parse("500ms").unwrap().micros, 500_000);
        assert_eq!(NdbDuration::parse("2d").unwrap().micros, 172_800_000_000);
        assert_eq!(NdbDuration::parse("-5s").unwrap().micros, -5_000_000);
    }

    #[test]
    fn duration_roundtrip() {
        let d = NdbDuration::from_secs(3661).expect("3661s in range");
        let s = d.to_human();
        let parsed = NdbDuration::parse(&s).unwrap();
        assert_eq!(d.micros, parsed.micros);
    }

    #[test]
    fn duration_from_millis_overflow() {
        assert!(NdbDuration::from_millis(i64::MAX).is_err());
    }

    #[test]
    fn duration_from_secs_overflow() {
        assert!(NdbDuration::from_secs(i64::MAX).is_err());
    }

    #[test]
    fn duration_from_minutes_overflow() {
        assert!(NdbDuration::from_minutes(i64::MAX).is_err());
    }

    #[test]
    fn duration_from_hours_overflow() {
        assert!(NdbDuration::from_hours(i64::MAX).is_err());
    }

    #[test]
    fn duration_from_days_overflow() {
        assert!(NdbDuration::from_days(i64::MAX).is_err());
    }

    #[test]
    fn duration_parse_overflow() {
        // Enough hours to overflow: i64::MAX / 3_600_000_000 ≈ 2_562_047_788
        let overflow_str = "9999999999999999h";
        assert!(NdbDuration::parse(overflow_str).is_none());
    }
}
