//! Scheduler configuration — cron timezone offset and related settings.
//!
//! The scheduler evaluates cron expressions using a fixed UTC offset rather
//! than a named IANA timezone. This avoids a dependency on a timezone database
//! and eliminates DST ambiguities: the offset is always a constant number of
//! seconds, never "local time as of the next DST transition".
//!
//! IANA timezone names (e.g. `"America/New_York"`) are explicitly rejected with
//! a descriptive error so operators don't accidentally configure a name that is
//! silently treated as UTC.

use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

/// Error returned when parsing a [`CronTimezone`] from a string.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum CronTimezoneError {
    /// The string does not match the expected `±HH:MM` format.
    #[error("invalid timezone offset format '{0}': expected ±HH:MM (e.g. \"+05:30\", \"-08:00\")")]
    Format(String),

    /// The offset is syntactically valid but falls outside ±18:00.
    #[error("timezone offset '{0}' is out of range: must be between -18:00 and +18:00")]
    OutOfRange(String),

    /// An IANA timezone name was supplied instead of a numeric offset.
    #[error(
        "IANA timezone name '{0}' is not supported; use a fixed offset like \"+05:30\" instead"
    )]
    IanaNotSupported(String),
}

/// A fixed UTC offset used when evaluating cron expressions.
///
/// Stores the offset as a signed count of seconds from UTC. The permitted range
/// is `−18:00 ..= +18:00` (−64800 ..= +64800 seconds), which covers all
/// inhabited time zones with a small margin.
///
/// Serializes and deserializes as a `±HH:MM` string (e.g. `"+05:30"`,
/// `"-08:00"`). IANA names are rejected at parse time with a clear error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CronTimezone(i32);

/// Maximum absolute offset: 18 hours in seconds.
const MAX_OFFSET_SECONDS: i32 = 18 * 3600;

impl CronTimezone {
    /// UTC (zero offset).
    pub const UTC: Self = Self(0);

    /// Return the UTC offset (zero offset).
    pub fn utc() -> Self {
        Self::UTC
    }

    /// Return the signed offset in seconds.
    pub fn offset_seconds(&self) -> i32 {
        self.0
    }

    /// Parse a `±HH:MM` string into a [`CronTimezone`].
    ///
    /// Accepts both `+00:00` and `-00:00` as UTC.
    ///
    /// # Errors
    ///
    /// - [`CronTimezoneError::IanaNotSupported`] — the string contains `/` or
    ///   letters (other than a leading `+`/`-`), indicating an IANA name.
    /// - [`CronTimezoneError::Format`] — the string does not conform to `±HH:MM`.
    /// - [`CronTimezoneError::OutOfRange`] — the offset exceeds ±18:00.
    pub fn parse(s: &str) -> Result<Self, CronTimezoneError> {
        // Detect IANA names: contain '/' or alphabetic characters after the
        // optional leading sign.
        let body = s.trim_start_matches(['+', '-']);
        if body.contains('/') || body.chars().any(|c| c.is_alphabetic()) {
            return Err(CronTimezoneError::IanaNotSupported(s.to_string()));
        }

        // Must start with '+' or '-'.
        let (sign, rest) = if let Some(r) = s.strip_prefix('+') {
            (1i32, r)
        } else if let Some(r) = s.strip_prefix('-') {
            (-1i32, r)
        } else {
            return Err(CronTimezoneError::Format(s.to_string()));
        };

        // Parse HH:MM. Both parts must be pure ASCII digits (no embedded signs,
        // spaces, or other characters that Rust's integer parser would accept).
        let (hh_str, mm_str) = rest
            .split_once(':')
            .ok_or_else(|| CronTimezoneError::Format(s.to_string()))?;

        if !hh_str.chars().all(|c| c.is_ascii_digit())
            || !mm_str.chars().all(|c| c.is_ascii_digit())
        {
            return Err(CronTimezoneError::Format(s.to_string()));
        }

        let hours: i32 = hh_str
            .parse()
            .map_err(|_| CronTimezoneError::Format(s.to_string()))?;
        let minutes: i32 = mm_str
            .parse()
            .map_err(|_| CronTimezoneError::Format(s.to_string()))?;

        // Validate field ranges.
        if !(0..=23).contains(&hours) || !(0..=59).contains(&minutes) {
            return Err(CronTimezoneError::Format(s.to_string()));
        }

        let offset = sign * (hours * 3600 + minutes * 60);

        if offset.abs() > MAX_OFFSET_SECONDS {
            return Err(CronTimezoneError::OutOfRange(s.to_string()));
        }

        Ok(Self(offset))
    }

    /// Format the offset as a `±HH:MM` string.
    pub fn as_string(&self) -> String {
        let abs = self.0.unsigned_abs();
        let hh = abs / 3600;
        let mm = (abs % 3600) / 60;
        if self.0 < 0 {
            format!("-{hh:02}:{mm:02}")
        } else {
            format!("+{hh:02}:{mm:02}")
        }
    }
}

impl Default for CronTimezone {
    fn default() -> Self {
        Self::UTC
    }
}

impl Serialize for CronTimezone {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.as_string())
    }
}

struct CronTimezoneVisitor;

impl<'de> Visitor<'de> for CronTimezoneVisitor {
    type Value = CronTimezone;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a UTC offset string in ±HH:MM format (e.g. \"+05:30\")")
    }

    fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
        CronTimezone::parse(v).map_err(de::Error::custom)
    }
}

impl<'de> Deserialize<'de> for CronTimezone {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_str(CronTimezoneVisitor)
    }
}

/// Scheduler configuration section.
///
/// Controls how the cron scheduler interprets time when evaluating job
/// expressions. All cron expressions are parsed in Vixie 5-field format;
/// the [`cron_timezone`](SchedulerConfig::cron_timezone) offset shifts UTC
/// epoch time into the configured local time before field decomposition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchedulerConfig {
    /// UTC offset applied when evaluating cron expressions.
    ///
    /// Defaults to `"+00:00"` (UTC). Set to a fixed offset such as
    /// `"+05:30"` or `"-08:00"` to evaluate schedules in a local timezone.
    /// IANA timezone names are not accepted — use the numeric offset form.
    #[serde(default = "CronTimezone::utc")]
    pub cron_timezone: CronTimezone,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            cron_timezone: CronTimezone::utc(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cron_timezone_utc_default() {
        assert_eq!(CronTimezone::utc().offset_seconds(), 0);
        assert_eq!(CronTimezone::UTC.offset_seconds(), 0);
    }

    #[test]
    fn cron_timezone_parse_positive() {
        let tz = CronTimezone::parse("+05:30").unwrap();
        assert_eq!(tz.offset_seconds(), 5 * 3600 + 30 * 60);
    }

    #[test]
    fn cron_timezone_parse_negative() {
        let tz = CronTimezone::parse("-05:00").unwrap();
        assert_eq!(tz.offset_seconds(), -5 * 3600);
    }

    #[test]
    fn cron_timezone_parse_zero_positive() {
        let tz = CronTimezone::parse("+00:00").unwrap();
        assert_eq!(tz.offset_seconds(), 0);
    }

    #[test]
    fn cron_timezone_parse_zero_negative() {
        let tz = CronTimezone::parse("-00:00").unwrap();
        assert_eq!(tz.offset_seconds(), 0);
    }

    #[test]
    fn cron_timezone_format_roundtrip() {
        for s in ["+05:30", "-08:00", "+00:00", "+18:00", "-18:00", "+12:45"] {
            let parsed = CronTimezone::parse(s).unwrap();
            assert_eq!(parsed.as_string(), s, "roundtrip failed for '{s}'");
        }
    }

    #[test]
    fn cron_timezone_iana_rejected() {
        let err = CronTimezone::parse("America/New_York").unwrap_err();
        assert!(
            matches!(err, CronTimezoneError::IanaNotSupported(_)),
            "expected IanaNotSupported, got {err:?}"
        );
    }

    #[test]
    fn cron_timezone_iana_rejected_simple_names() {
        for name in ["EST", "UTC", "GMT", "PST"] {
            let err = CronTimezone::parse(name).unwrap_err();
            assert!(
                matches!(
                    err,
                    CronTimezoneError::IanaNotSupported(_) | CronTimezoneError::Format(_)
                ),
                "expected rejection for '{name}', got {err:?}"
            );
        }
    }

    #[test]
    fn cron_timezone_bad_format_rejected() {
        for s in ["5h", "5:30", "++05:00", "05:30"] {
            let err = CronTimezone::parse(s).unwrap_err();
            assert!(
                matches!(
                    err,
                    CronTimezoneError::Format(_) | CronTimezoneError::IanaNotSupported(_)
                ),
                "expected Format or IanaNotSupported for '{s}', got {err:?}"
            );
        }
    }

    #[test]
    fn cron_timezone_out_of_range_rejected() {
        let err = CronTimezone::parse("+25:00").unwrap_err();
        assert!(
            matches!(
                err,
                CronTimezoneError::OutOfRange(_) | CronTimezoneError::Format(_)
            ),
            "expected OutOfRange or Format for +25:00, got {err:?}"
        );
    }

    #[test]
    fn cron_timezone_minutes_out_of_range_rejected() {
        let err = CronTimezone::parse("+05:99").unwrap_err();
        assert!(
            matches!(err, CronTimezoneError::Format(_)),
            "expected Format for +05:99, got {err:?}"
        );
    }

    #[test]
    fn scheduler_config_toml_roundtrip() {
        let cfg = SchedulerConfig::default();
        let toml_str = toml::to_string_pretty(&cfg).expect("serialize");
        let parsed: SchedulerConfig = toml::from_str(&toml_str).expect("deserialize");
        assert_eq!(parsed, cfg);
    }

    #[test]
    fn scheduler_config_bad_tz_rejected() {
        let toml_str = r#"cron_timezone = "EST""#;
        let result: Result<SchedulerConfig, _> = toml::from_str(toml_str);
        assert!(
            result.is_err(),
            "IANA name should be rejected at deserialization"
        );
    }

    #[test]
    fn scheduler_config_explicit_offset_deserializes() {
        let toml_str = r#"cron_timezone = "+05:30""#;
        let cfg: SchedulerConfig = toml::from_str(toml_str).expect("deserialize");
        assert_eq!(cfg.cron_timezone.offset_seconds(), 5 * 3600 + 30 * 60);
    }
}
