//! Extract and strip NodeDB-specific bitemporal clauses before sqlparser
//! sees the statement.
//!
//! Supported forms (case-insensitive, whitespace-tolerant):
//!
//! ```sql
//! SELECT ... FROM t FOR SYSTEM_TIME AS OF 1700000000000 WHERE ...
//! SELECT ... FROM t FOR VALID_TIME CONTAINS 1700000000000 WHERE ...
//! SELECT ... FROM t FOR VALID_TIME FROM 1700000000000 TO 1700001000000 WHERE ...
//! ```
//!
//! A function-form escape hatch is also accepted, anywhere in the statement,
//! for pgwire drivers that reject non-standard clauses:
//!
//! ```sql
//! SELECT __system_as_of__(1700000000000) FROM t WHERE ...
//! ```
//!
//! All three canonical clauses are recognised once per statement. Repeated
//! clauses return `Err`; conflicting valid-time forms (CONTAINS + FROM/TO)
//! return `Err`.
//!
//! Timestamps are **milliseconds since Unix epoch**.

use crate::temporal::{TemporalScope, ValidTime};

/// Output of the temporal preprocess stage.
pub struct Extracted {
    /// SQL with every temporal clause stripped, safe to hand to sqlparser.
    pub sql: String,
    /// Extracted temporal qualifier. Default when no clause was present.
    pub temporal: TemporalScope,
}

/// Error shape — surfaced as `SqlError::Parse` by the caller.
#[derive(Debug)]
pub struct TemporalParseError(pub String);

/// Strip temporal clauses from `sql` and return the rewritten text plus the
/// extracted `TemporalScope`. Returns `Ok(None)` when no temporal clause is
/// present so the caller can short-circuit to the existing pipeline.
pub fn extract(sql: &str) -> Result<Option<Extracted>, TemporalParseError> {
    let mut scope = TemporalScope::default();
    let mut working = sql.to_string();
    let mut any = false;

    if let Some((rewritten, ms)) = strip_system_time_as_of(&working)? {
        working = rewritten;
        scope.system_as_of_ms = Some(ms);
        any = true;
    }
    if let Some((rewritten, ms)) = strip_system_as_of_function(&working)? {
        if scope.system_as_of_ms.is_some() {
            return Err(TemporalParseError(
                "multiple FOR SYSTEM_TIME / __system_as_of__ clauses".into(),
            ));
        }
        working = rewritten;
        scope.system_as_of_ms = Some(ms);
        any = true;
    }
    if let Some((rewritten, vt)) = strip_valid_time(&working)? {
        working = rewritten;
        scope.valid_time = vt;
        any = true;
    }

    if any {
        Ok(Some(Extracted {
            sql: working,
            temporal: scope,
        }))
    } else {
        Ok(None)
    }
}

/// Match `FOR SYSTEM_TIME AS OF <integer>` case-insensitively and strip it.
fn strip_system_time_as_of(sql: &str) -> Result<Option<(String, i64)>, TemporalParseError> {
    let upper = sql.to_uppercase();
    let Some(start) = upper.find("FOR SYSTEM_TIME") else {
        return Ok(None);
    };
    // Locate `AS OF` after the keyword.
    let after_kw = start + "FOR SYSTEM_TIME".len();
    let tail = &upper[after_kw..];
    let Some(as_of_rel) = tail.trim_start().strip_prefix("AS OF") else {
        return Err(TemporalParseError(
            "FOR SYSTEM_TIME must be followed by AS OF <ms>".into(),
        ));
    };
    let leading_ws = tail.len() - tail.trim_start().len();
    let as_of_abs = after_kw + leading_ws + "AS OF".len();
    let (ms, end_abs) = parse_trailing_i64(sql, as_of_abs)?;
    let mut out = String::with_capacity(sql.len());
    out.push_str(&sql[..start]);
    out.push(' ');
    out.push_str(&sql[end_abs..]);
    let _ = as_of_rel; // silence unused-binding lint on case-normalised view
    Ok(Some((out, ms)))
}

/// Match `__system_as_of__(<int>)` anywhere in the statement and strip the
/// call by replacing it with the literal `TRUE` so the surrounding
/// expression still parses (e.g. `SELECT __system_as_of__(100), x` →
/// `SELECT TRUE, x`). Returning `TRUE` keeps the projection column count
/// stable; the planner ignores the synthetic column because the temporal
/// scope is carried out-of-band.
fn strip_system_as_of_function(sql: &str) -> Result<Option<(String, i64)>, TemporalParseError> {
    let upper = sql.to_uppercase();
    let Some(start) = upper.find("__SYSTEM_AS_OF__(") else {
        return Ok(None);
    };
    let after_open = start + "__SYSTEM_AS_OF__(".len();
    let Some(close_rel) = sql[after_open..].find(')') else {
        return Err(TemporalParseError(
            "__system_as_of__(...) missing closing paren".into(),
        ));
    };
    let close_abs = after_open + close_rel;
    let arg = sql[after_open..close_abs].trim();
    let ms: i64 = arg
        .parse()
        .map_err(|_| TemporalParseError(format!("__system_as_of__ arg not i64: {arg}")))?;
    let mut out = String::with_capacity(sql.len());
    out.push_str(&sql[..start]);
    out.push_str("TRUE");
    out.push_str(&sql[close_abs + 1..]);
    Ok(Some((out, ms)))
}

/// Match `FOR VALID_TIME CONTAINS <int>` or
/// `FOR VALID_TIME FROM <int> TO <int>` and strip whichever is present.
fn strip_valid_time(sql: &str) -> Result<Option<(String, ValidTime)>, TemporalParseError> {
    let upper = sql.to_uppercase();
    let Some(start) = upper.find("FOR VALID_TIME") else {
        return Ok(None);
    };
    let after_kw = start + "FOR VALID_TIME".len();
    let tail_upper = upper[after_kw..].trim_start();
    let leading_ws = upper[after_kw..].len() - tail_upper.len();

    if let Some(rest) = tail_upper.strip_prefix("CONTAINS") {
        let arg_abs = after_kw + leading_ws + "CONTAINS".len();
        let (ms, end_abs) = parse_trailing_i64(sql, arg_abs)?;
        let _ = rest;
        let mut out = String::with_capacity(sql.len());
        out.push_str(&sql[..start]);
        out.push(' ');
        out.push_str(&sql[end_abs..]);
        return Ok(Some((out, ValidTime::At(ms))));
    }
    if let Some(rest) = tail_upper.strip_prefix("FROM") {
        let arg_abs = after_kw + leading_ws + "FROM".len();
        let (lo, lo_end) = parse_trailing_i64(sql, arg_abs)?;
        let after_lo_upper = upper[lo_end..].trim_start();
        let leading_ws2 = upper[lo_end..].len() - after_lo_upper.len();
        let Some(_after_to) = after_lo_upper.strip_prefix("TO") else {
            return Err(TemporalParseError(
                "FOR VALID_TIME FROM <ms> must be followed by TO <ms>".into(),
            ));
        };
        let hi_arg = lo_end + leading_ws2 + "TO".len();
        let (hi, hi_end) = parse_trailing_i64(sql, hi_arg)?;
        let _ = rest;
        let mut out = String::with_capacity(sql.len());
        out.push_str(&sql[..start]);
        out.push(' ');
        out.push_str(&sql[hi_end..]);
        if hi <= lo {
            return Err(TemporalParseError(format!(
                "FOR VALID_TIME FROM {lo} TO {hi}: hi must be > lo"
            )));
        }
        return Ok(Some((out, ValidTime::Range(lo, hi))));
    }
    Err(TemporalParseError(
        "FOR VALID_TIME must be followed by CONTAINS or FROM".into(),
    ))
}

/// Parse an optionally-signed integer starting at `offset` in `sql`, skipping
/// leading whitespace. Returns `(value, end_offset_exclusive)`.
fn parse_trailing_i64(sql: &str, offset: usize) -> Result<(i64, usize), TemporalParseError> {
    let rest = &sql[offset..];
    let trimmed = rest.trim_start();
    let leading = rest.len() - trimmed.len();
    let end_rel = trimmed
        .char_indices()
        .take_while(|(i, c)| c.is_ascii_digit() || (*i == 0 && *c == '-'))
        .map(|(i, c)| i + c.len_utf8())
        .last()
        .ok_or_else(|| TemporalParseError(format!("expected integer at offset {offset}")))?;
    let num_str = &trimmed[..end_rel];
    let v: i64 = num_str
        .parse()
        .map_err(|_| TemporalParseError(format!("not an i64: {num_str}")))?;
    Ok((v, offset + leading + end_rel))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn passthrough_no_temporal() {
        assert!(extract("SELECT * FROM t WHERE x = 1").unwrap().is_none());
    }

    #[test]
    fn system_time_as_of() {
        let ex = extract("SELECT * FROM t FOR SYSTEM_TIME AS OF 100 WHERE x = 1")
            .unwrap()
            .unwrap();
        assert_eq!(ex.temporal.system_as_of_ms, Some(100));
        assert_eq!(ex.temporal.valid_time, ValidTime::Any);
        assert!(!ex.sql.to_uppercase().contains("FOR SYSTEM_TIME"));
        assert!(ex.sql.contains("WHERE x = 1"));
    }

    #[test]
    fn valid_time_contains() {
        let ex = extract("SELECT * FROM t FOR VALID_TIME CONTAINS 250")
            .unwrap()
            .unwrap();
        assert_eq!(ex.temporal.valid_time, ValidTime::At(250));
        assert!(!ex.sql.to_uppercase().contains("FOR VALID_TIME"));
    }

    #[test]
    fn valid_time_range() {
        let ex = extract("SELECT * FROM t FOR VALID_TIME FROM 100 TO 300 LIMIT 10")
            .unwrap()
            .unwrap();
        assert_eq!(ex.temporal.valid_time, ValidTime::Range(100, 300));
        assert!(ex.sql.contains("LIMIT 10"));
    }

    #[test]
    fn combined_system_and_valid() {
        let ex = extract(
            "SELECT * FROM t FOR SYSTEM_TIME AS OF 500 FOR VALID_TIME CONTAINS 250 LIMIT 5",
        )
        .unwrap()
        .unwrap();
        assert_eq!(ex.temporal.system_as_of_ms, Some(500));
        assert_eq!(ex.temporal.valid_time, ValidTime::At(250));
    }

    #[test]
    fn function_form() {
        let ex = extract("SELECT __system_as_of__(777), x FROM t")
            .unwrap()
            .unwrap();
        assert_eq!(ex.temporal.system_as_of_ms, Some(777));
        assert!(ex.sql.contains("TRUE"));
        assert!(!ex.sql.contains("__system_as_of__"));
    }

    #[test]
    fn invalid_range_rejects() {
        assert!(extract("SELECT * FROM t FOR VALID_TIME FROM 500 TO 100").is_err());
    }

    #[test]
    fn valid_time_missing_verb() {
        assert!(extract("SELECT * FROM t FOR VALID_TIME 100").is_err());
    }
}
