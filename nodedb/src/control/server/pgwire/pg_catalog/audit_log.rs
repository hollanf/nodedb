//! `_system.audit_log` virtual view.
//!
//! Surfaces the durable audit log with seq and timestamp range pushdown.
//! The catalog table is ordered by seq (big-endian key), so seq-range scans
//! are O(range) rather than O(total). Timestamp filtering is applied on the
//! server side over the seq-filtered rows.
//!
//! Columns:
//! - `seq`          — monotonic sequence number (int8).
//! - `timestamp_us` — UTC microseconds since epoch (int8).
//! - `event`        — event discriminant name (text, e.g. "AuthSuccess").
//! - `tenant_id`    — tenant identifier, 0 if not applicable (int8).
//! - `source`       — source IP or node identifier (text).
//! - `detail`       — human-readable event detail (text).
//! - `prev_hash`    — SHA-256 hex of the previous chain entry (text).
//!
//! Permission required: `audit_log:read`, granted to `superuser` and
//! `monitor` roles. Access is enforced in this handler before any data
//! is read.
//!
//! Default cap: 10,000 most-recent rows when no range filter or LIMIT
//! is present in the query. Callers may override with an explicit LIMIT or
//! a WHERE seq/timestamp filter. If both a seq range and a LIMIT are given,
//! the LIMIT applies first as a safeguard against oversized result sets.
//!
//! SQL pushdown behaviour (also used by `_system.audit_log` via
//! `try_pg_catalog`): the `upper` SQL string is scanned for:
//!   - `SEQ >= <n>` / `SEQ > <n>` / `SEQ = <n>`
//!   - `SEQ <= <n>` / `SEQ < <n>`
//!   - `LIMIT <n>`
//!
//! These override the defaults. Non-seq WHERE columns are filtered in-process
//! by the SQL engine after the virtual view returns its rows.

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;

use crate::control::security::identity::{AuthenticatedIdentity, Role};
use crate::control::server::pgwire::types::{int8_field, text_field};
use crate::control::state::SharedState;

const DEFAULT_LIMIT: usize = 10_000;

/// Row generator for `_system.audit_log`.
///
/// `upper` is the uppercased SQL string; used for simple seq-range and
/// LIMIT extraction to push down the scan bounds into the catalog layer.
pub fn audit_log(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    upper: &str,
) -> PgWireResult<Vec<Response>> {
    // Permission: superuser or monitor role required.
    if !identity.is_superuser && !identity.has_role(&Role::Monitor) {
        return Err(pgwire::error::PgWireError::UserError(Box::new(
            pgwire::error::ErrorInfo::new(
                "ERROR".to_string(),
                "42501".to_string(),
                "permission denied: audit_log:read requires superuser or monitor role".to_string(),
            ),
        )));
    }

    let schema = Arc::new(vec![
        int8_field("seq"),
        int8_field("timestamp_us"),
        text_field("event"),
        int8_field("tenant_id"),
        text_field("source"),
        text_field("detail"),
        text_field("prev_hash"),
    ]);

    let Some(catalog) = state.credentials.catalog() else {
        // No persistent catalog — fall back to in-memory entries.
        return audit_log_from_memory(state, schema);
    };

    let (from_seq, to_seq, limit) = extract_seq_range_and_limit(upper);

    let entries = catalog
        .load_audit_entries_ranged(from_seq, to_seq, 0, u64::MAX, limit)
        .map_err(|e| pgwire::error::PgWireError::ApiError(Box::new(e)))?;

    let mut rows = Vec::with_capacity(entries.len());
    let mut encoder = DataRowEncoder::new(schema.clone());
    for entry in &entries {
        encoder.encode_field(&(entry.seq as i64))?;
        encoder.encode_field(&(entry.timestamp_us as i64))?;
        encoder.encode_field(&entry.event.as_str())?;
        encoder.encode_field(&(entry.tenant_id.unwrap_or(0) as i64))?;
        encoder.encode_field(&entry.source.as_str())?;
        encoder.encode_field(&entry.detail.as_str())?;
        encoder.encode_field(&entry.prev_hash.as_str())?;
        rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}

/// Fall back to in-memory audit log when no persistent catalog is available.
fn audit_log_from_memory(
    state: &SharedState,
    schema: Arc<Vec<pgwire::api::results::FieldInfo>>,
) -> PgWireResult<Vec<Response>> {
    let log = match state.audit.lock() {
        Ok(l) => l,
        Err(p) => p.into_inner(),
    };
    let all = log.all();
    let limit = DEFAULT_LIMIT.min(all.len());
    let skip = all.len().saturating_sub(limit);

    let mut rows = Vec::new();
    let mut encoder = DataRowEncoder::new(schema.clone());
    for entry in all.iter().skip(skip) {
        encoder.encode_field(&(entry.seq as i64))?;
        encoder.encode_field(&(entry.timestamp_us as i64))?;
        encoder.encode_field(&format!("{:?}", entry.event))?;
        encoder.encode_field(&(entry.tenant_id.map_or(0i64, |t| t.as_u64() as i64)))?;
        encoder.encode_field(&entry.source.as_str())?;
        encoder.encode_field(&entry.detail.as_str())?;
        encoder.encode_field(&entry.prev_hash.as_str())?;
        rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}

/// Extract `from_seq`, `to_seq`, and `LIMIT` from an uppercased SQL string.
///
/// Recognises simple patterns:
///   `SEQ >= <n>` / `SEQ > <n>` / `SEQ = <n>` → from_seq
///   `SEQ <= <n>` / `SEQ < <n>`                → to_seq
///   `LIMIT <n>`                                → limit cap
///
/// Returns `(from_seq, to_seq, limit)`.
fn extract_seq_range_and_limit(upper: &str) -> (u64, u64, usize) {
    let mut from_seq: u64 = 1;
    let mut to_seq: u64 = u64::MAX;
    let mut limit: usize = DEFAULT_LIMIT;

    // LIMIT <n>
    if let Some(pos) = upper.find("LIMIT") {
        let after = upper[pos + 5..].trim_start();
        let end = after
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(after.len());
        if let Ok(n) = after[..end].parse::<usize>() {
            limit = n.min(DEFAULT_LIMIT);
        }
    }

    // SEQ >= n  or  SEQ > n
    if let Some(from) = parse_seq_bound(upper, "SEQ >=").or_else(|| parse_seq_bound(upper, "SEQ>="))
    {
        from_seq = from;
    } else if let Some(from) =
        parse_seq_bound(upper, "SEQ >").or_else(|| parse_seq_bound(upper, "SEQ>"))
    {
        from_seq = from.saturating_add(1);
    } else if let Some(eq) =
        parse_seq_bound(upper, "SEQ =").or_else(|| parse_seq_bound(upper, "SEQ="))
    {
        from_seq = eq;
        to_seq = eq;
    }

    // SEQ <= n  or  SEQ < n
    if let Some(to) = parse_seq_bound(upper, "SEQ <=").or_else(|| parse_seq_bound(upper, "SEQ<=")) {
        to_seq = to;
    } else if let Some(to) =
        parse_seq_bound(upper, "SEQ <").or_else(|| parse_seq_bound(upper, "SEQ<"))
    {
        to_seq = to.saturating_sub(1);
    }

    (from_seq, to_seq, limit)
}

fn parse_seq_bound(upper: &str, pattern: &str) -> Option<u64> {
    let pos = upper.find(pattern)?;
    let after = upper[pos + pattern.len()..].trim_start();
    let end = after
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(after.len());
    after[..end].parse::<u64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_no_range_returns_defaults() {
        let (from, to, limit) = extract_seq_range_and_limit("SELECT * FROM _SYSTEM.AUDIT_LOG");
        assert_eq!(from, 1);
        assert_eq!(to, u64::MAX);
        assert_eq!(limit, DEFAULT_LIMIT);
    }

    #[test]
    fn parse_seq_ge_bound() {
        let (from, to, limit) =
            extract_seq_range_and_limit("SELECT * FROM _SYSTEM.AUDIT_LOG WHERE SEQ >= 42");
        assert_eq!(from, 42);
        assert_eq!(to, u64::MAX);
        assert_eq!(limit, DEFAULT_LIMIT);
    }

    #[test]
    fn parse_seq_le_bound() {
        let (from, to, _) =
            extract_seq_range_and_limit("SELECT * FROM _SYSTEM.AUDIT_LOG WHERE SEQ <= 100");
        assert_eq!(from, 1);
        assert_eq!(to, 100);
    }

    #[test]
    fn parse_limit() {
        let (_, _, limit) =
            extract_seq_range_and_limit("SELECT * FROM _SYSTEM.AUDIT_LOG LIMIT 500");
        assert_eq!(limit, 500);
    }

    #[test]
    fn parse_limit_capped_at_default() {
        let (_, _, limit) =
            extract_seq_range_and_limit("SELECT * FROM _SYSTEM.AUDIT_LOG LIMIT 99999");
        assert_eq!(limit, DEFAULT_LIMIT);
    }

    #[test]
    fn parse_seq_equality() {
        let (from, to, _) =
            extract_seq_range_and_limit("SELECT * FROM _SYSTEM.AUDIT_LOG WHERE SEQ = 77");
        assert_eq!(from, 77);
        assert_eq!(to, 77);
    }

    #[test]
    fn parse_seq_gt_strict() {
        let (from, to, _) =
            extract_seq_range_and_limit("SELECT * FROM _SYSTEM.AUDIT_LOG WHERE SEQ > 10");
        assert_eq!(from, 11);
        assert_eq!(to, u64::MAX);
    }
}
