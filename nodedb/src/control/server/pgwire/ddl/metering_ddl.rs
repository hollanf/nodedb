//! Usage metering DDL commands.
//!
//! ```sql
//! DEFINE METERING DIMENSION 'api_calls' UNIT 'calls'
//! SHOW USAGE FOR AUTH USER 'user_42'
//! SHOW USAGE FOR ORG 'acme'
//! SHOW QUOTA FOR AUTH USER 'user_42'
//! ```

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::types::{sqlstate_error, text_field};

/// DEFINE METERING DIMENSION '<name>' UNIT '<unit>'
pub fn define_dimension(
    _state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if !identity.is_superuser {
        return Err(sqlstate_error(
            "42501",
            "permission denied: requires superuser",
        ));
    }
    if parts.len() < 5 {
        return Err(sqlstate_error(
            "42601",
            "syntax: DEFINE METERING DIMENSION '<name>' UNIT '<unit>'",
        ));
    }
    let _name = parts[3].trim_matches('\'');
    let _unit = parts
        .iter()
        .position(|p| p.to_uppercase() == "UNIT")
        .and_then(|i| parts.get(i + 1))
        .map(|s| s.trim_matches('\''))
        .unwrap_or("tokens");

    // Custom dimensions are stored in config, not in a catalog table.
    // For now, acknowledge the command.
    Ok(vec![Response::Execution(Tag::new(
        "DEFINE METERING DIMENSION",
    ))])
}

/// SHOW USAGE FOR AUTH USER '<id>' / SHOW USAGE FOR ORG '<id>'
pub fn show_usage(
    state: &SharedState,
    _identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    let (user_filter, org_filter) = parse_for_clause(parts);

    let events = state.usage_store.query(
        user_filter.as_deref(),
        org_filter.as_deref(),
        0, // All time.
    );

    let schema = Arc::new(vec![
        text_field("auth_user_id"),
        text_field("org_id"),
        text_field("collection"),
        text_field("operation"),
        text_field("tokens"),
        text_field("timestamp"),
    ]);

    let rows: Vec<_> = events
        .iter()
        .map(|e| {
            let mut enc = DataRowEncoder::new(schema.clone());
            let _ = enc.encode_field(&e.auth_user_id);
            let _ = enc.encode_field(&e.org_id);
            let _ = enc.encode_field(&e.collection);
            let _ = enc.encode_field(&e.operation);
            let _ = enc.encode_field(&e.tokens.to_string());
            let _ = enc.encode_field(&e.timestamp_secs.to_string());
            Ok(enc.take_row())
        })
        .collect();

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}

/// SHOW QUOTA FOR AUTH USER '<id>' / SHOW QUOTA FOR ORG '<id>'
pub fn show_quota(
    state: &SharedState,
    _identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    let (user_filter, _org_filter) = parse_for_clause(parts);
    let grantee_id = user_filter.as_deref().unwrap_or("");

    let quotas = state.quota_manager.list_quotas();

    let schema = Arc::new(vec![
        text_field("scope"),
        text_field("max_tokens"),
        text_field("used_tokens"),
        text_field("remaining"),
        text_field("pct_used"),
        text_field("enforcement"),
        text_field("exceeded"),
    ]);

    let rows: Vec<_> = quotas
        .iter()
        .filter_map(|q| state.quota_manager.get_status(&q.scope_name, grantee_id))
        .map(|s| {
            let mut enc = DataRowEncoder::new(schema.clone());
            let _ = enc.encode_field(&s.scope_name);
            let _ = enc.encode_field(&s.max_tokens.to_string());
            let _ = enc.encode_field(&s.used_tokens.to_string());
            let _ = enc.encode_field(&s.remaining.to_string());
            let _ = enc.encode_field(&format!("{:.1}%", s.pct_used * 100.0));
            let _ = enc.encode_field(&format!("{:?}", s.enforcement));
            let _ = enc.encode_field(&s.exceeded.to_string());
            Ok(enc.take_row())
        })
        .collect();

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}

/// SHOW USAGE FOR TENANT <id>
///
/// Returns real-time usage snapshot for a tenant from TenantUsage counters.
pub fn show_usage_for_tenant(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if !identity.is_superuser {
        return Err(sqlstate_error(
            "42501",
            "permission denied: requires superuser",
        ));
    }

    // SHOW USAGE FOR TENANT <id>
    let tid: u64 = parts
        .iter()
        .position(|p| p.eq_ignore_ascii_case("TENANT"))
        .and_then(|i| parts.get(i + 1))
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| sqlstate_error("42601", "syntax: SHOW USAGE FOR TENANT <id>"))?;

    let schema = Arc::new(vec![text_field("metric"), text_field("value")]);

    let tenants = match state.tenants.lock() {
        Ok(t) => t,
        Err(p) => p.into_inner(),
    };

    let mut rows = Vec::new();
    if let Some(usage) = tenants.usage(crate::types::TenantId::new(tid)) {
        let metrics: &[(&str, u64)] = &[
            ("memory_bytes", usage.memory_bytes),
            ("storage_bytes", usage.storage_bytes),
            ("active_requests", usage.active_requests as u64),
            ("qps_current", usage.requests_this_second as u64),
            ("total_requests", usage.total_requests),
            ("rejected_requests", usage.rejected_requests),
            ("active_connections", usage.active_connections as u64),
        ];
        for &(name, val) in metrics {
            let mut enc = DataRowEncoder::new(schema.clone());
            let _ = enc.encode_field(&name);
            let _ = enc.encode_field(&val.to_string());
            rows.push(Ok(enc.take_row()));
        }
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}

/// EXPORT USAGE FOR TENANT <id> [PERIOD '<month>'] FORMAT 'json'
///
/// Returns a billing-friendly JSON export of tenant usage from the UsageStore.
pub fn export_usage(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if !identity.is_superuser {
        return Err(sqlstate_error(
            "42501",
            "permission denied: requires superuser",
        ));
    }

    // Parse tenant_id.
    let tid: u64 = parts
        .iter()
        .position(|p| p.eq_ignore_ascii_case("TENANT"))
        .and_then(|i| parts.get(i + 1))
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| {
            sqlstate_error(
                "42601",
                "syntax: EXPORT USAGE FOR TENANT <id> [PERIOD '<month>'] FORMAT 'json'",
            )
        })?;

    // Parse optional PERIOD '<month>' (e.g., '2026-03').
    let since_secs = parts
        .iter()
        .position(|p| p.eq_ignore_ascii_case("PERIOD"))
        .and_then(|i| parts.get(i + 1))
        .and_then(|s| {
            let s = s.trim_matches('\'');
            // Parse YYYY-MM to epoch seconds.
            let mut iter = s.split('-');
            let year: i32 = iter.next()?.parse().ok()?;
            let month: u32 = iter.next()?.parse().ok()?;
            // Approximate: first day of month at midnight UTC.
            let days_since_epoch =
                (year as i64 - 1970) * 365 + (year as i64 - 1969) / 4 + (month as i64 - 1) * 30;
            Some(days_since_epoch.max(0) as u64 * 86400)
        })
        .unwrap_or(0);

    let json = state.usage_store.export_tenant_json(tid, since_secs);

    let schema = Arc::new(vec![text_field("usage_json")]);
    let mut enc = DataRowEncoder::new(schema.clone());
    let _ = enc.encode_field(&json);

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(vec![Ok(enc.take_row())]),
    ))])
}

/// Parse FOR AUTH USER '<id>' or FOR ORG '<id>' from parts.
fn parse_for_clause(parts: &[&str]) -> (Option<String>, Option<String>) {
    let for_idx = parts.iter().position(|p| p.to_uppercase() == "FOR");
    let Some(idx) = for_idx else {
        return (None, None);
    };

    let grantee_type = parts
        .get(idx + 1)
        .map(|s| s.to_uppercase())
        .unwrap_or_default();
    match grantee_type.as_str() {
        "AUTH" => {
            // FOR AUTH USER '<id>'
            let id = parts.get(idx + 3).map(|s| s.trim_matches('\'').to_string());
            (id, None)
        }
        "ORG" => {
            let id = parts.get(idx + 2).map(|s| s.trim_matches('\'').to_string());
            (None, id)
        }
        "USER" => {
            let id = parts.get(idx + 2).map(|s| s.trim_matches('\'').to_string());
            (id, None)
        }
        _ => (None, None),
    }
}
