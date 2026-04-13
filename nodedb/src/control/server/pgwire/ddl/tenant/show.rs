//! `SHOW TENANT USAGE` and `SHOW TENANT QUOTA` read-only handlers.

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::{sqlstate_error, text_field};

/// `SHOW TENANT USAGE [FOR <id>]` — runtime usage counters.
pub fn show_tenant_usage(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if !identity.is_superuser {
        return Err(sqlstate_error(
            "42501",
            "permission denied: only superuser can view tenant usage",
        ));
    }

    let filter_tid = parse_tenant_for_clause(parts);

    let tenants = match state.tenants.lock() {
        Ok(t) => t,
        Err(p) => p.into_inner(),
    };

    let schema = Arc::new(vec![
        text_field("tenant_id"),
        text_field("memory_bytes"),
        text_field("storage_bytes"),
        text_field("active_requests"),
        text_field("qps_current"),
        text_field("total_requests"),
        text_field("rejected_requests"),
        text_field("active_connections"),
    ]);

    let rows: Vec<_> = tenants
        .iter_usage()
        .filter(|(tid, _, _)| filter_tid.is_none() || Some(tid.as_u32()) == filter_tid)
        .map(|(tid, usage, _)| {
            let mut enc = DataRowEncoder::new(schema.clone());
            let _ = enc.encode_field(&tid.as_u32().to_string());
            let _ = enc.encode_field(&usage.memory_bytes.to_string());
            let _ = enc.encode_field(&usage.storage_bytes.to_string());
            let _ = enc.encode_field(&usage.active_requests.to_string());
            let _ = enc.encode_field(&usage.requests_this_second.to_string());
            let _ = enc.encode_field(&usage.total_requests.to_string());
            let _ = enc.encode_field(&usage.rejected_requests.to_string());
            let _ = enc.encode_field(&usage.active_connections.to_string());
            Ok(enc.take_row())
        })
        .collect();

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}

/// `SHOW TENANT QUOTA [FOR <id>]` — quota limits alongside current usage.
pub fn show_tenant_quota(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if !identity.is_superuser {
        return Err(sqlstate_error(
            "42501",
            "permission denied: only superuser can view tenant quotas",
        ));
    }

    let filter_tid = parse_tenant_for_clause(parts);

    let tenants = match state.tenants.lock() {
        Ok(t) => t,
        Err(p) => p.into_inner(),
    };

    let schema = Arc::new(vec![
        text_field("tenant_id"),
        text_field("quota_name"),
        text_field("limit"),
        text_field("current"),
        text_field("percent_used"),
    ]);

    let mut rows = Vec::new();

    for (tid, usage, quota) in tenants.iter_usage() {
        if filter_tid.is_some() && Some(tid.as_u32()) != filter_tid {
            continue;
        }
        let tid_str = tid.as_u32().to_string();

        let entries: &[(&str, u64, u64)] = &[
            ("memory_bytes", quota.max_memory_bytes, usage.memory_bytes),
            (
                "storage_bytes",
                quota.max_storage_bytes,
                usage.storage_bytes,
            ),
            (
                "concurrent_requests",
                quota.max_concurrent_requests as u64,
                usage.active_requests as u64,
            ),
            (
                "qps",
                quota.max_qps as u64,
                usage.requests_this_second as u64,
            ),
            (
                "connections",
                quota.max_connections as u64,
                usage.active_connections as u64,
            ),
        ];

        for &(name, limit, current) in entries {
            let pct = if limit > 0 {
                format!("{:.1}%", current as f64 / limit as f64 * 100.0)
            } else {
                "unlimited".to_string()
            };
            let mut enc = DataRowEncoder::new(schema.clone());
            let _ = enc.encode_field(&tid_str);
            let _ = enc.encode_field(&name);
            let _ = enc.encode_field(&limit.to_string());
            let _ = enc.encode_field(&current.to_string());
            let _ = enc.encode_field(&pct);
            rows.push(Ok(enc.take_row()));
        }
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}

/// Parse `FOR <tenant_id>` from DDL parts. Returns `Some(tid)` if present.
fn parse_tenant_for_clause(parts: &[&str]) -> Option<u32> {
    let for_idx = parts
        .iter()
        .position(|p: &&str| p.eq_ignore_ascii_case("FOR"))?;
    parts.get(for_idx + 1)?.parse::<u32>().ok()
}
