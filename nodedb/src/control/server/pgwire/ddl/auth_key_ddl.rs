//! Auth-scoped API key DDL commands.
//!
//! ```sql
//! CREATE AUTH KEY FOR AUTH USER 'x' WITH SCOPES 'profile:read' [RATE_LIMIT 100] [EXPIRES 30d]
//! ROTATE AUTH KEY '<key_id>' [OVERLAP 24h]
//! LIST AUTH KEYS [FOR AUTH USER 'x']
//! ```

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::types::{sqlstate_error, text_field};

/// CREATE AUTH KEY FOR AUTH USER '<id>' WITH SCOPES '...' [RATE_LIMIT N] [EXPIRES Nd]
pub fn create_auth_key(
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
    // CREATE AUTH KEY FOR AUTH USER '<id>' ...
    if parts.len() < 6 {
        return Err(sqlstate_error(
            "42601",
            "syntax: CREATE AUTH KEY FOR AUTH USER '<id>' [WITH SCOPES '...'] [RATE_LIMIT N] [EXPIRES Nd]",
        ));
    }
    let auth_user_id = parts[5].trim_matches('\'');

    // Parse scopes.
    let scopes: Vec<String> = parts
        .iter()
        .position(|p| p.to_uppercase() == "SCOPES")
        .map(|i| {
            parts[i + 1..]
                .iter()
                .take_while(|p| {
                    let u = p.to_uppercase();
                    u != "RATE_LIMIT" && u != "EXPIRES"
                })
                .map(|s| s.trim_matches('\'').trim_end_matches(',').to_string())
                .collect()
        })
        .unwrap_or_default();

    // Parse rate limit.
    let rate_limit = parts
        .iter()
        .position(|p| p.to_uppercase() == "RATE_LIMIT")
        .and_then(|i| parts.get(i + 1))
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);

    // Parse expires.
    let expires_days = parts
        .iter()
        .position(|p| p.to_uppercase() == "EXPIRES")
        .and_then(|i| parts.get(i + 1))
        .and_then(|s| {
            let s = s.trim_end_matches('d');
            s.parse::<u64>().ok()
        })
        .unwrap_or(0);

    let token = state.auth_api_keys.create_key(
        auth_user_id,
        identity.tenant_id.as_u32(),
        scopes,
        rate_limit,
        0, // burst = use default
        expires_days,
    );

    state.audit_record(
        crate::control::security::audit::AuditEvent::PrivilegeChange,
        Some(identity.tenant_id),
        &identity.username,
        &format!("created auth API key for user '{auth_user_id}'"),
    );

    let schema = Arc::new(vec![text_field("auth_api_key")]);
    let mut enc = DataRowEncoder::new(schema.clone());
    let _ = enc.encode_field(&token);
    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(vec![Ok(enc.take_row())]),
    ))])
}

/// ROTATE AUTH KEY '<key_id>' [OVERLAP 24h]
pub fn rotate_auth_key(
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
    if parts.len() < 4 {
        return Err(sqlstate_error(
            "42601",
            "syntax: ROTATE AUTH KEY '<key_id>' [OVERLAP 24h]",
        ));
    }
    let key_id = parts[3].trim_matches('\'');
    let overlap_hours = parts
        .iter()
        .position(|p| p.to_uppercase() == "OVERLAP")
        .and_then(|i| parts.get(i + 1))
        .and_then(|s| s.trim_end_matches('h').parse::<u64>().ok())
        .unwrap_or(24);

    let new_token = state
        .auth_api_keys
        .rotate(key_id, overlap_hours)
        .ok_or_else(|| sqlstate_error("42704", &format!("auth key '{key_id}' not found")))?;

    state.audit_record(
        crate::control::security::audit::AuditEvent::PrivilegeChange,
        Some(identity.tenant_id),
        &identity.username,
        &format!("rotated auth key '{key_id}' (overlap {overlap_hours}h)"),
    );

    let schema = Arc::new(vec![text_field("new_auth_api_key")]);
    let mut enc = DataRowEncoder::new(schema.clone());
    let _ = enc.encode_field(&new_token);
    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(vec![Ok(enc.take_row())]),
    ))])
}

/// LIST AUTH KEYS [FOR AUTH USER '<id>']
pub fn list_auth_keys(
    state: &SharedState,
    _identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    let user_filter = parts
        .iter()
        .position(|p| p.to_uppercase() == "USER")
        .and_then(|i| parts.get(i + 1))
        .map(|s| s.trim_matches('\''));

    let keys = if let Some(uid) = user_filter {
        state.auth_api_keys.list_for_user(uid)
    } else {
        state.auth_api_keys.list_all()
    };

    let schema = Arc::new(vec![
        text_field("key_id"),
        text_field("auth_user_id"),
        text_field("scopes"),
        text_field("rate_limit"),
        text_field("expires_at"),
        text_field("last_used_at"),
        text_field("last_used_ip"),
    ]);

    let rows: Vec<_> = keys
        .iter()
        .map(|k| {
            let mut enc = DataRowEncoder::new(schema.clone());
            let _ = enc.encode_field(&k.key_id);
            let _ = enc.encode_field(&k.auth_user_id);
            let _ = enc.encode_field(&k.scopes.join(", "));
            let _ = enc.encode_field(&k.rate_limit_qps.to_string());
            let _ = enc.encode_field(&if k.expires_at == 0 {
                "never".into()
            } else {
                k.expires_at.to_string()
            });
            let _ = enc.encode_field(&if k.last_used_at == 0 {
                "never".into()
            } else {
                k.last_used_at.to_string()
            });
            let _ = enc.encode_field(&k.last_used_ip);
            Ok(enc.take_row())
        })
        .collect();

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}
