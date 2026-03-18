use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::types::{int8_field, require_admin, sqlstate_error, text_field};

/// CREATE API KEY FOR <user> [EXPIRES <seconds>]
///
/// Returns the full API key (shown once). Requires admin.
pub fn create_api_key(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    // CREATE API KEY FOR <user> [EXPIRES <seconds>]
    if parts.len() < 5 {
        return Err(sqlstate_error(
            "42601",
            "syntax: CREATE API KEY FOR <user> [EXPIRES <seconds>]",
        ));
    }

    if !parts[1].eq_ignore_ascii_case("API")
        || !parts[2].eq_ignore_ascii_case("KEY")
        || !parts[3].eq_ignore_ascii_case("FOR")
    {
        return Err(sqlstate_error(
            "42601",
            "syntax: CREATE API KEY FOR <user> [EXPIRES <seconds>]",
        ));
    }

    let target_username = parts[4];

    // Users can create keys for themselves; admin required for others.
    if target_username != identity.username {
        require_admin(identity, "create API keys for other users")?;
    }

    // Look up the target user.
    let target_user = state
        .credentials
        .get_user(target_username)
        .ok_or_else(|| sqlstate_error("42704", &format!("user '{target_username}' not found")))?;

    // Parse optional EXPIRES.
    let mut expires_secs: u64 = 0;
    if parts.len() >= 7 && parts[5].eq_ignore_ascii_case("EXPIRES") {
        expires_secs = parts[6]
            .parse()
            .map_err(|_| sqlstate_error("42601", "EXPIRES must be a number of seconds"))?;
    }

    let catalog = state.credentials.catalog();
    let token = state
        .api_keys
        .create_key(
            target_username,
            target_user.user_id,
            target_user.tenant_id,
            expires_secs,
            vec![], // TODO: parse SCOPE from DDL when implemented
            catalog.as_ref(),
        )
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    state.audit_record(
        AuditEvent::PrivilegeChange,
        Some(identity.tenant_id),
        &identity.username,
        &format!("created API key for user '{target_username}'"),
    );

    // Return the token as a query result (shown once).
    let schema = Arc::new(vec![text_field("api_key")]);
    let mut encoder = DataRowEncoder::new(schema.clone());
    encoder
        .encode_field(&token)
        .map_err(|e| sqlstate_error("XX000", &format!("encode error: {e}")))?;
    let row = encoder.take_row();

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(vec![Ok(row)]),
    ))])
}

/// REVOKE API KEY <key_id>
pub fn revoke_api_key(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if parts.len() < 4 {
        return Err(sqlstate_error("42601", "syntax: REVOKE API KEY <key_id>"));
    }

    if !parts[1].eq_ignore_ascii_case("API") || !parts[2].eq_ignore_ascii_case("KEY") {
        return Err(sqlstate_error("42601", "syntax: REVOKE API KEY <key_id>"));
    }

    let key_id = parts[3];

    // Check if the key belongs to the current user or if they're admin.
    let keys = state.api_keys.list_keys_for_user(&identity.username);
    let owns_key = keys.iter().any(|k| k.key_id == key_id);
    if !owns_key {
        require_admin(identity, "revoke API keys for other users")?;
    }

    let catalog = state.credentials.catalog();
    let revoked = state
        .api_keys
        .revoke_key(key_id, catalog.as_ref())
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    if revoked {
        state.audit_record(
            AuditEvent::PrivilegeChange,
            Some(identity.tenant_id),
            &identity.username,
            &format!("revoked API key '{key_id}'"),
        );
        Ok(vec![Response::Execution(Tag::new("REVOKE API KEY"))])
    } else {
        Err(sqlstate_error(
            "42704",
            &format!("API key '{key_id}' not found"),
        ))
    }
}

/// LIST API KEYS [FOR <user>]
pub fn list_api_keys(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    let target_username = if parts.len() >= 5 && parts[3].eq_ignore_ascii_case("FOR") {
        let target = parts[4];
        if target != identity.username {
            require_admin(identity, "list API keys for other users")?;
        }
        target.to_string()
    } else if parts.len() >= 4 && parts[3].eq_ignore_ascii_case("FOR") {
        return Err(sqlstate_error("42601", "expected username after FOR"));
    } else {
        // Default: list own keys (or all if superuser).
        identity.username.clone()
    };

    let keys = if identity.is_superuser && target_username == identity.username {
        state.api_keys.list_all_keys()
    } else {
        state.api_keys.list_keys_for_user(&target_username)
    };

    let schema = Arc::new(vec![
        text_field("key_id"),
        text_field("username"),
        int8_field("expires_at"),
        text_field("is_revoked"),
        int8_field("created_at"),
    ]);

    let mut rows = Vec::with_capacity(keys.len());
    let mut encoder = DataRowEncoder::new(schema.clone());

    for key in &keys {
        encoder
            .encode_field(&key.key_id)
            .map_err(|e| sqlstate_error("XX000", &format!("encode error: {e}")))?;
        encoder
            .encode_field(&key.username)
            .map_err(|e| sqlstate_error("XX000", &format!("encode error: {e}")))?;
        encoder
            .encode_field(&(key.expires_at as i64))
            .map_err(|e| sqlstate_error("XX000", &format!("encode error: {e}")))?;
        encoder
            .encode_field(&if key.is_revoked { "t" } else { "f" })
            .map_err(|e| sqlstate_error("XX000", &format!("encode error: {e}")))?;
        encoder
            .encode_field(&(key.created_at as i64))
            .map_err(|e| sqlstate_error("XX000", &format!("encode error: {e}")))?;
        rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}
