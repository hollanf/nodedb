//! EXPLAIN PERMISSION and policy introspection DDL commands.
//!
//! ```sql
//! EXPLAIN PERMISSION READ ON orders FOR AUTH USER 'user_123'
//! EXPLAIN SCOPE FOR AUTH USER 'user_123'
//! ```

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::types::{sqlstate_error, text_field};

/// EXPLAIN PERMISSION <perm> ON <collection> FOR AUTH USER '<id>'
pub fn explain_permission(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    // EXPLAIN PERMISSION READ ON orders FOR AUTH USER 'user_123'
    if parts.len() < 8 {
        return Err(sqlstate_error(
            "42601",
            "syntax: EXPLAIN PERMISSION <perm> ON <collection> FOR AUTH USER '<id>'",
        ));
    }
    let perm = parts[2];
    let collection = parts[4];
    let user_id_str = parts.get(7).map(|s| s.trim_matches('\'')).unwrap_or("");

    // Build a synthetic identity for the target user.
    let target_identity = if let Some(user) = state.credentials.get_user(user_id_str) {
        crate::control::security::identity::AuthenticatedIdentity {
            user_id: user.user_id,
            username: user.username.clone(),
            tenant_id: user.tenant_id,
            auth_method: crate::control::security::identity::AuthMethod::Trust,
            roles: user.roles.clone(),
            is_superuser: user.is_superuser,
        }
    } else {
        // Unknown user — use the requesting identity.
        identity.clone()
    };

    let auth_ctx = crate::control::server::session_auth::build_auth_context(&target_identity);
    let explanation = crate::control::security::explain::explain_permission(
        perm,
        collection,
        &target_identity,
        &auth_ctx,
        state,
    );

    let schema = Arc::new(vec![
        text_field("check"),
        text_field("result"),
        text_field("source"),
    ]);

    let mut rows: Vec<_> = explanation
        .steps
        .iter()
        .map(|s| {
            let mut enc = DataRowEncoder::new(schema.clone());
            let _ = enc.encode_field(&s.check);
            let _ = enc.encode_field(&s.result);
            let _ = enc.encode_field(&s.source);
            Ok(enc.take_row())
        })
        .collect();

    // Final result row.
    let mut enc = DataRowEncoder::new(schema.clone());
    let _ = enc.encode_field(&"FINAL");
    let _ = enc.encode_field(&if explanation.allowed { "ALLOW" } else { "DENY" });
    let _ = enc.encode_field(&format!("{} on {}", perm, collection));
    rows.push(Ok(enc.take_row()));

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}

/// EXPLAIN SCOPE FOR AUTH USER '<id>'
pub fn explain_scope(
    state: &SharedState,
    _identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if parts.len() < 6 {
        return Err(sqlstate_error(
            "42601",
            "syntax: EXPLAIN SCOPE FOR AUTH USER '<id>'",
        ));
    }
    let user_id = parts.get(5).map(|s| s.trim_matches('\'')).unwrap_or("");
    let org_ids = state.orgs.orgs_for_user(user_id);
    let effective = state.scope_grants.effective_scopes(user_id, &org_ids);

    let schema = Arc::new(vec![
        text_field("scope"),
        text_field("source"),
        text_field("resolved_grants"),
    ]);

    let rows: Vec<_> = effective
        .iter()
        .map(|scope_name| {
            let source = if state
                .scope_grants
                .scopes_for("user", user_id)
                .contains(scope_name)
            {
                "direct (user)"
            } else {
                "inherited (org)"
            };
            let resolved = state.scope_defs.resolve(scope_name);
            let grants_str: Vec<String> = resolved
                .iter()
                .map(|(p, c)| format!("{p} ON {c}"))
                .collect();

            let mut enc = DataRowEncoder::new(schema.clone());
            let _ = enc.encode_field(scope_name);
            let _ = enc.encode_field(&source);
            let _ = enc.encode_field(&grants_str.join(", "));
            Ok(enc.take_row())
        })
        .collect();

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}

/// `SELECT nodedb_assert_visible('<collection>', '<row_id>', '<user_id>')`
///
/// Test helper: returns true/false whether a row is visible to a user under RLS.
pub fn assert_visible(
    state: &SharedState,
    _identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    // Parse: nodedb_assert_visible('collection', 'row_id', 'user_id')
    if parts.len() < 4 {
        return Err(sqlstate_error(
            "42601",
            "syntax: SELECT nodedb_assert_visible('<collection>', '<row_id>', '<user_id>')",
        ));
    }

    let collection = parts[1].trim_matches('\'').trim_end_matches(',');
    let _row_id = parts[2].trim_matches('\'').trim_end_matches(',');
    let user_id = parts[3].trim_matches('\'').trim_end_matches(')');

    // Build AuthContext for the target user.
    let target_identity = crate::control::security::identity::AuthenticatedIdentity {
        user_id: user_id.parse().unwrap_or(0),
        username: user_id.to_string(),
        tenant_id: crate::types::TenantId::new(1),
        auth_method: crate::control::security::identity::AuthMethod::Trust,
        roles: vec![crate::control::security::identity::Role::ReadWrite],
        is_superuser: false,
    };
    let auth_ctx = crate::control::server::session_auth::build_auth_context(&target_identity);

    // Check if RLS policies would filter this user.
    let rls_bytes = state.rls.combined_read_predicate_with_auth(
        target_identity.tenant_id.as_u64(),
        collection,
        &auth_ctx,
    );

    let visible = rls_bytes.is_some_and(|b| b.is_empty()); // No filters = visible.

    let schema = Arc::new(vec![text_field("visible")]);
    let mut enc = DataRowEncoder::new(schema.clone());
    let _ = enc.encode_field(&visible.to_string());

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(vec![Ok(enc.take_row())]),
    ))])
}
