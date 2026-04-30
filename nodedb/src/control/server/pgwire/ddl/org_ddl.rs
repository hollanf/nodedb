//! Organization management DDL commands.
//!
//! ```sql
//! CREATE ORG 'acme' IN TENANT 1
//! ALTER ORG 'acme' SET STATUS suspended
//! DROP ORG 'acme'
//! SHOW ORGS [IN TENANT 1]
//! SHOW MEMBERS OF ORG 'acme'
//! ```

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::types::{sqlstate_error, text_field};

/// Route org DDL commands.
pub fn handle_org(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if parts.is_empty() {
        return Err(sqlstate_error("42601", "empty org command"));
    }
    let cmd = parts[0].to_uppercase();
    match cmd.as_str() {
        "CREATE" => create_org(state, identity, parts),
        "ALTER" => alter_org(state, identity, parts),
        "DROP" => drop_org(state, identity, parts),
        _ => Err(sqlstate_error(
            "42601",
            "expected CREATE ORG, ALTER ORG, or DROP ORG",
        )),
    }
}

/// CREATE ORG '<org_id>' IN TENANT <id>
fn create_org(
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
    // CREATE ORG '<name>' IN TENANT <id>
    if parts.len() < 3 {
        return Err(sqlstate_error(
            "42601",
            "syntax: CREATE ORG '<name>' [IN TENANT <id>]",
        ));
    }
    let org_id = parts[2].trim_matches('\'');
    let tenant_id = parts
        .iter()
        .position(|p| p.to_uppercase() == "TENANT")
        .and_then(|i| parts.get(i + 1))
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or_else(|| identity.tenant_id.as_u64());

    state
        .orgs
        .create_org(org_id, org_id, tenant_id)
        .map_err(|e| sqlstate_error("23505", &e.to_string()))?;

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("created org '{org_id}' in tenant {tenant_id}"),
    );

    Ok(vec![Response::Execution(Tag::new("CREATE ORG"))])
}

/// ALTER ORG '<org_id>' SET STATUS <status>
fn alter_org(
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
    if parts.len() < 6 {
        return Err(sqlstate_error(
            "42601",
            "syntax: ALTER ORG '<id>' SET STATUS <status>",
        ));
    }
    let org_id = parts[2].trim_matches('\'');
    let status = parts[5].to_lowercase();

    let found = state
        .orgs
        .set_status(org_id, &status)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    if !found {
        return Err(sqlstate_error(
            "42704",
            &format!("org '{org_id}' not found"),
        ));
    }

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("org '{org_id}' status set to {status}"),
    );

    Ok(vec![Response::Execution(Tag::new("ALTER ORG"))])
}

/// DROP ORG '<org_id>'
fn drop_org(
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
    if parts.len() < 3 {
        return Err(sqlstate_error("42601", "syntax: DROP ORG '<org_id>'"));
    }
    let org_id = parts[2].trim_matches('\'');

    let found = state
        .orgs
        .drop_org(org_id)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    if !found {
        return Err(sqlstate_error(
            "42704",
            &format!("org '{org_id}' not found"),
        ));
    }

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("dropped org '{org_id}'"),
    );

    Ok(vec![Response::Execution(Tag::new("DROP ORG"))])
}

/// SHOW ORGS [IN TENANT <id>]
pub fn show_orgs(
    state: &SharedState,
    _identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    let tenant_filter = parts
        .iter()
        .position(|p| p.to_uppercase() == "TENANT")
        .and_then(|i| parts.get(i + 1))
        .and_then(|s| s.parse::<u64>().ok());

    let orgs = state.orgs.list(tenant_filter);

    let schema = Arc::new(vec![
        text_field("org_id"),
        text_field("name"),
        text_field("tenant_id"),
        text_field("status"),
    ]);

    let rows: Vec<_> = orgs
        .iter()
        .map(|o| {
            let mut enc = DataRowEncoder::new(schema.clone());
            let _ = enc.encode_field(&o.org_id);
            let _ = enc.encode_field(&o.name);
            let _ = enc.encode_field(&o.tenant_id.to_string());
            let _ = enc.encode_field(&o.status);
            Ok(enc.take_row())
        })
        .collect();

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}

/// SHOW MEMBERS OF ORG '<org_id>'
pub fn show_members(
    state: &SharedState,
    _identity: &AuthenticatedIdentity, // Required by DDL dispatch signature
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    // SHOW MEMBERS OF ORG '<id>'
    let org_id = parts
        .iter()
        .position(|p| p.to_uppercase() == "ORG")
        .and_then(|i| parts.get(i + 1))
        .map(|s| s.trim_matches('\''))
        .ok_or_else(|| sqlstate_error("42601", "syntax: SHOW MEMBERS OF ORG '<org_id>'"))?;

    let members = state.orgs.members_of(org_id);

    let schema = Arc::new(vec![
        text_field("user_id"),
        text_field("org_id"),
        text_field("role"),
        text_field("joined_at"),
    ]);

    let rows: Vec<_> = members
        .iter()
        .map(|m| {
            let mut enc = DataRowEncoder::new(schema.clone());
            let _ = enc.encode_field(&m.auth_user_id);
            let _ = enc.encode_field(&m.org_id);
            let _ = enc.encode_field(&m.role);
            let _ = enc.encode_field(&m.joined_at.to_string());
            Ok(enc.take_row())
        })
        .collect();

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}
