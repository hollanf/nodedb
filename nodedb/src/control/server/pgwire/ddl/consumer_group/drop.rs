//! `DROP CONSUMER GROUP` DDL handler.
//!
//! Syntax: `DROP CONSUMER GROUP <name> ON <stream>`

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::{require_admin, sqlstate_error};

/// Handle `DROP CONSUMER GROUP <name> ON <stream>`
pub fn drop_consumer_group(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    require_admin(identity, "drop consumer groups")?;

    // parts: ["DROP", "CONSUMER", "GROUP", "<name>", "ON", "<stream>"]
    if parts.len() < 6 || !parts[4].eq_ignore_ascii_case("ON") {
        return Err(sqlstate_error(
            "42601",
            "expected DROP CONSUMER GROUP <name> ON <stream>",
        ));
    }

    let group_name = parts[3].to_lowercase();
    let stream_name = parts[5].to_lowercase();
    let tenant_id = identity.tenant_id.as_u64();

    let catalog = state
        .credentials
        .catalog()
        .as_ref()
        .ok_or_else(|| sqlstate_error("XX000", "system catalog not available"))?;

    let existed = catalog
        .delete_consumer_group(tenant_id, &stream_name, &group_name)
        .map_err(|e| sqlstate_error("XX000", &format!("catalog delete: {e}")))?;

    if !existed {
        return Err(sqlstate_error(
            "42704",
            &format!("consumer group '{group_name}' does not exist on stream '{stream_name}'"),
        ));
    }

    state
        .group_registry
        .unregister(tenant_id, &stream_name, &group_name);

    // Delete committed offsets for this group.
    if let Err(e) = state
        .offset_store
        .delete_group(tenant_id, &stream_name, &group_name)
    {
        tracing::warn!(
            error = %e,
            "failed to delete offsets for consumer group {group_name}"
        );
    }

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("DROP CONSUMER GROUP {group_name} ON {stream_name}"),
    );

    Ok(vec![Response::Execution(Tag::new("DROP CONSUMER GROUP"))])
}
