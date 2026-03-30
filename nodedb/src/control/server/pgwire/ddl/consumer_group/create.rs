//! `CREATE CONSUMER GROUP` DDL handler.
//!
//! Syntax: `CREATE CONSUMER GROUP <name> ON <stream>`

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::event::cdc::consumer_group::ConsumerGroupDef;

use super::super::super::types::{require_admin, sqlstate_error};

/// Handle `CREATE CONSUMER GROUP <name> ON <stream>`
pub fn create_consumer_group(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    require_admin(identity, "create consumer groups")?;

    // parts: ["CREATE", "CONSUMER", "GROUP", "<name>", "ON", "<stream>"]
    if parts.len() < 6 || !parts[4].eq_ignore_ascii_case("ON") {
        return Err(sqlstate_error(
            "42601",
            "expected CREATE CONSUMER GROUP <name> ON <stream>",
        ));
    }

    let group_name = parts[3].to_lowercase();
    let stream_name = parts[5].to_lowercase();
    let tenant_id = identity.tenant_id.as_u32();

    // Verify the stream exists.
    if state.stream_registry.get(tenant_id, &stream_name).is_none() {
        return Err(sqlstate_error(
            "42704",
            &format!("change stream '{stream_name}' does not exist"),
        ));
    }

    // Check for duplicate group.
    if state
        .group_registry
        .get(tenant_id, &stream_name, &group_name)
        .is_some()
    {
        return Err(sqlstate_error(
            "42710",
            &format!("consumer group '{group_name}' already exists on stream '{stream_name}'"),
        ));
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| sqlstate_error("XX000", "system clock error"))?
        .as_secs();

    let def = ConsumerGroupDef {
        tenant_id,
        name: group_name.clone(),
        stream_name: stream_name.clone(),
        owner: identity.username.clone(),
        created_at: now,
    };

    let catalog = state
        .credentials
        .catalog()
        .as_ref()
        .ok_or_else(|| sqlstate_error("XX000", "system catalog not available"))?;

    catalog
        .put_consumer_group(&def)
        .map_err(|e| sqlstate_error("XX000", &format!("catalog write: {e}")))?;

    state.group_registry.register(def);

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("CREATE CONSUMER GROUP {group_name} ON {stream_name}"),
    );

    Ok(vec![Response::Execution(Tag::new("CREATE CONSUMER GROUP"))])
}
