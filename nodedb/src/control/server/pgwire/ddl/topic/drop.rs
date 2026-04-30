//! `DROP TOPIC` DDL handler.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::{require_admin, sqlstate_error};

pub fn drop_topic(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    require_admin(identity, "drop topics")?;

    // parts: ["DROP", "TOPIC", "<name>"]
    if parts.len() < 3 {
        return Err(sqlstate_error("42601", "expected DROP TOPIC <name>"));
    }

    let name = parts[2].to_lowercase();
    let tenant_id = identity.tenant_id.as_u64();

    let catalog = state
        .credentials
        .catalog()
        .as_ref()
        .ok_or_else(|| sqlstate_error("XX000", "system catalog not available"))?;

    let existed = catalog
        .delete_ep_topic(tenant_id, &name)
        .map_err(|e| sqlstate_error("XX000", &format!("catalog delete: {e}")))?;

    if !existed {
        return Err(sqlstate_error(
            "42704",
            &format!("topic '{name}' does not exist"),
        ));
    }

    state.ep_topic_registry.unregister(tenant_id, &name);

    // Remove the buffer.
    let buffer_key = format!("topic:{name}");
    state.cdc_router.remove_buffer(tenant_id, &buffer_key);

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("DROP TOPIC {name}"),
    );

    Ok(vec![Response::Execution(Tag::new("DROP TOPIC"))])
}
