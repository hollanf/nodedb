//! `DROP MATERIALIZED VIEW` handler for streaming MVs.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::{require_admin, sqlstate_error};

/// Handle `DROP MATERIALIZED VIEW <name>` for streaming MVs.
///
/// Only drops streaming MVs (not regular periodic MVs).
pub fn drop_streaming_mv(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
) -> PgWireResult<Vec<Response>> {
    require_admin(identity, "drop streaming materialized views")?;

    let tenant_id = identity.tenant_id.as_u64();
    let name = name.to_lowercase();

    // Check if this is a streaming MV (not a regular MV).
    if state.mv_registry.get_def(tenant_id, &name).is_none() {
        return Err(sqlstate_error(
            "42704",
            &format!("streaming materialized view '{name}' does not exist"),
        ));
    }

    let catalog = state
        .credentials
        .catalog()
        .as_ref()
        .ok_or_else(|| sqlstate_error("XX000", "system catalog not available"))?;

    catalog
        .delete_streaming_mv(tenant_id, &name)
        .map_err(|e| sqlstate_error("XX000", &format!("catalog delete: {e}")))?;

    state.mv_registry.unregister(tenant_id, &name);

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("DROP MATERIALIZED VIEW {name} (streaming)"),
    );

    Ok(vec![Response::Execution(Tag::new(
        "DROP MATERIALIZED VIEW",
    ))])
}
