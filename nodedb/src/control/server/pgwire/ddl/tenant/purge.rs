//! `PURGE TENANT <id> CONFIRM` — Data Plane meta op that deletes
//! ALL tenant data across every engine. Superuser-only, requires
//! the literal `CONFIRM` keyword.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::super::super::types::sqlstate_error;

pub async fn purge_tenant(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if !identity.is_superuser {
        return Err(sqlstate_error(
            "42501",
            "permission denied: only superuser can purge tenants",
        ));
    }

    if parts.len() < 4 {
        return Err(sqlstate_error("42601", "syntax: PURGE TENANT <id> CONFIRM"));
    }

    let tid: u64 = parts[2]
        .parse()
        .map_err(|_| sqlstate_error("42601", "TENANT ID must be a numeric value"))?;

    if tid == 0 {
        return Err(sqlstate_error("42501", "cannot purge system tenant (0)"));
    }

    if !parts[3].eq_ignore_ascii_case("CONFIRM") {
        return Err(sqlstate_error(
            "42601",
            "PURGE TENANT requires CONFIRM keyword to prevent accidental data destruction",
        ));
    }

    let tenant_id = TenantId::new(tid);

    state.audit_record(
        AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!("PURGE TENANT {tid} CONFIRM — deleting all data across all engines"),
    );

    let plan = crate::bridge::envelope::PhysicalPlan::Meta(
        crate::bridge::physical_plan::MetaOp::PurgeTenant { tenant_id: tid },
    );

    match super::super::sync_dispatch::dispatch_async(
        state,
        tenant_id,
        "__system",
        plan,
        std::time::Duration::from_secs(300),
    )
    .await
    {
        Ok(_) => {
            state.audit_record(
                AuditEvent::AdminAction,
                Some(tenant_id),
                &identity.username,
                &format!("PURGE TENANT {tid} completed successfully"),
            );
            Ok(vec![Response::Execution(Tag::new("PURGE TENANT"))])
        }
        Err(e) => Err(sqlstate_error("XX000", &format!("purge failed: {e}"))),
    }
}
