//! `REINDEX INDEX name` / `REINDEX TABLE collection` — rebuild indexes.
//!
//! Dispatches a MetaOp::Checkpoint to the Data Plane via the distributed
//! maintenance helper, which forces index structures to be rebuilt.

use pgwire::api::results::{Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

/// Handle `REINDEX INDEX name` or `REINDEX TABLE collection`.
pub fn handle_reindex(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if parts.len() < 3 {
        return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "42601".to_owned(),
            "syntax: REINDEX INDEX <name> or REINDEX TABLE <collection>".to_owned(),
        ))));
    }

    let target_type = parts[1].to_uppercase();
    let target_name = parts[2].to_lowercase();
    let tenant_id = identity.tenant_id;

    match target_type.as_str() {
        "INDEX" => {
            super::distributed::dispatch_maintenance_to_all_cores(
                state,
                tenant_id,
                crate::bridge::physical_plan::MetaOp::Checkpoint,
            );
            tracing::info!(index = %target_name, "REINDEX INDEX dispatched");
        }
        "TABLE" => {
            if let Some(catalog) = state.credentials.catalog()
                && catalog
                    .get_collection(tenant_id.as_u64(), &target_name)
                    .ok()
                    .flatten()
                    .is_none()
            {
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42P01".to_owned(),
                    format!("collection \"{target_name}\" does not exist"),
                ))));
            }

            super::distributed::dispatch_maintenance_to_all_cores(
                state,
                tenant_id,
                crate::bridge::physical_plan::MetaOp::Checkpoint,
            );
            tracing::info!(collection = %target_name, "REINDEX TABLE dispatched");
        }
        _ => {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42601".to_owned(),
                "syntax: REINDEX INDEX <name> or REINDEX TABLE <collection>".to_owned(),
            ))));
        }
    }

    Ok(vec![Response::Execution(Tag::new("REINDEX"))])
}
