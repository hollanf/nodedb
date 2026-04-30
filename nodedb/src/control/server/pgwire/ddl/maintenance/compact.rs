//! `COMPACT collection [PARTITION 'name']` — trigger manual compaction.
//!
//! Dispatches a MetaOp::Compact to the Data Plane via the standard
//! dispatch path. The Data Plane merges segments for the receiving core.

use pgwire::api::results::{Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

/// Handle `COMPACT collection [PARTITION 'name']`.
pub fn handle_compact(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if parts.len() < 2 {
        return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "42601".to_owned(),
            "COMPACT requires a collection name".to_owned(),
        ))));
    }

    let collection = parts[1].to_lowercase();
    let tenant_id = identity.tenant_id;

    // Verify collection exists.
    if let Some(catalog) = state.credentials.catalog()
        && catalog
            .get_collection(tenant_id.as_u64(), &collection)
            .ok()
            .flatten()
            .is_none()
    {
        return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "42P01".to_owned(),
            format!("collection \"{collection}\" does not exist"),
        ))));
    }

    // Dispatch MetaOp::Compact to all Data Plane cores via distributed helper.
    super::distributed::dispatch_maintenance_to_all_cores(
        state,
        tenant_id,
        crate::bridge::physical_plan::MetaOp::Compact,
    );

    tracing::info!(%collection, "COMPACT dispatched");

    Ok(vec![Response::Execution(Tag::new("COMPACT"))])
}
