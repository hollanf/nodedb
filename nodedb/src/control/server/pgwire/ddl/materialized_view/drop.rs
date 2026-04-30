//! `DROP MATERIALIZED VIEW [IF EXISTS]` handler.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::sqlstate_error;

pub fn drop_materialized_view(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if parts.len() < 4 {
        return Err(sqlstate_error(
            "42601",
            "syntax: DROP MATERIALIZED VIEW [IF EXISTS] <name>",
        ));
    }

    let tenant_id = identity.tenant_id;

    let (name, if_exists) = if parts.len() >= 6
        && parts[3].to_uppercase() == "IF"
        && parts[4].to_uppercase() == "EXISTS"
    {
        (parts[5].to_lowercase(), true)
    } else {
        (parts[3].to_lowercase(), false)
    };

    // Pre-check existence so `IF EXISTS` + missing is a no-op
    // that never touches raft.
    let exists_before = if let Some(catalog) = state.credentials.catalog() {
        matches!(
            catalog.get_materialized_view(tenant_id.as_u64(), &name),
            Ok(Some(_))
        )
    } else {
        false
    };
    if !exists_before && !if_exists {
        return Err(sqlstate_error(
            "42P01",
            &format!("materialized view '{name}' does not exist"),
        ));
    }
    if !exists_before {
        return Ok(vec![Response::Execution(Tag::new(
            "DROP MATERIALIZED VIEW",
        ))]);
    }

    let entry = crate::control::catalog_entry::CatalogEntry::DeleteMaterializedView {
        tenant_id: tenant_id.as_u64(),
        name: name.clone(),
    };
    let log_index = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
        .map_err(|e| sqlstate_error("XX000", &format!("metadata propose: {e}")))?;
    if log_index == 0
        && let Some(catalog) = state.credentials.catalog()
    {
        catalog
            .delete_materialized_view(tenant_id.as_u64(), &name)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    }

    // Also drop the view's target collection created by CREATE MATERIALIZED VIEW.
    // The target lives as a normal collection to support INSERT...SELECT refresh
    // and SELECT reads; leaving it behind would leak storage and shadow any
    // later CREATE COLLECTION with the same name.
    if let Some(catalog) = state.credentials.catalog()
        && matches!(
            catalog.get_collection(tenant_id.as_u64(), &name),
            Ok(Some(_))
        )
    {
        let coll_entry = crate::control::catalog_entry::CatalogEntry::DeactivateCollection {
            tenant_id: tenant_id.as_u64(),
            name: name.clone(),
        };
        let _ = crate::control::metadata_proposer::propose_catalog_entry(state, &coll_entry)
            .map_err(|e| sqlstate_error("XX000", &format!("metadata propose: {e}")))?;
    }

    tracing::info!(view = name, "materialized view dropped");

    Ok(vec![Response::Execution(Tag::new(
        "DROP MATERIALIZED VIEW",
    ))])
}
