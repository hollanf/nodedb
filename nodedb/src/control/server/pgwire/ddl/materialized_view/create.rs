//! `CREATE MATERIALIZED VIEW` handler — replicates through the
//! metadata raft group via `CatalogEntry::PutMaterializedView`.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::catalog::StoredMaterializedView;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::sqlstate_error;
use super::parse::parse_create_mv;

/// `CREATE MATERIALIZED VIEW <name> ON <source> AS SELECT ... [WITH (...)]`
pub fn create_materialized_view(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let (name, source, query_sql, refresh_mode) = parse_create_mv(sql)?;

    let tenant_id = identity.tenant_id;

    // Validate source collection exists.
    if let Some(catalog) = state.credentials.catalog() {
        match catalog.get_collection(tenant_id.as_u32(), &source) {
            Ok(Some(_)) => {}
            _ => {
                return Err(sqlstate_error(
                    "42P01",
                    &format!("source collection '{source}' does not exist"),
                ));
            }
        }

        if let Ok(Some(_)) = catalog.get_materialized_view(tenant_id.as_u32(), &name) {
            return Err(sqlstate_error(
                "42P07",
                &format!("materialized view '{name}' already exists"),
            ));
        }
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let view = StoredMaterializedView {
        tenant_id: tenant_id.as_u32(),
        name: name.clone(),
        source: source.clone(),
        query_sql,
        refresh_mode,
        owner: identity.username.clone(),
        created_at: now,
        // Stamped by the metadata applier at commit time.
        descriptor_version: 0,
        modification_hlc: nodedb_types::Hlc::ZERO,
    };

    // Propose through the metadata raft group. Every node's
    // applier writes the definition to local `SystemCatalog` redb
    // so the view is visible cluster-wide. The refresh loop picks
    // it up on its next tick.
    let entry =
        crate::control::catalog_entry::CatalogEntry::PutMaterializedView(Box::new(view.clone()));
    let log_index = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
        .map_err(|e| sqlstate_error("XX000", &format!("metadata propose: {e}")))?;
    if log_index == 0
        && let Some(catalog) = state.credentials.catalog()
    {
        catalog
            .put_materialized_view(&view)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    }

    tracing::info!(
        view = name,
        source,
        tenant = tenant_id.as_u32(),
        "materialized view created"
    );

    Ok(vec![Response::Execution(Tag::new(
        "CREATE MATERIALIZED VIEW",
    ))])
}
