//! `ALTER COLLECTION ... SET {RETENTION,LEGAL_HOLD,APPEND_ONLY,LAST_VALUE_CACHE}`
//! — non-schema enforcement knobs propagated through `CatalogEntry::PutCollection`.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::catalog::SystemCatalog;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::super::types::sqlstate_error;

/// ALTER COLLECTION <name> SET RETENTION = '<value>'
pub fn alter_collection_set_retention(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
    value: &str,
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id.as_u32();
    let Some(catalog) = state.credentials.catalog() else {
        return Err(sqlstate_error("XX000", "no catalog available"));
    };
    let mut coll = catalog
        .get_collection(tenant_id, name)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?
        .ok_or_else(|| sqlstate_error("42P01", &format!("collection '{name}' not found")))?;

    crate::data::executor::enforcement::retention::parse_retention_period(value)
        .map_err(|e| sqlstate_error("22023", &e))?;

    coll.retention_period = Some(value.to_string());
    persist_and_bump(state, catalog, &coll)
}

/// ALTER COLLECTION <name> SET LEGAL_HOLD = TRUE|FALSE TAG '<tag>'
pub fn alter_collection_set_legal_hold(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
    enabled: bool,
    tag: &str,
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id.as_u32();
    let Some(catalog) = state.credentials.catalog() else {
        return Err(sqlstate_error("XX000", "no catalog available"));
    };
    let mut coll = catalog
        .get_collection(tenant_id, name)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?
        .ok_or_else(|| sqlstate_error("42P01", &format!("collection '{name}' not found")))?;

    if enabled {
        if coll.legal_holds.iter().any(|h| h.tag == tag) {
            return Err(sqlstate_error(
                "23505",
                &format!("legal hold tag '{tag}' already exists on {name}"),
            ));
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        coll.legal_holds
            .push(crate::control::security::catalog::LegalHold {
                tag: tag.to_string(),
                created_at: now,
                created_by: identity.username.clone(),
            });
    } else {
        let before = coll.legal_holds.len();
        coll.legal_holds.retain(|h| h.tag != tag);
        if coll.legal_holds.len() == before {
            return Err(sqlstate_error(
                "42704",
                &format!("legal hold tag '{tag}' not found on {name}"),
            ));
        }
    }

    persist_and_bump(state, catalog, &coll)
}

/// ALTER COLLECTION <name> SET APPEND_ONLY
pub fn alter_collection_set_append_only(
    state: &SharedState,
    _identity: &AuthenticatedIdentity,
    name: &str,
) -> PgWireResult<Vec<Response>> {
    let tenant_id = _identity.tenant_id.as_u32();
    let Some(catalog) = state.credentials.catalog() else {
        return Err(sqlstate_error("XX000", "no catalog available"));
    };
    let mut coll = catalog
        .get_collection(tenant_id, name)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?
        .ok_or_else(|| sqlstate_error("42P01", &format!("collection '{name}' not found")))?;

    if coll.append_only {
        return Err(sqlstate_error(
            "42710",
            &format!("collection '{name}' is already append-only"),
        ));
    }
    coll.append_only = true;
    persist_and_bump(state, catalog, &coll)
}

/// ALTER COLLECTION <name> SET LAST_VALUE_CACHE = TRUE|FALSE
pub fn alter_collection_set_last_value_cache(
    state: &SharedState,
    _identity: &AuthenticatedIdentity,
    name: &str,
    enabled: bool,
) -> PgWireResult<Vec<Response>> {
    let tenant_id = _identity.tenant_id.as_u32();
    let Some(catalog) = state.credentials.catalog() else {
        return Err(sqlstate_error("XX000", "no catalog available"));
    };
    let mut coll = catalog
        .get_collection(tenant_id, name)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?
        .ok_or_else(|| sqlstate_error("42P01", &format!("collection '{name}' not found")))?;

    if !coll.collection_type.is_timeseries() {
        return Err(sqlstate_error(
            "42809",
            &format!("'{name}' is not a timeseries collection"),
        ));
    }
    coll.lvc_enabled = enabled;
    persist_and_bump(state, catalog, &coll)
}

fn persist_and_bump(
    state: &SharedState,
    catalog: &SystemCatalog,
    coll: &crate::control::security::catalog::StoredCollection,
) -> PgWireResult<Vec<Response>> {
    let entry = crate::control::catalog_entry::CatalogEntry::PutCollection(Box::new(coll.clone()));
    let log_index = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    if log_index == 0 {
        catalog
            .put_collection(coll)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    }
    state.schema_version.bump();
    Ok(vec![Response::Execution(Tag::new("ALTER COLLECTION"))])
}
