//! DDL handlers for permission tree management.
//!
//! ```sql
//! ALTER COLLECTION documents SET PERMISSION_TREE = '{
//!   "resource_column": "id",
//!   "graph_index": "resource_tree",
//!   "permission_table": "permissions"
//! }';
//!
//! ALTER COLLECTION documents DROP PERMISSION_TREE;
//!
//! SELECT RESOLVE_PERMISSION('user-42', 'doc-123', 'documents');
//! ```

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::permission_tree::types::PermissionTreeDef;
use crate::control::state::SharedState;

use super::super::types::sqlstate_error;

/// ALTER COLLECTION <name> SET PERMISSION_TREE = '<json>'
pub async fn set_permission_tree(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let upper = sql.to_uppercase();

    // Extract collection name: between "ALTER COLLECTION " and " SET PERMISSION_TREE".
    let start = "ALTER COLLECTION ".len();
    let end = upper
        .find(" SET PERMISSION_TREE")
        .ok_or_else(|| sqlstate_error("42601", "expected SET PERMISSION_TREE"))?;
    let collection = sql[start..end].trim().to_lowercase();

    // Extract JSON: everything after '=' trimmed, between single quotes.
    let eq_pos = sql[end..]
        .find('=')
        .ok_or_else(|| sqlstate_error("42601", "expected '=' after SET PERMISSION_TREE"))?;
    let json_part = sql[end + eq_pos + 1..].trim();
    let json_str = if json_part.starts_with('\'') && json_part.ends_with('\'') {
        &json_part[1..json_part.len() - 1]
    } else {
        json_part
    };

    let def: PermissionTreeDef = sonic_rs::from_str(json_str)
        .map_err(|e| sqlstate_error("42601", &format!("invalid PERMISSION_TREE JSON: {e}")))?;

    def.validate()
        .map_err(|e| sqlstate_error("42601", &format!("invalid PERMISSION_TREE: {e}")))?;

    let tenant_id = identity.tenant_id;

    // Verify collection exists.
    let Some(catalog) = state.credentials.catalog() else {
        return Err(sqlstate_error("XX000", "catalog unavailable"));
    };
    let mut coll = catalog
        .get_collection(tenant_id.as_u64(), &collection)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?
        .ok_or_else(|| {
            sqlstate_error(
                "42P01",
                &format!("collection '{collection}' does not exist"),
            )
        })?;

    if !coll.is_active {
        return Err(sqlstate_error(
            "42P01",
            &format!("collection '{collection}' is not active"),
        ));
    }

    // Serialize and persist.
    let def_json = sonic_rs::to_string(&def)
        .map_err(|e| sqlstate_error("XX000", &format!("serialize PERMISSION_TREE: {e}")))?;
    coll.permission_tree_def = Some(def_json);
    catalog
        .put_collection(&coll)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    // Update in-memory cache.
    state
        .permission_cache
        .write()
        .await
        .register_tree_def(tenant_id.as_u64(), &collection, def);

    // Audit.
    state
        .audit
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .record(
            crate::control::security::audit::AuditEvent::AdminAction,
            Some(tenant_id),
            &identity.username,
            &format!("SET PERMISSION_TREE on '{collection}'"),
        );

    Ok(vec![Response::Execution(Tag::new("ALTER COLLECTION"))])
}

/// ALTER COLLECTION <name> DROP PERMISSION_TREE
pub async fn drop_permission_tree(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let upper = sql.to_uppercase();

    let start = "ALTER COLLECTION ".len();
    let end = upper
        .find(" DROP PERMISSION_TREE")
        .ok_or_else(|| sqlstate_error("42601", "expected DROP PERMISSION_TREE"))?;
    let collection = sql[start..end].trim().to_lowercase();

    let tenant_id = identity.tenant_id;

    let Some(catalog) = state.credentials.catalog() else {
        return Err(sqlstate_error("XX000", "catalog unavailable"));
    };
    let mut coll = catalog
        .get_collection(tenant_id.as_u64(), &collection)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?
        .ok_or_else(|| {
            sqlstate_error(
                "42P01",
                &format!("collection '{collection}' does not exist"),
            )
        })?;

    coll.permission_tree_def = None;
    catalog
        .put_collection(&coll)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    // Update in-memory cache.
    state
        .permission_cache
        .write()
        .await
        .unregister_tree_def(tenant_id.as_u64(), &collection);

    state
        .audit
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .record(
            crate::control::security::audit::AuditEvent::AdminAction,
            Some(tenant_id),
            &identity.username,
            &format!("DROP PERMISSION_TREE on '{collection}'"),
        );

    Ok(vec![Response::Execution(Tag::new("ALTER COLLECTION"))])
}
