//! Collection DDL: CREATE COLLECTION, DROP COLLECTION, SHOW COLLECTIONS.

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::audit::AuditEvent;
use crate::control::security::catalog::StoredCollection;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::types::{int8_field, sqlstate_error, text_field};
use super::schema_validation::{extract_vector_fields, parse_fields_clause};

pub use super::schema_validation::validate_document_schema;

/// CREATE COLLECTION <name> [FIELDS (<field> <type>, ...)]
///
/// Creates a collection owned by the current user in the current tenant.
pub fn create_collection(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if parts.len() < 3 {
        return Err(sqlstate_error(
            "42601",
            "syntax: CREATE COLLECTION <name> [FIELDS (<field> <type>, ...)]",
        ));
    }

    let name_lower = parts[2].to_lowercase();
    let name = name_lower.as_str();
    let tenant_id = identity.tenant_id;

    // Check if collection already exists.
    if let Some(catalog) = state.credentials.catalog()
        && let Ok(Some(existing)) = catalog.get_collection(tenant_id.as_u32(), name)
        && existing.is_active
    {
        return Err(sqlstate_error(
            "42P07",
            &format!("collection '{name}' already exists"),
        ));
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    // Parse optional FIELDS clause: CREATE COLLECTION name FIELDS (field type, ...)
    let fields = parse_fields_clause(parts);

    let coll = StoredCollection {
        tenant_id: tenant_id.as_u32(),
        name: name.to_string(),
        owner: identity.username.clone(),
        created_at: now,
        fields,
        is_active: true,
    };

    // Persist to catalog.
    if let Some(catalog) = state.credentials.catalog() {
        catalog
            .put_collection(&coll)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    }

    // Set ownership.
    let catalog = state.credentials.catalog();
    state
        .permissions
        .set_owner(
            "collection",
            tenant_id,
            name,
            &identity.username,
            catalog.as_ref(),
        )
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    // If vector fields are declared, dispatch SetVectorParams for each.
    let vector_fields = extract_vector_fields(&coll.fields);
    if !vector_fields.is_empty() {
        for (field_name, _dim, metric) in &vector_fields {
            // Use default HNSW params (m=16, ef=200) with the declared metric.
            // The field_name becomes the named vector field key.
            tracing::info!(
                %name,
                field = %field_name,
                %metric,
                "auto-configuring vector field"
            );
            // Note: SetVectorParams is dispatched later when the first insert
            // arrives, because the Data Plane core is selected by vShard routing
            // at dispatch time. The catalog stores the declaration; the Data Plane
            // honors it on first insert via the field_name in VectorInsert.
        }
    }

    state.audit_record(
        AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!("created collection '{name}'"),
    );

    Ok(vec![Response::Execution(Tag::new("CREATE COLLECTION"))])
}

/// DROP COLLECTION <name>
///
/// Marks collection as inactive. Requires owner or admin.
pub fn drop_collection(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if parts.len() < 3 {
        return Err(sqlstate_error("42601", "syntax: DROP COLLECTION <name>"));
    }

    let name_lower = parts[2].to_lowercase();
    let name = name_lower.as_str();
    let tenant_id = identity.tenant_id;

    // Check ownership or admin.
    let is_owner = state
        .permissions
        .get_owner("collection", tenant_id, name)
        .as_deref()
        == Some(&identity.username);

    if !is_owner
        && !identity.is_superuser
        && !identity.has_role(&crate::control::security::identity::Role::TenantAdmin)
    {
        return Err(sqlstate_error(
            "42501",
            "permission denied: only owner, superuser, or tenant_admin can drop collections",
        ));
    }

    // Mark as inactive in catalog.
    if let Some(catalog) = state.credentials.catalog() {
        if let Ok(Some(mut coll)) = catalog.get_collection(tenant_id.as_u32(), name) {
            coll.is_active = false;
            catalog
                .put_collection(&coll)
                .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        } else {
            return Err(sqlstate_error(
                "42P01",
                &format!("collection '{name}' does not exist"),
            ));
        }
    }

    state.audit_record(
        AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!("dropped collection '{name}'"),
    );

    Ok(vec![Response::Execution(Tag::new("DROP COLLECTION"))])
}

/// INSERT INTO <collection> (col1, col2, ...) VALUES (val1, val2, ...)
///
/// Intercepts INSERT for schemaless collections. Parses column names
/// and values manually, serializes as JSON, dispatches as PointPut.
/// Returns `None` if the collection has a typed schema (let DataFusion handle it).
pub async fn insert_document(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> Option<PgWireResult<Vec<Response>>> {
    // Extract collection name from the original SQL, normalized to lowercase
    // per PostgreSQL convention (unquoted identifiers fold to lowercase).
    let upper = sql.to_uppercase();
    let into_pos = upper.find("INSERT INTO ")?;
    let after_into = sql[into_pos + 12..].trim_start();
    let coll_name_str = after_into.split_whitespace().next()?;
    let coll_name_lower = coll_name_str.to_lowercase();
    let coll_name = coll_name_lower.as_str();

    // Check if collection is schemaless (no declared fields).
    let tenant_id = identity.tenant_id;
    if let Some(catalog) = state.credentials.catalog()
        && let Ok(Some(coll)) = catalog.get_collection(tenant_id.as_u32(), coll_name)
        && !coll.fields.is_empty()
    {
        // Typed collection — let DataFusion handle it.
        return None;
    }

    // Find the column list: first (...) in the SQL.
    let first_open = match sql.find('(') {
        Some(p) => p,
        None => {
            return Some(Err(sqlstate_error(
                "42601",
                "missing column list in INSERT",
            )));
        }
    };
    // Find matching close paren (before VALUES).
    let values_kw = match upper.find("VALUES") {
        Some(p) => p,
        None => return Some(Err(sqlstate_error("42601", "missing VALUES clause"))),
    };
    let first_close = match sql[first_open..values_kw].rfind(')') {
        Some(p) => first_open + p,
        None => {
            return Some(Err(sqlstate_error(
                "42601",
                "missing closing ) for column list",
            )));
        }
    };
    let cols_str = &sql[first_open + 1..first_close];
    let columns: Vec<&str> = cols_str.split(',').map(|c| c.trim()).collect();

    // Find VALUES (...).
    let after_values = sql[values_kw + 6..].trim_start();
    let vals_open = match after_values.find('(') {
        Some(p) => p,
        None => return Some(Err(sqlstate_error("42601", "missing VALUES (...)"))),
    };
    let vals_close = match after_values.rfind(')') {
        Some(p) => p,
        None => return Some(Err(sqlstate_error("42601", "missing closing ) for VALUES"))),
    };
    let vals_str = &after_values[vals_open + 1..vals_close];
    let values: Vec<&str> = split_values(vals_str);

    if columns.len() != values.len() {
        return Some(Err(sqlstate_error(
            "42601",
            &format!(
                "column count ({}) doesn't match value count ({})",
                columns.len(),
                values.len()
            ),
        )));
    }

    // First column should be 'id'. Build JSON document from the rest.
    let mut doc_id = String::new();
    let mut fields = serde_json::Map::new();

    for (col, val) in columns.iter().zip(values.iter()) {
        let col = col.trim().trim_matches('"');
        let val = val.trim();
        if col.eq_ignore_ascii_case("id") {
            doc_id = val.trim_matches('\'').to_string();
        } else {
            let json_val = parse_sql_value(val);
            fields.insert(col.to_string(), json_val);
        }
    }

    if doc_id.is_empty() {
        // Auto-generate ID.
        doc_id = format!(
            "{:016x}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
    }

    let value_bytes = serde_json::to_vec(&fields).unwrap_or_default();
    // Route per-document for even distribution across cores.
    // DocumentScan broadcasts to all cores to find all documents.
    let vshard_id = crate::types::VShardId::from_key(doc_id.as_bytes());

    let plan = crate::bridge::envelope::PhysicalPlan::PointPut {
        collection: coll_name.to_string(),
        document_id: doc_id,
        value: value_bytes,
    };

    if let Err(e) = crate::control::server::dispatch_utils::wal_append_if_write(
        &state.wal, tenant_id, vshard_id, &plan,
    ) {
        return Some(Err(sqlstate_error("XX000", &e.to_string())));
    }

    match crate::control::server::dispatch_utils::dispatch_to_data_plane(
        state, tenant_id, vshard_id, plan, 0,
    )
    .await
    {
        Ok(_) => Some(Ok(vec![Response::Execution(Tag::new("INSERT"))])),
        Err(e) => Some(Err(sqlstate_error("XX000", &e.to_string()))),
    }
}

/// Split VALUES content respecting quoted strings.
/// `'hello', 42, 'it''s'` → ["'hello'", "42", "'it''s'"]
fn split_values(s: &str) -> Vec<&str> {
    let mut results = Vec::new();
    let mut start = 0;
    let mut in_quote = false;
    let bytes = s.as_bytes();

    for i in 0..bytes.len() {
        match bytes[i] {
            b'\'' => in_quote = !in_quote,
            b',' if !in_quote => {
                results.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    if start < s.len() {
        results.push(&s[start..]);
    }
    results
}

/// Parse a SQL literal value to a serde_json::Value.
fn parse_sql_value(val: &str) -> serde_json::Value {
    let trimmed = val.trim();
    if trimmed.eq_ignore_ascii_case("NULL") {
        return serde_json::Value::Null;
    }
    if trimmed.eq_ignore_ascii_case("TRUE") {
        return serde_json::Value::Bool(true);
    }
    if trimmed.eq_ignore_ascii_case("FALSE") {
        return serde_json::Value::Bool(false);
    }
    // Quoted string.
    if trimmed.starts_with('\'') && trimmed.ends_with('\'') {
        let inner = &trimmed[1..trimmed.len() - 1];
        let unescaped = inner.replace("''", "'");
        return serde_json::Value::String(unescaped);
    }
    // Number.
    if let Ok(i) = trimmed.parse::<i64>() {
        return serde_json::json!(i);
    }
    if let Ok(f) = trimmed.parse::<f64>() {
        return serde_json::json!(f);
    }
    // ARRAY[...] — keep as string for now.
    serde_json::Value::String(trimmed.to_string())
}

/// CREATE INDEX <name> ON <collection> (<field>)
///
/// Creates an index owned by the collection's owner.
pub fn create_index(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    // CREATE INDEX <name> ON <collection> (<field>)
    if parts.len() < 6 {
        return Err(sqlstate_error(
            "42601",
            "syntax: CREATE INDEX <name> ON <collection> (<field>)",
        ));
    }

    let index_name = parts[2];
    if !parts[3].eq_ignore_ascii_case("ON") {
        return Err(sqlstate_error("42601", "expected ON after index name"));
    }
    let collection = parts[4];
    let field = parts[5].trim_matches(|c| c == '(' || c == ')');
    let tenant_id = identity.tenant_id;

    // Verify collection exists and user has CREATE permission.
    if let Some(catalog) = state.credentials.catalog() {
        match catalog.get_collection(tenant_id.as_u32(), collection) {
            Ok(Some(coll)) if coll.is_active => {
                // Check: must be collection owner, superuser, or tenant_admin.
                let is_owner = coll.owner == identity.username;
                if !is_owner
                    && !identity.is_superuser
                    && !identity.has_role(&crate::control::security::identity::Role::TenantAdmin)
                {
                    return Err(sqlstate_error(
                        "42501",
                        "permission denied: must be collection owner or admin to create indexes",
                    ));
                }
            }
            _ => {
                return Err(sqlstate_error(
                    "42P01",
                    &format!("collection '{collection}' does not exist"),
                ));
            }
        }
    }

    // Index ownership inherits from the collection owner.
    let catalog = state.credentials.catalog();
    let index_owner = if let Some(cat) = catalog {
        cat.get_collection(tenant_id.as_u32(), collection)
            .ok()
            .flatten()
            .map(|c| c.owner)
            .unwrap_or_else(|| identity.username.clone())
    } else {
        identity.username.clone()
    };

    state
        .permissions
        .set_owner(
            "index",
            tenant_id,
            index_name,
            &index_owner,
            catalog.as_ref(),
        )
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    state.audit_record(
        AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!("created index '{index_name}' on '{collection}' ({field})"),
    );

    Ok(vec![Response::Execution(Tag::new("CREATE INDEX"))])
}

/// DROP INDEX <name>
pub fn drop_index(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if parts.len() < 3 {
        return Err(sqlstate_error("42601", "syntax: DROP INDEX <name>"));
    }

    let index_name = parts[2];
    let tenant_id = identity.tenant_id;

    // Check ownership or admin.
    let is_owner = state
        .permissions
        .get_owner("index", tenant_id, index_name)
        .as_deref()
        == Some(&identity.username);

    if !is_owner
        && !identity.is_superuser
        && !identity.has_role(&crate::control::security::identity::Role::TenantAdmin)
    {
        return Err(sqlstate_error(
            "42501",
            "permission denied: must be index owner or admin",
        ));
    }

    // Remove ownership record.
    let catalog = state.credentials.catalog();
    state
        .permissions
        .remove_owner("index", tenant_id, index_name, catalog.as_ref())
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    state.audit_record(
        AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!("dropped index '{index_name}'"),
    );

    Ok(vec![Response::Execution(Tag::new("DROP INDEX"))])
}

/// SHOW COLLECTIONS
///
/// Lists all active collections for the current tenant.
pub fn show_collections(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id;

    let schema = Arc::new(vec![
        text_field("name"),
        text_field("owner"),
        int8_field("created_at"),
    ]);

    let collections = if let Some(catalog) = state.credentials.catalog() {
        if identity.is_superuser {
            catalog
                .load_all_collections()
                .unwrap_or_default()
                .into_iter()
                .filter(|c| c.is_active)
                .collect::<Vec<_>>()
        } else {
            catalog
                .load_collections_for_tenant(tenant_id.as_u32())
                .unwrap_or_default()
        }
    } else {
        Vec::new()
    };

    let mut rows = Vec::with_capacity(collections.len());
    let mut encoder = DataRowEncoder::new(schema.clone());

    for coll in &collections {
        encoder
            .encode_field(&coll.name)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&coll.owner)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&(coll.created_at as i64))
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}

/// SHOW INDEXES [ON <collection>]
///
/// Lists indexes for the current tenant (optionally filtered by collection).
pub fn show_indexes(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id;

    // Parse optional ON <collection> filter.
    let filter_collection = if parts.len() >= 4
        && parts[1].eq_ignore_ascii_case("INDEXES")
        && parts[2].eq_ignore_ascii_case("ON")
    {
        Some(parts[3])
    } else {
        None
    };

    let schema = Arc::new(vec![text_field("index_name"), text_field("owner")]);

    // List all index owners for this tenant.
    let indexes = state.permissions.list_owners("index", tenant_id);

    let mut rows = Vec::new();
    let mut encoder = DataRowEncoder::new(schema.clone());

    for (index_name, owner) in &indexes {
        // If filtering by collection, only show indexes whose name starts with the collection.
        // Convention: index names are typically "<collection>_<field>_idx".
        if let Some(coll) = filter_collection
            && !index_name.starts_with(coll)
        {
            continue;
        }

        encoder
            .encode_field(index_name)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(owner)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}
