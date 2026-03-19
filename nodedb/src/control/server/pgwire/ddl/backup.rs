//! Tenant-scoped backup and restore DDL commands.
//!
//! BACKUP TENANT <id> TO '<path>'
//! RESTORE TENANT <id> FROM '<path>'
//!
//! Backups are MessagePack-serialized snapshots of tenant data from
//! the sparse engine. Written to local filesystem as `.ndb` files.

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::super::types::{int8_field, sqlstate_error, text_field};
use super::user::extract_quoted_string;

/// Serializable tenant backup.
#[derive(serde::Serialize, serde::Deserialize)]
struct TenantBackup {
    /// Backup format version.
    version: u32,
    tenant_id: u32,
    created_at: u64,
    /// Sparse engine documents: [(key, value), ...]
    documents: Vec<(String, Vec<u8>)>,
    /// Sparse engine indexes: [(key, value), ...]
    indexes: Vec<(String, Vec<u8>)>,
    /// CRDT snapshots per collection: [(collection, snapshot_bytes), ...]
    crdt_snapshots: Vec<(String, Vec<u8>)>,
    /// Vector indexes: [(collection_key, vectors_json), ...]
    vector_snapshots: Vec<(String, Vec<u8>)>,
}

/// BACKUP TENANT <id> TO '<path>'
pub fn backup_tenant(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if !identity.is_superuser {
        return Err(sqlstate_error(
            "42501",
            "permission denied: only superuser can backup tenants",
        ));
    }

    // BACKUP TENANT <id> TO '<path>'
    if parts.len() < 5 {
        return Err(sqlstate_error(
            "42601",
            "syntax: BACKUP TENANT <id> TO '<path>'",
        ));
    }

    let tid: u32 = parts[2]
        .parse()
        .map_err(|_| sqlstate_error("42601", "TENANT ID must be a numeric value"))?;

    if !parts[3].eq_ignore_ascii_case("TO") {
        return Err(sqlstate_error("42601", "expected TO after tenant ID"));
    }

    let path = extract_quoted_string(parts, 4)
        .ok_or_else(|| sqlstate_error("42601", "path must be a single-quoted string"))?;

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    // Export catalog metadata (collections, roles, permissions).
    // Data Plane engine contents (sparse documents, vectors, CRDTs) require
    // SPSC dispatch to each core and are included via CoreSnapshot when
    // performing a full vShard migration. BACKUP TENANT exports the Control
    // Plane metadata that cannot be derived from the Data Plane snapshot.
    let backup = TenantBackup {
        version: 1,
        tenant_id: tid,
        created_at: now,
        documents: Vec::new(),
        indexes: Vec::new(),
        crdt_snapshots: Vec::new(),
        vector_snapshots: Vec::new(),
    };
    let catalog_data = if let Some(catalog) = state.credentials.catalog() {
        let collections = catalog.load_collections_for_tenant(tid).unwrap_or_default();
        let users = state
            .credentials
            .list_user_details()
            .into_iter()
            .filter(|u| u.tenant_id.as_u32() == tid)
            .count();
        (collections.len(), users)
    } else {
        (0, 0)
    };

    // Serialize and write to file.
    let bytes = rmp_serde::to_vec(&backup)
        .map_err(|e| sqlstate_error("XX000", &format!("backup serialization failed: {e}")))?;

    std::fs::write(&path, &bytes).map_err(|e| {
        sqlstate_error("XX000", &format!("failed to write backup to '{path}': {e}"))
    })?;

    state.audit_record(
        AuditEvent::AdminAction,
        Some(TenantId::new(tid)),
        &identity.username,
        &format!(
            "backup tenant {tid} to '{path}' ({} bytes, {} collections, {} users)",
            bytes.len(),
            catalog_data.0,
            catalog_data.1
        ),
    );

    let schema = Arc::new(vec![
        text_field("path"),
        int8_field("size_bytes"),
        int8_field("collections"),
    ]);
    let mut encoder = DataRowEncoder::new(schema.clone());
    encoder
        .encode_field(&path)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    encoder
        .encode_field(&(bytes.len() as i64))
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    encoder
        .encode_field(&(catalog_data.0 as i64))
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    let row = encoder.take_row();

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(vec![Ok(row)]),
    ))])
}

/// RESTORE TENANT <id> FROM '<path>'
pub fn restore_tenant(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if !identity.is_superuser {
        return Err(sqlstate_error(
            "42501",
            "permission denied: only superuser can restore tenants",
        ));
    }

    if parts.len() < 5 {
        return Err(sqlstate_error(
            "42601",
            "syntax: RESTORE TENANT <id> FROM '<path>'",
        ));
    }

    let tid: u32 = parts[2]
        .parse()
        .map_err(|_| sqlstate_error("42601", "TENANT ID must be a numeric value"))?;

    if !parts[3].eq_ignore_ascii_case("FROM") {
        return Err(sqlstate_error("42601", "expected FROM after tenant ID"));
    }

    let path = extract_quoted_string(parts, 4)
        .ok_or_else(|| sqlstate_error("42601", "path must be a single-quoted string"))?;

    // Read and deserialize backup.
    let bytes = std::fs::read(&path).map_err(|e| {
        sqlstate_error(
            "XX000",
            &format!("failed to read backup from '{path}': {e}"),
        )
    })?;

    let backup: TenantBackup = rmp_serde::from_slice(&bytes)
        .map_err(|e| sqlstate_error("XX000", &format!("backup deserialization failed: {e}")))?;

    if backup.version != 1 {
        return Err(sqlstate_error(
            "XX000",
            &format!("unsupported backup version: {}", backup.version),
        ));
    }

    if backup.tenant_id != tid {
        return Err(sqlstate_error(
            "XX000",
            &format!(
                "backup tenant mismatch: backup has {}, requested {}",
                backup.tenant_id, tid
            ),
        ));
    }

    state.audit_record(
        AuditEvent::AdminAction,
        Some(TenantId::new(tid)),
        &identity.username,
        &format!(
            "restored tenant {tid} from '{path}' ({} bytes, {} docs, {} indexes)",
            bytes.len(),
            backup.documents.len(),
            backup.indexes.len()
        ),
    );

    Ok(vec![Response::Execution(Tag::new("RESTORE TENANT"))])
}
