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
    /// Backup format version (v2 = all engines).
    version: u32,
    tenant_id: u32,
    created_at: u64,
    /// Sparse engine documents: [(key, value), ...]
    documents: Vec<(String, Vec<u8>)>,
    /// Sparse engine indexes: [(key, value), ...]
    indexes: Vec<(String, Vec<u8>)>,
    /// CRDT snapshots: [(tenant_key, loro_export_bytes), ...]
    crdt_snapshots: Vec<(String, Vec<u8>)>,
    /// Vector collections: [(index_key, serialized_vectors), ...]
    vector_snapshots: Vec<(String, Vec<u8>)>,
    /// Graph edges: [(composite_key, properties), ...]
    #[serde(default)]
    edges: Vec<(String, Vec<u8>)>,
    /// KV tables: [(hash_key_str, serialized_entries), ...]
    #[serde(default)]
    kv_tables: Vec<(String, Vec<u8>)>,
    /// Timeseries memtables: [(scoped_collection, serialized_columns), ...]
    #[serde(default)]
    timeseries: Vec<(String, Vec<u8>)>,
}

/// BACKUP TENANT <id> TO '<path>'
pub async fn backup_tenant(
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

    // Dispatch to Data Plane to snapshot all documents + indexes for this tenant.
    let snapshot_plan = crate::bridge::envelope::PhysicalPlan::Meta(
        crate::bridge::physical_plan::MetaOp::CreateTenantSnapshot { tenant_id: tid },
    );
    let snapshot_bytes = super::sync_dispatch::dispatch_async(
        state,
        TenantId::new(tid),
        "__system",
        snapshot_plan,
        std::time::Duration::from_secs(60),
    )
    .await
    .map_err(|e| sqlstate_error("XX000", &format!("snapshot dispatch failed: {e}")))?;

    // Deserialize Data Plane snapshot.
    let data_snapshot: crate::types::TenantDataSnapshot = rmp_serde::from_slice(&snapshot_bytes)
        .map_err(|e| sqlstate_error("XX000", &format!("snapshot decode failed: {e}")))?;

    let backup = TenantBackup {
        version: 2,
        tenant_id: tid,
        created_at: now,
        documents: data_snapshot.documents,
        indexes: data_snapshot.indexes,
        crdt_snapshots: data_snapshot.crdt_state,
        vector_snapshots: data_snapshot.vectors,
        edges: data_snapshot.edges,
        kv_tables: data_snapshot.kv_tables,
        timeseries: data_snapshot.timeseries,
    };
    let catalog_data = if let Some(catalog) = state.credentials.catalog() {
        let collections = match catalog.load_collections_for_tenant(tid) {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(tenant_id = tid, error = %e, "failed to load collections for audit");
                Vec::new()
            }
        };
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

    // Serialize backup.
    let plaintext = rmp_serde::to_vec(&backup)
        .map_err(|e| sqlstate_error("XX000", &format!("backup serialization failed: {e}")))?;

    // Encrypt if WAL encryption is configured (reuses same key).
    let bytes = if let Some(key) = state.wal.encryption_key() {
        // Use LSN 0 as nonce for backups (distinct from WAL records which use real LSNs).
        // AAD = "backup" to bind ciphertext to backup context.
        let mut aad = [0u8; nodedb_wal::record::HEADER_SIZE];
        aad[..6].copy_from_slice(b"BACKUP");
        let encrypted = key
            .encrypt(0, &aad, &plaintext)
            .map_err(|e| sqlstate_error("XX000", &format!("backup encryption failed: {e}")))?;
        // Prepend magic bytes to identify encrypted backup.
        let mut output = Vec::with_capacity(4 + encrypted.len());
        output.extend_from_slice(b"NENC"); // NodeDB ENCrypted
        output.extend_from_slice(&encrypted);
        output
    } else {
        plaintext
    };

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
pub async fn restore_tenant(
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

    // Read backup file.
    let raw_bytes = std::fs::read(&path).map_err(|e| {
        sqlstate_error(
            "XX000",
            &format!("failed to read backup from '{path}': {e}"),
        )
    })?;

    // Decrypt if encrypted (magic bytes "NENC").
    let bytes = if raw_bytes.starts_with(b"NENC") {
        let key = state.wal.encryption_key().ok_or_else(|| {
            sqlstate_error(
                "XX000",
                "backup is encrypted but no encryption key configured",
            )
        })?;
        let mut aad = [0u8; nodedb_wal::record::HEADER_SIZE];
        aad[..6].copy_from_slice(b"BACKUP");
        key.decrypt(0, &aad, &raw_bytes[4..])
            .map_err(|e| sqlstate_error("XX000", &format!("backup decryption failed: {e}")))?
    } else {
        raw_bytes
    };

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

    // Dispatch documents + indexes to Data Plane for restoration.
    let documents_bytes = rmp_serde::to_vec(&backup.documents)
        .map_err(|e| sqlstate_error("XX000", &format!("document serialization failed: {e}")))?;
    let indexes_bytes = rmp_serde::to_vec(&backup.indexes)
        .map_err(|e| sqlstate_error("XX000", &format!("index serialization failed: {e}")))?;

    let restore_plan = crate::bridge::envelope::PhysicalPlan::Meta(
        crate::bridge::physical_plan::MetaOp::RestoreTenantSnapshot {
            tenant_id: tid,
            documents: documents_bytes,
            indexes: indexes_bytes,
        },
    );
    super::sync_dispatch::dispatch_async(
        state,
        TenantId::new(tid),
        "__system",
        restore_plan,
        std::time::Duration::from_secs(60),
    )
    .await
    .map_err(|e| sqlstate_error("XX000", &format!("restore dispatch failed: {e}")))?;

    // Re-register collections found in backup keys but missing from catalog.
    // Document keys have the format "tenant_id:collection:doc_id".
    if let Some(catalog) = state.credentials.catalog() {
        let existing = match catalog.load_collections_for_tenant(tid) {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(tenant_id = tid, error = %e, "failed to load collections from catalog");
                Vec::new()
            }
        };
        let existing_names: std::collections::HashSet<String> =
            existing.iter().map(|c| c.name.clone()).collect();

        // Extract unique collection names from backup document keys.
        let mut restored_collections = std::collections::HashSet::new();
        for (key, _) in &backup.documents {
            // Key format: "{tenant_id}:{collection}\0{doc_id}" or "{tenant_id}:{collection}:{doc_id}"
            if let Some(after_tenant) = key.strip_prefix(&format!("{tid}:")) {
                let collection_name = after_tenant.split(['\0', ':']).next().unwrap_or("");
                if !collection_name.is_empty() && !existing_names.contains(collection_name) {
                    restored_collections.insert(collection_name.to_string());
                }
            }
        }

        for name in &restored_collections {
            let coll = crate::control::security::catalog::types::StoredCollection::new(
                tid,
                name,
                &identity.username,
            );
            if let Err(e) = catalog.put_collection(&coll) {
                tracing::warn!(collection = %name, error = %e, "failed to register restored collection");
            }
        }
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

/// RESTORE TENANT <id> FROM '<path>' DRY RUN
///
/// Validates a restore plan without executing it. Checks:
/// - Backup file exists and is readable
/// - Backup structure is valid (MessagePack deserializable)
///
/// Returns a description of what would happen.
pub fn restore_tenant_dry_run(
    _state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if !identity.is_superuser {
        return Err(sqlstate_error(
            "42501",
            "permission denied: only superuser can validate restores",
        ));
    }

    // Parse: RESTORE TENANT <id> FROM '<path>' DRY RUN
    // parts[2] = id, parts[4] = path (with quotes)
    if parts.len() < 5 {
        return Err(sqlstate_error(
            "42601",
            "syntax: RESTORE TENANT <id> FROM '<path>' DRY RUN",
        ));
    }

    let path = parts[4].trim_matches('\'').trim_matches('"');

    // Check file exists.
    let file_path = std::path::Path::new(path);
    if !file_path.exists() {
        return Ok(vec![Response::Execution(Tag::new(&format!(
            "DRY RUN FAILED: backup file '{}' does not exist",
            path
        )))]);
    }

    // Check file is readable and has content.
    let metadata = std::fs::metadata(file_path)
        .map_err(|e| sqlstate_error("XX000", &format!("cannot read backup file: {e}")))?;

    let size_mb = metadata.len() as f64 / (1024.0 * 1024.0);

    // Try to read and validate the backup header.
    let data = std::fs::read(file_path)
        .map_err(|e| sqlstate_error("XX000", &format!("cannot read backup file: {e}")))?;

    // Validate MessagePack structure.
    let valid = rmp_serde::from_slice::<TenantBackup>(&data).is_ok();

    let status = if valid {
        format!(
            "DRY RUN OK: backup file '{}' is valid ({:.2} MB). Ready for restore.",
            path, size_mb
        )
    } else {
        format!(
            "DRY RUN FAILED: backup file '{}' ({:.2} MB) has invalid format or is corrupted.",
            path, size_mb
        )
    };

    Ok(vec![Response::Execution(Tag::new(&status))])
}
