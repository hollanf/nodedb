//! Audit WAL archival to S3/object store.
//!
//! Archives sealed audit WAL segments to cloud storage for long-term retention.
//! Supports legal hold flag to prevent archival deletion.
//! Reuses the same `object_store` infrastructure as timeseries S3 archival.

use std::path::Path;

use object_store::ObjectStoreExt;
use object_store::aws::AmazonS3Builder;
use tracing::info;

/// Configuration for audit WAL archival.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AuditArchiveConfig {
    /// S3 bucket name.
    pub bucket: String,
    /// Key prefix (e.g. `"audit-wal/"`).
    pub prefix: String,
    /// S3 endpoint (for MinIO, R2, Spaces, etc.). `None` = AWS default.
    pub endpoint: Option<String>,
    /// AWS region.
    pub region: String,
    /// Whether legal hold prevents deletion of archived segments.
    pub legal_hold: bool,
}

/// Build an object_store S3 client from config.
fn build_store(config: &AuditArchiveConfig) -> crate::Result<object_store::aws::AmazonS3> {
    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(&config.bucket)
        .with_region(&config.region);

    if let Some(ref endpoint) = config.endpoint {
        builder = builder
            .with_endpoint(endpoint)
            .with_virtual_hosted_style_request(false);
    }

    // Credentials from environment (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY)
    // or IAM role (EC2/ECS).
    builder.build().map_err(|e| crate::Error::Storage {
        engine: "audit_archive".into(),
        detail: format!("failed to build S3 client: {e}"),
    })
}

/// Archive all sealed audit WAL segments to S3.
///
/// Reads segment files from the audit WAL directory, uploads each one,
/// then writes a marker file with metadata. Does NOT delete local segments
/// after upload (deletion is a separate retention policy operation).
///
/// Returns the number of segments archived.
pub async fn archive_audit_segments(
    audit_wal_dir: &Path,
    config: &AuditArchiveConfig,
) -> crate::Result<u64> {
    let store = build_store(config)?;

    // List all .seg files in the audit WAL directory.
    let mut archived = 0u64;

    let entries = std::fs::read_dir(audit_wal_dir).map_err(|e| crate::Error::Storage {
        engine: "audit_archive".into(),
        detail: format!("failed to read audit WAL directory: {e}"),
    })?;

    for entry in entries {
        let entry = entry.map_err(|e| crate::Error::Storage {
            engine: "audit_archive".into(),
            detail: format!("failed to read directory entry: {e}"),
        })?;

        let path = entry.path();
        let file_name = match path.file_name().and_then(|n| n.to_str()) {
            Some(n) if n.ends_with(".seg") => n.to_string(),
            _ => continue, // Skip non-segment files.
        };

        // Read the segment file.
        let data = std::fs::read(&path).map_err(|e| crate::Error::Storage {
            engine: "audit_archive".into(),
            detail: format!("failed to read segment {file_name}: {e}"),
        })?;

        // Upload to S3.
        let object_key = format!("{}{}", config.prefix, file_name);
        let location = object_store::path::Path::from(object_key.clone());

        store
            .put(&location, object_store::PutPayload::from(data))
            .await
            .map_err(|e| crate::Error::Storage {
                engine: "audit_archive".into(),
                detail: format!("failed to upload {file_name} to S3: {e}"),
            })?;

        info!(segment = %file_name, key = %object_key, "audit WAL segment archived to S3");
        archived += 1;
    }

    // Write a marker file with archival metadata.
    if archived > 0 {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .map_err(|e| crate::Error::Internal {
                detail: format!("system clock error: {e}"),
            })?;

        let marker = serde_json::json!({
            "segments_archived": archived,
            "archived_at_ms": now_ms,
            "legal_hold": config.legal_hold,
        });
        let marker_key = format!("{}archive-marker-{now_ms}.json", config.prefix);
        let marker_location = object_store::path::Path::from(marker_key);
        let marker_bytes = serde_json::to_vec(&marker).map_err(|e| crate::Error::Internal {
            detail: format!("failed to serialize audit archive marker: {e}"),
        })?;

        let _ = store
            .put(
                &marker_location,
                object_store::PutPayload::from(marker_bytes),
            )
            .await;
    }

    Ok(archived)
}
