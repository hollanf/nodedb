//! Error types shared between NodeDB Origin and Lite.
//!
//! These are the errors that application code sees through the `NodeDb` trait.
//! Infrastructure-specific errors (WAL, io_uring, Raft) live in their
//! respective crates and are not part of the portable type system.

/// Errors returned by `NodeDb` trait methods.
///
/// Every variant is actionable — clients can programmatically handle each one.
#[derive(Debug, thiserror::Error)]
pub enum NodeDbError {
    // ─── Write path ──────────────────────────────────────────────────
    #[error("constraint violation on {collection}: {detail}")]
    ConstraintViolation { collection: String, detail: String },

    #[error("authorization denied on {resource}")]
    AuthorizationDenied { resource: String },

    #[error("request exceeded deadline")]
    DeadlineExceeded,

    #[error("write conflict on {collection}/{document_id}, retry with idempotency key")]
    WriteConflict {
        collection: String,
        document_id: String,
    },

    // ─── Read path ───────────────────────────────────────────────────
    #[error("collection '{collection}' not found")]
    CollectionNotFound { collection: String },

    #[error("document '{document_id}' not found in '{collection}'")]
    DocumentNotFound {
        collection: String,
        document_id: String,
    },

    // ─── Sync ────────────────────────────────────────────────────────
    #[error("sync connection failed: {detail}")]
    SyncConnectionFailed { detail: String },

    #[error("sync delta rejected: {reason}")]
    SyncDeltaRejected {
        reason: String,
        compensation: Option<crate::sync::compensation::CompensationHint>,
    },

    #[error("shape subscription failed for '{shape_id}': {detail}")]
    ShapeSubscriptionFailed { shape_id: String, detail: String },

    // ─── Storage ─────────────────────────────────────────────────────
    #[error("storage error: {detail}")]
    Storage { detail: String },

    #[error("serialization error ({format}): {detail}")]
    Serialization { format: String, detail: String },

    // ─── Client input ────────────────────────────────────────────────
    #[error("bad request: {detail}")]
    BadRequest { detail: String },

    #[error("SQL not enabled (compile with 'sql' feature)")]
    SqlNotEnabled,

    // ─── Memory ──────────────────────────────────────────────────────
    #[error("memory budget exhausted for engine {engine}")]
    MemoryExhausted { engine: String },

    // ─── Internal ────────────────────────────────────────────────────
    #[error("internal error: {detail}")]
    Internal { detail: String },
}

/// Result alias for NodeDb operations.
pub type NodeDbResult<T> = std::result::Result<T, NodeDbError>;

impl From<std::io::Error> for NodeDbError {
    fn from(e: std::io::Error) -> Self {
        Self::Storage {
            detail: e.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_display_constraint() {
        let e = NodeDbError::ConstraintViolation {
            collection: "users".into(),
            detail: "duplicate email".into(),
        };
        assert!(e.to_string().contains("constraint violation"));
        assert!(e.to_string().contains("users"));
    }

    #[test]
    fn error_display_sync_rejected() {
        let e = NodeDbError::SyncDeltaRejected {
            reason: "unique violation".into(),
            compensation: Some(
                crate::sync::compensation::CompensationHint::UniqueViolation {
                    field: "email".into(),
                    conflicting_value: "a@b.com".into(),
                },
            ),
        };
        assert!(e.to_string().contains("sync delta rejected"));
    }

    #[test]
    fn error_display_sql_not_enabled() {
        let e = NodeDbError::SqlNotEnabled;
        assert!(e.to_string().contains("SQL not enabled"));
    }

    #[test]
    fn io_error_converts() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file missing");
        let e: NodeDbError = io_err.into();
        assert!(matches!(e, NodeDbError::Storage { .. }));
    }
}
