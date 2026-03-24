//! Standardized error types for the NodeDB public API.
//!
//! These are the errors that application code sees through the `NodeDb` trait.
//! Every error carries a stable numeric [`ErrorCode`] for programmatic handling
//! and a human-readable message for debugging.
//!
//! # Error code ranges
//!
//! | Range       | Category      | Examples                              |
//! |-------------|---------------|---------------------------------------|
//! | 1000–1099   | Write path    | constraint violation, write conflict   |
//! | 1100–1199   | Read path     | collection/document not found          |
//! | 1200–1299   | Query         | plan error, fan-out exceeded           |
//! | 2000–2099   | Auth/Security | authorization denied, auth expired     |
//! | 3000–3099   | Sync          | connection failed, delta rejected      |
//! | 4000–4099   | Storage       | I/O, segment corruption, cold storage  |
//! | 4100–4199   | WAL           | checksum mismatch, sealed              |
//! | 4200–4299   | Serialization | encode/decode failures                 |
//! | 5000–5099   | Config        | invalid configuration                  |
//! | 6000–6099   | Cluster       | no leader, not leader, migration       |
//! | 7000–7099   | Memory        | budget exhausted, ceiling exceeded     |
//! | 8000–8099   | Encryption    | key load, DEK, rotation                |
//! | 9000–9099   | Internal      | catch-all, bridge, dispatch            |
//!
//! Infrastructure-specific errors (WAL, io_uring, Raft) live in their
//! respective crates. At the public API boundary they convert into
//! `NodeDbError` so consumers never see internal implementation details.

use std::fmt;

// ---------------------------------------------------------------------------
// Error codes
// ---------------------------------------------------------------------------

/// Stable numeric error codes for programmatic error handling.
///
/// Clients can match on these codes without parsing error messages.
/// Codes are intentionally sparse within each range so new codes can be
/// added without renumbering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ErrorCode(pub u16);

impl ErrorCode {
    // Write path (1000–1099)
    pub const CONSTRAINT_VIOLATION: Self = Self(1000);
    pub const WRITE_CONFLICT: Self = Self(1001);
    pub const DEADLINE_EXCEEDED: Self = Self(1002);
    pub const PREVALIDATION_REJECTED: Self = Self(1003);

    // Read path (1100–1199)
    pub const COLLECTION_NOT_FOUND: Self = Self(1100);
    pub const DOCUMENT_NOT_FOUND: Self = Self(1101);

    // Query (1200–1299)
    pub const PLAN_ERROR: Self = Self(1200);
    pub const FAN_OUT_EXCEEDED: Self = Self(1201);
    pub const SQL_NOT_ENABLED: Self = Self(1202);

    // Auth / Security (2000–2099)
    pub const AUTHORIZATION_DENIED: Self = Self(2000);
    pub const AUTH_EXPIRED: Self = Self(2001);

    // Sync (3000–3099)
    pub const SYNC_CONNECTION_FAILED: Self = Self(3000);
    pub const SYNC_DELTA_REJECTED: Self = Self(3001);
    pub const SHAPE_SUBSCRIPTION_FAILED: Self = Self(3002);

    // Storage (4000–4099)
    pub const STORAGE: Self = Self(4000);
    pub const SEGMENT_CORRUPTED: Self = Self(4001);
    pub const COLD_STORAGE: Self = Self(4002);
    pub const PARQUET: Self = Self(4003);

    // WAL (4100–4199)
    pub const WAL: Self = Self(4100);
    pub const WAL_CHECKSUM_MISMATCH: Self = Self(4101);
    pub const WAL_SEALED: Self = Self(4102);

    // Serialization (4200–4299)
    pub const SERIALIZATION: Self = Self(4200);
    pub const CODEC: Self = Self(4201);

    // Config (5000–5099)
    pub const CONFIG: Self = Self(5000);

    // Cluster (6000–6099)
    pub const NO_LEADER: Self = Self(6000);
    pub const NOT_LEADER: Self = Self(6001);
    pub const MIGRATION_IN_PROGRESS: Self = Self(6002);
    pub const NODE_UNREACHABLE: Self = Self(6003);
    pub const CLUSTER: Self = Self(6010);

    // Memory (7000–7099)
    pub const MEMORY_EXHAUSTED: Self = Self(7000);

    // Encryption (8000–8099)
    pub const ENCRYPTION: Self = Self(8000);

    // Internal (9000–9099)
    pub const INTERNAL: Self = Self(9000);
    pub const BRIDGE: Self = Self(9001);
    pub const DISPATCH: Self = Self(9002);
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NDB-{:04}", self.0)
    }
}

// ---------------------------------------------------------------------------
// NodeDbError
// ---------------------------------------------------------------------------

/// Errors returned by `NodeDb` trait methods.
///
/// Every variant is actionable — clients can programmatically handle each one
/// via the associated [`ErrorCode`]. Internal infrastructure errors (WAL,
/// Raft, io_uring) are mapped to appropriate public variants at the API
/// boundary so callers never see implementation details.
#[derive(Debug, thiserror::Error)]
pub enum NodeDbError {
    // ── Write path ──────────────────────────────────────────────────────
    #[error("[{code}] constraint violation on {collection}: {detail}",
            code = ErrorCode::CONSTRAINT_VIOLATION)]
    ConstraintViolation { collection: String, detail: String },

    #[error("[{code}] write conflict on {collection}/{document_id}, retry with idempotency key",
            code = ErrorCode::WRITE_CONFLICT)]
    WriteConflict {
        collection: String,
        document_id: String,
    },

    #[error("[{code}] request exceeded deadline", code = ErrorCode::DEADLINE_EXCEEDED)]
    DeadlineExceeded,

    #[error("[{code}] pre-validation rejected: {constraint} — {reason}",
            code = ErrorCode::PREVALIDATION_REJECTED)]
    PrevalidationRejected { constraint: String, reason: String },

    // ── Read path ───────────────────────────────────────────────────────
    #[error("[{code}] collection '{collection}' not found",
            code = ErrorCode::COLLECTION_NOT_FOUND)]
    CollectionNotFound { collection: String },

    #[error("[{code}] document '{document_id}' not found in '{collection}'",
            code = ErrorCode::DOCUMENT_NOT_FOUND)]
    DocumentNotFound {
        collection: String,
        document_id: String,
    },

    // ── Query ───────────────────────────────────────────────────────────
    #[error("[{code}] query plan error: {detail}", code = ErrorCode::PLAN_ERROR)]
    PlanError { detail: String },

    #[error("[{code}] query fan-out exceeded: {shards_touched} shards > limit {limit}",
            code = ErrorCode::FAN_OUT_EXCEEDED)]
    FanOutExceeded { shards_touched: u16, limit: u16 },

    #[error("[{code}] SQL not enabled (compile with 'sql' feature)",
            code = ErrorCode::SQL_NOT_ENABLED)]
    SqlNotEnabled,

    // ── Auth / Security ─────────────────────────────────────────────────
    #[error("[{code}] authorization denied on {resource}",
            code = ErrorCode::AUTHORIZATION_DENIED)]
    AuthorizationDenied { resource: String },

    #[error("[{code}] auth expired: re-authenticate before proceeding",
            code = ErrorCode::AUTH_EXPIRED)]
    AuthExpired { detail: String },

    // ── Sync ────────────────────────────────────────────────────────────
    #[error("[{code}] sync connection failed: {detail}",
            code = ErrorCode::SYNC_CONNECTION_FAILED)]
    SyncConnectionFailed { detail: String },

    #[error("[{code}] sync delta rejected: {reason}",
            code = ErrorCode::SYNC_DELTA_REJECTED)]
    SyncDeltaRejected {
        reason: String,
        compensation: Option<crate::sync::compensation::CompensationHint>,
    },

    #[error("[{code}] shape subscription failed for '{shape_id}': {detail}",
            code = ErrorCode::SHAPE_SUBSCRIPTION_FAILED)]
    ShapeSubscriptionFailed { shape_id: String, detail: String },

    // ── Storage ─────────────────────────────────────────────────────────
    #[error("[{code}] storage error: {detail}", code = ErrorCode::STORAGE)]
    Storage { detail: String },

    #[error("[{code}] segment corrupted: {detail}", code = ErrorCode::SEGMENT_CORRUPTED)]
    SegmentCorrupted { detail: String },

    #[error("[{code}] cold storage error: {detail}", code = ErrorCode::COLD_STORAGE)]
    ColdStorage { detail: String },

    // ── WAL ─────────────────────────────────────────────────────────────
    #[error("[{code}] WAL error: {detail}", code = ErrorCode::WAL)]
    Wal { detail: String },

    // ── Serialization ───────────────────────────────────────────────────
    #[error("[{code}] serialization error ({format}): {detail}",
            code = ErrorCode::SERIALIZATION)]
    Serialization { format: String, detail: String },

    #[error("[{code}] codec error: {detail}", code = ErrorCode::CODEC)]
    Codec { detail: String },

    // ── Config ──────────────────────────────────────────────────────────
    #[error("[{code}] configuration error: {detail}", code = ErrorCode::CONFIG)]
    Config { detail: String },

    // ── Cluster ─────────────────────────────────────────────────────────
    #[error("[{code}] no serving leader for the requested partition",
            code = ErrorCode::NO_LEADER)]
    NoLeader { detail: String },

    #[error("[{code}] not leader; redirect to leader at {leader_addr}",
            code = ErrorCode::NOT_LEADER)]
    NotLeader { leader_addr: String },

    #[error("[{code}] migration in progress", code = ErrorCode::MIGRATION_IN_PROGRESS)]
    MigrationInProgress { detail: String },

    #[error("[{code}] node unreachable: {detail}", code = ErrorCode::NODE_UNREACHABLE)]
    NodeUnreachable { detail: String },

    #[error("[{code}] cluster error: {detail}", code = ErrorCode::CLUSTER)]
    Cluster { detail: String },

    // ── Memory ──────────────────────────────────────────────────────────
    #[error("[{code}] memory budget exhausted for engine {engine}",
            code = ErrorCode::MEMORY_EXHAUSTED)]
    MemoryExhausted { engine: String },

    // ── Encryption ──────────────────────────────────────────────────────
    #[error("[{code}] encryption error: {detail}", code = ErrorCode::ENCRYPTION)]
    Encryption { detail: String },

    // ── Bridge / Dispatch ───────────────────────────────────────────────
    #[error("[{code}] bridge error: {detail}", code = ErrorCode::BRIDGE)]
    Bridge { detail: String },

    #[error("[{code}] dispatch error: {detail}", code = ErrorCode::DISPATCH)]
    Dispatch { detail: String },

    // ── Internal ────────────────────────────────────────────────────────
    #[error("[{code}] internal error: {detail}", code = ErrorCode::INTERNAL)]
    Internal { detail: String },

    // ── Client input ────────────────────────────────────────────────────
    #[error("[{code}] bad request: {detail}", code = ErrorCode::CONFIG)]
    BadRequest { detail: String },
}

impl NodeDbError {
    /// The stable numeric error code for this error.
    pub fn error_code(&self) -> ErrorCode {
        match self {
            // Write path
            Self::ConstraintViolation { .. } => ErrorCode::CONSTRAINT_VIOLATION,
            Self::WriteConflict { .. } => ErrorCode::WRITE_CONFLICT,
            Self::DeadlineExceeded => ErrorCode::DEADLINE_EXCEEDED,
            Self::PrevalidationRejected { .. } => ErrorCode::PREVALIDATION_REJECTED,

            // Read path
            Self::CollectionNotFound { .. } => ErrorCode::COLLECTION_NOT_FOUND,
            Self::DocumentNotFound { .. } => ErrorCode::DOCUMENT_NOT_FOUND,

            // Query
            Self::PlanError { .. } => ErrorCode::PLAN_ERROR,
            Self::FanOutExceeded { .. } => ErrorCode::FAN_OUT_EXCEEDED,
            Self::SqlNotEnabled => ErrorCode::SQL_NOT_ENABLED,

            // Auth
            Self::AuthorizationDenied { .. } => ErrorCode::AUTHORIZATION_DENIED,
            Self::AuthExpired { .. } => ErrorCode::AUTH_EXPIRED,

            // Sync
            Self::SyncConnectionFailed { .. } => ErrorCode::SYNC_CONNECTION_FAILED,
            Self::SyncDeltaRejected { .. } => ErrorCode::SYNC_DELTA_REJECTED,
            Self::ShapeSubscriptionFailed { .. } => ErrorCode::SHAPE_SUBSCRIPTION_FAILED,

            // Storage
            Self::Storage { .. } => ErrorCode::STORAGE,
            Self::SegmentCorrupted { .. } => ErrorCode::SEGMENT_CORRUPTED,
            Self::ColdStorage { .. } => ErrorCode::COLD_STORAGE,

            // WAL
            Self::Wal { .. } => ErrorCode::WAL,

            // Serialization
            Self::Serialization { .. } => ErrorCode::SERIALIZATION,
            Self::Codec { .. } => ErrorCode::CODEC,

            // Config
            Self::Config { .. } => ErrorCode::CONFIG,

            // Cluster
            Self::NoLeader { .. } => ErrorCode::NO_LEADER,
            Self::NotLeader { .. } => ErrorCode::NOT_LEADER,
            Self::MigrationInProgress { .. } => ErrorCode::MIGRATION_IN_PROGRESS,
            Self::NodeUnreachable { .. } => ErrorCode::NODE_UNREACHABLE,
            Self::Cluster { .. } => ErrorCode::CLUSTER,

            // Memory
            Self::MemoryExhausted { .. } => ErrorCode::MEMORY_EXHAUSTED,

            // Encryption
            Self::Encryption { .. } => ErrorCode::ENCRYPTION,

            // Bridge / Dispatch
            Self::Bridge { .. } => ErrorCode::BRIDGE,
            Self::Dispatch { .. } => ErrorCode::DISPATCH,

            // Internal
            Self::Internal { .. } => ErrorCode::INTERNAL,

            // Client input
            Self::BadRequest { .. } => ErrorCode::CONFIG,
        }
    }

    /// Whether this error is retriable by the client.
    pub fn is_retriable(&self) -> bool {
        matches!(
            self,
            Self::WriteConflict { .. }
                | Self::DeadlineExceeded
                | Self::NoLeader { .. }
                | Self::NotLeader { .. }
                | Self::MigrationInProgress { .. }
                | Self::NodeUnreachable { .. }
                | Self::SyncConnectionFailed { .. }
                | Self::Bridge { .. }
                | Self::MemoryExhausted { .. }
        )
    }

    /// Whether this error indicates the client sent invalid input.
    pub fn is_client_error(&self) -> bool {
        matches!(
            self,
            Self::BadRequest { .. }
                | Self::ConstraintViolation { .. }
                | Self::CollectionNotFound { .. }
                | Self::DocumentNotFound { .. }
                | Self::AuthorizationDenied { .. }
                | Self::AuthExpired { .. }
                | Self::Config { .. }
                | Self::SqlNotEnabled
        )
    }
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
    fn error_code_display() {
        assert_eq!(ErrorCode::CONSTRAINT_VIOLATION.to_string(), "NDB-1000");
        assert_eq!(ErrorCode::INTERNAL.to_string(), "NDB-9000");
        assert_eq!(ErrorCode::WAL.to_string(), "NDB-4100");
    }

    #[test]
    fn error_display_includes_code() {
        let e = NodeDbError::ConstraintViolation {
            collection: "users".into(),
            detail: "duplicate email".into(),
        };
        let msg = e.to_string();
        assert!(msg.contains("NDB-1000"));
        assert!(msg.contains("constraint violation"));
        assert!(msg.contains("users"));
    }

    #[test]
    fn error_code_method() {
        let e = NodeDbError::WriteConflict {
            collection: "orders".into(),
            document_id: "abc".into(),
        };
        assert_eq!(e.error_code(), ErrorCode::WRITE_CONFLICT);
        assert_eq!(e.error_code().0, 1001);
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
        assert!(e.to_string().contains("NDB-3001"));
        assert!(e.to_string().contains("sync delta rejected"));
    }

    #[test]
    fn error_display_sql_not_enabled() {
        let e = NodeDbError::SqlNotEnabled;
        assert!(e.to_string().contains("SQL not enabled"));
        assert_eq!(e.error_code(), ErrorCode::SQL_NOT_ENABLED);
    }

    #[test]
    fn io_error_converts() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file missing");
        let e: NodeDbError = io_err.into();
        assert!(matches!(e, NodeDbError::Storage { .. }));
        assert_eq!(e.error_code(), ErrorCode::STORAGE);
    }

    #[test]
    fn retriable_errors() {
        assert!(
            NodeDbError::WriteConflict {
                collection: "x".into(),
                document_id: "y".into(),
            }
            .is_retriable()
        );
        assert!(NodeDbError::DeadlineExceeded.is_retriable());
        assert!(
            !NodeDbError::BadRequest {
                detail: "bad".into()
            }
            .is_retriable()
        );
    }

    #[test]
    fn client_errors() {
        assert!(
            NodeDbError::BadRequest {
                detail: "bad".into()
            }
            .is_client_error()
        );
        assert!(
            !NodeDbError::Internal {
                detail: "oops".into()
            }
            .is_client_error()
        );
    }
}
