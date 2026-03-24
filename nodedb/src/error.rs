use crate::types::{RequestId, TenantId, VShardId};
use nodedb_types::error::NodeDbError;

/// Internal error classes for NodeDB Origin.
///
/// Every error is actionable — clients can programmatically handle each variant.
/// Cross-plane errors surface deterministic codes, never opaque strings.
///
/// At the public API boundary, `Error` converts to [`NodeDbError`] via `From`,
/// so external consumers never see infrastructure details like `WalError` or
/// `CrdtError`.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    // --- Write path errors ---
    #[error("constraint violation on {collection}: {detail}")]
    RejectedConstraint {
        collection: String,
        constraint: String,
        detail: String,
    },

    #[error("authorization denied for tenant {tenant_id} on {resource}")]
    RejectedAuthz {
        tenant_id: TenantId,
        resource: String,
    },

    #[error("request {request_id} exceeded deadline")]
    DeadlineExceeded { request_id: RequestId },

    #[error("write conflict on {collection}/{document_id}, retry with idempotency key")]
    ConflictRetry {
        collection: String,
        document_id: String,
    },

    #[error("CRDT delta pre-validation rejected: {constraint} — {reason}")]
    RejectedPrevalidation { constraint: String, reason: String },

    // --- Read path errors ---
    #[error("collection {collection} not found for tenant {tenant_id}")]
    CollectionNotFound {
        tenant_id: TenantId,
        collection: String,
    },

    #[error("document {document_id} not found in {collection}")]
    DocumentNotFound {
        collection: String,
        document_id: String,
    },

    // --- Routing errors ---
    #[error("vshard {vshard_id} has no serving leader")]
    NoLeader { vshard_id: VShardId },

    #[error("not leader for vshard {vshard_id}; leader is node {leader_node} at {leader_addr}")]
    NotLeader {
        vshard_id: VShardId,
        leader_node: u64,
        leader_addr: String,
    },

    #[error("query fan-out exceeded: {shards_touched} shards > limit {limit}")]
    FanOutExceeded { shards_touched: u16, limit: u16 },

    // --- Client input errors ---
    #[error("bad request: {detail}")]
    BadRequest { detail: String },

    #[error("query plan error: {detail}")]
    PlanError { detail: String },

    // --- Infrastructure errors ---
    #[error("WAL error: {0}")]
    Wal(#[from] nodedb_wal::WalError),

    #[error("dispatch error: {detail}")]
    Dispatch { detail: String },

    #[error("storage error ({engine}): {detail}")]
    Storage { engine: String, detail: String },

    #[error("cold storage error: {detail}")]
    ColdStorage { detail: String },

    #[error("serialization error ({format}): {detail}")]
    Serialization { format: String, detail: String },

    #[error("codec error: {detail}")]
    Codec { detail: String },

    #[error("segment corrupted: {detail}")]
    SegmentCorrupted { detail: String },

    #[error("memory budget exhausted for engine {engine}")]
    MemoryExhausted { engine: String },

    #[error("CRDT engine error: {0}")]
    Crdt(#[from] nodedb_crdt::CrdtError),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("configuration error: {detail}")]
    Config { detail: String },

    #[error("encryption error: {detail}")]
    Encryption { detail: String },

    #[error("bridge error: {detail}")]
    Bridge { detail: String },

    #[error("version compatibility: {detail}")]
    VersionCompat { detail: String },

    #[error("internal error: {detail}")]
    Internal { detail: String },
}

/// Result alias for NodeDB operations.
pub type Result<T> = std::result::Result<T, Error>;

// ---------------------------------------------------------------------------
// From impls for domain-specific errors
// ---------------------------------------------------------------------------

impl From<crate::control::pubsub::TopicError> for Error {
    fn from(e: crate::control::pubsub::TopicError) -> Self {
        Self::BadRequest {
            detail: e.to_string(),
        }
    }
}

impl From<crate::engine::timeseries::ilp::IlpError> for Error {
    fn from(e: crate::engine::timeseries::ilp::IlpError) -> Self {
        Self::BadRequest {
            detail: e.to_string(),
        }
    }
}

impl From<crate::engine::timeseries::columnar_segment::SegmentError> for Error {
    fn from(e: crate::engine::timeseries::columnar_segment::SegmentError) -> Self {
        Self::Storage {
            engine: "timeseries".into(),
            detail: e.to_string(),
        }
    }
}

impl From<crate::engine::timeseries::query::QueryError> for Error {
    fn from(e: crate::engine::timeseries::query::QueryError) -> Self {
        Self::Storage {
            engine: "timeseries".into(),
            detail: e.to_string(),
        }
    }
}

impl From<crate::control::security::crl::CrlError> for Error {
    fn from(e: crate::control::security::crl::CrlError) -> Self {
        Self::Config {
            detail: e.to_string(),
        }
    }
}

impl From<crate::control::security::jwt::JwtError> for Error {
    fn from(e: crate::control::security::jwt::JwtError) -> Self {
        Self::RejectedAuthz {
            tenant_id: TenantId::new(0),
            resource: e.to_string(),
        }
    }
}

// ---------------------------------------------------------------------------
// From<Error> for NodeDbError — the public API boundary conversion
// ---------------------------------------------------------------------------

impl From<Error> for NodeDbError {
    fn from(e: Error) -> Self {
        match e {
            // Write path
            Error::RejectedConstraint {
                collection, detail, ..
            } => NodeDbError::ConstraintViolation { collection, detail },
            Error::RejectedAuthz { resource, .. } => NodeDbError::AuthorizationDenied { resource },
            Error::DeadlineExceeded { .. } => NodeDbError::DeadlineExceeded,
            Error::ConflictRetry {
                collection,
                document_id,
            } => NodeDbError::WriteConflict {
                collection,
                document_id,
            },
            Error::RejectedPrevalidation { constraint, reason } => {
                NodeDbError::PrevalidationRejected { constraint, reason }
            }

            // Read path
            Error::CollectionNotFound { collection, .. } => {
                NodeDbError::CollectionNotFound { collection }
            }
            Error::DocumentNotFound {
                collection,
                document_id,
            } => NodeDbError::DocumentNotFound {
                collection,
                document_id,
            },

            // Routing / Cluster
            Error::NoLeader { vshard_id } => NodeDbError::NoLeader {
                detail: format!("vshard {vshard_id} has no serving leader"),
            },
            Error::NotLeader { leader_addr, .. } => NodeDbError::NotLeader { leader_addr },
            Error::FanOutExceeded {
                shards_touched,
                limit,
            } => NodeDbError::FanOutExceeded {
                shards_touched,
                limit,
            },

            // Client input
            Error::BadRequest { detail } => NodeDbError::BadRequest { detail },
            Error::PlanError { detail } => NodeDbError::PlanError { detail },

            // Infrastructure — flatten to opaque public variants
            Error::Wal(wal_err) => NodeDbError::Wal {
                detail: wal_err.to_string(),
            },
            Error::Dispatch { detail } => NodeDbError::Dispatch { detail },
            Error::Storage { detail, .. } => NodeDbError::Storage { detail },
            Error::ColdStorage { detail } => NodeDbError::ColdStorage { detail },
            Error::Serialization { format, detail } => {
                NodeDbError::Serialization { format, detail }
            }
            Error::Codec { detail } => NodeDbError::Codec { detail },
            Error::SegmentCorrupted { detail } => NodeDbError::SegmentCorrupted { detail },
            Error::MemoryExhausted { engine } => NodeDbError::MemoryExhausted { engine },
            Error::Crdt(crdt_err) => NodeDbError::Internal {
                detail: crdt_err.to_string(),
            },
            Error::Io(io_err) => NodeDbError::Storage {
                detail: io_err.to_string(),
            },
            Error::Config { detail } => NodeDbError::Config { detail },
            Error::Encryption { detail } => NodeDbError::Encryption { detail },
            Error::Bridge { detail } => NodeDbError::Bridge { detail },
            Error::VersionCompat { detail } => NodeDbError::Cluster { detail },
            Error::Internal { detail } => NodeDbError::Internal { detail },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_display_constraint() {
        let e = Error::RejectedConstraint {
            collection: "users".into(),
            constraint: "users_email_unique".into(),
            detail: "duplicate email".into(),
        };
        assert!(e.to_string().contains("constraint violation"));
        assert!(e.to_string().contains("users"));
    }

    #[test]
    fn error_display_deadline() {
        let e = Error::DeadlineExceeded {
            request_id: RequestId::new(42),
        };
        assert!(e.to_string().contains("req:42"));
        assert!(e.to_string().contains("deadline"));
    }

    #[test]
    fn error_display_fan_out() {
        let e = Error::FanOutExceeded {
            shards_touched: 32,
            limit: 16,
        };
        assert!(e.to_string().contains("32"));
        assert!(e.to_string().contains("16"));
    }

    #[test]
    fn crdt_error_converts() {
        let crdt_err = nodedb_crdt::CrdtError::ConstraintViolation {
            constraint: "test".into(),
            collection: "col".into(),
            detail: "detail".into(),
        };
        let e: Error = crdt_err.into();
        assert!(matches!(e, Error::Crdt(_)));
    }

    #[test]
    fn internal_error_to_nodedb_error() {
        let e = Error::Wal(nodedb_wal::WalError::Sealed);
        let public: NodeDbError = e.into();
        assert!(matches!(public, NodeDbError::Wal { .. }));
        assert!(public.to_string().contains("NDB-4100"));
    }

    #[test]
    fn constraint_to_nodedb_error() {
        let e = Error::RejectedConstraint {
            collection: "users".into(),
            constraint: "unique_email".into(),
            detail: "dup".into(),
        };
        let public: NodeDbError = e.into();
        assert!(matches!(public, NodeDbError::ConstraintViolation { .. }));
    }

    #[test]
    fn io_error_to_nodedb_error() {
        let e = Error::Io(std::io::Error::new(std::io::ErrorKind::NotFound, "gone"));
        let public: NodeDbError = e.into();
        assert!(matches!(public, NodeDbError::Storage { .. }));
    }
}
