use crate::types::{RequestId, TenantId, VShardId};

/// Deterministic error classes.
///
/// Every error is actionable — clients can programmatically handle each variant.
/// Cross-plane errors surface deterministic codes, never opaque strings.
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

    #[error("serialization error ({format}): {detail}")]
    Serialization { format: String, detail: String },

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

    #[error("internal error: {detail}")]
    Internal { detail: String },
}

/// Result alias for NodeDB operations.
pub type Result<T> = std::result::Result<T, Error>;

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
}
