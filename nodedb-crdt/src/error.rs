//! Error types for the CRDT engine.

/// Errors produced by CRDT operations.
#[derive(Debug, thiserror::Error)]
pub enum CrdtError {
    /// A constraint was violated during validation.
    #[error("constraint violation: {constraint} on collection `{collection}`: {detail}")]
    ConstraintViolation {
        constraint: String,
        collection: String,
        detail: String,
    },

    /// The delta could not be applied to the current state.
    #[error("delta application failed: {0}")]
    DeltaApplyFailed(String),

    /// Loro internal error.
    #[error("loro error: {0}")]
    Loro(String),

    /// Dead-letter queue is full.
    #[error("dead-letter queue full: capacity {capacity}, pending {pending}")]
    DlqFull { capacity: usize, pending: usize },

    /// The collection does not exist.
    #[error("unknown collection: {0}")]
    UnknownCollection(String),
}

pub type Result<T> = std::result::Result<T, CrdtError>;
