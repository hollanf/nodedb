//! Machine-matchable structured error details.

use serde::{Deserialize, Serialize};

/// Structured error details for programmatic matching.
///
/// Clients match on the variant to determine the error category, then
/// extract structured fields. The `message` on [`crate::error::NodeDbError`]
/// carries the human-readable explanation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[non_exhaustive]
pub enum ErrorDetails {
    // Write path
    ConstraintViolation {
        collection: String,
    },
    WriteConflict {
        collection: String,
        document_id: String,
    },
    DeadlineExceeded,
    PrevalidationRejected {
        constraint: String,
    },
    AppendOnlyViolation {
        collection: String,
    },
    BalanceViolation {
        collection: String,
    },
    PeriodLocked {
        collection: String,
    },
    StateTransitionViolation {
        collection: String,
    },
    TransitionCheckViolation {
        collection: String,
    },
    TypeGuardViolation {
        collection: String,
    },
    RetentionViolation {
        collection: String,
    },
    LegalHoldActive {
        collection: String,
    },
    TypeMismatch {
        collection: String,
    },
    Overflow {
        collection: String,
    },
    InsufficientBalance {
        collection: String,
    },
    RateExceeded {
        gate: String,
    },

    // Read path
    CollectionNotFound {
        collection: String,
    },
    DocumentNotFound {
        collection: String,
        document_id: String,
    },
    CollectionDraining {
        collection: String,
    },
    CollectionDeactivated {
        collection: String,
        /// Wall-clock nanoseconds when retention elapses and the
        /// collection becomes unrecoverable. Clients can render a
        /// human-readable countdown.
        retention_expires_at_ns: u64,
        /// Copy-pasteable SQL the user can run to restore the
        /// collection. Populated with the actual name, so the error
        /// is actionable without further lookup.
        undrop_hint: String,
    },

    // Query
    PlanError {
        phase: String,
        detail: String,
    },
    FanOutExceeded {
        shards_touched: u16,
        limit: u16,
    },
    SqlNotEnabled,

    // Auth
    AuthorizationDenied {
        resource: String,
    },
    AuthExpired,

    // Sync
    SyncConnectionFailed,
    SyncDeltaRejected {
        compensation: Option<crate::sync::compensation::CompensationHint>,
    },
    ShapeSubscriptionFailed {
        shape_id: String,
    },

    // Storage (opaque infrastructure)
    Storage {
        component: String,
        op: String,
        detail: String,
    },
    SegmentCorrupted {
        segment_id: u64,
        corruption: String,
        detail: String,
    },
    ColdStorage {
        backend: String,
        op: String,
        detail: String,
    },
    Wal {
        stage: String,
        detail: String,
    },

    // Serialization
    Serialization {
        format: String,
    },
    Codec {
        codec: String,
        op: String,
        detail: String,
    },

    // Config
    Config,
    BadRequest,

    // Cluster
    NoLeader,
    NotLeader {
        leader_addr: String,
    },
    MigrationInProgress,
    NodeUnreachable,
    Cluster,

    // Memory
    MemoryExhausted {
        engine: String,
    },

    // Encryption
    Encryption {
        cipher: String,
        detail: String,
    },

    // Engine ops
    Array {
        array: String,
    },

    // Bridge / Dispatch / Internal
    Bridge {
        plane: String,
        op: String,
        detail: String,
    },
    Dispatch {
        stage: String,
        detail: String,
    },
    Internal {
        component: String,
        detail: String,
    },
}
