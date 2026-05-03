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

    #[error(
        "offset regression on stream '{stream}' group '{group}' partition {partition_id}: \
         attempted LSN {attempted_lsn} < current committed LSN {current_lsn}"
    )]
    OffsetRegression {
        stream: String,
        group: String,
        partition_id: u32,
        current_lsn: u64,
        attempted_lsn: u64,
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

    #[error("append-only violation on {collection}: {detail}")]
    AppendOnlyViolation { collection: String, detail: String },

    #[error("balance violation on {collection}: {detail}")]
    BalanceViolation { collection: String, detail: String },

    #[error("period locked on {collection}: {detail}")]
    PeriodLocked { collection: String, detail: String },

    #[error("retention violation on {collection}: {detail}")]
    RetentionViolation { collection: String, detail: String },

    #[error("legal hold active on {collection}: {detail}")]
    LegalHoldActive { collection: String, detail: String },

    #[error("state transition violation on {collection}: {detail}")]
    StateTransitionViolation { collection: String, detail: String },

    #[error("transition check violation on {collection}: {detail}")]
    TransitionCheckViolation { collection: String, detail: String },

    #[error("type guard violation on {collection}: {detail}")]
    TypeGuardViolation { collection: String, detail: String },

    #[error("type mismatch on {collection} key {key}: {detail}")]
    TypeMismatch {
        collection: String,
        key: String,
        detail: String,
    },

    #[error("arithmetic overflow on {collection} key {key}")]
    OverflowError { collection: String, key: String },

    #[error("insufficient balance on {collection} key {key}: {detail}")]
    InsufficientBalance {
        collection: String,
        key: String,
        detail: String,
    },

    #[error("rate limit exceeded for {gate}: {detail}")]
    RateExceeded {
        gate: String,
        detail: String,
        retry_after_ms: u64,
    },

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

    #[error(
        "collection '{collection}' is soft-deleted for tenant {tenant_id}; \
         UNDROP before {retention_expires_at_ns} ns"
    )]
    CollectionDeactivated {
        tenant_id: TenantId,
        collection: String,
        retention_expires_at_ns: u64,
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

    /// The planner tried to acquire a descriptor lease at a version
    /// being drained by an in-flight DDL. The pgwire layer catches
    /// this variant and retries the whole statement up to
    /// `PLAN_RETRY_BUDGET` times with backoff. If every retry
    /// fails, the error surfaces to the client.
    #[error("retryable schema change on {descriptor}")]
    RetryableSchemaChanged { descriptor: String },

    /// The Raft entry the proposer was waiting on at `(group_id, log_index)`
    /// was overwritten by a leader-election no-op (the previous leader
    /// stepped down before the user entry committed; the new leader
    /// committed an empty entry at the same index, truncating the
    /// uncommitted data).
    ///
    /// **Critical**: this is the silent-data-loss bug killer — without
    /// surfacing this case, `tracker.complete(Ok([]))` on the no-op
    /// would tell the proposer their INSERT succeeded when in fact
    /// the row was never replicated. Callers (gateway, async raft
    /// proposer) MUST treat this as retryable and re-propose.
    #[error(
        "raft entry at group {group_id} index {log_index} was overwritten by leader change; retry needed"
    )]
    RetryableLeaderChange { group_id: u64, log_index: u64 },

    #[error("execution limit exceeded: {detail}")]
    ExecutionLimitExceeded { detail: String },

    #[error("operation limit exceeded: {limit_name} = {value} exceeds cap {max}")]
    LimitExceeded {
        limit_name: &'static str,
        value: u64,
        max: u64,
    },

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

    /// Memory pressure at Emergency level — the named engine is over 95% budget.
    /// The write is rejected; the caller must retry when pressure subsides.
    /// Maps to SQLSTATE 53200 (out_of_memory / insufficient_resources).
    #[error("backpressure: engine {engine} is at Emergency pressure; retry later")]
    Backpressure { engine: nodedb_mem::EngineId },

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

    /// DROP / PURGE refused because other catalog objects still
    /// reference the target. Operator must either drop them first or
    /// retry with `CASCADE`. `dependents` lists `(kind, name)` pairs.
    #[error(
        "cannot drop {root_kind} '{root_name}' for tenant {tenant_id}: \
         {dependent_count} dependent object(s) exist; use CASCADE to drop them atomically"
    )]
    DependentObjectsExist {
        tenant_id: u64,
        root_kind: &'static str,
        root_name: String,
        dependent_count: usize,
        dependents: Vec<(String, String)>,
    },

    /// MV-graph cycle detected (or graph exceeded `MAX_DEPTH`) during
    /// cascade enumeration. Treated as a blocker rather than silently
    /// truncating.
    #[error(
        "cascade cycle or depth limit ({depth}) exceeded while enumerating \
         dependents of '{root}' for tenant {tenant_id}"
    )]
    CascadeCycle {
        tenant_id: u64,
        root: String,
        depth: usize,
    },

    /// A cross-shard write was attempted inside an explicit transaction block.
    ///
    /// Calvin cross-shard atomicity requires auto-commit (single-statement).
    /// Options:
    ///   1. Remove BEGIN/COMMIT to use auto-commit.
    ///   2. SET cross_shard_txn = 'best_effort_non_atomic' for non-atomic dispatch.
    #[error(
        "cross-shard write inside explicit transaction block is not supported. \
         Calvin cross-shard atomicity requires auto-commit (single-statement). \
         Options: 1) Remove BEGIN/COMMIT to use auto-commit. \
         2) SET cross_shard_txn = 'best_effort_non_atomic' for non-atomic dispatch."
    )]
    CrossShardInExplicitTransaction,

    /// The Calvin sequencer inbox is unavailable — this node is running in
    /// embedded/local mode without a cluster deployment.
    #[error(
        "cross-shard transactions require a cluster deployment with the Calvin sequencer; \
         this node is running in embedded/local mode"
    )]
    SequencerUnavailable,

    /// The OLLP dependent-read retry loop exhausted its retry budget.
    ///
    /// The predicate's matching set kept changing across retries. Consider
    /// rephrasing as a static-key UPDATE if possible.
    #[error(
        "OLLP dependent-read exhausted {retries} retries; the predicate's matching set kept \
         changing across retries. Consider rephrasing as a static-key UPDATE if possible."
    )]
    OllpExhausted { retries: u8 },
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

impl From<crate::storage::quarantine::engines::FtsOrQuarantine> for Error {
    fn from(e: crate::storage::quarantine::engines::FtsOrQuarantine) -> Self {
        Self::SegmentCorrupted {
            detail: e.to_string(),
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
            } => NodeDbError::constraint_violation(collection, detail),
            Error::RejectedAuthz { resource, .. } => NodeDbError::authorization_denied(resource),
            err @ Error::OffsetRegression { .. } => NodeDbError::bad_request(err.to_string()),
            Error::DeadlineExceeded { .. } => NodeDbError::deadline_exceeded(),
            Error::ConflictRetry {
                collection,
                document_id,
            } => NodeDbError::write_conflict(collection, document_id),
            Error::RejectedPrevalidation { constraint, reason } => {
                NodeDbError::prevalidation_rejected(constraint, reason)
            }
            Error::AppendOnlyViolation {
                collection, detail, ..
            } => NodeDbError::append_only_violation(collection, detail),
            Error::BalanceViolation {
                collection, detail, ..
            } => NodeDbError::balance_violation(collection, detail),
            Error::PeriodLocked {
                collection, detail, ..
            } => NodeDbError::period_locked(collection, detail),
            Error::RetentionViolation {
                collection, detail, ..
            } => NodeDbError::retention_violation(collection, detail),
            Error::LegalHoldActive {
                collection, detail, ..
            } => NodeDbError::legal_hold_active(collection, detail),
            Error::StateTransitionViolation {
                collection, detail, ..
            } => NodeDbError::state_transition_violation(collection, detail),
            Error::TransitionCheckViolation {
                collection, detail, ..
            } => NodeDbError::transition_check_violation(collection, detail),
            Error::TypeGuardViolation {
                collection, detail, ..
            } => NodeDbError::type_guard_violation(collection, detail),
            Error::TypeMismatch {
                collection, detail, ..
            } => NodeDbError::type_mismatch(collection, detail),
            Error::OverflowError { collection, key } => {
                NodeDbError::overflow(collection, format!("key {key}"))
            }
            Error::InsufficientBalance {
                collection,
                key,
                detail,
            } => NodeDbError::insufficient_balance(collection, format!("key {key}: {detail}")),
            Error::RateExceeded { gate, detail, .. } => NodeDbError::rate_exceeded(gate, detail),

            // Read path
            Error::CollectionNotFound { collection, .. } => {
                NodeDbError::collection_not_found(collection)
            }
            Error::DocumentNotFound {
                collection,
                document_id,
            } => NodeDbError::document_not_found(collection, document_id),
            Error::CollectionDeactivated {
                collection,
                retention_expires_at_ns,
                ..
            } => NodeDbError::collection_deactivated(collection, retention_expires_at_ns),

            // Routing / Cluster
            Error::NoLeader { vshard_id } => {
                NodeDbError::no_leader(format!("vshard {vshard_id} has no serving leader"))
            }
            Error::NotLeader { leader_addr, .. } => NodeDbError::not_leader(leader_addr),
            Error::FanOutExceeded {
                shards_touched,
                limit,
            } => NodeDbError::fan_out_exceeded(shards_touched, limit),

            // Client input
            Error::BadRequest { detail } => NodeDbError::bad_request(detail),
            Error::PlanError { detail } => NodeDbError::plan_error(detail),
            Error::RetryableSchemaChanged { descriptor } => {
                NodeDbError::plan_error(format!("retryable schema change on {descriptor}"))
            }
            Error::RetryableLeaderChange {
                group_id,
                log_index,
            } => NodeDbError::dispatch(format!(
                "raft leader change overwrote entry at group {group_id} index {log_index}; retry exhausted"
            )),
            Error::ExecutionLimitExceeded { detail } => NodeDbError::bad_request(detail),
            Error::LimitExceeded {
                limit_name,
                value,
                max,
            } => {
                NodeDbError::bad_request(format!("{limit_name} = {value} exceeds server cap {max}"))
            }

            // Infrastructure — flatten to opaque public variants
            Error::Wal(wal_err) => NodeDbError::wal(wal_err),
            Error::Dispatch { detail } => NodeDbError::dispatch(detail),
            Error::Storage { detail, .. } => NodeDbError::storage(detail),
            Error::ColdStorage { detail } => NodeDbError::cold_storage(detail),
            Error::Serialization { format, detail } => NodeDbError::serialization(format, detail),
            Error::Codec { detail } => NodeDbError::codec(detail),
            Error::SegmentCorrupted { detail } => NodeDbError::segment_corrupted(detail),
            Error::MemoryExhausted { engine } => NodeDbError::memory_exhausted(engine),
            Error::Backpressure { engine } => NodeDbError::memory_exhausted(engine.to_string()),
            Error::Crdt(crdt_err) => NodeDbError::internal(crdt_err),
            Error::Io(io_err) => NodeDbError::storage(io_err),
            Error::Config { detail } => NodeDbError::config(detail),
            Error::Encryption { detail } => NodeDbError::encryption(detail),
            Error::Bridge { detail } => NodeDbError::bridge(detail),
            Error::VersionCompat { detail } => NodeDbError::cluster(detail),
            Error::Internal { detail } => NodeDbError::internal(detail),
            Error::DependentObjectsExist {
                tenant_id: _,
                root_kind,
                root_name,
                dependent_count,
                dependents,
            } => {
                let names: Vec<String> =
                    dependents.iter().map(|(k, n)| format!("{k}:{n}")).collect();
                NodeDbError::bad_request(format!(
                    "cannot drop {root_kind} '{root_name}': {dependent_count} dependent(s) exist ({})",
                    names.join(", ")
                ))
            }
            Error::CascadeCycle {
                tenant_id: _,
                root,
                depth,
            } => NodeDbError::internal(format!(
                "cascade cycle / depth-limit ({depth}) exceeded on '{root}'"
            )),
            Error::CrossShardInExplicitTransaction => NodeDbError::bad_request(
                "cross-shard write inside explicit transaction block is not supported. \
                 Calvin cross-shard atomicity requires auto-commit (single-statement). \
                 Options: 1) Remove BEGIN/COMMIT to use auto-commit. \
                 2) SET cross_shard_txn = 'best_effort_non_atomic' for non-atomic dispatch."
                    .to_owned(),
            ),
            Error::SequencerUnavailable => NodeDbError::bad_request(
                "cross-shard transactions require a cluster deployment with the Calvin sequencer; \
                 this node is running in embedded/local mode"
                    .to_owned(),
            ),
            Error::OllpExhausted { retries } => NodeDbError::bad_request(format!(
                "OLLP dependent-read exhausted {retries} retries; the predicate's matching set \
                 kept changing across retries. Consider rephrasing as a static-key UPDATE if possible."
            )),
        }
    }
}

// ---------------------------------------------------------------------------
// TypedClusterError ↔ Error conversions
// ---------------------------------------------------------------------------

/// Convert a wire-level typed cluster error into the internal `Error` type.
///
/// Used by the C-β gateway layer (C-γ) to translate remote executor errors
/// into actionable local errors. The `NotLeader` variant preserves the
/// machine-readable group/term fields so the gateway retry loop can update
/// its routing table.
impl From<nodedb_cluster::rpc_codec::TypedClusterError> for Error {
    fn from(e: nodedb_cluster::rpc_codec::TypedClusterError) -> Self {
        use nodedb_cluster::rpc_codec::TypedClusterError;
        match e {
            TypedClusterError::NotLeader {
                group_id,
                leader_node_id,
                leader_addr,
                ..
            } => Error::NotLeader {
                // Clamp group_id to valid vShard range — group IDs may exceed 1024
                // for cluster-managed Raft groups; best-effort for display purposes.
                vshard_id: crate::types::VShardId::new(
                    (group_id as u32).min(crate::types::VShardId::COUNT - 1),
                ),
                leader_node: leader_node_id.unwrap_or(0),
                leader_addr: leader_addr.unwrap_or_default(),
            },
            TypedClusterError::DescriptorMismatch { collection, .. } => {
                Error::RetryableSchemaChanged {
                    descriptor: collection,
                }
            }
            TypedClusterError::DeadlineExceeded { .. } => Error::DeadlineExceeded {
                request_id: crate::types::RequestId::new(0),
            },
            TypedClusterError::Internal { message, .. } => Error::Internal { detail: message },
        }
    }
}

/// Build a `TypedClusterError::NotLeader` from an `Error::NotLeader`.
impl From<Error> for nodedb_cluster::rpc_codec::TypedClusterError {
    fn from(e: Error) -> Self {
        use nodedb_cluster::rpc_codec::TypedClusterError;
        match e {
            Error::NotLeader {
                vshard_id,
                leader_node,
                leader_addr,
            } => TypedClusterError::NotLeader {
                group_id: vshard_id.as_u32() as u64,
                leader_node_id: if leader_node == 0 {
                    None
                } else {
                    Some(leader_node)
                },
                leader_addr: if leader_addr.is_empty() {
                    None
                } else {
                    Some(leader_addr)
                },
                term: 0,
            },
            Error::DeadlineExceeded { .. } => TypedClusterError::DeadlineExceeded { elapsed_ms: 0 },
            other => TypedClusterError::Internal {
                code: 0,
                message: other.to_string(),
            },
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
        assert!(public.is_storage());
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
        assert!(public.is_constraint_violation());
    }

    #[test]
    fn io_error_to_nodedb_error() {
        let e = Error::Io(std::io::Error::new(std::io::ErrorKind::NotFound, "gone"));
        let public: NodeDbError = e.into();
        assert!(public.is_storage());
    }
}
