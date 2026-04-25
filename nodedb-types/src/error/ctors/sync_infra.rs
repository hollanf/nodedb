//! Sync (3000s), storage (4000s), serialization (4200s), config (5000s),
//! cluster (6000s), memory (7000s), encryption (8000s), internal (9000s)
//! constructors.

use std::fmt;

use super::super::code::ErrorCode;
use super::super::details::ErrorDetails;
use super::super::types::NodeDbError;

impl NodeDbError {
    // ── Sync ──

    pub fn sync_connection_failed(detail: impl fmt::Display) -> Self {
        Self {
            code: ErrorCode::SYNC_CONNECTION_FAILED,
            message: format!("sync connection failed: {detail}"),
            details: ErrorDetails::SyncConnectionFailed,
            cause: None,
        }
    }

    pub fn sync_delta_rejected(
        reason: impl fmt::Display,
        compensation: Option<crate::sync::compensation::CompensationHint>,
    ) -> Self {
        Self {
            code: ErrorCode::SYNC_DELTA_REJECTED,
            message: format!("sync delta rejected: {reason}"),
            details: ErrorDetails::SyncDeltaRejected { compensation },
            cause: None,
        }
    }

    pub fn shape_subscription_failed(
        shape_id: impl Into<String>,
        detail: impl fmt::Display,
    ) -> Self {
        let shape_id = shape_id.into();
        Self {
            code: ErrorCode::SHAPE_SUBSCRIPTION_FAILED,
            message: format!("shape subscription failed for '{shape_id}': {detail}"),
            details: ErrorDetails::ShapeSubscriptionFailed { shape_id },
            cause: None,
        }
    }

    // ── Storage ──

    pub fn storage(detail: impl fmt::Display) -> Self {
        Self {
            code: ErrorCode::STORAGE,
            message: format!("storage error: {detail}"),
            details: ErrorDetails::Storage,
            cause: None,
        }
    }

    pub fn segment_corrupted(detail: impl fmt::Display) -> Self {
        Self {
            code: ErrorCode::SEGMENT_CORRUPTED,
            message: format!("segment corrupted: {detail}"),
            details: ErrorDetails::SegmentCorrupted,
            cause: None,
        }
    }

    pub fn cold_storage(detail: impl fmt::Display) -> Self {
        Self {
            code: ErrorCode::COLD_STORAGE,
            message: format!("cold storage error: {detail}"),
            details: ErrorDetails::ColdStorage,
            cause: None,
        }
    }

    pub fn wal(detail: impl fmt::Display) -> Self {
        Self {
            code: ErrorCode::WAL,
            message: format!("WAL error: {detail}"),
            details: ErrorDetails::Wal,
            cause: None,
        }
    }

    // ── Serialization ──

    pub fn serialization(format: impl Into<String>, detail: impl fmt::Display) -> Self {
        let format = format.into();
        Self {
            code: ErrorCode::SERIALIZATION,
            message: format!("serialization error ({format}): {detail}"),
            details: ErrorDetails::Serialization { format },
            cause: None,
        }
    }

    pub fn codec(detail: impl fmt::Display) -> Self {
        Self {
            code: ErrorCode::CODEC,
            message: format!("codec error: {detail}"),
            details: ErrorDetails::Codec,
            cause: None,
        }
    }

    // ── Config ──

    pub fn config(detail: impl fmt::Display) -> Self {
        Self {
            code: ErrorCode::CONFIG,
            message: format!("configuration error: {detail}"),
            details: ErrorDetails::Config,
            cause: None,
        }
    }

    pub fn bad_request(detail: impl fmt::Display) -> Self {
        Self {
            code: ErrorCode::BAD_REQUEST,
            message: format!("bad request: {detail}"),
            details: ErrorDetails::BadRequest,
            cause: None,
        }
    }

    // ── Cluster ──

    pub fn no_leader(detail: impl fmt::Display) -> Self {
        Self {
            code: ErrorCode::NO_LEADER,
            message: format!("no serving leader: {detail}"),
            details: ErrorDetails::NoLeader,
            cause: None,
        }
    }

    pub fn not_leader(leader_addr: impl Into<String>) -> Self {
        let leader_addr = leader_addr.into();
        Self {
            code: ErrorCode::NOT_LEADER,
            message: format!("not leader; redirect to leader at {leader_addr}"),
            details: ErrorDetails::NotLeader { leader_addr },
            cause: None,
        }
    }

    pub fn migration_in_progress(detail: impl fmt::Display) -> Self {
        Self {
            code: ErrorCode::MIGRATION_IN_PROGRESS,
            message: format!("migration in progress: {detail}"),
            details: ErrorDetails::MigrationInProgress,
            cause: None,
        }
    }

    pub fn node_unreachable(detail: impl fmt::Display) -> Self {
        Self {
            code: ErrorCode::NODE_UNREACHABLE,
            message: format!("node unreachable: {detail}"),
            details: ErrorDetails::NodeUnreachable,
            cause: None,
        }
    }

    pub fn cluster(detail: impl fmt::Display) -> Self {
        Self {
            code: ErrorCode::CLUSTER,
            message: format!("cluster error: {detail}"),
            details: ErrorDetails::Cluster,
            cause: None,
        }
    }

    // ── Memory ──

    pub fn memory_exhausted(engine: impl Into<String>) -> Self {
        let engine = engine.into();
        Self {
            code: ErrorCode::MEMORY_EXHAUSTED,
            message: format!("memory budget exhausted for engine {engine}"),
            details: ErrorDetails::MemoryExhausted { engine },
            cause: None,
        }
    }

    // ── Encryption ──

    pub fn encryption(detail: impl fmt::Display) -> Self {
        Self {
            code: ErrorCode::ENCRYPTION,
            message: format!("encryption error: {detail}"),
            details: ErrorDetails::Encryption,
            cause: None,
        }
    }

    // ── Engine: Array ──

    pub fn array(array: impl Into<String>, detail: impl fmt::Display) -> Self {
        let array = array.into();
        Self {
            code: ErrorCode::ARRAY,
            message: format!("array engine error on '{array}': {detail}"),
            details: ErrorDetails::Array { array },
            cause: None,
        }
    }

    // ── Bridge / Dispatch / Internal ──

    pub fn bridge(detail: impl fmt::Display) -> Self {
        Self {
            code: ErrorCode::BRIDGE,
            message: format!("bridge error: {detail}"),
            details: ErrorDetails::Bridge,
            cause: None,
        }
    }

    pub fn dispatch(detail: impl fmt::Display) -> Self {
        Self {
            code: ErrorCode::DISPATCH,
            message: format!("dispatch error: {detail}"),
            details: ErrorDetails::Dispatch,
            cause: None,
        }
    }

    pub fn internal(detail: impl fmt::Display) -> Self {
        Self {
            code: ErrorCode::INTERNAL,
            message: format!("internal error: {detail}"),
            details: ErrorDetails::Internal,
            cause: None,
        }
    }
}
