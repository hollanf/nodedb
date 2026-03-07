use std::sync::Arc;
use std::time::Instant;

/// A request envelope sent from the Control Plane (Tokio) to the Data Plane (TPC).
///
/// This is a lightweight descriptor — bulk data lives behind `Arc<[u8]>` or
/// slab handles, not inline in the ring buffer slot.
#[derive(Debug, Clone)]
pub struct Request {
    /// Globally unique request identifier (monotonic per connection for 24h).
    pub request_id: u64,

    /// Tenant scope for isolation and quota enforcement.
    pub tenant_id: u32,

    /// Virtual shard this request targets.
    pub vshard_id: u16,

    /// Opaque execution plan digest (interpreted by the Data Plane).
    pub plan: Plan,

    /// Absolute deadline — Data Plane must abandon work after this instant.
    pub deadline: Instant,

    /// Priority class for scheduling on the Data Plane core.
    pub priority: Priority,

    /// Distributed trace identifier for cross-plane observability.
    pub trace_id: u64,
}

/// A response envelope sent from the Data Plane (TPC) back to the Control Plane (Tokio).
#[derive(Debug)]
pub struct Response {
    /// Matches the originating `Request::request_id`.
    pub request_id: u64,

    /// Outcome of the execution.
    pub status: Status,

    /// Number of bytes produced by this response (for backpressure accounting).
    pub bytes_produced: u64,

    /// The LSN watermark at which this response was computed.
    pub watermark_lsn: u64,

    /// The result payload. `None` if the request was cancelled or failed.
    pub payload: Option<Arc<[u8]>>,
}

/// The execution plan payload. Kept as an opaque blob at the bridge layer —
/// the bridge doesn't interpret plans, it just moves them.
#[derive(Debug, Clone)]
pub enum Plan {
    /// Raw serialized plan bytes (zero-copy via Arc).
    Bytes(Arc<[u8]>),

    /// Slab handle — the actual plan lives in a shared slab allocator
    /// and the Data Plane looks it up by index.
    SlabHandle { slab_id: u32, offset: u32, len: u32 },
}

/// Request priority for Data Plane scheduling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum Priority {
    /// Control signals, cancellations, barrier coordination.
    Critical = 0,
    /// Interactive queries with latency SLOs.
    Interactive = 1,
    /// Batch operations, background compaction requests.
    Background = 2,
}

/// Execution outcome status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    /// Execution completed successfully.
    Ok,
    /// Partial result available (streaming response).
    Partial,
    /// Request was cancelled via `CANCEL(request_id)`.
    Cancelled,
    /// Deadline expired before completion.
    DeadlineExceeded,
    /// Execution failed with a deterministic error code.
    Error(ErrorCode),
}

/// Deterministic error codes for cross-plane communication.
/// These must be stable across versions for client compatibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum ErrorCode {
    /// Generic internal error.
    Internal = 1,
    /// Requested vShard is not owned by this node.
    VShardNotLocal = 2,
    /// Tenant quota exceeded.
    QuotaExceeded = 3,
    /// Constraint violation (UNIQUE, FK) at commit.
    ConstraintViolation = 4,
    /// Engine memory budget exhausted.
    MemoryBudgetExhausted = 5,
    /// I/O error on the Data Plane.
    IoError = 6,
}
