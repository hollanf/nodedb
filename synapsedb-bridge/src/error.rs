/// Errors produced by the cross-runtime bridge.
#[derive(Debug, thiserror::Error)]
pub enum BridgeError {
    /// The ring buffer is full and the producer cannot enqueue.
    /// The caller should yield and retry after the consumer drains.
    #[error("ring buffer full (capacity: {capacity}, pending: {pending})")]
    Full { capacity: usize, pending: usize },

    /// The ring buffer is empty and the consumer has nothing to dequeue.
    #[error("ring buffer empty")]
    Empty,

    /// The other side of the channel has been dropped.
    #[error("channel disconnected: {side} side dropped")]
    Disconnected { side: &'static str },

    /// Backpressure threshold exceeded — the caller must throttle.
    #[error("backpressure: queue utilization at {percent}% (threshold: {threshold}%)")]
    Backpressure { percent: u8, threshold: u8 },

    /// Request deadline expired before the Data Plane could process it.
    #[error("deadline exceeded for request {request_id}")]
    DeadlineExceeded { request_id: u64 },
}

pub type Result<T> = std::result::Result<T, BridgeError>;
