//! WAL change notification bus.
//!
//! Tails the WAL and broadcasts committed mutations to subscribers on
//! the Control Plane. This is the foundation for:
//!
//! - Live queries: `SELECT * FROM orders WHERE status = 'pending'` pushes
//!   changes in real-time to connected WebSocket clients.
//! - Phase 5 Shape evaluator: evaluates CRDT mutations against client shapes
//!   and pushes matching deltas.
//! - LISTEN/NOTIFY: PostgreSQL-compatible async notifications.
//!
//! The bus is a broadcast channel: each subscriber gets all mutations.
//! Subscribers can filter by collection/tenant on their end.

use tracing::debug;

use crate::types::{Lsn, TenantId};

/// A single mutation event broadcast by the change stream.
#[derive(Debug, Clone)]
pub struct ChangeEvent {
    /// WAL LSN of this mutation.
    pub lsn: Lsn,
    /// Tenant that performed the mutation.
    pub tenant_id: TenantId,
    /// Collection affected.
    pub collection: String,
    /// Document ID affected.
    pub document_id: String,
    /// Type of mutation.
    pub operation: ChangeOperation,
    /// Timestamp (epoch milliseconds).
    pub timestamp_ms: u64,
}

/// Type of mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeOperation {
    Insert,
    Update,
    Delete,
}

impl ChangeOperation {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Insert => "INSERT",
            Self::Update => "UPDATE",
            Self::Delete => "DELETE",
        }
    }
}

/// A subscription handle. Receives change events via a bounded channel.
pub struct Subscription {
    /// Unique subscription ID.
    pub id: u64,
    /// Receives change events. Bounded buffer — drops oldest on overflow.
    pub receiver: tokio::sync::broadcast::Receiver<ChangeEvent>,
    /// Optional collection filter (None = all collections).
    pub collection_filter: Option<String>,
    /// Optional tenant filter (None = all tenants).
    pub tenant_filter: Option<TenantId>,
}

/// The change stream bus: broadcasts WAL mutations to subscribers.
///
/// Lives on the Control Plane (Send + Sync). The Data Plane writes
/// mutations to the WAL; the change stream tails the WAL and broadcasts
/// change events to all subscribers.
pub struct ChangeStream {
    /// Broadcast sender — all subscribers receive from this.
    sender: tokio::sync::broadcast::Sender<ChangeEvent>,
    /// Next subscription ID.
    next_sub_id: std::sync::atomic::AtomicU64,
    /// Active subscription count (for monitoring).
    active_subscriptions: std::sync::atomic::AtomicU64,
    /// Total events published.
    events_published: std::sync::atomic::AtomicU64,
    /// Last LSN processed from the WAL.
    last_lsn: std::sync::atomic::AtomicU64,
    /// Ring buffer of recent change events for SHOW CHANGES queries.
    /// RwLock allows concurrent reads (query_changes) with exclusive writes (publish).
    recent_changes: std::sync::RwLock<std::collections::VecDeque<ChangeEvent>>,
    /// Max recent changes to retain.
    recent_capacity: usize,
}

impl ChangeStream {
    /// Create a new change stream with the given buffer capacity.
    ///
    /// `capacity` is the number of events buffered before slow
    /// subscribers start losing events (backpressure policy: drop oldest).
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = tokio::sync::broadcast::channel(capacity);
        Self {
            sender,
            next_sub_id: std::sync::atomic::AtomicU64::new(1),
            active_subscriptions: std::sync::atomic::AtomicU64::new(0),
            events_published: std::sync::atomic::AtomicU64::new(0),
            last_lsn: std::sync::atomic::AtomicU64::new(0),
            recent_changes: std::sync::RwLock::new(std::collections::VecDeque::with_capacity(
                capacity,
            )),
            recent_capacity: capacity,
        }
    }

    /// Subscribe to the change stream.
    ///
    /// Returns a `Subscription` with a broadcast receiver. The subscriber
    /// receives all events and can filter locally by collection/tenant.
    pub fn subscribe(
        &self,
        collection_filter: Option<String>,
        tenant_filter: Option<TenantId>,
    ) -> Subscription {
        let id = self
            .next_sub_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let receiver = self.sender.subscribe();
        self.active_subscriptions
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        debug!(
            id,
            ?collection_filter,
            ?tenant_filter,
            "change stream: new subscription"
        );
        Subscription {
            id,
            receiver,
            collection_filter,
            tenant_filter,
        }
    }

    /// Publish a change event to all subscribers.
    ///
    /// Called by the WAL tail loop when a committed mutation is observed.
    /// If no subscribers are active, the event is dropped silently.
    pub fn publish(&self, event: ChangeEvent) {
        self.last_lsn
            .store(event.lsn.as_u64(), std::sync::atomic::Ordering::Relaxed);
        self.events_published
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Store in recent changes ring buffer.
        if let Ok(mut buf) = self.recent_changes.write() {
            if buf.len() >= self.recent_capacity {
                buf.pop_front();
            }
            buf.push_back(event.clone());
        }

        // broadcast::send returns Err if no receivers — that's fine.
        let _ = self.sender.send(event);
    }

    /// Publish a batch of events (e.g., from a Transaction WAL record).
    pub fn publish_batch(&self, events: &[ChangeEvent]) {
        for event in events {
            self.publish(event.clone());
        }
    }

    /// Number of active subscriptions.
    pub fn subscriber_count(&self) -> u64 {
        self.active_subscriptions
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Total events published since creation.
    pub fn events_published(&self) -> u64 {
        self.events_published
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Last WAL LSN processed.
    pub fn last_lsn(&self) -> u64 {
        self.last_lsn.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Query recent changes for a collection since a given timestamp.
    ///
    /// Returns change events matching the collection filter that occurred
    /// at or after `since_ms` (epoch milliseconds). Limited to the ring
    /// buffer capacity (most recent N events).
    pub fn query_changes(
        &self,
        collection: Option<&str>,
        since_ms: u64,
        limit: usize,
    ) -> Vec<ChangeEvent> {
        let buf = match self.recent_changes.read() {
            Ok(b) => b,
            Err(p) => p.into_inner(),
        };
        buf.iter()
            .filter(|e| e.timestamp_ms >= since_ms && collection.is_none_or(|c| e.collection == c))
            .take(limit)
            .cloned()
            .collect()
    }

    /// Record that a subscriber disconnected.
    pub fn unsubscribe(&self) {
        self.active_subscriptions
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn publish_and_receive() {
        let stream = ChangeStream::new(64);
        let mut sub = stream.subscribe(None, None);

        let event = ChangeEvent {
            lsn: Lsn::new(1),
            tenant_id: TenantId::new(1),
            collection: "users".into(),
            document_id: "u1".into(),
            operation: ChangeOperation::Insert,
            timestamp_ms: 1000,
        };
        stream.publish(event);

        let received = sub.receiver.recv().await.unwrap();
        assert_eq!(received.document_id, "u1");
        assert_eq!(received.operation, ChangeOperation::Insert);
    }

    #[tokio::test]
    async fn multiple_subscribers() {
        let stream = ChangeStream::new(64);
        let mut sub1 = stream.subscribe(None, None);
        let mut sub2 = stream.subscribe(Some("orders".into()), None);

        let event = ChangeEvent {
            lsn: Lsn::new(2),
            tenant_id: TenantId::new(1),
            collection: "orders".into(),
            document_id: "o1".into(),
            operation: ChangeOperation::Update,
            timestamp_ms: 2000,
        };
        stream.publish(event);

        // Both subscribers receive the event.
        let r1 = sub1.receiver.recv().await.unwrap();
        let r2 = sub2.receiver.recv().await.unwrap();
        assert_eq!(r1.document_id, "o1");
        assert_eq!(r2.document_id, "o1");
    }

    #[test]
    fn metrics_tracking() {
        let stream = ChangeStream::new(64);
        let _sub = stream.subscribe(None, None);
        assert_eq!(stream.subscriber_count(), 1);

        for i in 0..10 {
            stream.publish(ChangeEvent {
                lsn: Lsn::new(i),
                tenant_id: TenantId::new(1),
                collection: "test".into(),
                document_id: format!("d{i}"),
                operation: ChangeOperation::Insert,
                timestamp_ms: 0,
            });
        }
        assert_eq!(stream.events_published(), 10);
        assert_eq!(stream.last_lsn(), 9);

        stream.unsubscribe();
        assert_eq!(stream.subscriber_count(), 0);
    }
}
