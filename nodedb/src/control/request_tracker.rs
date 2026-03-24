use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};

use tokio::sync::mpsc;

use crate::bridge::envelope::Response;
use crate::types::RequestId;

/// Routes Data Plane responses back to the waiting Control Plane session.
///
/// Each dispatched request registers an mpsc sender here. The background
/// response poller forwards responses as they arrive. For streaming queries,
/// multiple partial responses arrive before the final one.
///
/// - Partial responses (`response.partial == true`): forwarded but request
///   stays in the map for more chunks.
/// - Final response (`response.partial == false`): forwarded and request
///   removed from the map.
#[derive(Default)]
pub struct RequestTracker {
    pending: Mutex<HashMap<RequestId, mpsc::UnboundedSender<Response>>>,
}

impl RequestTracker {
    pub fn new() -> Self {
        Self {
            pending: Mutex::new(HashMap::new()),
        }
    }

    fn lock_pending(&self) -> MutexGuard<'_, HashMap<RequestId, mpsc::UnboundedSender<Response>>> {
        match self.pending.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Register a pending request. Returns a receiver the session awaits.
    ///
    /// For non-streaming requests, exactly one response arrives.
    /// For streaming requests, multiple partial responses arrive before the final one.
    pub fn register(&self, id: RequestId) -> mpsc::UnboundedReceiver<Response> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.lock_pending().insert(id, tx);
        rx
    }

    /// Forward a response from the Data Plane to the waiting session.
    ///
    /// - If `response.partial` is true: sends the chunk but keeps the
    ///   request in the map for subsequent chunks.
    /// - If `response.partial` is false: sends the final chunk and
    ///   removes the request from the map.
    ///
    /// Returns `false` if the request was already cancelled/timed out.
    pub fn complete(&self, response: Response) -> bool {
        let is_final = !response.partial;
        let mut pending = self.lock_pending();

        if is_final {
            // Final response: remove from map and send.
            if let Some(tx) = pending.remove(&response.request_id) {
                tx.send(response).is_ok()
            } else {
                false
            }
        } else {
            // Partial response: send but keep in map.
            if let Some(tx) = pending.get(&response.request_id) {
                tx.send(response).is_ok()
            } else {
                false
            }
        }
    }

    /// Remove a pending request (e.g., on session disconnect).
    pub fn cancel(&self, id: &RequestId) {
        self.lock_pending().remove(id);
    }

    /// Register and return a one-shot-style receiver that resolves on the first response.
    ///
    /// For backward compatibility with callers that expect a single response.
    /// The returned future resolves when any response (partial or final) arrives.
    /// For streaming, use `register()` directly.
    pub fn register_oneshot(
        &self,
        id: RequestId,
    ) -> impl std::future::Future<Output = Result<Response, ()>> {
        let mut rx = self.register(id);
        async move { rx.recv().await.ok_or(()) }
    }

    /// Number of in-flight requests.
    pub fn in_flight(&self) -> usize {
        self.lock_pending().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::{Payload, Status};
    use crate::types::Lsn;

    fn make_response(id: u64) -> Response {
        Response {
            request_id: RequestId::new(id),
            status: Status::Ok,
            attempt: 1,
            partial: false,
            payload: Payload::empty(),
            watermark_lsn: Lsn::ZERO,
            error_code: None,
        }
    }

    fn make_partial(id: u64, data: &str) -> Response {
        Response {
            request_id: RequestId::new(id),
            status: Status::Partial,
            attempt: 1,
            partial: true,
            payload: Payload::from_vec(data.as_bytes().to_vec()),
            watermark_lsn: Lsn::ZERO,
            error_code: None,
        }
    }

    #[test]
    fn register_and_complete() {
        let tracker = RequestTracker::new();
        let mut rx = tracker.register(RequestId::new(1));
        assert_eq!(tracker.in_flight(), 1);

        assert!(tracker.complete(make_response(1)));
        assert_eq!(tracker.in_flight(), 0);

        let resp = rx.try_recv().unwrap();
        assert_eq!(resp.request_id, RequestId::new(1));
    }

    #[test]
    fn complete_unknown_returns_false() {
        let tracker = RequestTracker::new();
        assert!(!tracker.complete(make_response(999)));
    }

    #[test]
    fn cancel_removes_pending() {
        let tracker = RequestTracker::new();
        let _rx = tracker.register(RequestId::new(5));
        assert_eq!(tracker.in_flight(), 1);
        tracker.cancel(&RequestId::new(5));
        assert_eq!(tracker.in_flight(), 0);
    }

    #[test]
    fn streaming_partial_then_final() {
        let tracker = RequestTracker::new();
        let mut rx = tracker.register(RequestId::new(10));

        // Send two partial chunks.
        assert!(tracker.complete(make_partial(10, "chunk1")));
        assert_eq!(tracker.in_flight(), 1); // Still in map.
        assert!(tracker.complete(make_partial(10, "chunk2")));
        assert_eq!(tracker.in_flight(), 1); // Still in map.

        // Send final response.
        assert!(tracker.complete(make_response(10)));
        assert_eq!(tracker.in_flight(), 0); // Removed.

        // All three responses should be receivable.
        let r1 = rx.try_recv().unwrap();
        assert!(r1.partial);
        let r2 = rx.try_recv().unwrap();
        assert!(r2.partial);
        let r3 = rx.try_recv().unwrap();
        assert!(!r3.partial);
    }
}
