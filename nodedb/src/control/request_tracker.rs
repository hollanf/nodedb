use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};

use tokio::sync::oneshot;

use crate::bridge::envelope::Response;
use crate::types::RequestId;

/// Routes Data Plane responses back to the waiting Control Plane session.
///
/// Each dispatched request registers a oneshot sender here. The background
/// response poller completes the oneshot when the Data Plane produces a result.
#[derive(Default)]
pub struct RequestTracker {
    pending: Mutex<HashMap<RequestId, oneshot::Sender<Response>>>,
}

impl RequestTracker {
    pub fn new() -> Self {
        Self {
            pending: Mutex::new(HashMap::new()),
        }
    }

    /// Lock the pending map, recovering from poison if needed.
    fn lock_pending(&self) -> MutexGuard<'_, HashMap<RequestId, oneshot::Sender<Response>>> {
        match self.pending.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Register a pending request. Returns a receiver the session awaits.
    pub fn register(&self, id: RequestId) -> oneshot::Receiver<Response> {
        let (tx, rx) = oneshot::channel();
        self.lock_pending().insert(id, tx);
        rx
    }

    /// Complete a pending request with a response from the Data Plane.
    ///
    /// Returns `false` if the request was already cancelled/timed out (receiver dropped).
    pub fn complete(&self, response: Response) -> bool {
        if let Some(tx) = self.lock_pending().remove(&response.request_id) {
            tx.send(response).is_ok()
        } else {
            false
        }
    }

    /// Remove a pending request (e.g., on session disconnect).
    pub fn cancel(&self, id: &RequestId) {
        self.lock_pending().remove(id);
    }

    /// Number of in-flight requests.
    pub fn in_flight(&self) -> usize {
        self.lock_pending().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::Status;
    use crate::types::Lsn;
    use std::sync::Arc;

    fn make_response(id: u64) -> Response {
        Response {
            request_id: RequestId::new(id),
            status: Status::Ok,
            attempt: 1,
            partial: false,
            payload: Arc::from([].as_slice()),
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
}
