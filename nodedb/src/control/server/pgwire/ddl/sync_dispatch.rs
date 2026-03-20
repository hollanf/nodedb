//! Shared synchronous dispatch helper for DDL and DSL handlers.
//!
//! All DDL/DSL modules that need to send a [`PhysicalPlan`] to the Data Plane
//! and block on the result use [`dispatch_sync`] rather than duplicating the
//! same boilerplate.

use std::time::{Duration, Instant};

use crate::bridge::envelope::{PhysicalPlan, Priority, Request, Status};
use crate::control::state::SharedState;
use crate::types::{ReadConsistency, RequestId, TenantId, VShardId};

/// Send `plan` to the Data Plane and block until a response arrives or
/// `timeout` elapses.
///
/// Returns the raw response payload on success, or a human-readable error
/// string on failure.  Callers are responsible for mapping the `Err` variant
/// to their own error type (e.g. via `.map_err(|e| sqlstate_error("XX000",
/// &e))`).
pub fn dispatch_sync(
    state: &SharedState,
    tenant_id: TenantId,
    collection: &str,
    plan: PhysicalPlan,
    timeout: Duration,
) -> Result<Vec<u8>, String> {
    let vshard_id = VShardId::from_collection(collection);
    let request_id = RequestId::new(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64,
    );

    let request = Request {
        request_id,
        tenant_id,
        vshard_id,
        plan,
        deadline: Instant::now() + timeout,
        priority: Priority::Normal,
        trace_id: 0,
        consistency: ReadConsistency::Strong,
        idempotency_key: None,
    };

    let rx = state.tracker.register(request_id);

    match state.dispatcher.lock() {
        Ok(mut d) => d.dispatch(request).map_err(|e| e.to_string())?,
        Err(p) => p
            .into_inner()
            .dispatch(request)
            .map_err(|e| e.to_string())?,
    };

    let resp = rx
        .blocking_recv()
        .map_err(|_| "response channel closed".to_string())?;

    if resp.status != Status::Ok {
        let detail = resp
            .error_code
            .as_ref()
            .map(|c| format!("{c:?}"))
            .unwrap_or_else(|| String::from_utf8_lossy(&resp.payload).into_owned());
        return Err(detail);
    }

    Ok(resp.payload.to_vec())
}
