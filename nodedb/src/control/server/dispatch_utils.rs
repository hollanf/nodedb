//! Shared dispatch utilities used by both the pgwire and native endpoints.
//!
//! Re-exports from focused sub-modules for backward compatibility.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::bridge::envelope::{PhysicalPlan, Priority, Request, Response};
use crate::control::state::SharedState;
use crate::types::{ReadConsistency, RequestId, TenantId, VShardId};

pub use super::broadcast::broadcast_to_all_cores;
pub use super::graph_dispatch::cross_core_bfs;
pub use super::wal_dispatch::wal_append_if_write;

static DISPATCH_COUNTER: AtomicU64 = AtomicU64::new(1_000_000);

/// Default request deadline.
const DEFAULT_DEADLINE: Duration = Duration::from_secs(30);

/// Dispatch a physical plan to the Data Plane and await the response.
///
/// Creates a request envelope, registers with the tracker for correlation,
/// dispatches via the SPSC bridge, and awaits the response with a timeout.
pub async fn dispatch_to_data_plane(
    shared: &SharedState,
    tenant_id: TenantId,
    vshard_id: VShardId,
    plan: PhysicalPlan,
    trace_id: u64,
) -> crate::Result<Response> {
    let request_id = RequestId::new(DISPATCH_COUNTER.fetch_add(1, Ordering::Relaxed));
    let request = Request {
        request_id,
        tenant_id,
        vshard_id,
        plan,
        deadline: Instant::now() + DEFAULT_DEADLINE,
        priority: Priority::Normal,
        trace_id,
        consistency: ReadConsistency::Strong,
        idempotency_key: None,
    };

    let rx = shared.tracker.register(request_id);

    match shared.dispatcher.lock() {
        Ok(mut d) => d.dispatch(request)?,
        Err(poisoned) => poisoned.into_inner().dispatch(request)?,
    };

    tokio::time::timeout(DEFAULT_DEADLINE, rx)
        .await
        .map_err(|_| crate::Error::DeadlineExceeded { request_id })?
        .map_err(|_| crate::Error::Dispatch {
            detail: "response channel closed".into(),
        })
}
