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

/// Current wall-clock time as milliseconds since Unix epoch.
///
/// Returns 0 if the system clock is before the epoch (should never happen
/// on correctly configured systems).
fn current_timestamp_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

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
    // Extract write metadata before the plan is moved into the request.
    let change_meta = extract_write_metadata(&plan, tenant_id);

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

    let response = tokio::time::timeout(DEFAULT_DEADLINE, rx)
        .await
        .map_err(|_| crate::Error::DeadlineExceeded { request_id })?
        .map_err(|_| crate::Error::Dispatch {
            detail: "response channel closed".into(),
        })?;

    // Publish change events for successful writes.
    if response.status == crate::bridge::envelope::Status::Ok
        && let Some((collection, doc_id, op)) = change_meta
    {
        use crate::control::change_stream::ChangeEvent;
        shared.change_stream.publish(ChangeEvent {
            lsn: response.watermark_lsn,
            tenant_id,
            collection,
            document_id: doc_id,
            operation: op,
            timestamp_ms: current_timestamp_ms(),
            after: None,
        });
    }

    Ok(response)
}

/// Extract write metadata from a physical plan for change event publishing.
///
/// `_tenant_id` is reserved for future tenant-scoped change stream filtering.
fn extract_write_metadata(
    plan: &PhysicalPlan,
    _tenant_id: TenantId,
) -> Option<(
    String,
    String,
    crate::control::change_stream::ChangeOperation,
)> {
    use crate::control::change_stream::ChangeOperation;
    match plan {
        PhysicalPlan::PointPut {
            collection,
            document_id,
            ..
        } => Some((
            collection.clone(),
            document_id.clone(),
            ChangeOperation::Insert,
        )),
        PhysicalPlan::PointDelete {
            collection,
            document_id,
        } => Some((
            collection.clone(),
            document_id.clone(),
            ChangeOperation::Delete,
        )),
        PhysicalPlan::PointUpdate {
            collection,
            document_id,
            ..
        } => Some((
            collection.clone(),
            document_id.clone(),
            ChangeOperation::Update,
        )),
        PhysicalPlan::Upsert {
            collection,
            document_id,
            ..
        } => Some((
            collection.clone(),
            document_id.clone(),
            ChangeOperation::Insert,
        )),
        PhysicalPlan::BulkUpdate { collection, .. } => {
            Some((collection.clone(), "*".into(), ChangeOperation::Update))
        }
        PhysicalPlan::BulkDelete { collection, .. } => {
            Some((collection.clone(), "*".into(), ChangeOperation::Delete))
        }
        PhysicalPlan::Truncate { collection } => {
            Some((collection.clone(), "*".into(), ChangeOperation::Delete))
        }
        _ => None,
    }
}
