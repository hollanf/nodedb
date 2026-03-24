//! Broadcast dispatch: fan a plan to all Data Plane cores and merge results.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::bridge::envelope::{PhysicalPlan, Priority, Request, Response};
use crate::control::state::SharedState;
use crate::types::{Lsn, ReadConsistency, RequestId, TenantId, VShardId};

static BROADCAST_COUNTER: AtomicU64 = AtomicU64::new(2_000_000);

/// Default request deadline.
const DEFAULT_DEADLINE: Duration = Duration::from_secs(30);

/// Broadcast a physical plan to ALL Data Plane cores and merge responses.
///
/// Used for scans (DocumentScan, Aggregate, etc.) where data is distributed
/// across cores. Each core scans its local storage, and all results are
/// concatenated. The merged response payload is a JSON array of all results.
pub async fn broadcast_to_all_cores(
    shared: &SharedState,
    tenant_id: TenantId,
    plan: PhysicalPlan,
    trace_id: u64,
) -> crate::Result<Response> {
    let num_cores = match shared.dispatcher.lock() {
        Ok(d) => d.num_cores(),
        Err(p) => p.into_inner().num_cores(),
    };

    let mut receivers = Vec::with_capacity(num_cores);
    for core_id in 0..num_cores {
        let request_id = RequestId::new(BROADCAST_COUNTER.fetch_add(1, Ordering::Relaxed));
        let vshard_id = VShardId::new(core_id as u16);
        let request = Request {
            request_id,
            tenant_id,
            vshard_id,
            plan: plan.clone(),
            deadline: Instant::now() + DEFAULT_DEADLINE,
            priority: Priority::Normal,
            trace_id,
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
        };

        let rx = shared.tracker.register(request_id);
        match shared.dispatcher.lock() {
            Ok(mut d) => d.dispatch_to_core(core_id, request)?,
            Err(p) => p.into_inner().dispatch_to_core(core_id, request)?,
        };
        receivers.push(rx);
    }

    // Await all responses and merge payloads.
    let mut merged_payload: Vec<u8> = Vec::new();
    let mut max_lsn = Lsn::ZERO;
    let mut had_error = false;
    let mut error_msg = String::new();

    merged_payload.push(b'[');
    let mut first = true;

    for rx in receivers {
        let resp = tokio::time::timeout(DEFAULT_DEADLINE, rx)
            .await
            .map_err(|_| crate::Error::Dispatch {
                detail: "broadcast timeout".into(),
            })?
            .map_err(|_| crate::Error::Dispatch {
                detail: "broadcast channel closed".into(),
            })?;

        if resp.status == crate::bridge::envelope::Status::Error {
            if let Some(ref ec) = resp.error_code {
                match ec {
                    crate::bridge::envelope::ErrorCode::NotFound => continue,
                    _ => {
                        had_error = true;
                        error_msg = format!("{ec:?}");
                    }
                }
            }
            continue;
        }

        if resp.watermark_lsn > max_lsn {
            max_lsn = resp.watermark_lsn;
        }

        if !resp.payload.is_empty() {
            let json_text =
                crate::data::executor::response_codec::decode_payload_to_json(&resp.payload);
            let json_bytes = json_text.as_bytes();

            if json_bytes.starts_with(b"[") && json_bytes.ends_with(b"]") {
                let inner = &json_bytes[1..json_bytes.len() - 1];
                if !inner.is_empty() {
                    if !first {
                        merged_payload.push(b',');
                    }
                    merged_payload.extend_from_slice(inner);
                    first = false;
                }
            } else if !json_bytes.is_empty() {
                if !first {
                    merged_payload.push(b',');
                }
                merged_payload.extend_from_slice(json_bytes);
                first = false;
            }
        }
    }

    merged_payload.push(b']');

    if had_error && first {
        return Err(crate::Error::Dispatch { detail: error_msg });
    }

    Ok(Response {
        request_id: RequestId::new(0),
        status: crate::bridge::envelope::Status::Ok,
        attempt: 1,
        partial: false,
        payload: crate::bridge::envelope::Payload::from_vec(merged_payload),
        watermark_lsn: max_lsn,
        error_code: None,
    })
}
