//! Background apply loop — reads committed Raft entries from the mpsc channel,
//! dispatches them through the SPSC bridge to the Data Plane, and resolves
//! propose waiters with the result.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tracing::debug;

use nodedb_cluster::rpc_codec::RaftRpc;
use nodedb_cluster::wire::{VShardEnvelope, VShardMessageType, WIRE_VERSION};

use crate::bridge::envelope::{PhysicalPlan, Priority, Request, Status};
use crate::bridge::physical_plan::MetaOp;
use crate::control::array_sync::raft_apply::{apply_array_op, apply_array_schema};
use crate::control::state::SharedState;
use crate::control::wal_replication::{
    ReplicatedEntry, ReplicatedWrite, decode_forwarded_plans, from_replicated_entry,
};
use crate::types::{ReadConsistency, TenantId, TraceId, VShardId};

use super::applier::ApplyBatch;
use super::propose_tracker::ProposeTracker;

/// Outcome of dispatching a single request through the SPSC bridge.
enum DispatchOutcome {
    Ok(Vec<u8>),
    /// Failure with a human-readable reason (dispatch error, timeout,
    /// closed channel, or an error response from the Data Plane).
    Failed(String),
}

/// Build a `Request` for a committed write with default deadline / priority.
fn build_request(
    state: &Arc<SharedState>,
    tenant_id: TenantId,
    vshard_id: VShardId,
    plan: PhysicalPlan,
    event_source: crate::event::EventSource,
) -> Request {
    Request {
        request_id: state.next_request_id(),
        tenant_id,
        vshard_id,
        plan,
        deadline: Instant::now() + Duration::from_secs(30),
        priority: Priority::Normal,
        trace_id: TraceId::generate(),
        consistency: ReadConsistency::Strong,
        idempotency_key: None,
        event_source,
        user_roles: Vec::new(),
    }
}

/// Dispatch a request through the SPSC bridge and await its response.
///
/// Returns:
/// - `Ok(payload)` on success
/// - `Failed(reason)` for dispatch error, channel close, deadline exceeded,
///   or an error-status response from the Data Plane
async fn dispatch_and_await(state: &Arc<SharedState>, request: Request) -> DispatchOutcome {
    let request_id = request.request_id;
    let mut rx = state.tracker.register(request_id);

    let dispatch_result = match state.dispatcher.lock() {
        Ok(mut d) => d.dispatch(request),
        Err(poisoned) => poisoned.into_inner().dispatch(request),
    };

    if let Err(e) = dispatch_result {
        return DispatchOutcome::Failed(format!("dispatch failed: {e}"));
    }

    match tokio::time::timeout(Duration::from_secs(30), async { rx.recv().await.ok_or(()) }).await {
        Ok(Ok(resp)) => {
            if resp.status == Status::Error {
                let reason = resp
                    .error_code
                    .as_ref()
                    .map(|c| format!("{c:?}"))
                    .unwrap_or_else(|| "execution error".into());
                DispatchOutcome::Failed(reason)
            } else {
                DispatchOutcome::Ok(resp.payload.to_vec())
            }
        }
        Ok(Err(_)) => DispatchOutcome::Failed("response channel closed".to_string()),
        Err(_) => DispatchOutcome::Failed("deadline exceeded".to_string()),
    }
}

/// Run the background loop that applies committed Raft entries to the local Data Plane.
///
/// This task reads from the apply channel, deserializes each entry, dispatches
/// the write to the Data Plane via SPSC, and notifies proposers.
pub async fn run_apply_loop(
    mut apply_rx: mpsc::Receiver<ApplyBatch>,
    state: Arc<SharedState>,
    tracker: Arc<ProposeTracker>,
) {
    while let Some(batch) = apply_rx.recv().await {
        for entry in &batch.entries {
            // Extract idempotency key once at the top so every
            // tracker.complete on this entry can pass it. Returns 0
            // for unparseable / pre-key entries; the tracker treats
            // 0 as "no key" (no mismatch detection).
            let applied_key = ReplicatedEntry::from_bytes(&entry.data)
                .map(|e| e.idempotency_key)
                .unwrap_or(0);

            // ── Array CRDT variants — handled on the Control Plane, bypass Data Plane ──
            if let Some(replicated) = ReplicatedEntry::from_bytes(&entry.data) {
                match replicated.write {
                    ReplicatedWrite::ArrayOp {
                        ref array,
                        ref op_bytes,
                        ..
                    } => {
                        apply_array_op(
                            &state,
                            &tracker,
                            batch.group_id,
                            entry.index,
                            applied_key,
                            array,
                            op_bytes,
                        )
                        .await;
                        continue;
                    }
                    ReplicatedWrite::ArraySchema {
                        ref array,
                        ref snapshot_payload,
                        schema_hlc_bytes,
                    } => {
                        apply_array_schema(
                            &state,
                            &tracker,
                            batch.group_id,
                            entry.index,
                            applied_key,
                            crate::control::array_sync::raft_apply::ArraySchemaPayload {
                                array,
                                snapshot_payload,
                                schema_hlc_bytes,
                            },
                        );
                        continue;
                    }
                    // ── Cross-shard forwarded transaction batch ──
                    ReplicatedWrite::CrossShardForward {
                        txn_id,
                        tenant_id,
                        ref plans_bytes,
                        source_vshard,
                        coordinator_log_index,
                    } => {
                        apply_cross_shard_forward(
                            &state,
                            &tracker,
                            batch.group_id,
                            entry.index,
                            applied_key,
                            txn_id,
                            tenant_id,
                            plans_bytes,
                            source_vshard,
                            coordinator_log_index,
                        )
                        .await;
                        continue;
                    }
                    _ => {}
                }
            }

            let decoded =
                from_replicated_entry(&entry.data, Some(state.surrogate_assigner.as_ref()));
            let (tenant_id, vshard_id, plan) = match decoded {
                Ok(Some(t)) => t,
                Ok(None) => {
                    // Couldn't deserialize — might be a different format or corrupted.
                    debug!(
                        group_id = batch.group_id,
                        index = entry.index,
                        "skipping non-ReplicatedEntry commit"
                    );
                    tracker.complete(batch.group_id, entry.index, applied_key, Ok(vec![]));
                    continue;
                }
                Err(e) => {
                    tracing::warn!(
                        group_id = batch.group_id,
                        index = entry.index,
                        error = %e,
                        "failed to decode replicated entry (surrogate bind error)"
                    );
                    tracker.complete(
                        batch.group_id,
                        entry.index,
                        applied_key,
                        Err(crate::Error::Internal {
                            detail: format!("decode replicated entry: {e}"),
                        }),
                    );
                    continue;
                }
            };

            let request = build_request(
                &state,
                tenant_id,
                vshard_id,
                plan,
                crate::event::EventSource::User,
            );

            let result = match dispatch_and_await(&state, request).await {
                DispatchOutcome::Ok(payload) => Ok(payload),
                DispatchOutcome::Failed(reason) => {
                    tracing::warn!(
                        group_id = batch.group_id,
                        index = entry.index,
                        reason = %reason,
                        "applying committed write failed"
                    );
                    Err(crate::Error::Internal { detail: reason })
                }
            };

            tracker.complete(batch.group_id, entry.index, applied_key, result);
        }
    }
}

/// Apply a cross-shard forwarded transaction batch.
///
/// Decodes `plans_bytes` into a `Vec<PhysicalPlan>`, wraps them in a
/// `MetaOp::TransactionBatch`, and dispatches to the local Data Plane. If the
/// apply fails, sends a `CrossShardAbort` VShardEnvelope back to the
/// coordinator's `source_vshard` leader node.
///
/// Local rollback on failure is handled by the single-shard `TransactionBatch`
/// handler — the abort notification is purely informational to the coordinator.
///
/// TODO(cross-shard-abort): compensate locally on the coordinator shard;
/// tracked separately.
#[allow(clippy::too_many_arguments)]
async fn apply_cross_shard_forward(
    state: &Arc<SharedState>,
    tracker: &Arc<ProposeTracker>,
    group_id: u64,
    log_index: u64,
    applied_key: u64,
    txn_id: u64,
    tenant_id: u64,
    plans_bytes: &[u8],
    source_vshard: u32,
    _coordinator_log_index: u64,
) {
    let plans = match decode_forwarded_plans(plans_bytes) {
        Ok(p) => p,
        Err(e) => {
            tracing::error!(
                group_id,
                log_index,
                txn_id,
                error = %e,
                "cross-shard forward: failed to decode plans_bytes; aborting"
            );
            tracker.complete(
                group_id,
                log_index,
                applied_key,
                Err(crate::Error::CrossShardAborted {
                    txn_id,
                    source_vshard,
                    reason: format!("decode error: {e}"),
                }),
            );
            return;
        }
    };

    let request = build_request(
        state,
        TenantId::new(tenant_id),
        VShardId::new(source_vshard),
        PhysicalPlan::Meta(MetaOp::TransactionBatch { plans }),
        crate::event::EventSource::RaftFollower,
    );

    let result = match dispatch_and_await(state, request).await {
        DispatchOutcome::Ok(payload) => Ok(payload),
        DispatchOutcome::Failed(reason) => {
            tracing::error!(
                group_id,
                log_index,
                txn_id,
                reason = %reason,
                "cross-shard forward apply failed; sending abort notice to coordinator"
            );
            send_abort_notice(state, txn_id, source_vshard, &reason).await;
            Err(crate::Error::CrossShardAborted {
                txn_id,
                source_vshard,
                reason,
            })
        }
    };

    tracker.complete(group_id, log_index, applied_key, result);
}

/// Fire-and-forget: send a `CrossShardAbort` VShardEnvelope to the leader node
/// of `source_vshard`. Logs on error but never blocks the apply loop.
async fn send_abort_notice(
    state: &Arc<SharedState>,
    txn_id: u64,
    source_vshard: u32,
    reason: &str,
) {
    let (transport, target_node) = match resolve_coordinator_transport(state, source_vshard) {
        Some(pair) => pair,
        None => {
            // Single-node mode or routing table unavailable — nothing to send.
            return;
        }
    };

    let notice = nodedb_cluster::cross_shard_txn::AbortNotice {
        txn_id,
        reason: reason.to_string(),
    };
    let payload = match zerompk::to_msgpack_vec(&notice) {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!(txn_id, error = %e, "failed to serialize AbortNotice; abort not sent");
            return;
        }
    };

    let envelope = VShardEnvelope {
        version: WIRE_VERSION,
        msg_type: VShardMessageType::CrossShardAbort,
        source_node: state.node_id,
        target_node,
        vshard_id: source_vshard,
        payload,
    };
    let rpc = RaftRpc::VShardEnvelope(envelope.to_bytes());

    // Spawn a detached task so the apply loop is not blocked waiting for
    // the network round-trip. A failure here is observable via tracing;
    // the coordinator will time out waiting for acks if the notice is
    // lost, which is acceptable for v1.
    tokio::spawn(async move {
        if let Err(e) = transport.send_rpc(target_node, rpc).await {
            tracing::warn!(
                txn_id,
                target_node,
                error = %e,
                "failed to deliver CrossShardAbort notice to coordinator"
            );
        }
    });
}

/// Look up the cluster transport and coordinator node for `source_vshard`.
///
/// Returns `None` in single-node mode (no transport) or if the routing
/// table does not map the vshard.
fn resolve_coordinator_transport(
    state: &Arc<SharedState>,
    source_vshard: u32,
) -> Option<(Arc<nodedb_cluster::NexarTransport>, u64)> {
    let transport = state.cluster_transport.as_ref()?.clone();
    let routing = state.cluster_routing.as_ref()?;
    let target_node = routing
        .read()
        .unwrap_or_else(|p| p.into_inner())
        .leader_for_vshard(source_vshard)
        .ok()?;
    Some((transport, target_node))
}
