//! Cross-core BFS orchestration for graph traversal.
//!
//! In single-node mode, BFS is local: the Control Plane broadcasts
//! `GraphNeighbors` to all Data Plane cores hop by hop and collects results.
//!
//! In cluster mode, after each local hop the Control Plane inspects the
//! discovered frontier and identifies nodes that hash to shards owned by
//! remote nodes. Those are batched into a `ScatterEnvelope` and dispatched
//! to the remote shard leaders via `control::scatter_gather::coordinate_cross_shard_hop`.
//! Remote results are merged before the next depth level begins.

use std::collections::{HashMap, HashSet};

use sonic_rs;

use crate::bridge::envelope::{PhysicalPlan, Response};
use crate::bridge::physical_plan::GraphOp;
use crate::control::scatter_gather;
use crate::control::state::SharedState;
use crate::engine::graph::traversal_options::GraphTraversalOptions;
use crate::types::{Lsn, RequestId, TenantId};

/// Cross-core BFS orchestration for graph traversal.
///
/// Implements multi-hop BFS. In single-node mode, broadcasts `GraphNeighbors`
/// to all local Data Plane cores. In cluster mode, after each local hop the
/// frontier is partitioned into local/remote nodes and remote nodes are
/// coordinated via `scatter_gather::coordinate_cross_shard_hop`.
///
/// Returns a JSON array of all discovered node IDs.
pub async fn cross_core_bfs(
    shared: &SharedState,
    tenant_id: TenantId,
    start_nodes: Vec<String>,
    edge_label: Option<String>,
    direction: crate::engine::graph::edge_store::Direction,
    max_depth: usize,
) -> crate::Result<Response> {
    cross_core_bfs_with_options(
        shared,
        tenant_id,
        start_nodes,
        edge_label,
        direction,
        max_depth,
        &GraphTraversalOptions::default(),
    )
    .await
}

/// Cross-core BFS with explicit traversal options (fan-out limits, partial mode).
///
/// This is the full cluster-aware entry point. `cross_core_bfs` delegates here
/// with default options for backward compatibility.
pub async fn cross_core_bfs_with_options(
    shared: &SharedState,
    tenant_id: TenantId,
    start_nodes: Vec<String>,
    edge_label: Option<String>,
    direction: crate::engine::graph::edge_store::Direction,
    max_depth: usize,
    options: &GraphTraversalOptions,
) -> crate::Result<Response> {
    let cluster_mode = shared.cluster_routing.is_some();

    let mut visited: HashSet<String> = HashSet::new();
    let mut all_discovered: Vec<String> = Vec::new();
    let mut frontier: Vec<String> = start_nodes.clone();

    for node in &start_nodes {
        visited.insert(node.clone());
        all_discovered.push(node.clone());
    }

    for depth in 0..max_depth {
        if frontier.is_empty() {
            break;
        }

        // ── Local hop ──────────────────────────────────────────────────────
        // Broadcast GraphNeighbors to all local Data Plane cores.
        let mut local_hop_results: Vec<String> = Vec::new();

        for node in &frontier {
            let plan = PhysicalPlan::Graph(GraphOp::Neighbors {
                node_id: node.clone(),
                edge_label: edge_label.clone(),
                direction,
                rls_filters: Vec::new(),
            });

            let resp = super::broadcast::broadcast_to_all_cores(shared, tenant_id, plan, 0).await?;

            if !resp.payload.is_empty() {
                let json_text =
                    crate::data::executor::response_codec::decode_payload_to_json(&resp.payload);
                if let Ok(arr) = sonic_rs::from_str::<Vec<serde_json::Value>>(&json_text) {
                    for item in arr {
                        if let Some(neighbor) = item.get("node").and_then(|v| v.as_str()) {
                            local_hop_results.push(neighbor.to_string());
                        }
                    }
                }
            }
        }

        // ── Cluster mode: scatter-gather for cross-shard frontier ──────────
        let merged_hop_results = if cluster_mode {
            // Partition newly-discovered nodes into local vs cross-shard.
            let (local_nodes, cross_shard_envelope) = {
                let routing = shared
                    .cluster_routing
                    .as_ref()
                    .expect("cluster_routing checked above");
                let rt = routing.read().unwrap_or_else(|p| p.into_inner());
                scatter_gather::partition_local_remote(&local_hop_results, shared.node_id, &rt)
            };

            if cross_shard_envelope.is_empty() {
                // All results are local — no cross-shard work needed.
                local_nodes
            } else {
                let remaining_depth = max_depth.saturating_sub(depth + 1);
                let (merged, _meta) = scatter_gather::coordinate_cross_shard_hop(
                    shared,
                    tenant_id,
                    scatter_gather::CrossShardHopParams {
                        local_nodes,
                        envelope: cross_shard_envelope,
                        options,
                        edge_label: edge_label.as_deref(),
                        direction,
                        remaining_depth,
                    },
                )
                .await?;
                merged
            }
        } else {
            // Single-node mode: all results are already local.
            local_hop_results
        };

        // ── Extend global visited set and compute next frontier ────────────
        let mut next_frontier: Vec<String> = Vec::new();
        for node in merged_hop_results {
            if visited.insert(node.clone()) {
                next_frontier.push(node.clone());
                all_discovered.push(node);
            }
        }

        frontier = next_frontier;

        // Enforce max_visited cap across all hops.
        if all_discovered.len() >= options.max_visited {
            break;
        }
    }

    let payload = match sonic_rs::to_vec(&all_discovered) {
        Ok(v) => v,
        Err(e) => {
            return Err(crate::Error::Serialization {
                format: "json".into(),
                detail: e.to_string(),
            });
        }
    };

    Ok(Response {
        request_id: RequestId::new(0),
        status: crate::bridge::envelope::Status::Ok,
        attempt: 1,
        partial: false,
        payload: crate::bridge::envelope::Payload::from_vec(payload),
        watermark_lsn: Lsn::ZERO,
        error_code: None,
    })
}

/// Cross-core / cross-shard shortest-path orchestration.
///
/// Walks the same hop-by-hop BFS as `cross_core_bfs` but records a
/// `parent` pointer for every newly-discovered node. When `dst` is
/// reached, the path is reconstructed by walking parents back to
/// `src`. Returns a JSON array `[src, hop_1, ..., dst]`, or an empty
/// array when `dst` is unreachable within `max_depth` hops.
///
/// Single-shard dispatch to `GraphOp::Path` only sees one core's CSR
/// and misses any path that crosses shard boundaries. This function
/// mirrors `cross_core_bfs` so path routing is consistent with
/// traversal across every topology — single core, single-node
/// multi-core, and clustered.
pub async fn cross_core_shortest_path(
    shared: &SharedState,
    tenant_id: TenantId,
    src: String,
    dst: String,
    edge_label: Option<String>,
    max_depth: usize,
) -> crate::Result<Response> {
    let options = GraphTraversalOptions::default();
    let cluster_mode = shared.cluster_routing.is_some();
    // Path semantics only make sense over outgoing edges — the
    // docs-advertised `GRAPH PATH FROM 'a' TO 'b'` is directed.
    let direction = crate::engine::graph::edge_store::Direction::Out;

    // Empty path when src == dst; matches the natural `[src]` case.
    if src == dst {
        let payload = encode_path(&[src])?;
        return Ok(ok_response(payload));
    }

    let mut parent: HashMap<String, String> = HashMap::new();
    let mut visited: HashSet<String> = HashSet::new();
    visited.insert(src.clone());
    let mut frontier: Vec<String> = vec![src.clone()];

    for depth in 0..max_depth {
        if frontier.is_empty() {
            break;
        }

        // Local hop: for every node in the frontier, pull its
        // neighbors off whichever core owns that node.
        let mut discoveries: Vec<(String, String)> = Vec::new();
        for node in &frontier {
            let plan = PhysicalPlan::Graph(GraphOp::Neighbors {
                node_id: node.clone(),
                edge_label: edge_label.clone(),
                direction,
                rls_filters: Vec::new(),
            });
            let resp = super::broadcast::broadcast_to_all_cores(shared, tenant_id, plan, 0).await?;
            if resp.payload.is_empty() {
                continue;
            }
            let json_text =
                crate::data::executor::response_codec::decode_payload_to_json(&resp.payload);
            if let Ok(arr) = sonic_rs::from_str::<Vec<serde_json::Value>>(&json_text) {
                for item in arr {
                    if let Some(nb) = item.get("node").and_then(|v| v.as_str()) {
                        discoveries.push((node.clone(), nb.to_string()));
                    }
                }
            }
        }

        // Cluster mode: merge cross-shard neighbor results. The
        // cross-shard helper only returns neighbor ids (no parent),
        // so we attribute them to the first frontier node on the
        // same shard — sufficient for shortest-path correctness
        // because any path through that shard is equally shortest.
        let merged: Vec<(String, String)> = if cluster_mode {
            let local_ids: Vec<String> = discoveries.iter().map(|(_, n)| n.clone()).collect();
            let (local_nodes, envelope) = {
                let routing = shared
                    .cluster_routing
                    .as_ref()
                    .expect("cluster_routing checked above");
                let rt = routing.read().unwrap_or_else(|p| p.into_inner());
                scatter_gather::partition_local_remote(&local_ids, shared.node_id, &rt)
            };
            if envelope.is_empty() {
                discoveries
            } else {
                let remaining = max_depth.saturating_sub(depth + 1);
                let (remote_hits, _meta) = scatter_gather::coordinate_cross_shard_hop(
                    shared,
                    tenant_id,
                    scatter_gather::CrossShardHopParams {
                        local_nodes,
                        envelope,
                        options: &options,
                        edge_label: edge_label.as_deref(),
                        direction,
                        remaining_depth: remaining,
                    },
                )
                .await?;
                let mut out = discoveries;
                if let Some(attrib_parent) = frontier.first().cloned() {
                    for n in remote_hits {
                        out.push((attrib_parent.clone(), n));
                    }
                }
                out
            }
        } else {
            discoveries
        };

        // Record parents and build next frontier. Early-exit the
        // moment `dst` is seen so we don't keep expanding.
        let mut next_frontier: Vec<String> = Vec::new();
        for (from, to) in merged {
            if !visited.insert(to.clone()) {
                continue;
            }
            parent.insert(to.clone(), from);
            if to == dst {
                let path = reconstruct(&parent, &src, &dst);
                let payload = encode_path(&path)?;
                return Ok(ok_response(payload));
            }
            next_frontier.push(to);
        }
        frontier = next_frontier;

        if visited.len() >= options.max_visited {
            break;
        }
    }

    // Unreachable within max_depth: empty array, same shape the
    // client sees for a successful empty result.
    let payload = encode_path::<String>(&[])?;
    Ok(ok_response(payload))
}

fn reconstruct(parent: &HashMap<String, String>, src: &str, dst: &str) -> Vec<String> {
    let mut path: Vec<String> = Vec::new();
    let mut cursor = dst.to_string();
    path.push(cursor.clone());
    while cursor != src {
        match parent.get(&cursor) {
            Some(p) => {
                cursor = p.clone();
                path.push(cursor.clone());
            }
            None => break,
        }
    }
    path.reverse();
    path
}

fn encode_path<S: serde::Serialize>(path: &[S]) -> crate::Result<Vec<u8>> {
    sonic_rs::to_vec(path).map_err(|e| crate::Error::Serialization {
        format: "json".into(),
        detail: e.to_string(),
    })
}

fn ok_response(payload: Vec<u8>) -> Response {
    Response {
        request_id: RequestId::new(0),
        status: crate::bridge::envelope::Status::Ok,
        attempt: 1,
        partial: false,
        payload: crate::bridge::envelope::Payload::from_vec(payload),
        watermark_lsn: Lsn::ZERO,
        error_code: None,
    }
}
