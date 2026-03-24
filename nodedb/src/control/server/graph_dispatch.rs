//! Cross-core BFS orchestration for graph traversal.

use crate::bridge::envelope::{PhysicalPlan, Response};
use crate::control::state::SharedState;
use crate::types::{Lsn, RequestId, TenantId};

/// Cross-core BFS orchestration for graph traversal.
///
/// Implements multi-hop BFS across all Data Plane cores. Each hop:
/// 1. Broadcasts GraphNeighbors for all frontier nodes to all cores
/// 2. Collects discovered neighbors from all cores
/// 3. Builds the next frontier (nodes not yet visited)
/// 4. Repeats until max_depth or no new nodes
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
    use std::collections::HashSet;

    let mut visited: HashSet<String> = HashSet::new();
    let mut all_discovered: Vec<String> = Vec::new();
    let mut frontier: Vec<String> = start_nodes.clone();

    for node in &start_nodes {
        visited.insert(node.clone());
        all_discovered.push(node.clone());
    }

    for _depth in 0..max_depth {
        if frontier.is_empty() {
            break;
        }

        let mut next_frontier: Vec<String> = Vec::new();

        for node in &frontier {
            let plan = PhysicalPlan::GraphNeighbors {
                node_id: node.clone(),
                edge_label: edge_label.clone(),
                direction,
            };

            let resp = super::broadcast::broadcast_to_all_cores(shared, tenant_id, plan, 0).await?;

            if !resp.payload.is_empty() {
                let json_text =
                    crate::data::executor::response_codec::decode_payload_to_json(&resp.payload);
                if let Ok(arr) = serde_json::from_str::<Vec<serde_json::Value>>(&json_text) {
                    for item in arr {
                        if let Some(neighbor) = item.get("node").and_then(|v| v.as_str())
                            && visited.insert(neighbor.to_string())
                        {
                            next_frontier.push(neighbor.to_string());
                            all_discovered.push(neighbor.to_string());
                        }
                    }
                }
            }
        }

        frontier = next_frontier;
    }

    let payload = serde_json::to_vec(&all_discovered).unwrap_or_else(|_| b"[]".to_vec());

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
