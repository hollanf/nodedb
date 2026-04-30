//! Response parsing helpers for native protocol results.

use std::collections::HashMap;

use nodedb_types::error::NodeDbResult;
use nodedb_types::id::{EdgeId, NodeId};
use nodedb_types::result::{SearchResult, SubGraph, SubGraphEdge, SubGraphNode};

/// Parse search results from a native response.
pub(crate) fn parse_search_results(
    resp: &nodedb_types::protocol::NativeResponse,
) -> NodeDbResult<Vec<SearchResult>> {
    let rows = match &resp.rows {
        Some(r) => r,
        None => return Ok(Vec::new()),
    };

    let mut results = Vec::new();
    for row in rows {
        if let Some(text) = row.first().and_then(|v| v.as_str()) {
            if let Ok(items) = serde_json::from_str::<Vec<serde_json::Value>>(text) {
                for item in items {
                    if let Some(sr) = parse_single_search_result(&item) {
                        results.push(sr);
                    }
                }
            } else if let Ok(item) = serde_json::from_str::<serde_json::Value>(text)
                && let Some(sr) = parse_single_search_result(&item)
            {
                results.push(sr);
            }
        }
    }
    Ok(results)
}

fn parse_single_search_result(v: &serde_json::Value) -> Option<SearchResult> {
    let id = v.get("id")?.as_str()?.to_string();
    let distance = v.get("distance")?.as_f64()? as f32;
    Some(SearchResult {
        id,
        node_id: None,
        distance,
        metadata: HashMap::new(),
    })
}

// Re-export for use by client.rs
pub(crate) use nodedb_types::conversion::json_to_value;

/// Parse a graph traversal response into a SubGraph.
pub(crate) fn parse_subgraph_response(
    resp: &nodedb_types::protocol::NativeResponse,
) -> NodeDbResult<SubGraph> {
    let rows = match &resp.rows {
        Some(r) => r,
        None => return Ok(SubGraph::empty()),
    };

    let mut nodes = Vec::new();
    let mut edges = Vec::new();

    for row in rows {
        let text = match row.first().and_then(|v| v.as_str()) {
            Some(t) => t,
            None => continue,
        };

        if let Ok(val) = serde_json::from_str::<serde_json::Value>(text) {
            if let Some(obj) = val.as_object() {
                if let Some(ns) = obj.get("nodes").and_then(|v| v.as_array()) {
                    for n in ns {
                        if let Some(id) = n.get("id").and_then(|v| v.as_str()) {
                            let depth = n.get("depth").and_then(|v| v.as_u64()).unwrap_or(0) as u8;
                            nodes.push(SubGraphNode {
                                id: NodeId::from_validated(id.to_owned()),
                                depth,
                                properties: HashMap::new(),
                            });
                        }
                    }
                }
                if let Some(es) = obj.get("edges").and_then(|v| v.as_array()) {
                    for e in es {
                        let from = e
                            .get("from")
                            .or_else(|| e.get("src"))
                            .and_then(|v| v.as_str())
                            .unwrap_or("");
                        let to = e
                            .get("to")
                            .or_else(|| e.get("dst"))
                            .and_then(|v| v.as_str())
                            .unwrap_or("");
                        let label = e.get("label").and_then(|v| v.as_str()).unwrap_or("");
                        edges.push(SubGraphEdge {
                            id: EdgeId::try_first(
                                NodeId::from_validated(from.to_owned()),
                                NodeId::from_validated(to.to_owned()),
                                label,
                            )
                            .expect("server wire label already validated"),
                            from: NodeId::from_validated(from.to_owned()),
                            to: NodeId::from_validated(to.to_owned()),
                            label: label.to_string(),
                            properties: HashMap::new(),
                        });
                    }
                }
            }
            if let Some(arr) = val.as_array() {
                for item in arr {
                    if let Some(id) = item.get("id").and_then(|v| v.as_str()) {
                        let depth = item.get("depth").and_then(|v| v.as_u64()).unwrap_or(0) as u8;
                        nodes.push(SubGraphNode {
                            id: NodeId::from_validated(id.to_owned()),
                            depth,
                            properties: HashMap::new(),
                        });
                    }
                }
            }
        }
    }

    Ok(SubGraph { nodes, edges })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_search_result_from_json() {
        let v = serde_json::json!({"id": "vec-1", "distance": 0.123});
        let sr =
            parse_single_search_result(&v).expect("failed to parse search result from valid JSON");
        assert_eq!(sr.id, "vec-1");
        assert!((sr.distance - 0.123).abs() < 0.001);
    }
}
