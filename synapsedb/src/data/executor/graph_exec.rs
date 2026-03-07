//! Graph engine execution handlers for `CoreLoop`.
//!
//! Separated from `core_loop.rs` to keep per-file line counts within limits.
//! Each method handles one `PhysicalPlan` graph variant.

use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, Response};

use super::core_loop::CoreLoop;
use super::task::ExecutionTask;

impl CoreLoop {
    pub(super) fn execute_edge_put(
        &mut self,
        task: &ExecutionTask,
        src_id: &str,
        label: &str,
        dst_id: &str,
        properties: &[u8],
    ) -> Response {
        debug!(core = self.core_id, %src_id, %label, %dst_id, "edge put");
        match self.edge_store.put_edge(src_id, label, dst_id, properties) {
            Ok(()) => {
                self.csr.add_edge(src_id, label, dst_id);
                self.response_ok(task)
            }
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    pub(super) fn execute_edge_delete(
        &mut self,
        task: &ExecutionTask,
        src_id: &str,
        label: &str,
        dst_id: &str,
    ) -> Response {
        debug!(core = self.core_id, %src_id, %label, %dst_id, "edge delete");
        match self.edge_store.delete_edge(src_id, label, dst_id) {
            Ok(_) => {
                self.csr.remove_edge(src_id, label, dst_id);
                self.response_ok(task)
            }
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    pub(super) fn execute_graph_hop(
        &self,
        task: &ExecutionTask,
        start_nodes: &[String],
        edge_label: &Option<String>,
        direction: crate::engine::graph::edge_store::Direction,
        depth: usize,
    ) -> Response {
        debug!(
            core = self.core_id,
            ?start_nodes,
            ?edge_label,
            ?direction,
            depth,
            "graph hop"
        );
        let refs: Vec<&str> = start_nodes.iter().map(String::as_str).collect();
        let result = self
            .csr
            .traverse_bfs(&refs, edge_label.as_deref(), direction, depth);
        match serde_json::to_vec(&result) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => {
                warn!(core = self.core_id, error = %e, "graph hop serialization failed");
                self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                )
            }
        }
    }

    pub(super) fn execute_graph_neighbors(
        &self,
        task: &ExecutionTask,
        node_id: &str,
        edge_label: &Option<String>,
        direction: crate::engine::graph::edge_store::Direction,
    ) -> Response {
        debug!(core = self.core_id, %node_id, ?edge_label, ?direction, "graph neighbors");
        let neighbors = self
            .csr
            .neighbors(node_id, edge_label.as_deref(), direction);
        let result: Vec<serde_json::Value> = neighbors
            .iter()
            .map(|(label, node)| serde_json::json!({"label": label, "node": node}))
            .collect();
        match serde_json::to_vec(&result) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => {
                warn!(core = self.core_id, error = %e, "graph neighbors serialization failed");
                self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                )
            }
        }
    }

    pub(super) fn execute_graph_path(
        &self,
        task: &ExecutionTask,
        src: &str,
        dst: &str,
        edge_label: &Option<String>,
        max_depth: usize,
    ) -> Response {
        debug!(core = self.core_id, %src, %dst, ?edge_label, max_depth, "graph path");
        match self
            .csr
            .shortest_path(src, dst, edge_label.as_deref(), max_depth)
        {
            Some(path) => match serde_json::to_vec(&path) {
                Ok(payload) => self.response_with_payload(task, payload),
                Err(e) => {
                    warn!(core = self.core_id, error = %e, "graph path serialization failed");
                    self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    )
                }
            },
            None => self.response_error(task, ErrorCode::NotFound),
        }
    }

    pub(super) fn execute_graph_subgraph(
        &self,
        task: &ExecutionTask,
        start_nodes: &[String],
        edge_label: &Option<String>,
        depth: usize,
    ) -> Response {
        debug!(
            core = self.core_id,
            ?start_nodes,
            ?edge_label,
            depth,
            "graph subgraph"
        );
        let refs: Vec<&str> = start_nodes.iter().map(String::as_str).collect();
        let edges = self.csr.subgraph(&refs, edge_label.as_deref(), depth);
        let result: Vec<serde_json::Value> = edges
            .iter()
            .map(|(s, l, d)| serde_json::json!({"src": s, "label": l, "dst": d}))
            .collect();
        match serde_json::to_vec(&result) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => {
                warn!(core = self.core_id, error = %e, "graph subgraph serialization failed");
                self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                )
            }
        }
    }
}
