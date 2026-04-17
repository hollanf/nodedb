//! Graph operation handlers: EdgePut, EdgeDelete, GraphHop, GraphNeighbors,
//! GraphPath, GraphSubgraph.

use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, Response};

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::scoping::{scoped_node, unscoped};
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    pub(in crate::data::executor) fn execute_edge_put(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        src_id: &str,
        label: &str,
        dst_id: &str,
        properties: &[u8],
    ) -> Response {
        let scoped_src = scoped_node(tid, src_id);
        let scoped_dst = scoped_node(tid, dst_id);
        debug!(core = self.core_id, %scoped_src, %label, %scoped_dst, "edge put");

        if self.deleted_nodes.contains(&scoped_src) {
            return self.response_error(
                task,
                ErrorCode::RejectedDanglingEdge {
                    missing_node: src_id.to_string(),
                },
            );
        }
        if self.deleted_nodes.contains(&scoped_dst) {
            return self.response_error(
                task,
                ErrorCode::RejectedDanglingEdge {
                    missing_node: dst_id.to_string(),
                },
            );
        }

        match self
            .edge_store
            .put_edge(&scoped_src, label, &scoped_dst, properties)
        {
            Ok(()) => {
                let weight = crate::engine::graph::csr::extract_weight_from_properties(properties);
                let csr_result = if weight != 1.0 {
                    self.csr
                        .add_edge_weighted(&scoped_src, label, &scoped_dst, weight)
                } else {
                    self.csr.add_edge(&scoped_src, label, &scoped_dst)
                };
                match csr_result {
                    Ok(()) => {
                        self.checkpoint_coordinator.mark_dirty("sparse", 1);
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
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// Apply a batched edge insert in a single SPSC round-trip.
    ///
    /// Iterates `edges` applying the same dangling-node / WAL / CSR
    /// invariants as `execute_edge_put`. On first failure returns an
    /// error response with the index of the offending edge in `detail`,
    /// so the Control Plane can revert the prefix that succeeded. No
    /// `Err` is ever logged-and-swallowed; the DDL path requires the
    /// full batch to succeed or fails the whole operation.
    pub(in crate::data::executor) fn execute_edge_put_batch(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        edges: &[crate::bridge::physical_plan::BatchEdge],
    ) -> Response {
        debug!(core = self.core_id, count = edges.len(), "edge put batch");
        for (idx, edge) in edges.iter().enumerate() {
            let scoped_src = scoped_node(tid, &edge.src_id);
            let scoped_dst = scoped_node(tid, &edge.dst_id);
            if self.deleted_nodes.contains(&scoped_src) {
                return self.response_error(
                    task,
                    ErrorCode::RejectedDanglingEdge {
                        missing_node: edge.src_id.clone(),
                    },
                );
            }
            if self.deleted_nodes.contains(&scoped_dst) {
                return self.response_error(
                    task,
                    ErrorCode::RejectedDanglingEdge {
                        missing_node: edge.dst_id.clone(),
                    },
                );
            }
            match self
                .edge_store
                .put_edge(&scoped_src, &edge.label, &scoped_dst, &[])
            {
                Ok(()) => {
                    if let Err(e) = self.csr.add_edge(&scoped_src, &edge.label, &scoped_dst) {
                        return self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: format!("edge {idx} (label interning): {e}"),
                            },
                        );
                    }
                }
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("edge {idx}: {e}"),
                        },
                    );
                }
            }
        }
        if !edges.is_empty() {
            self.checkpoint_coordinator
                .mark_dirty("sparse", edges.len());
        }
        self.response_ok(task)
    }

    /// Apply a batched edge delete in a single SPSC round-trip. Used by
    /// `CREATE GRAPH INDEX` rollback when a later batch fails.
    pub(in crate::data::executor) fn execute_edge_delete_batch(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        edges: &[crate::bridge::physical_plan::BatchEdge],
    ) -> Response {
        debug!(core = self.core_id, count = edges.len(), "edge delete batch");
        for edge in edges {
            let scoped_src = scoped_node(tid, &edge.src_id);
            let scoped_dst = scoped_node(tid, &edge.dst_id);
            let _ = self
                .edge_store
                .delete_edge(&scoped_src, &edge.label, &scoped_dst);
            self.csr.remove_edge(&scoped_src, &edge.label, &scoped_dst);
        }
        if !edges.is_empty() {
            self.checkpoint_coordinator
                .mark_dirty("sparse", edges.len());
        }
        self.response_ok(task)
    }

    pub(in crate::data::executor) fn execute_edge_delete(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        src_id: &str,
        label: &str,
        dst_id: &str,
    ) -> Response {
        let scoped_src = scoped_node(tid, src_id);
        let scoped_dst = scoped_node(tid, dst_id);
        debug!(core = self.core_id, %scoped_src, %label, %scoped_dst, "edge delete");
        match self.edge_store.delete_edge(&scoped_src, label, &scoped_dst) {
            Ok(_) => {
                self.csr.remove_edge(&scoped_src, label, &scoped_dst);
                self.checkpoint_coordinator.mark_dirty("sparse", 1);
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

    pub(in crate::data::executor) fn execute_graph_hop(
        &self,
        task: &ExecutionTask,
        tid: u32,
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
        let depth = depth.min(crate::engine::graph::traversal_options::MAX_GRAPH_TRAVERSAL_DEPTH);
        let scoped: Vec<String> = start_nodes.iter().map(|n| scoped_node(tid, n)).collect();
        let refs: Vec<&str> = scoped.iter().map(String::as_str).collect();
        let result = self.csr.traverse_bfs(
            &refs,
            edge_label.as_deref(),
            direction,
            depth,
            self.graph_tuning.max_visited,
        );
        // Unscope node_ids in BFS result before returning to client.
        let unscoped: Vec<String> = result.iter().map(|n| unscoped(n).to_string()).collect();
        if let Some(ref m) = self.metrics {
            m.record_graph_traversal();
        }
        match super::super::response_codec::encode(&unscoped) {
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

    pub(in crate::data::executor) fn execute_graph_neighbors(
        &self,
        task: &ExecutionTask,
        tid: u32,
        node_id: &str,
        edge_label: &Option<String>,
        direction: crate::engine::graph::edge_store::Direction,
    ) -> Response {
        let scoped_id = scoped_node(tid, node_id);
        debug!(core = self.core_id, %scoped_id, ?edge_label, ?direction, "graph neighbors");
        let neighbors = self
            .csr
            .neighbors(&scoped_id, edge_label.as_deref(), direction);
        let result: Vec<_> = neighbors
            .iter()
            .map(
                |(label, node)| super::super::response_codec::NeighborEntry {
                    label: label.as_str(),
                    node: unscoped(node),
                },
            )
            .collect();
        if let Some(ref m) = self.metrics {
            m.record_graph_traversal();
        }
        match super::super::response_codec::encode(&result) {
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

    pub(in crate::data::executor) fn execute_graph_neighbors_multi(
        &self,
        task: &ExecutionTask,
        tid: u32,
        node_ids: &[String],
        edge_label: &Option<String>,
        direction: crate::engine::graph::edge_store::Direction,
        max_results: u32,
    ) -> Response {
        debug!(
            core = self.core_id,
            count = node_ids.len(),
            ?edge_label,
            ?direction,
            max_results,
            "graph neighbors multi"
        );
        // `max_results == 0` means "unbounded" — treat as usize::MAX so
        // the cap check below becomes a no-op without branching inside
        // the hot loop.
        let cap: usize = if max_results == 0 {
            usize::MAX
        } else {
            max_results as usize
        };
        // Collect owned `(src, label, node)` triples so the encoder can
        // borrow from stable storage.
        let mut owned: Vec<(String, String, String)> =
            Vec::with_capacity(node_ids.len().min(cap) * 4);
        let mut truncated = false;
        'outer: for raw_src in node_ids {
            let scoped_src = scoped_node(tid, raw_src);
            let neighbors = self
                .csr
                .neighbors(&scoped_src, edge_label.as_deref(), direction);
            for (label, node) in neighbors {
                if owned.len() >= cap {
                    truncated = true;
                    break 'outer;
                }
                owned.push((raw_src.clone(), label, unscoped(&node).to_string()));
            }
        }
        let entries: Vec<super::super::response_codec::NeighborMultiEntry> = owned
            .iter()
            .map(
                |(src, label, node)| super::super::response_codec::NeighborMultiEntry {
                    src: src.as_str(),
                    label: label.as_str(),
                    node: node.as_str(),
                },
            )
            .collect();
        if let Some(ref m) = self.metrics {
            m.record_graph_traversal();
        }
        match super::super::response_codec::encode(&entries) {
            Ok(payload) => {
                if truncated {
                    // Per-RPC cap fired — surface via the `partial`
                    // envelope flag so the BFS orchestrator knows this
                    // hop was cut short and can propagate the condition
                    // to the client.
                    self.response_partial(task, payload)
                } else {
                    self.response_with_payload(task, payload)
                }
            }
            Err(e) => {
                warn!(
                    core = self.core_id,
                    error = %e,
                    "graph neighbors-multi serialization failed"
                );
                self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                )
            }
        }
    }

    pub(in crate::data::executor) fn execute_graph_path(
        &self,
        task: &ExecutionTask,
        tid: u32,
        src: &str,
        dst: &str,
        edge_label: &Option<String>,
        max_depth: usize,
    ) -> Response {
        let max_depth =
            max_depth.min(crate::engine::graph::traversal_options::MAX_GRAPH_TRAVERSAL_DEPTH);
        let scoped_src = scoped_node(tid, src);
        let scoped_dst = scoped_node(tid, dst);
        debug!(core = self.core_id, %scoped_src, %scoped_dst, ?edge_label, max_depth, "graph path");
        match self.csr.shortest_path(
            &scoped_src,
            &scoped_dst,
            edge_label.as_deref(),
            max_depth,
            self.graph_tuning.max_visited,
        ) {
            Some(path) => {
                let unscoped: Vec<String> = path.iter().map(|n| unscoped(n).to_string()).collect();
                if let Some(ref m) = self.metrics {
                    m.record_graph_traversal();
                }
                match super::super::response_codec::encode(&unscoped) {
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
                }
            }
            None => self.response_error(task, ErrorCode::NotFound),
        }
    }

    pub(in crate::data::executor) fn execute_graph_subgraph(
        &self,
        task: &ExecutionTask,
        tid: u32,
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
        let depth = depth.min(crate::engine::graph::traversal_options::MAX_GRAPH_TRAVERSAL_DEPTH);
        let scoped: Vec<String> = start_nodes.iter().map(|n| scoped_node(tid, n)).collect();
        let refs: Vec<&str> = scoped.iter().map(String::as_str).collect();
        let edges = self.csr.subgraph(
            &refs,
            edge_label.as_deref(),
            depth,
            self.graph_tuning.max_visited,
        );
        let result: Vec<_> = edges
            .iter()
            .map(|(s, l, d)| super::super::response_codec::SubgraphEdge {
                src: unscoped(s),
                label: l.as_str(),
                dst: unscoped(d),
            })
            .collect();
        if let Some(ref m) = self.metrics {
            m.record_graph_traversal();
        }
        match super::super::response_codec::encode(&result) {
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
