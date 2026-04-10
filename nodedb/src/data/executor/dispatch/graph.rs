//! Graph operation dispatch.

use crate::bridge::envelope::Response;
use crate::bridge::physical_plan::GraphOp;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    pub(super) fn dispatch_graph(&mut self, task: &ExecutionTask, op: &GraphOp) -> Response {
        let tid = task.request.tenant_id.as_u32();
        match op {
            GraphOp::EdgePut {
                src_id,
                label,
                dst_id,
                properties,
            } => self.execute_edge_put(task, tid, src_id, label, dst_id, properties),

            GraphOp::EdgeDelete {
                src_id,
                label,
                dst_id,
            } => self.execute_edge_delete(task, tid, src_id, label, dst_id),

            GraphOp::Hop {
                start_nodes,
                edge_label,
                direction,
                depth,
                options: _,
                rls_filters: _,
            } => self.execute_graph_hop(task, tid, start_nodes, edge_label, *direction, *depth),

            GraphOp::Neighbors {
                node_id,
                edge_label,
                direction,
                rls_filters: _,
            } => self.execute_graph_neighbors(task, tid, node_id, edge_label, *direction),

            GraphOp::Path {
                src,
                dst,
                edge_label,
                max_depth,
                options: _,
                rls_filters: _,
            } => self.execute_graph_path(task, tid, src, dst, edge_label, *max_depth),

            GraphOp::Subgraph {
                start_nodes,
                edge_label,
                depth,
                options: _,
                rls_filters: _,
            } => self.execute_graph_subgraph(task, tid, start_nodes, edge_label, *depth),

            GraphOp::RagFusion {
                collection,
                query_vector,
                vector_top_k,
                edge_label,
                direction,
                expansion_depth,
                final_top_k,
                rrf_k,
                options,
            } => self.execute_graph_rag_fusion(
                task,
                tid,
                collection,
                query_vector,
                *vector_top_k,
                edge_label,
                *direction,
                *expansion_depth,
                *final_top_k,
                *rrf_k,
                options.max_visited,
            ),

            GraphOp::Algo { algorithm, params } => {
                self.execute_graph_algo(task, tid, algorithm, params)
            }

            GraphOp::Match { query } => self.execute_graph_match(task, query),

            GraphOp::SetNodeLabels { node_id, labels } => {
                let scoped = super::super::scoping::scoped_node(tid, node_id);
                for label in labels {
                    self.csr.add_node_label(&scoped, label);
                }
                self.response_ok(task)
            }

            GraphOp::RemoveNodeLabels { node_id, labels } => {
                let scoped = super::super::scoping::scoped_node(tid, node_id);
                for label in labels {
                    self.csr.remove_node_label(&scoped, label);
                }
                self.response_ok(task)
            }
        }
    }
}
