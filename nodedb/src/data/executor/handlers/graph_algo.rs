//! Graph algorithm dispatch handler.
//!
//! Routes `PhysicalPlan::GraphAlgo` to the appropriate algorithm
//! implementation in `engine::graph::algo::*`. Each algorithm runs
//! on the in-memory CSR index and returns JSON-serialized results.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::scoping::scoped_node;
use crate::data::executor::task::ExecutionTask;
use crate::engine::graph::algo::params::{AlgoParams, GraphAlgorithm};

impl CoreLoop {
    pub(in crate::data::executor) fn execute_graph_algo(
        &self,
        task: &ExecutionTask,
        tid: u32,
        algorithm: &GraphAlgorithm,
        params: &AlgoParams,
    ) -> Response {
        debug!(
            core = self.core_id,
            algorithm = algorithm.name(),
            collection = %params.collection,
            "graph algorithm dispatch"
        );

        // Validate source_node for SSSP.
        if *algorithm == GraphAlgorithm::Sssp && params.source_node.is_none() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "SSSP requires FROM '<source_node>'".into(),
                },
            );
        }

        // Scope source_node with tenant_id so it matches CSR node keys.
        // Only clone params when source_node needs to be rewritten.
        let scoped_params;
        let params = if params.source_node.is_some() {
            let mut p = params.clone();
            p.source_node = p.source_node.map(|n| scoped_node(tid, &n));
            scoped_params = p;
            &scoped_params
        } else {
            params
        };

        use crate::engine::graph::algo;

        let result: Result<Vec<u8>, crate::Error> = match algorithm {
            // ── Graphalytics (5 core algorithms) ──
            GraphAlgorithm::PageRank => algo::pagerank::run(&self.csr, params).to_msgpack(),
            GraphAlgorithm::Wcc => algo::wcc::run(&self.csr).to_msgpack(),
            GraphAlgorithm::LabelPropagation => {
                algo::label_propagation::run(&self.csr, params).to_msgpack()
            }
            GraphAlgorithm::Lcc => algo::lcc::run(
                &self.csr,
                self.graph_tuning.lcc_high_degree_threshold,
                self.graph_tuning.lcc_sample_pairs,
            )
            .to_msgpack(),
            GraphAlgorithm::Sssp => {
                algo::sssp::run(&self.csr, params).and_then(|batch| batch.to_msgpack())
            }
            // ── Centrality suite ──
            GraphAlgorithm::Betweenness => algo::betweenness::run(&self.csr, params).to_msgpack(),
            GraphAlgorithm::Closeness => algo::closeness::run(&self.csr).to_msgpack(),
            GraphAlgorithm::Harmonic => algo::harmonic::run(&self.csr).to_msgpack(),
            GraphAlgorithm::Degree => algo::degree::run(&self.csr, params).to_msgpack(),
            // ── Extended algorithms ──
            GraphAlgorithm::Louvain => algo::louvain::run(&self.csr, params).to_msgpack(),
            GraphAlgorithm::Triangles => algo::triangles::run(&self.csr, params).to_msgpack(),
            GraphAlgorithm::Diameter => algo::diameter::run(&self.csr, params).to_msgpack(),
            GraphAlgorithm::KCore => algo::kcore::run(&self.csr).to_msgpack(),
        };

        match result {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(task, ErrorCode::from(e)),
        }
    }
}
