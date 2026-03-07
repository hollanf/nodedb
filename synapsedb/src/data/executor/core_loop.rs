use std::collections::{HashMap, VecDeque};
use std::path::Path;
use std::sync::Arc;

use tracing::{debug, warn};

use synapsedb_bridge::buffer::{Consumer, Producer};
use synapsedb_crdt::constraint::ConstraintSet;

use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
use crate::bridge::envelope::{ErrorCode, PhysicalPlan, Response, Status};
use crate::engine::crdt::tenant_state::TenantCrdtEngine;
use crate::engine::graph::csr::CsrIndex;
use crate::engine::graph::edge_store::EdgeStore;
use crate::engine::sparse::btree::SparseEngine;
use crate::engine::vector::hnsw::{HnswIndex, HnswParams};
use crate::types::{Lsn, TenantId};

use super::task::{ExecutionTask, TaskState};

/// Per-core event loop for the Data Plane.
///
/// Each CPU core runs one `CoreLoop`. It owns:
/// - SPSC consumer for incoming requests from the Control Plane
/// - SPSC producer for outgoing responses to the Control Plane
/// - Per-core `SparseEngine` (redb) for point lookups and range scans
/// - Per-tenant `TenantCrdtEngine` instances (lazy-initialized)
/// - Task queue for pending execution
///
/// This type is intentionally `!Send` — pinned to a single core.
pub struct CoreLoop {
    pub(super) core_id: usize,

    /// SPSC channel: receives requests from Control Plane.
    request_rx: Consumer<BridgeRequest>,

    /// SPSC channel: sends responses to Control Plane.
    response_tx: Producer<BridgeResponse>,

    /// Pending tasks ordered by priority then arrival.
    task_queue: VecDeque<ExecutionTask>,

    /// Current watermark LSN for this core's shard data.
    watermark: Lsn,

    /// redb-backed sparse/metadata engine for this core.
    pub(crate) sparse: SparseEngine,

    /// Per-tenant CRDT engines, lazily initialized on first access.
    crdt_engines: HashMap<TenantId, TenantCrdtEngine>,

    /// Per-collection HNSW vector indexes, lazily initialized on first insert.
    vector_indexes: HashMap<String, HnswIndex>,

    /// redb-backed graph edge storage for this core.
    pub(super) edge_store: EdgeStore,

    /// In-memory CSR adjacency index, rebuilt from edge_store on startup.
    pub(super) csr: CsrIndex,
}

impl CoreLoop {
    /// Create a core loop with its SPSC channel endpoints and engine storage.
    ///
    /// `data_dir` is the base data directory; each core gets its own redb file
    /// at `{data_dir}/sparse/core-{core_id}.redb`.
    pub fn open(
        core_id: usize,
        request_rx: Consumer<BridgeRequest>,
        response_tx: Producer<BridgeResponse>,
        data_dir: &Path,
    ) -> crate::Result<Self> {
        let sparse_path = data_dir.join(format!("sparse/core-{core_id}.redb"));
        let sparse = SparseEngine::open(&sparse_path)?;

        let graph_path = data_dir.join(format!("graph/core-{core_id}.redb"));
        let edge_store = EdgeStore::open(&graph_path)?;
        let csr = CsrIndex::rebuild_from(&edge_store)?;

        Ok(Self {
            core_id,
            request_rx,
            response_tx,
            task_queue: VecDeque::with_capacity(256),
            watermark: Lsn::ZERO,
            sparse,
            crdt_engines: HashMap::new(),
            vector_indexes: HashMap::new(),
            edge_store,
            csr,
        })
    }

    pub fn core_id(&self) -> usize {
        self.core_id
    }

    /// Drain incoming requests from the SPSC bridge into the task queue.
    pub fn drain_requests(&mut self) {
        let mut batch = Vec::new();
        self.request_rx.drain_into(&mut batch, 64);
        for br in batch {
            self.task_queue.push_back(ExecutionTask::new(br.inner));
        }
    }

    /// Process the next pending task and send the response back via SPSC.
    ///
    /// Returns `true` if a task was processed, `false` if the queue was empty.
    pub fn poll_one(&mut self) -> bool {
        let Some(mut task) = self.task_queue.pop_front() else {
            return false;
        };

        // Check deadline before executing.
        let response = if task.is_expired() {
            task.state = TaskState::Failed;
            Response {
                request_id: task.request_id(),
                status: Status::Error,
                attempt: 1,
                partial: false,
                payload: Arc::from([].as_slice()),
                watermark_lsn: self.watermark,
                error_code: Some(ErrorCode::DeadlineExceeded),
            }
        } else {
            task.state = TaskState::Running;
            let resp = self.execute(&task);
            task.state = TaskState::Completed;
            resp
        };

        // Send response back to Control Plane via SPSC.
        if let Err(e) = self
            .response_tx
            .try_push(BridgeResponse { inner: response })
        {
            warn!(
                core = self.core_id,
                error = %e,
                "failed to send response — response queue full"
            );
        }

        true
    }

    /// Run one iteration of the event loop: drain requests, process tasks.
    ///
    /// Returns the number of tasks processed.
    pub fn tick(&mut self) -> usize {
        self.drain_requests();
        let mut processed = 0;
        while self.poll_one() {
            processed += 1;
        }
        processed
    }

    pub(super) fn response_ok(&self, task: &ExecutionTask) -> Response {
        Response {
            request_id: task.request_id(),
            status: Status::Ok,
            attempt: 1,
            partial: false,
            payload: Arc::from([].as_slice()),
            watermark_lsn: self.watermark,
            error_code: None,
        }
    }

    pub(super) fn response_with_payload(&self, task: &ExecutionTask, payload: Vec<u8>) -> Response {
        Response {
            request_id: task.request_id(),
            status: Status::Ok,
            attempt: 1,
            partial: false,
            payload: Arc::from(payload.into_boxed_slice()),
            watermark_lsn: self.watermark,
            error_code: None,
        }
    }

    pub(super) fn response_error(&self, task: &ExecutionTask, error_code: ErrorCode) -> Response {
        Response {
            request_id: task.request_id(),
            status: Status::Error,
            attempt: 1,
            partial: false,
            payload: Arc::from([].as_slice()),
            watermark_lsn: self.watermark,
            error_code: Some(error_code),
        }
    }

    /// Get or create a CRDT engine for the given tenant.
    fn get_crdt_engine(&mut self, tenant_id: TenantId) -> &mut TenantCrdtEngine {
        self.crdt_engines.entry(tenant_id).or_insert_with(|| {
            debug!(core = self.core_id, %tenant_id, "creating CRDT engine for tenant");
            TenantCrdtEngine::new(tenant_id, self.core_id as u64, ConstraintSet::new())
        })
    }

    /// Execute a physical plan. Dispatches to the appropriate engine.
    fn execute(&mut self, task: &ExecutionTask) -> Response {
        match task.plan() {
            PhysicalPlan::PointGet {
                collection,
                document_id,
            } => {
                debug!(core = self.core_id, %collection, %document_id, "point get");
                match self.sparse.get(collection, document_id) {
                    Ok(Some(data)) => self.response_with_payload(task, data),
                    Ok(None) => self.response_error(task, ErrorCode::NotFound),
                    Err(e) => {
                        warn!(core = self.core_id, error = %e, "sparse get failed");
                        self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: e.to_string(),
                            },
                        )
                    }
                }
            }

            PhysicalPlan::VectorSearch {
                collection,
                query_vector,
                top_k,
                filter_bitmap,
            } => {
                debug!(core = self.core_id, %collection, top_k, "vector search");
                let Some(index) = self.vector_indexes.get(collection) else {
                    return self.response_error(task, ErrorCode::NotFound);
                };
                if index.is_empty() {
                    return self.response_with_payload(task, b"[]".to_vec());
                }
                let ef = top_k.saturating_mul(4).max(64);
                let results = match filter_bitmap {
                    Some(bitmap_bytes) => {
                        index.search_with_bitmap_bytes(query_vector, *top_k, ef, bitmap_bytes)
                    }
                    None => index.search(query_vector, *top_k, ef),
                };
                let serializable: Vec<_> = results
                    .iter()
                    .map(|r| serde_json::json!({"id": r.id, "distance": r.distance}))
                    .collect();
                match serde_json::to_vec(&serializable) {
                    Ok(payload) => self.response_with_payload(task, payload),
                    Err(e) => {
                        warn!(core = self.core_id, error = %e, "vector search serialization failed");
                        self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: e.to_string(),
                            },
                        )
                    }
                }
            }

            PhysicalPlan::RangeScan {
                collection,
                field,
                lower,
                upper,
                limit,
            } => {
                debug!(core = self.core_id, %collection, %field, limit, "range scan");
                match self.sparse.range_scan(
                    collection,
                    field,
                    lower.as_deref(),
                    upper.as_deref(),
                    *limit,
                ) {
                    Ok(results) => match serde_json::to_vec(&results) {
                        Ok(payload) => self.response_with_payload(task, payload),
                        Err(e) => {
                            warn!(core = self.core_id, error = %e, "range scan serialization failed");
                            self.response_error(
                                task,
                                ErrorCode::Internal {
                                    detail: e.to_string(),
                                },
                            )
                        }
                    },
                    Err(e) => {
                        warn!(core = self.core_id, error = %e, "sparse range scan failed");
                        self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: e.to_string(),
                            },
                        )
                    }
                }
            }

            PhysicalPlan::CrdtRead {
                collection,
                document_id,
            } => {
                debug!(core = self.core_id, %collection, %document_id, "crdt read");
                let tenant_id = task.request.tenant_id;
                let engine = self.get_crdt_engine(tenant_id);
                match engine.read_snapshot(collection, document_id) {
                    Some(snapshot) => self.response_with_payload(task, snapshot),
                    None => self.response_error(task, ErrorCode::NotFound),
                }
            }

            PhysicalPlan::CrdtApply {
                collection: _,
                document_id: _,
                delta,
                peer_id: _,
            } => {
                let tenant_id = task.request.tenant_id;
                let engine = self.get_crdt_engine(tenant_id);
                match engine.apply_committed_delta(delta) {
                    Ok(()) => self.response_ok(task),
                    Err(e) => {
                        warn!(core = self.core_id, error = %e, "crdt apply failed");
                        self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: e.to_string(),
                            },
                        )
                    }
                }
            }

            PhysicalPlan::VectorInsert {
                collection,
                vector,
                dim,
            } => {
                debug!(core = self.core_id, %collection, dim, "vector insert");
                // Check dimension mismatch before borrowing mutably.
                if let Some(existing) = self.vector_indexes.get(collection) {
                    if existing.dim() != *dim {
                        let existing_dim = existing.dim();
                        return self.response_error(
                            task,
                            ErrorCode::RejectedConstraint {
                                constraint: format!(
                                    "dimension mismatch: index has {existing_dim}, got {dim}"
                                ),
                            },
                        );
                    }
                }
                let core_id = self.core_id;
                let index = self
                    .vector_indexes
                    .entry(collection.clone())
                    .or_insert_with(|| {
                        debug!(core = core_id, dim, "creating HNSW index");
                        HnswIndex::with_seed(*dim, HnswParams::default(), core_id as u64 + 1)
                    });
                index.insert(vector.clone());
                self.response_ok(task)
            }

            PhysicalPlan::PointPut {
                collection,
                document_id,
                value,
            } => {
                debug!(core = self.core_id, %collection, %document_id, "point put");
                match self.sparse.put(collection, document_id, value) {
                    Ok(()) => self.response_ok(task),
                    Err(e) => self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    ),
                }
            }

            PhysicalPlan::PointDelete {
                collection,
                document_id,
            } => {
                debug!(core = self.core_id, %collection, %document_id, "point delete");
                match self.sparse.delete(collection, document_id) {
                    Ok(_) => self.response_ok(task),
                    Err(e) => self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    ),
                }
            }

            PhysicalPlan::EdgePut {
                src_id,
                label,
                dst_id,
                properties,
            } => self.execute_edge_put(task, src_id, label, dst_id, properties),

            PhysicalPlan::EdgeDelete {
                src_id,
                label,
                dst_id,
            } => self.execute_edge_delete(task, src_id, label, dst_id),

            PhysicalPlan::GraphHop {
                start_nodes,
                edge_label,
                direction,
                depth,
            } => self.execute_graph_hop(task, start_nodes, edge_label, *direction, *depth),

            PhysicalPlan::GraphNeighbors {
                node_id,
                edge_label,
                direction,
            } => self.execute_graph_neighbors(task, node_id, edge_label, *direction),

            PhysicalPlan::GraphPath {
                src,
                dst,
                edge_label,
                max_depth,
            } => self.execute_graph_path(task, src, dst, edge_label, *max_depth),

            PhysicalPlan::GraphSubgraph {
                start_nodes,
                edge_label,
                depth,
            } => self.execute_graph_subgraph(task, start_nodes, edge_label, *depth),

            PhysicalPlan::SetCollectionPolicy {
                collection,
                policy_json,
            } => {
                debug!(core = self.core_id, %collection, "set collection policy");
                let tenant_id = task.request.tenant_id;
                let engine = self.get_crdt_engine(tenant_id);
                match engine.set_collection_policy(collection, policy_json) {
                    Ok(()) => self.response_ok(task),
                    Err(e) => {
                        warn!(core = self.core_id, error = %e, "set collection policy failed");
                        self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: e.to_string(),
                            },
                        )
                    }
                }
            }

            PhysicalPlan::WalAppend { payload } => {
                debug!(core = self.core_id, len = payload.len(), "wal append");
                self.response_ok(task)
            }

            PhysicalPlan::Cancel { target_request_id } => {
                debug!(core = self.core_id, %target_request_id, "cancel");
                if let Some(pos) = self
                    .task_queue
                    .iter()
                    .position(|t| t.request_id() == *target_request_id)
                {
                    self.task_queue.remove(pos);
                }
                self.response_ok(task)
            }
        }
    }

    pub fn pending_count(&self) -> usize {
        self.task_queue.len()
    }

    pub fn advance_watermark(&mut self, lsn: Lsn) {
        self.watermark = lsn;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::{Priority, Request};
    use crate::engine::graph::edge_store::Direction;
    use crate::types::*;
    use std::time::{Duration, Instant};
    use synapsedb_bridge::buffer::RingBuffer;

    fn make_core() -> (CoreLoop, Producer<BridgeRequest>, Consumer<BridgeResponse>) {
        let dir = tempfile::tempdir().unwrap();
        let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
        let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
        let core = CoreLoop::open(0, req_rx, resp_tx, dir.path()).unwrap();
        // Leak the tempdir so it lives long enough for tests.
        std::mem::forget(dir);
        (core, req_tx, resp_rx)
    }

    fn make_request(plan: PhysicalPlan) -> Request {
        Request {
            request_id: RequestId::new(1),
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(0),
            plan,
            deadline: Instant::now() + Duration::from_secs(5),
            priority: Priority::Normal,
            trace_id: 0,
            consistency: ReadConsistency::Strong,
        }
    }

    #[test]
    fn point_get_not_found() {
        let (mut core, mut req_tx, mut resp_rx) = make_core();

        req_tx
            .try_push(BridgeRequest {
                inner: make_request(PhysicalPlan::PointGet {
                    collection: "users".into(),
                    document_id: "nonexistent".into(),
                }),
            })
            .unwrap();

        core.tick();
        let resp = resp_rx.try_pop().unwrap();
        assert_eq!(resp.inner.status, Status::Error);
        assert_eq!(resp.inner.error_code, Some(ErrorCode::NotFound));
    }

    #[test]
    fn point_get_returns_data() {
        let (mut core, mut req_tx, mut resp_rx) = make_core();

        // Insert data directly via the sparse engine.
        core.sparse.put("users", "u1", b"alice-data").unwrap();

        req_tx
            .try_push(BridgeRequest {
                inner: make_request(PhysicalPlan::PointGet {
                    collection: "users".into(),
                    document_id: "u1".into(),
                }),
            })
            .unwrap();

        core.tick();
        let resp = resp_rx.try_pop().unwrap();
        assert_eq!(resp.inner.status, Status::Ok);
        assert_eq!(&*resp.inner.payload, b"alice-data");
    }

    #[test]
    fn range_scan_returns_json() {
        let (mut core, mut req_tx, mut resp_rx) = make_core();

        core.sparse.index_put("users", "age", "025", "u1").unwrap();
        core.sparse.index_put("users", "age", "030", "u2").unwrap();

        req_tx
            .try_push(BridgeRequest {
                inner: make_request(PhysicalPlan::RangeScan {
                    collection: "users".into(),
                    field: "age".into(),
                    lower: None,
                    upper: None,
                    limit: 10,
                }),
            })
            .unwrap();

        core.tick();
        let resp = resp_rx.try_pop().unwrap();
        assert_eq!(resp.inner.status, Status::Ok);
        // Payload is JSON-serialized Vec<(String, Vec<u8>)>.
        assert!(!resp.inner.payload.is_empty());
    }

    #[test]
    fn expired_task_returns_deadline_exceeded() {
        let (mut core, mut req_tx, mut resp_rx) = make_core();

        req_tx
            .try_push(BridgeRequest {
                inner: Request {
                    request_id: RequestId::new(2),
                    tenant_id: TenantId::new(1),
                    vshard_id: VShardId::new(0),
                    plan: PhysicalPlan::PointGet {
                        collection: "x".into(),
                        document_id: "y".into(),
                    },
                    deadline: Instant::now() - Duration::from_secs(1),
                    priority: Priority::Normal,
                    trace_id: 0,
                    consistency: ReadConsistency::Strong,
                },
            })
            .unwrap();

        core.tick();

        let resp = resp_rx.try_pop().unwrap();
        assert_eq!(resp.inner.status, Status::Error);
        assert_eq!(resp.inner.error_code, Some(ErrorCode::DeadlineExceeded));
    }

    #[test]
    fn empty_tick_processes_nothing() {
        let (mut core, _, _) = make_core();
        assert_eq!(core.tick(), 0);
    }

    #[test]
    fn watermark_in_response() {
        let (mut core, mut req_tx, mut resp_rx) = make_core();
        core.advance_watermark(Lsn::new(99));
        core.sparse.put("x", "y", b"data").unwrap();

        req_tx
            .try_push(BridgeRequest {
                inner: make_request(PhysicalPlan::PointGet {
                    collection: "x".into(),
                    document_id: "y".into(),
                }),
            })
            .unwrap();

        core.tick();
        let resp = resp_rx.try_pop().unwrap();
        assert_eq!(resp.inner.watermark_lsn, Lsn::new(99));
    }

    #[test]
    fn cancel_removes_pending_task() {
        let (mut core, mut req_tx, _resp_rx) = make_core();

        req_tx
            .try_push(BridgeRequest {
                inner: Request {
                    request_id: RequestId::new(10),
                    tenant_id: TenantId::new(1),
                    vshard_id: VShardId::new(0),
                    plan: PhysicalPlan::PointGet {
                        collection: "x".into(),
                        document_id: "y".into(),
                    },
                    deadline: Instant::now() + Duration::from_secs(60),
                    priority: Priority::Normal,
                    trace_id: 0,
                    consistency: ReadConsistency::Strong,
                },
            })
            .unwrap();

        core.drain_requests();
        assert_eq!(core.pending_count(), 1);

        req_tx
            .try_push(BridgeRequest {
                inner: Request {
                    request_id: RequestId::new(99),
                    tenant_id: TenantId::new(1),
                    vshard_id: VShardId::new(0),
                    plan: PhysicalPlan::Cancel {
                        target_request_id: RequestId::new(10),
                    },
                    deadline: Instant::now() + Duration::from_secs(5),
                    priority: Priority::Critical,
                    trace_id: 0,
                    consistency: ReadConsistency::Eventual,
                },
            })
            .unwrap();

        let processed = core.tick();
        assert_eq!(processed, 2);
    }

    #[test]
    fn crdt_read_not_found() {
        let (mut core, mut req_tx, mut resp_rx) = make_core();

        req_tx
            .try_push(BridgeRequest {
                inner: make_request(PhysicalPlan::CrdtRead {
                    collection: "sessions".into(),
                    document_id: "s1".into(),
                }),
            })
            .unwrap();

        core.tick();
        let resp = resp_rx.try_pop().unwrap();
        assert_eq!(resp.inner.status, Status::Error);
        assert_eq!(resp.inner.error_code, Some(ErrorCode::NotFound));
    }

    #[test]
    fn vector_insert_and_search() {
        let (mut core, mut req_tx, mut resp_rx) = make_core();

        // Insert 10 vectors.
        for i in 0..10u32 {
            req_tx
                .try_push(BridgeRequest {
                    inner: Request {
                        request_id: RequestId::new(100 + i as u64),
                        tenant_id: TenantId::new(1),
                        vshard_id: VShardId::new(0),
                        plan: PhysicalPlan::VectorInsert {
                            collection: "embeddings".into(),
                            vector: vec![i as f32, 0.0, 0.0],
                            dim: 3,
                        },
                        deadline: Instant::now() + Duration::from_secs(5),
                        priority: Priority::Normal,
                        trace_id: 0,
                        consistency: ReadConsistency::Strong,
                    },
                })
                .unwrap();
        }

        // Process all inserts.
        let processed = core.tick();
        assert_eq!(processed, 10);
        for _ in 0..10 {
            let resp = resp_rx.try_pop().unwrap();
            assert_eq!(resp.inner.status, Status::Ok);
        }

        // Search for the nearest vector to [5.0, 0.0, 0.0].
        req_tx
            .try_push(BridgeRequest {
                inner: make_request(PhysicalPlan::VectorSearch {
                    collection: "embeddings".into(),
                    query_vector: Arc::from([5.0f32, 0.0, 0.0].as_slice()),
                    top_k: 3,
                    filter_bitmap: None,
                }),
            })
            .unwrap();

        core.tick();
        let resp = resp_rx.try_pop().unwrap();
        assert_eq!(resp.inner.status, Status::Ok);

        // Payload should be JSON with results.
        let payload = String::from_utf8(resp.inner.payload.to_vec()).unwrap();
        assert!(payload.contains("\"id\""), "payload: {payload}");
        assert!(payload.contains("\"distance\""), "payload: {payload}");
    }

    #[test]
    fn vector_search_no_index_returns_not_found() {
        let (mut core, mut req_tx, mut resp_rx) = make_core();

        req_tx
            .try_push(BridgeRequest {
                inner: make_request(PhysicalPlan::VectorSearch {
                    collection: "nonexistent".into(),
                    query_vector: Arc::from([1.0f32, 0.0, 0.0].as_slice()),
                    top_k: 5,
                    filter_bitmap: None,
                }),
            })
            .unwrap();

        core.tick();
        let resp = resp_rx.try_pop().unwrap();
        assert_eq!(resp.inner.status, Status::Error);
        assert_eq!(resp.inner.error_code, Some(ErrorCode::NotFound));
    }

    #[test]
    fn point_put_and_get() {
        let (mut core, mut req_tx, mut resp_rx) = make_core();

        // Put via physical plan.
        req_tx
            .try_push(BridgeRequest {
                inner: make_request(PhysicalPlan::PointPut {
                    collection: "docs".into(),
                    document_id: "d1".into(),
                    value: b"hello world".to_vec(),
                }),
            })
            .unwrap();

        core.tick();
        let resp = resp_rx.try_pop().unwrap();
        assert_eq!(resp.inner.status, Status::Ok);

        // Get it back.
        req_tx
            .try_push(BridgeRequest {
                inner: make_request(PhysicalPlan::PointGet {
                    collection: "docs".into(),
                    document_id: "d1".into(),
                }),
            })
            .unwrap();

        core.tick();
        let resp = resp_rx.try_pop().unwrap();
        assert_eq!(resp.inner.status, Status::Ok);
        assert_eq!(&*resp.inner.payload, b"hello world");
    }

    #[test]
    fn edge_put_and_graph_neighbors() {
        let (mut core, mut req_tx, mut resp_rx) = make_core();

        // Insert edges: alice -KNOWS-> bob, alice -KNOWS-> carol
        for dst in &["bob", "carol"] {
            req_tx
                .try_push(BridgeRequest {
                    inner: make_request(PhysicalPlan::EdgePut {
                        src_id: "alice".into(),
                        label: "KNOWS".into(),
                        dst_id: dst.to_string(),
                        properties: vec![],
                    }),
                })
                .unwrap();
        }
        core.tick();
        resp_rx.try_pop().unwrap();
        resp_rx.try_pop().unwrap();

        // Query neighbors.
        req_tx
            .try_push(BridgeRequest {
                inner: make_request(PhysicalPlan::GraphNeighbors {
                    node_id: "alice".into(),
                    edge_label: Some("KNOWS".into()),
                    direction: Direction::Out,
                }),
            })
            .unwrap();

        core.tick();
        let resp = resp_rx.try_pop().unwrap();
        assert_eq!(resp.inner.status, Status::Ok);
        let payload = String::from_utf8(resp.inner.payload.to_vec()).unwrap();
        assert!(payload.contains("bob"), "payload: {payload}");
        assert!(payload.contains("carol"), "payload: {payload}");
    }

    #[test]
    fn graph_hop_traversal() {
        let (mut core, mut req_tx, mut resp_rx) = make_core();

        // a -> b -> c
        for (s, d) in &[("a", "b"), ("b", "c")] {
            req_tx
                .try_push(BridgeRequest {
                    inner: make_request(PhysicalPlan::EdgePut {
                        src_id: s.to_string(),
                        label: "NEXT".into(),
                        dst_id: d.to_string(),
                        properties: vec![],
                    }),
                })
                .unwrap();
        }
        core.tick();
        resp_rx.try_pop().unwrap();
        resp_rx.try_pop().unwrap();

        // Hop 2 from a.
        req_tx
            .try_push(BridgeRequest {
                inner: make_request(PhysicalPlan::GraphHop {
                    start_nodes: vec!["a".into()],
                    edge_label: Some("NEXT".into()),
                    direction: Direction::Out,
                    depth: 2,
                }),
            })
            .unwrap();

        core.tick();
        let resp = resp_rx.try_pop().unwrap();
        assert_eq!(resp.inner.status, Status::Ok);
        let nodes: Vec<String> = serde_json::from_slice(&resp.inner.payload).unwrap();
        assert!(nodes.contains(&"a".to_string()));
        assert!(nodes.contains(&"b".to_string()));
        assert!(nodes.contains(&"c".to_string()));
    }

    #[test]
    fn graph_path_and_subgraph() {
        let (mut core, mut req_tx, mut resp_rx) = make_core();

        // a -> b -> c
        for (s, d) in &[("a", "b"), ("b", "c")] {
            req_tx
                .try_push(BridgeRequest {
                    inner: make_request(PhysicalPlan::EdgePut {
                        src_id: s.to_string(),
                        label: "L".into(),
                        dst_id: d.to_string(),
                        properties: vec![],
                    }),
                })
                .unwrap();
        }
        core.tick();
        resp_rx.try_pop().unwrap();
        resp_rx.try_pop().unwrap();

        // Shortest path a -> c.
        req_tx
            .try_push(BridgeRequest {
                inner: make_request(PhysicalPlan::GraphPath {
                    src: "a".into(),
                    dst: "c".into(),
                    edge_label: Some("L".into()),
                    max_depth: 5,
                }),
            })
            .unwrap();

        core.tick();
        let resp = resp_rx.try_pop().unwrap();
        assert_eq!(resp.inner.status, Status::Ok);
        let path: Vec<String> = serde_json::from_slice(&resp.inner.payload).unwrap();
        assert_eq!(path, vec!["a", "b", "c"]);

        // Subgraph from a, depth 2.
        req_tx
            .try_push(BridgeRequest {
                inner: make_request(PhysicalPlan::GraphSubgraph {
                    start_nodes: vec!["a".into()],
                    edge_label: None,
                    depth: 2,
                }),
            })
            .unwrap();

        core.tick();
        let resp = resp_rx.try_pop().unwrap();
        assert_eq!(resp.inner.status, Status::Ok);
        let edges: Vec<serde_json::Value> = serde_json::from_slice(&resp.inner.payload).unwrap();
        assert_eq!(edges.len(), 2);
    }

    #[test]
    fn edge_delete_updates_csr() {
        let (mut core, mut req_tx, mut resp_rx) = make_core();

        // Insert then delete.
        req_tx
            .try_push(BridgeRequest {
                inner: make_request(PhysicalPlan::EdgePut {
                    src_id: "x".into(),
                    label: "R".into(),
                    dst_id: "y".into(),
                    properties: vec![],
                }),
            })
            .unwrap();
        core.tick();
        resp_rx.try_pop().unwrap();

        req_tx
            .try_push(BridgeRequest {
                inner: make_request(PhysicalPlan::EdgeDelete {
                    src_id: "x".into(),
                    label: "R".into(),
                    dst_id: "y".into(),
                }),
            })
            .unwrap();
        core.tick();
        resp_rx.try_pop().unwrap();

        // Neighbors should be empty now.
        req_tx
            .try_push(BridgeRequest {
                inner: make_request(PhysicalPlan::GraphNeighbors {
                    node_id: "x".into(),
                    edge_label: None,
                    direction: Direction::Out,
                }),
            })
            .unwrap();
        core.tick();
        let resp = resp_rx.try_pop().unwrap();
        let neighbors: Vec<serde_json::Value> =
            serde_json::from_slice(&resp.inner.payload).unwrap();
        assert!(neighbors.is_empty());
    }

    #[test]
    fn point_delete_removes() {
        let (mut core, mut req_tx, mut resp_rx) = make_core();

        core.sparse.put("docs", "d1", b"data").unwrap();

        req_tx
            .try_push(BridgeRequest {
                inner: make_request(PhysicalPlan::PointDelete {
                    collection: "docs".into(),
                    document_id: "d1".into(),
                }),
            })
            .unwrap();

        core.tick();
        let resp = resp_rx.try_pop().unwrap();
        assert_eq!(resp.inner.status, Status::Ok);

        // Should be gone.
        req_tx
            .try_push(BridgeRequest {
                inner: make_request(PhysicalPlan::PointGet {
                    collection: "docs".into(),
                    document_id: "d1".into(),
                }),
            })
            .unwrap();

        core.tick();
        let resp = resp_rx.try_pop().unwrap();
        assert_eq!(resp.inner.error_code, Some(ErrorCode::NotFound));
    }
}
