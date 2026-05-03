//! Static and active (dependent-read) txn dispatch to the Data Plane.

use std::time::Instant;

use tracing::error;

use nodedb_cluster::calvin::types::SequencedTxn;

use super::scheduler::Scheduler;
use crate::bridge::envelope::{Priority, Request};
use crate::bridge::physical_plan::PhysicalPlan;
use crate::bridge::physical_plan::meta::MetaOp;
use crate::bridge::physical_plan::{CrdtOp, DocumentOp, GraphOp, KvOp, TimeseriesOp, VectorOp};
use crate::control::cluster::calvin::scheduler::lock_manager::{LockKey, TxnId};
use crate::types::{ReadConsistency, VShardId};

impl Scheduler {
    fn local_calvin_plans(
        &self,
        plans: Vec<PhysicalPlan>,
        epoch: u64,
        position: u32,
    ) -> crate::Result<Vec<PhysicalPlan>> {
        fn plan_vshard(plan: &PhysicalPlan) -> Option<VShardId> {
            let collection = match plan {
                PhysicalPlan::Document(
                    DocumentOp::PointPut { collection, .. }
                    | DocumentOp::PointInsert { collection, .. }
                    | DocumentOp::PointDelete { collection, .. }
                    | DocumentOp::PointUpdate { collection, .. }
                    | DocumentOp::BatchInsert { collection, .. }
                    | DocumentOp::InsertSelect {
                        target_collection: collection,
                        ..
                    }
                    | DocumentOp::Upsert { collection, .. }
                    | DocumentOp::BulkUpdate { collection, .. }
                    | DocumentOp::BulkDelete { collection, .. },
                ) => collection.as_str(),
                PhysicalPlan::Kv(
                    KvOp::Put { collection, .. }
                    | KvOp::Insert { collection, .. }
                    | KvOp::InsertIfAbsent { collection, .. }
                    | KvOp::InsertOnConflictUpdate { collection, .. }
                    | KvOp::Delete { collection, .. }
                    | KvOp::BatchPut { collection, .. },
                ) => collection.as_str(),
                PhysicalPlan::Vector(
                    VectorOp::Insert { collection, .. }
                    | VectorOp::BatchInsert { collection, .. }
                    | VectorOp::Delete { collection, .. }
                    | VectorOp::SparseInsert { collection, .. }
                    | VectorOp::SparseDelete { collection, .. }
                    | VectorOp::MultiVectorInsert { collection, .. },
                ) => collection.as_str(),
                PhysicalPlan::Graph(
                    GraphOp::EdgePut { collection, .. } | GraphOp::EdgeDelete { collection, .. },
                ) => collection.as_str(),
                PhysicalPlan::Timeseries(TimeseriesOp::Ingest { collection, .. }) => {
                    collection.as_str()
                }
                PhysicalPlan::Columnar(crate::bridge::physical_plan::ColumnarOp::Insert {
                    collection,
                    ..
                }) => collection.as_str(),
                PhysicalPlan::Crdt(
                    CrdtOp::Apply { collection, .. }
                    | CrdtOp::ListInsert { collection, .. }
                    | CrdtOp::ListDelete { collection, .. },
                ) => collection.as_str(),
                _ => return None,
            };
            Some(VShardId::from_collection(collection))
        }

        let mut local = Vec::new();
        for plan in plans {
            let Some(plan_vshard) = plan_vshard(&plan) else {
                return Err(crate::Error::Internal {
                    detail: format!(
                        "calvin txn {epoch}/{position} contains non-routable plan for vshard {}",
                        self.vshard_id
                    ),
                });
            };
            if plan_vshard.as_u32() == self.vshard_id {
                local.push(plan);
            }
        }

        if local.is_empty() {
            return Err(crate::Error::Internal {
                detail: format!(
                    "calvin txn {epoch}/{position} contains no local plans for vshard {}",
                    self.vshard_id
                ),
            });
        }

        Ok(local)
    }

    /// Dispatch a static-set ready transaction to the Data Plane executor.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn dispatch_txn(
        &mut self,
        txn: SequencedTxn,
        txn_id: TxnId,
        keys: std::collections::BTreeSet<LockKey>,
        lock_acquired_time: Instant,
    ) {
        let request_id = self.next_request_id();
        let tenant_id = txn.tx_class.tenant_id;
        let vshard_id = VShardId::new(self.vshard_id);
        let epoch = txn.epoch;
        let position = txn.position;

        let plans = match super::super::helpers::decode_plans(&txn.tx_class.plans) {
            Ok(p) => p,
            Err(e) => {
                error!(
                    vshard_id = self.vshard_id,
                    epoch,
                    position,
                    error = %e,
                    "calvin scheduler: plan decode failed; releasing locks and skipping txn"
                );
                self.on_txn_complete(txn_id);
                return;
            }
        };
        let plans = match self.local_calvin_plans(plans, epoch, position) {
            Ok(p) => p,
            Err(e) => {
                error!(
                    vshard_id = self.vshard_id,
                    epoch,
                    position,
                    error = %e,
                    "calvin scheduler: static txn routing failed; releasing locks"
                );
                self.on_txn_complete(txn_id);
                return;
            }
        };
        let plan = PhysicalPlan::Meta(MetaOp::CalvinExecuteStatic {
            epoch,
            position,
            tenant_id,
            plans,
            epoch_system_ms: txn.epoch_system_ms,
        });

        // no-determinism: request deadline is ephemeral, not written to WAL
        let deadline = Instant::now()
            + std::time::Duration::from_millis(
                self.config.epoch_duration_ms * u64::from(self.config.txn_deadline_multiplier),
            );

        let request = Request {
            request_id,
            tenant_id,
            vshard_id,
            plan,
            deadline,
            priority: Priority::Normal,
            trace_id: nodedb_types::TraceId([0u8; 16]),
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
        };

        let resp_rx = self.shared.tracker.register(request_id);

        let dispatch_result = match self.shared.dispatcher.lock() {
            Ok(mut d) => d.dispatch(request),
            Err(poisoned) => poisoned.into_inner().dispatch(request),
        };

        if let Err(e) = dispatch_result {
            error!(
                vshard_id = self.vshard_id,
                epoch,
                position,
                error = %e,
                "calvin scheduler: dispatch failed; releasing locks"
            );
            self.on_txn_complete(txn_id);
            return;
        }

        self.metrics.record_dispatch();

        // no-determinism: executor latency observability, off-WAL path
        let dispatch_instant = Instant::now();

        self.spawn_response_bridge(txn_id, request_id, resp_rx);

        self.pending.insert(
            txn_id,
            super::super::types::PendingTxn {
                txn,
                keys,
                request_id,
                // no-determinism: dispatch_time is scheduler observability, not Calvin WAL data
                dispatch_time: dispatch_instant,
                lock_acquired_time,
            },
        );
    }

    /// Dispatch an active dependent-read txn once all passive results are in.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn dispatch_active_txn(
        &mut self,
        txn: SequencedTxn,
        txn_id: TxnId,
        keys: std::collections::BTreeSet<LockKey>,
        lock_acquired_time: Instant,
        injected_reads: std::collections::BTreeMap<
            crate::bridge::physical_plan::meta::PassiveReadKeyId,
            nodedb_types::Value,
        >,
    ) {
        let request_id = self.next_request_id();
        let tenant_id = txn.tx_class.tenant_id;
        let vshard_id = VShardId::new(self.vshard_id);
        let epoch = txn.epoch;
        let position = txn.position;

        let plans = match super::super::helpers::decode_plans(&txn.tx_class.plans) {
            Ok(p) => p,
            Err(e) => {
                error!(
                    vshard_id = self.vshard_id,
                    epoch,
                    position,
                    error = %e,
                    "calvin scheduler: active plan decode failed; releasing locks"
                );
                self.on_txn_complete(txn_id);
                return;
            }
        };
        let plans = match self.local_calvin_plans(plans, epoch, position) {
            Ok(p) => p,
            Err(e) => {
                error!(
                    vshard_id = self.vshard_id,
                    epoch,
                    position,
                    error = %e,
                    "calvin scheduler: active txn routing failed; releasing locks"
                );
                self.on_txn_complete(txn_id);
                return;
            }
        };
        let plan = PhysicalPlan::Meta(MetaOp::CalvinExecuteActive {
            epoch,
            position,
            tenant_id,
            plans,
            injected_reads,
            epoch_system_ms: txn.epoch_system_ms,
        });

        // no-determinism: request deadline is ephemeral, not written to WAL
        let deadline = Instant::now()
            + std::time::Duration::from_millis(
                self.config.epoch_duration_ms * u64::from(self.config.txn_deadline_multiplier),
            );

        let request = Request {
            request_id,
            tenant_id,
            vshard_id,
            plan,
            deadline,
            priority: Priority::Normal,
            trace_id: nodedb_types::TraceId([0u8; 16]),
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
        };

        let resp_rx = self.shared.tracker.register(request_id);

        let dispatch_result = match self.shared.dispatcher.lock() {
            Ok(mut d) => d.dispatch(request),
            Err(poisoned) => poisoned.into_inner().dispatch(request),
        };

        if let Err(e) = dispatch_result {
            error!(
                vshard_id = self.vshard_id,
                epoch,
                position,
                error = %e,
                "calvin scheduler: active dispatch failed; releasing locks"
            );
            self.on_txn_complete(txn_id);
            return;
        }

        self.metrics.record_dispatch();

        // no-determinism: executor latency observability, off-WAL path
        let dispatch_instant = Instant::now();

        self.spawn_response_bridge(txn_id, request_id, resp_rx);

        self.pending.insert(
            txn_id,
            super::super::types::PendingTxn {
                txn,
                keys,
                request_id,
                // no-determinism: dispatch_time is scheduler observability, not Calvin WAL data
                dispatch_time: dispatch_instant,
                lock_acquired_time,
            },
        );
    }
}
