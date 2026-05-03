//! Static and active (dependent-read) txn dispatch to the Data Plane.

use std::sync::Arc;
use std::time::Instant;

use tracing::{error, warn};

use nodedb_cluster::calvin::types::SequencedTxn;

use super::scheduler::Scheduler;
use crate::bridge::envelope::{Priority, Request, Status};
use crate::bridge::physical_plan::PhysicalPlan;
use crate::bridge::physical_plan::meta::MetaOp;
use crate::control::cluster::calvin::scheduler::lock_manager::{LockKey, TxnId};
use crate::types::{ReadConsistency, VShardId};

impl Scheduler {
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

        let mut resp_rx = self.tracker.register(request_id);

        let dispatch_result = match self.dispatcher.lock() {
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

        self.pending.insert(
            txn_id,
            super::super::types::PendingTxn {
                txn,
                keys,
                request_id,
                // no-determinism: dispatch_time is scheduler observability, not Calvin WAL data
                dispatch_time: Instant::now(),
                lock_acquired_time,
            },
        );

        let wal = Arc::clone(&self.wal);
        let vshard_id_copy = self.vshard_id;
        let metrics = Arc::clone(&self.metrics);

        tokio::spawn(async move {
            if let Some(response) = resp_rx.recv().await {
                // no-determinism: executor latency observability, off-WAL path
                let elapsed_ms = dispatch_instant.elapsed().as_millis() as u64;
                metrics.record_executor_txn_duration_ms(elapsed_ms);

                if response.status == Status::Ok {
                    if let Err(e) =
                        wal.append_calvin_applied(VShardId::new(vshard_id_copy), epoch, position)
                    {
                        error!(
                            vshard_id = vshard_id_copy,
                            epoch,
                            position,
                            error = %e,
                            "calvin: failed to write CalvinApplied WAL record"
                        );
                    }
                } else {
                    warn!(
                        vshard_id = vshard_id_copy,
                        epoch,
                        position,
                        "calvin: executor response was not Ok; locks NOT released (shard degraded)"
                    );
                    metrics.record_executor_error();
                    metrics.record_infra_abort(
                        crate::control::cluster::calvin::scheduler::metrics::infra_abort_reason::IO_ERROR,
                    );
                }
                metrics.record_completed();
            }
        });
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

        let mut resp_rx = self.tracker.register(request_id);

        let dispatch_result = match self.dispatcher.lock() {
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

        self.pending.insert(
            txn_id,
            super::super::types::PendingTxn {
                txn,
                keys,
                request_id,
                // no-determinism: dispatch_time is scheduler observability, not Calvin WAL data
                dispatch_time: Instant::now(),
                lock_acquired_time,
            },
        );

        let wal = Arc::clone(&self.wal);
        let vshard_id_copy = self.vshard_id;
        let metrics = Arc::clone(&self.metrics);

        tokio::spawn(async move {
            if let Some(response) = resp_rx.recv().await {
                // no-determinism: executor latency observability, off-WAL path
                let elapsed_ms = dispatch_instant.elapsed().as_millis() as u64;
                metrics.record_executor_txn_duration_ms(elapsed_ms);

                if response.status == Status::Ok {
                    if let Err(e) =
                        wal.append_calvin_applied(VShardId::new(vshard_id_copy), epoch, position)
                    {
                        error!(
                            vshard_id = vshard_id_copy,
                            epoch,
                            position,
                            error = %e,
                            "calvin: failed to write CalvinApplied WAL record (active)"
                        );
                    }
                } else {
                    warn!(
                        vshard_id = vshard_id_copy,
                        epoch,
                        position,
                        "calvin: active executor response not Ok; locks NOT released (shard degraded)"
                    );
                    metrics.record_executor_error();
                    metrics.record_infra_abort(
                        crate::control::cluster::calvin::scheduler::metrics::infra_abort_reason::IO_ERROR,
                    );
                }
                metrics.record_completed();
            }
        });
    }
}
