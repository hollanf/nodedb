//! KV Get, Put, Delete, Truncate handlers.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use crate::engine::kv::current_ms;

impl CoreLoop {
    pub(in crate::data::executor) fn execute_kv_get(
        &self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        key: &[u8],
        rls_filters: &[u8],
    ) -> Response {
        debug!(core = self.core_id, %collection, "kv get");
        let now_ms = current_ms();
        match self.kv_engine.get(tid, collection, key, now_ms) {
            Some(value) => {
                // RLS post-fetch: evaluate filters against KV value.
                if !crate::data::executor::handlers::rls_eval::rls_check_msgpack_bytes(
                    rls_filters,
                    &value,
                ) {
                    return self.response_error(task, ErrorCode::NotFound);
                }
                if let Some(ref m) = self.metrics {
                    m.record_kv_get();
                }
                self.response_with_payload(task, value)
            }
            None => self.response_error(task, ErrorCode::NotFound),
        }
    }

    pub(in crate::data::executor) fn execute_kv_put(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        key: &[u8],
        value: &[u8],
        ttl_ms: u64,
    ) -> Response {
        debug!(core = self.core_id, %collection, "kv put");

        // Memory budget check: reject new PUTs when over budget.
        if self.kv_engine.is_over_budget() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "KV memory budget exceeded, retry later".into(),
                },
            );
        }

        let now_ms = current_ms();
        let _old = self
            .kv_engine
            .put(tid, collection, key, value, ttl_ms, now_ms);
        if let Some(ref m) = self.metrics {
            m.record_kv_put();
        }

        // Emit write event to Event Plane.
        let key_str = String::from_utf8_lossy(key);
        self.emit_write_event(
            task,
            collection,
            crate::event::WriteOp::Insert,
            &key_str,
            Some(value),
            None,
        );

        self.response_ok(task)
    }

    pub(in crate::data::executor) fn execute_kv_delete(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        keys: &[Vec<u8>],
    ) -> Response {
        debug!(core = self.core_id, %collection, count = keys.len(), "kv delete");
        let now_ms = current_ms();
        let count = self.kv_engine.delete(tid, collection, keys, now_ms);
        if let Some(ref m) = self.metrics {
            m.record_kv_delete();
        }

        // Emit delete events to Event Plane (one per deleted key).
        if count > 0 {
            for key in keys {
                let key_str = String::from_utf8_lossy(key);
                self.emit_write_event(
                    task,
                    collection,
                    crate::event::WriteOp::Delete,
                    &key_str,
                    None,
                    None,
                );
            }
        }

        match response_codec::encode_count("deleted", count) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    pub(in crate::data::executor) fn execute_kv_truncate(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
    ) -> Response {
        debug!(core = self.core_id, %collection, "kv truncate");
        let count = self.kv_engine.truncate(tid, collection);
        match response_codec::encode_count("deleted", count) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }
}
