//! KV engine operation handlers for the Data Plane executor.

mod atomic;
mod batch;
mod crud;
mod field;
mod index;
mod scan;
mod ttl;

use crate::bridge::envelope::Response;
use crate::bridge::physical_plan::KvOp;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Dispatch a KV operation to the appropriate handler.
    pub(in crate::data::executor) fn execute_kv(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        op: &KvOp,
    ) -> Response {
        match op {
            KvOp::Get {
                collection,
                key,
                rls_filters,
            } => self.execute_kv_get(task, tid, collection, key, rls_filters),
            KvOp::Put {
                collection,
                key,
                value,
                ttl_ms,
            } => self.execute_kv_put(task, tid, collection, key, value, *ttl_ms),
            KvOp::Delete { collection, keys } => {
                self.execute_kv_delete(task, tid, collection, keys)
            }
            KvOp::Scan {
                collection,
                cursor,
                count,
                filters,
                match_pattern,
            } => self.execute_kv_scan(
                task,
                tid,
                collection,
                cursor,
                *count,
                match_pattern.as_deref(),
                filters,
            ),
            KvOp::Expire {
                collection,
                key,
                ttl_ms,
            } => self.execute_kv_expire(task, tid, collection, key, *ttl_ms),
            KvOp::Persist { collection, key } => {
                self.execute_kv_persist(task, tid, collection, key)
            }
            KvOp::BatchGet { collection, keys } => {
                self.execute_kv_batch_get(task, tid, collection, keys)
            }
            KvOp::BatchPut {
                collection,
                entries,
                ttl_ms,
            } => self.execute_kv_batch_put(task, tid, collection, entries, *ttl_ms),
            KvOp::RegisterIndex {
                collection,
                field,
                field_position,
                backfill,
            } => self.execute_kv_register_index(
                task,
                tid,
                collection,
                field,
                *field_position,
                *backfill,
            ),
            KvOp::DropIndex { collection, field } => {
                self.execute_kv_drop_index(task, tid, collection, field)
            }
            KvOp::FieldGet {
                collection,
                key,
                fields,
            } => self.execute_kv_field_get(task, tid, collection, key, fields),
            KvOp::FieldSet {
                collection,
                key,
                updates,
            } => self.execute_kv_field_set(task, tid, collection, key, updates),
            KvOp::GetTtl { collection, key } => self.execute_kv_get_ttl(task, tid, collection, key),
            KvOp::Truncate { collection } => self.execute_kv_truncate(task, tid, collection),
            KvOp::Incr {
                collection,
                key,
                delta,
                ttl_ms,
            } => self.execute_kv_incr(task, tid, collection, key, *delta, *ttl_ms),
            KvOp::IncrFloat {
                collection,
                key,
                delta,
            } => self.execute_kv_incr_float(task, tid, collection, key, *delta),
            KvOp::Cas {
                collection,
                key,
                expected,
                new_value,
            } => self.execute_kv_cas(task, tid, collection, key, expected, new_value),
            KvOp::GetSet {
                collection,
                key,
                new_value,
            } => self.execute_kv_getset(task, tid, collection, key, new_value),
        }
    }
}
