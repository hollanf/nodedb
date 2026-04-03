//! KV secondary index handlers: RegisterIndex, DropIndex.

use tracing::debug;

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::kv::current_ms;

impl CoreLoop {
    pub(in crate::data::executor) fn execute_kv_register_index(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        field: &str,
        field_position: usize,
        backfill: bool,
    ) -> Response {
        debug!(core = self.core_id, %collection, %field, "kv register index");
        let now_ms = current_ms();
        let backfilled =
            self.kv_engine
                .register_index(tid, collection, field, field_position, backfill, now_ms);
        let payload = serde_json::json!({
            "index": field,
            "backfilled": backfilled,
            "write_amp_estimate": format!("{:.0}%", 15.0 + 10.0 * self.kv_engine.index_count(tid, collection) as f64),
        })
        .to_string()
        .into_bytes();
        self.response_with_payload(task, payload)
    }

    pub(in crate::data::executor) fn execute_kv_drop_index(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        field: &str,
    ) -> Response {
        debug!(core = self.core_id, %collection, %field, "kv drop index");
        let removed = self.kv_engine.drop_index(tid, collection, field);
        let payload = serde_json::json!({
            "index": field,
            "entries_removed": removed,
        })
        .to_string()
        .into_bytes();
        self.response_with_payload(task, payload)
    }
}
