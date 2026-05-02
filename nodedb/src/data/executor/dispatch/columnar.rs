//! Dispatch for ColumnarOp variants (scan, insert, update, delete).

use crate::bridge::envelope::Response;
use crate::bridge::physical_plan::ColumnarOp;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::columnar_read::ColumnarScanParams;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    pub(super) fn dispatch_columnar(&mut self, task: &ExecutionTask, op: &ColumnarOp) -> Response {
        match op {
            ColumnarOp::Scan {
                collection,
                projection,
                limit,
                filters,
                rls_filters,
                sort_keys,
                system_as_of_ms,
                valid_at_ms,
                prefilter,
            } => self.execute_columnar_scan(
                task,
                ColumnarScanParams {
                    collection,
                    projection,
                    limit: *limit,
                    filters,
                    rls_filters,
                    sort_keys,
                    system_as_of_ms: *system_as_of_ms,
                    valid_at_ms: *valid_at_ms,
                    prefilter: prefilter.as_ref(),
                },
            ),

            ColumnarOp::Insert {
                collection,
                payload,
                format,
                intent,
                on_conflict_updates,
                surrogates,
            } => {
                if let Some(r) = self.check_engine_pressure(task, nodedb_mem::EngineId::Columnar) {
                    return r;
                }
                self.execute_columnar_insert(
                    task,
                    collection,
                    payload,
                    format,
                    *intent,
                    on_conflict_updates,
                    surrogates,
                )
            }

            ColumnarOp::Update {
                collection,
                filters,
                updates,
            } => {
                if let Some(r) = self.check_engine_pressure(task, nodedb_mem::EngineId::Columnar) {
                    return r;
                }
                self.execute_columnar_update(task, collection, filters, updates)
            }

            ColumnarOp::Delete {
                collection,
                filters,
            } => {
                if let Some(r) = self.check_engine_pressure(task, nodedb_mem::EngineId::Columnar) {
                    return r;
                }
                self.execute_columnar_delete(task, collection, filters)
            }
        }
    }
}
