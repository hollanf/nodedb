//! Nested-loop join execution.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

use super::merge_join_docs_binary;

impl CoreLoop {
    /// Nested loop join: O(N×M) fallback for non-equi joins, theta joins,
    /// and cross joins where hash join can't operate.
    ///
    /// For each left row, iterates all right rows and evaluates the join
    /// condition. Supports inner/left/right/full join types.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::data::executor) fn execute_nested_loop_join(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        left_collection: &str,
        right_collection: &str,
        condition: &[u8],
        join_type: &str,
        limit: usize,
    ) -> Response {
        debug!(
            core = self.core_id,
            %left_collection,
            %right_collection,
            %join_type,
            "nested loop join"
        );

        let scan_limit = (limit * 10).min(50000);

        let left_docs = match self.scan_collection(tid, left_collection, scan_limit) {
            Ok(d) => d,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };
        let right_docs = match self.scan_collection(tid, right_collection, scan_limit) {
            Ok(d) => d,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };

        // Parse join condition predicates.
        let predicates: Vec<crate::bridge::scan_filter::ScanFilter> = if condition.is_empty() {
            Vec::new() // Cross join — no condition.
        } else {
            match zerompk::from_msgpack(condition) {
                Ok(f) => f,
                Err(e) => {
                    tracing::warn!(core = self.core_id, error = %e, "malformed join condition");
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("join condition deserialization: {e}"),
                        },
                    );
                }
            }
        };

        let is_left = join_type == "left" || join_type == "full";
        let is_right = join_type == "right" || join_type == "full";

        let mut right_matched: Vec<bool> = vec![false; right_docs.len()];
        let mut results = Vec::new();

        for (_, left_bytes) in &left_docs {
            if results.len() >= limit {
                break;
            }

            let mut left_matched = false;
            for (ri, (_, right_bytes)) in right_docs.iter().enumerate() {
                if results.len() >= limit {
                    break;
                }

                // Evaluate condition against merged row.
                // Nested loop conditions reference prefixed fields (e.g., "left.id"),
                // so we must decode+merge before evaluating. This is inherent to NLJ.
                let passes = if predicates.is_empty() {
                    true // Cross join.
                } else {
                    let merged = merge_join_docs_binary(
                        left_bytes,
                        Some(right_bytes),
                        left_collection,
                        right_collection,
                    );
                    predicates.iter().all(|p| p.matches(&merged))
                };

                if passes {
                    left_matched = true;
                    right_matched[ri] = true;
                    results.push(merge_join_docs_binary(
                        left_bytes,
                        Some(right_bytes),
                        left_collection,
                        right_collection,
                    ));
                }
            }

            // LEFT/FULL: emit unmatched left rows.
            if !left_matched && is_left {
                results.push(merge_join_docs_binary(
                    left_bytes,
                    None,
                    left_collection,
                    right_collection,
                ));
            }
        }

        // RIGHT/FULL: emit unmatched right rows.
        if is_right {
            for (ri, (_, right_bytes)) in right_docs.iter().enumerate() {
                if results.len() >= limit {
                    break;
                }
                if !right_matched[ri] {
                    results.push(merge_join_docs_binary(
                        &[],
                        Some(right_bytes),
                        "",
                        right_collection,
                    ));
                }
            }
        }

        match super::super::super::response_codec::encode_json_vec(&results) {
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
