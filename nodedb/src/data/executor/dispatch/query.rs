//! Dispatch for QueryOp variants (aggregates, joins, recursive scans, facets).

use crate::bridge::envelope::Response;
use crate::bridge::physical_plan::QueryOp;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::join::{
    BroadcastJoinParams, HashJoinParams, InlineHashJoinParams, JoinParams,
};
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    pub(super) fn dispatch_query(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        op: &QueryOp,
    ) -> Response {
        match op {
            QueryOp::Aggregate {
                collection,
                group_by,
                aggregates,
                filters,
                having,
                limit,
                sub_group_by,
                sub_aggregates,
            } => self.execute_aggregate(
                task,
                tid,
                collection,
                group_by,
                aggregates,
                filters,
                having,
                *limit,
                sub_group_by,
                sub_aggregates,
            ),

            QueryOp::HashJoin {
                left_collection,
                right_collection,
                left_alias,
                right_alias,
                on,
                join_type,
                limit,
                projection,
                post_filters,
                inline_left,
                inline_right,
                inline_left_bitmap,
                inline_right_bitmap,
                ..
            } => self.execute_hash_join(HashJoinParams {
                join: JoinParams {
                    task,
                    on,
                    join_type,
                    limit: *limit,
                    projection,
                    post_filter_bytes: post_filters,
                },
                tid,
                left_collection,
                right_collection,
                left_alias: left_alias.as_deref(),
                right_alias: right_alias.as_deref(),
                inline_left: inline_left.as_deref(),
                inline_right: inline_right.as_deref(),
                inline_left_bitmap: inline_left_bitmap.as_deref(),
                inline_right_bitmap: inline_right_bitmap.as_deref(),
            }),

            QueryOp::InlineHashJoin {
                left_data,
                right_data,
                right_alias,
                on,
                join_type,
                limit,
                projection,
                post_filters,
            } => self.execute_inline_hash_join(InlineHashJoinParams {
                join: JoinParams {
                    task,
                    on,
                    join_type,
                    limit: *limit,
                    projection,
                    post_filter_bytes: post_filters,
                },
                left_data,
                right_data,
                right_alias: right_alias.as_deref(),
            }),

            QueryOp::NestedLoopJoin {
                left_collection,
                right_collection,
                condition,
                join_type,
                limit,
            } => self.execute_nested_loop_join(
                task,
                tid,
                left_collection,
                right_collection,
                condition,
                join_type,
                *limit,
            ),

            QueryOp::SortMergeJoin {
                left_collection,
                right_collection,
                on,
                join_type,
                limit,
                pre_sorted,
            } => self.execute_sort_merge_join(
                task,
                tid,
                left_collection,
                right_collection,
                on,
                join_type,
                *limit,
                *pre_sorted,
            ),

            QueryOp::RecursiveScan {
                collection,
                base_filters,
                recursive_filters,
                join_link,
                max_iterations,
                distinct,
                limit,
            } => self.execute_recursive_scan(
                task,
                tid,
                collection,
                base_filters,
                recursive_filters,
                join_link.as_ref(),
                *max_iterations,
                *distinct,
                *limit,
            ),

            QueryOp::FacetCounts {
                collection,
                filters,
                fields,
                limit_per_facet,
            } => {
                self.execute_facet_counts(task, tid, collection, filters, fields, *limit_per_facet)
            }

            QueryOp::PartialAggregate {
                collection,
                group_by,
                aggregates,
                filters,
            } => self.execute_aggregate(
                task,
                tid,
                collection,
                group_by,
                aggregates,
                filters,
                &[],
                usize::MAX,
                &[],
                &[],
            ),

            QueryOp::BroadcastJoin {
                large_collection,
                small_collection,
                large_alias,
                small_alias,
                broadcast_data,
                on,
                join_type,
                limit,
                projection,
                post_filters,
                ..
            } => self.execute_broadcast_join(BroadcastJoinParams {
                join: JoinParams {
                    task,
                    on,
                    join_type,
                    limit: *limit,
                    projection,
                    post_filter_bytes: post_filters,
                },
                tid,
                large_collection,
                small_collection,
                large_alias: large_alias.as_deref(),
                small_alias: small_alias.as_deref(),
                broadcast_data,
            }),

            QueryOp::ShuffleJoin {
                left_collection,
                right_collection,
                on,
                join_type,
                limit,
                ..
            } => self.execute_hash_join(HashJoinParams {
                join: JoinParams {
                    task,
                    on,
                    join_type,
                    limit: *limit,
                    projection: &[],
                    post_filter_bytes: &[],
                },
                tid,
                left_collection,
                right_collection,
                left_alias: None,
                right_alias: None,
                inline_left: None,
                inline_right: None,
                inline_left_bitmap: None,
                inline_right_bitmap: None,
            }),
        }
    }
}
