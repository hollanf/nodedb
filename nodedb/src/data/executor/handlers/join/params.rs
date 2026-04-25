//! Shared parameter structs for join execution handlers.

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::JoinProjection;
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::task::ExecutionTask;

/// Common join configuration shared across hash, inline-hash, and broadcast joins.
pub(crate) struct JoinParams<'a> {
    pub task: &'a ExecutionTask,
    pub on: &'a [(String, String)],
    pub join_type: &'a str,
    pub limit: usize,
    pub projection: &'a [JoinProjection],
    pub post_filter_bytes: &'a [u8],
}

/// Hash join: scans both sides from storage (or executes inline sub-plans).
pub(crate) struct HashJoinParams<'a> {
    pub join: JoinParams<'a>,
    pub tid: u32,
    pub left_collection: &'a str,
    pub right_collection: &'a str,
    pub left_alias: Option<&'a str>,
    pub right_alias: Option<&'a str>,
    pub inline_left: Option<&'a PhysicalPlan>,
    pub inline_right: Option<&'a PhysicalPlan>,
    /// Bitmap-producer sub-plan for the left side. When `Some`, the executor
    /// runs this sub-plan first, collects surrogates, and injects the bitmap
    /// into the right (probe) side's scan prefilter.
    pub inline_left_bitmap: Option<&'a PhysicalPlan>,
    /// Bitmap-producer sub-plan for the right side. Same semantics as
    /// `inline_left_bitmap` but applied to the right collection.
    pub inline_right_bitmap: Option<&'a PhysicalPlan>,
}

/// Inline hash join: both sides are pre-gathered as msgpack byte arrays.
pub(crate) struct InlineHashJoinParams<'a> {
    pub join: JoinParams<'a>,
    pub left_data: &'a [u8],
    pub right_data: &'a [u8],
    pub right_alias: Option<&'a str>,
}

/// Broadcast join: small side is pre-serialized, large side scanned locally.
pub(crate) struct BroadcastJoinParams<'a> {
    pub join: JoinParams<'a>,
    pub tid: u32,
    pub large_collection: &'a str,
    pub small_collection: &'a str,
    pub large_alias: Option<&'a str>,
    pub small_alias: Option<&'a str>,
    pub broadcast_data: &'a [u8],
}

impl JoinParams<'_> {
    /// Apply post-join WHERE filters and projection to result rows.
    ///
    /// Shared tail logic for hash, inline-hash, and broadcast joins:
    /// deserializes post-filter predicates, retains matching rows, then
    /// applies column projection — all on raw msgpack bytes.
    pub fn filter_and_project(&self, results: &mut Vec<Vec<u8>>) {
        if !self.post_filter_bytes.is_empty() {
            let filters: Vec<ScanFilter> =
                zerompk::from_msgpack(self.post_filter_bytes).unwrap_or_default();
            if !filters.is_empty() {
                results.retain(|row| super::binary_row_matches_filters(row, &filters));
            }
        }

        if !self.projection.is_empty() {
            for row in results.iter_mut() {
                *row = super::binary_row_project(row, self.projection);
            }
        }
    }
}
