//! Data Plane core runtime and query execution tuning.

use serde::{Deserialize, Serialize};

/// Data Plane core runtime tuning.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataPlaneTuning {
    #[serde(default = "default_idle_poll_timeout_ms")]
    pub idle_poll_timeout_ms: i32,
    #[serde(default = "default_max_consecutive_panics")]
    pub max_consecutive_panics: u32,
    #[serde(default = "default_panic_window_secs")]
    pub panic_window_secs: u64,
    #[serde(default = "default_degraded_cooldown_secs")]
    pub degraded_cooldown_secs: u64,
}

impl Default for DataPlaneTuning {
    fn default() -> Self {
        Self {
            idle_poll_timeout_ms: default_idle_poll_timeout_ms(),
            max_consecutive_panics: default_max_consecutive_panics(),
            panic_window_secs: default_panic_window_secs(),
            degraded_cooldown_secs: default_degraded_cooldown_secs(),
        }
    }
}

fn default_idle_poll_timeout_ms() -> i32 {
    100
}
fn default_max_consecutive_panics() -> u32 {
    3
}
fn default_panic_window_secs() -> u64 {
    60
}
fn default_degraded_cooldown_secs() -> u64 {
    30
}

/// Query execution tuning for the Data Plane executor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryTuning {
    #[serde(default = "default_sort_run_size")]
    pub sort_run_size: usize,
    #[serde(default = "default_stream_chunk_size")]
    pub stream_chunk_size: usize,
    #[serde(default = "default_aggregate_scan_cap")]
    pub aggregate_scan_cap: usize,
    #[serde(default = "default_arrow_batch_max_rows")]
    pub arrow_batch_max_rows: usize,
    #[serde(default = "default_arrow_batch_max_bytes")]
    pub arrow_batch_max_bytes: usize,
    #[serde(default = "default_bitmap_over_fetch_factor")]
    pub bitmap_over_fetch_factor: usize,
    #[serde(default = "default_bfs_memory_budget_bytes")]
    pub bfs_memory_budget_bytes: usize,
    #[serde(default = "default_bfs_bytes_per_node")]
    pub bfs_bytes_per_node: usize,
}

impl Default for QueryTuning {
    fn default() -> Self {
        Self {
            sort_run_size: default_sort_run_size(),
            stream_chunk_size: default_stream_chunk_size(),
            aggregate_scan_cap: default_aggregate_scan_cap(),
            arrow_batch_max_rows: default_arrow_batch_max_rows(),
            arrow_batch_max_bytes: default_arrow_batch_max_bytes(),
            bitmap_over_fetch_factor: default_bitmap_over_fetch_factor(),
            bfs_memory_budget_bytes: default_bfs_memory_budget_bytes(),
            bfs_bytes_per_node: default_bfs_bytes_per_node(),
        }
    }
}

fn default_sort_run_size() -> usize {
    100_000
}
fn default_stream_chunk_size() -> usize {
    1_000
}
fn default_aggregate_scan_cap() -> usize {
    10_000_000
}
fn default_arrow_batch_max_rows() -> usize {
    65_536
}
fn default_arrow_batch_max_bytes() -> usize {
    8 * 1024 * 1024
}
fn default_bitmap_over_fetch_factor() -> usize {
    3
}
fn default_bfs_memory_budget_bytes() -> usize {
    256 * 1024
}
fn default_bfs_bytes_per_node() -> usize {
    192
}
