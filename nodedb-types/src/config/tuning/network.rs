//! Network, bridge, WAL, and cluster transport tuning.

use serde::{Deserialize, Serialize};

/// SPSC bridge and slab allocator tuning.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BridgeTuning {
    #[serde(default = "default_slab_page_size")]
    pub slab_page_size: usize,
    #[serde(default = "default_slab_pool_size")]
    pub slab_pool_size: usize,
}

impl Default for BridgeTuning {
    fn default() -> Self {
        Self {
            slab_page_size: default_slab_page_size(),
            slab_pool_size: default_slab_pool_size(),
        }
    }
}

fn default_slab_page_size() -> usize {
    64 * 1024
}
fn default_slab_pool_size() -> usize {
    256
}

/// Network, deadline, and session tuning.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkTuning {
    #[serde(default = "default_deadline_secs")]
    pub default_deadline_secs: u64,
    #[serde(default = "default_copy_deadline_secs")]
    pub copy_deadline_secs: u64,
    #[serde(default = "default_max_ws_sessions")]
    pub max_ws_sessions: usize,
    #[serde(default = "default_subscription_buffer_size")]
    pub subscription_buffer_size: usize,
    #[serde(default = "default_subscription_idle_timeout_secs")]
    pub subscription_idle_timeout_secs: u64,
    #[serde(default = "default_token_refresh_window_secs")]
    pub token_refresh_window_secs: u64,
    #[serde(default = "default_drain_timeout_secs")]
    pub drain_timeout_secs: u64,
    #[serde(default = "default_max_frame_size")]
    pub max_frame_size: u32,
}

impl Default for NetworkTuning {
    fn default() -> Self {
        Self {
            default_deadline_secs: default_deadline_secs(),
            copy_deadline_secs: default_copy_deadline_secs(),
            max_ws_sessions: default_max_ws_sessions(),
            subscription_buffer_size: default_subscription_buffer_size(),
            subscription_idle_timeout_secs: default_subscription_idle_timeout_secs(),
            token_refresh_window_secs: default_token_refresh_window_secs(),
            drain_timeout_secs: default_drain_timeout_secs(),
            max_frame_size: default_max_frame_size(),
        }
    }
}

fn default_deadline_secs() -> u64 {
    30
}
fn default_copy_deadline_secs() -> u64 {
    120
}
fn default_max_ws_sessions() -> usize {
    10_000
}
fn default_subscription_buffer_size() -> usize {
    1024
}
fn default_subscription_idle_timeout_secs() -> u64 {
    300
}
fn default_token_refresh_window_secs() -> u64 {
    300
}
fn default_drain_timeout_secs() -> u64 {
    30
}
fn default_max_frame_size() -> u32 {
    16 * 1024 * 1024
}

/// WAL writer and double-write buffer tuning.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalTuning {
    #[serde(default = "default_wal_write_buffer_size")]
    pub write_buffer_size: usize,
    #[serde(default = "default_wal_alignment")]
    pub alignment: usize,
    #[serde(default = "default_dwb_capacity")]
    pub dwb_capacity: usize,
}

impl Default for WalTuning {
    fn default() -> Self {
        Self {
            write_buffer_size: default_wal_write_buffer_size(),
            alignment: default_wal_alignment(),
            dwb_capacity: default_dwb_capacity(),
        }
    }
}

fn default_wal_write_buffer_size() -> usize {
    256 * 1024
}
fn default_wal_alignment() -> usize {
    4096
}
fn default_dwb_capacity() -> usize {
    64
}

/// Cluster transport tuning for QUIC connections and Raft consensus.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterTransportTuning {
    #[serde(default = "default_raft_tick_interval_ms")]
    pub raft_tick_interval_ms: u64,
    #[serde(default = "default_election_timeout_min_secs")]
    pub election_timeout_min_secs: u64,
    #[serde(default = "default_election_timeout_max_secs")]
    pub election_timeout_max_secs: u64,
    #[serde(default = "default_rpc_timeout_secs")]
    pub rpc_timeout_secs: u64,
    #[serde(default = "default_quic_keep_alive_secs")]
    pub quic_keep_alive_secs: u64,
    #[serde(default = "default_quic_idle_timeout_secs")]
    pub quic_idle_timeout_secs: u64,
    #[serde(default = "default_quic_max_bi_streams")]
    pub quic_max_bi_streams: u32,
    #[serde(default = "default_quic_max_uni_streams")]
    pub quic_max_uni_streams: u32,
    #[serde(default = "default_quic_receive_window")]
    pub quic_receive_window: u32,
    #[serde(default = "default_quic_send_window")]
    pub quic_send_window: u32,
    #[serde(default = "default_quic_stream_receive_window")]
    pub quic_stream_receive_window: u32,
}

impl Default for ClusterTransportTuning {
    fn default() -> Self {
        Self {
            raft_tick_interval_ms: default_raft_tick_interval_ms(),
            election_timeout_min_secs: default_election_timeout_min_secs(),
            election_timeout_max_secs: default_election_timeout_max_secs(),
            rpc_timeout_secs: default_rpc_timeout_secs(),
            quic_keep_alive_secs: default_quic_keep_alive_secs(),
            quic_idle_timeout_secs: default_quic_idle_timeout_secs(),
            quic_max_bi_streams: default_quic_max_bi_streams(),
            quic_max_uni_streams: default_quic_max_uni_streams(),
            quic_receive_window: default_quic_receive_window(),
            quic_send_window: default_quic_send_window(),
            quic_stream_receive_window: default_quic_stream_receive_window(),
        }
    }
}

fn default_raft_tick_interval_ms() -> u64 {
    10
}
fn default_election_timeout_min_secs() -> u64 {
    60
}
fn default_election_timeout_max_secs() -> u64 {
    120
}
fn default_rpc_timeout_secs() -> u64 {
    5
}
fn default_quic_keep_alive_secs() -> u64 {
    5
}
fn default_quic_idle_timeout_secs() -> u64 {
    30
}
fn default_quic_max_bi_streams() -> u32 {
    256
}
fn default_quic_max_uni_streams() -> u32 {
    256
}
fn default_quic_receive_window() -> u32 {
    16 * 1024 * 1024
}
fn default_quic_send_window() -> u32 {
    16 * 1024 * 1024
}
fn default_quic_stream_receive_window() -> u32 {
    4 * 1024 * 1024
}
