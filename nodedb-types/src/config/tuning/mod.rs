mod data_plane;
mod engines;
mod network;

pub use data_plane::{DataPlaneTuning, QueryTuning};
pub use engines::{GraphTuning, SparseTuning, TimeseriesToning, VectorTuning};
pub use network::{BridgeTuning, ClusterTransportTuning, NetworkTuning, WalTuning};

use serde::{Deserialize, Serialize};

/// Top-level tuning configuration.
///
/// All fields have sensible defaults derived from the current hardcoded values.
/// Override via the `[tuning]` section in `config.toml`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TuningConfig {
    #[serde(default)]
    pub data_plane: DataPlaneTuning,
    #[serde(default)]
    pub query: QueryTuning,
    #[serde(default)]
    pub vector: VectorTuning,
    #[serde(default)]
    pub sparse: SparseTuning,
    #[serde(default)]
    pub graph: GraphTuning,
    #[serde(default)]
    pub timeseries: TimeseriesToning,
    #[serde(default)]
    pub bridge: BridgeTuning,
    #[serde(default)]
    pub network: NetworkTuning,
    #[serde(default)]
    pub wal: WalTuning,
    #[serde(default)]
    pub cluster_transport: ClusterTransportTuning,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_tuning_roundtrip() {
        let cfg = TuningConfig::default();
        let toml_str = toml::to_string_pretty(&cfg).expect("serialize");
        let parsed: TuningConfig = toml::from_str(&toml_str).expect("deserialize");

        assert_eq!(parsed.data_plane.idle_poll_timeout_ms, 100);
        assert_eq!(parsed.query.sort_run_size, 100_000);
        assert_eq!(parsed.vector.flat_index_threshold, 10_000);
        assert_eq!(parsed.sparse.bm25_k1, 1.2);
        assert_eq!(parsed.graph.max_visited, 100_000);
        assert_eq!(parsed.timeseries.memtable_budget_bytes, 64 * 1024 * 1024);
        assert_eq!(parsed.bridge.slab_page_size, 64 * 1024);
        assert_eq!(parsed.network.default_deadline_secs, 30);
        assert_eq!(parsed.wal.write_buffer_size, 256 * 1024);
        assert_eq!(parsed.cluster_transport.raft_tick_interval_ms, 10);
    }

    #[test]
    fn partial_override() {
        let toml_str = r#"
[query]
sort_run_size = 50000

[network]
default_deadline_secs = 60
"#;
        let cfg: TuningConfig = toml::from_str(toml_str).expect("deserialize");
        assert_eq!(cfg.query.sort_run_size, 50_000);
        assert_eq!(cfg.network.default_deadline_secs, 60);
        assert_eq!(cfg.query.aggregate_scan_cap, 10_000_000);
        assert_eq!(cfg.vector.seal_threshold, 65_536);
    }

    #[test]
    fn empty_toml_yields_defaults() {
        let cfg: TuningConfig = toml::from_str("").expect("deserialize");
        assert_eq!(cfg.data_plane.max_consecutive_panics, 3);
        assert_eq!(cfg.graph.max_depth, 10);
    }
}
