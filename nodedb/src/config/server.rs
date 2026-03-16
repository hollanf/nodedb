use std::net::SocketAddr;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Top-level server configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// Address to bind the client-facing listener.
    pub listen: SocketAddr,

    /// Data directory for WAL, segments, and indexes.
    pub data_dir: PathBuf,

    /// Number of Data Plane cores. Defaults to available CPUs minus one
    /// (reserving one core for the Control Plane).
    pub data_plane_cores: usize,

    /// Global memory ceiling in bytes. The memory governor enforces this.
    pub memory_limit: usize,

    /// Per-engine budget configuration.
    pub engines: super::EngineConfig,
}

impl Default for ServerConfig {
    fn default() -> Self {
        let cores = std::thread::available_parallelism()
            .map(|n| n.get().saturating_sub(1).max(1))
            .unwrap_or(1);

        Self {
            listen: SocketAddr::from(([127, 0, 0, 1], 5433)),
            data_dir: PathBuf::from("./data"),
            data_plane_cores: cores,
            memory_limit: 1024 * 1024 * 1024, // 1 GiB default
            engines: EngineConfig::default(),
        }
    }
}

impl ServerConfig {
    /// Load configuration from a TOML file, falling back to defaults.
    pub fn from_file(path: &std::path::Path) -> crate::Result<Self> {
        let content = std::fs::read_to_string(path).map_err(|e| crate::Error::Config {
            detail: format!("failed to read config file {}: {e}", path.display()),
        })?;
        toml::from_str(&content).map_err(|e| crate::Error::Config {
            detail: format!("invalid TOML config: {e}"),
        })
    }

    /// WAL directory within the data directory.
    pub fn wal_dir(&self) -> PathBuf {
        self.data_dir.join("wal")
    }

    /// Segments directory within the data directory.
    pub fn segments_dir(&self) -> PathBuf {
        self.data_dir.join("segments")
    }
}

use super::EngineConfig;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_valid() {
        let cfg = ServerConfig::default();
        assert!(cfg.data_plane_cores >= 1);
        assert_eq!(cfg.memory_limit, 1024 * 1024 * 1024);
        assert_eq!(cfg.listen.port(), 5433);
    }

    #[test]
    fn wal_dir_derived() {
        let cfg = ServerConfig::default();
        assert_eq!(cfg.wal_dir(), PathBuf::from("./data/wal"));
    }

    #[test]
    fn toml_roundtrip() {
        let cfg = ServerConfig::default();
        let toml_str = toml::to_string_pretty(&cfg).unwrap();
        let parsed: ServerConfig = toml::from_str(&toml_str).unwrap();
        assert_eq!(parsed.listen, cfg.listen);
        assert_eq!(parsed.memory_limit, cfg.memory_limit);
    }
}
