//! Configuration file loading.
//!
//! Loads settings from `~/.config/nodedb/config.toml`. CLI flags override
//! config file values, and environment variables override both.

use std::path::PathBuf;

use serde::Deserialize;

/// Persistent CLI configuration.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct CliConfig {
    pub host: Option<String>,
    pub port: Option<u16>,
    pub user: Option<String>,
    pub password: Option<String>,
    pub format: Option<String>,
    pub timing: Option<bool>,
    pub history_size: Option<usize>,
    pub editor: Option<String>,
}

impl CliConfig {
    /// Load from the default config path. Returns `Default` if the file
    /// doesn't exist or can't be parsed.
    pub fn load() -> Self {
        let path = Self::path();
        if !path.exists() {
            return Self::default();
        }
        match std::fs::read_to_string(&path) {
            Ok(contents) => match toml::from_str(&contents) {
                Ok(config) => config,
                Err(e) => {
                    eprintln!("warning: failed to parse {}: {e}", path.display());
                    Self::default()
                }
            },
            Err(_) => Self::default(),
        }
    }

    /// Config file path: `~/.config/nodedb/config.toml`.
    pub fn path() -> PathBuf {
        dirs::config_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join("nodedb")
            .join("config.toml")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let cfg = CliConfig::default();
        assert!(cfg.host.is_none());
        assert!(cfg.port.is_none());
        assert!(cfg.format.is_none());
    }

    #[test]
    fn parse_toml() {
        let toml_str = r#"
host = "db.example.com"
port = 6433
user = "myuser"
format = "json"
timing = true
history_size = 5000
"#;
        let cfg: CliConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(cfg.host.as_deref(), Some("db.example.com"));
        assert_eq!(cfg.port, Some(6433));
        assert_eq!(cfg.format.as_deref(), Some("json"));
        assert_eq!(cfg.timing, Some(true));
        assert_eq!(cfg.history_size, Some(5000));
    }

    #[test]
    fn config_path() {
        let path = CliConfig::path();
        assert!(path.to_str().unwrap().contains("nodedb"));
    }
}
