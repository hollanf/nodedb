//! Webhook delivery configuration types.

use serde::{Deserialize, Serialize};

/// Webhook delivery configuration for a change stream.
///
/// When set on a stream, the Event Plane spawns a background task that
/// POSTs each event to the configured URL with retry and DLQ.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct WebhookConfig {
    /// Target URL to POST events to.
    pub url: String,
    /// Maximum retry attempts before DLQ. Default: 3.
    pub max_retries: u32,
    /// Per-request timeout in seconds. Default: 5.
    pub timeout_secs: u64,
    /// Optional custom headers (key → value).
    #[serde(default)]
    pub headers: Vec<(String, String)>,
}

impl Default for WebhookConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            max_retries: 3,
            timeout_secs: 5,
            headers: Vec::new(),
        }
    }
}

impl WebhookConfig {
    pub fn is_configured(&self) -> bool {
        !self.url.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_not_configured() {
        let cfg = WebhookConfig::default();
        assert!(!cfg.is_configured());
        assert_eq!(cfg.max_retries, 3);
        assert_eq!(cfg.timeout_secs, 5);
        assert!(cfg.headers.is_empty());
    }

    #[test]
    fn with_url_is_configured() {
        let cfg = WebhookConfig {
            url: "https://example.com/hook".into(),
            ..Default::default()
        };
        assert!(cfg.is_configured());
    }

    #[test]
    fn serde_roundtrip() {
        let cfg = WebhookConfig {
            url: "https://example.com".into(),
            max_retries: 5,
            timeout_secs: 10,
            headers: vec![("Authorization".into(), "Bearer tok".into())],
        };
        let bytes = serde_json::to_vec(&cfg).unwrap();
        let decoded: WebhookConfig = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(decoded.url, cfg.url);
        assert_eq!(decoded.max_retries, 5);
        assert_eq!(decoded.headers.len(), 1);
    }

    #[test]
    fn deserialize_missing_headers_defaults_to_empty() {
        let json = r#"{"url":"http://x","max_retries":1,"timeout_secs":2}"#;
        let cfg: WebhookConfig = serde_json::from_str(json).unwrap();
        assert!(cfg.headers.is_empty());
    }
}
