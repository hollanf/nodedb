//! Usage quota enforcement: hard (block), soft (warn), throttle, overage.

use std::collections::HashMap;
use std::sync::RwLock;

use serde::{Deserialize, Serialize};
use tracing::{info, warn};

/// Quota enforcement mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum QuotaEnforcement {
    /// Block requests when quota exceeded.
    Hard,
    /// Log warning but allow requests.
    Soft,
    /// Throttle (reduce rate limit) when nearing quota.
    Throttle,
    /// Allow overage with per-token billing.
    Overage,
}

/// A quota definition attached to a scope.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuotaDefinition {
    /// Scope this quota applies to.
    pub scope_name: String,
    /// Maximum tokens per period.
    pub max_tokens: u64,
    /// Period in seconds (e.g., 2592000 = 30 days).
    pub period_secs: u64,
    /// Enforcement mode.
    pub enforcement: QuotaEnforcement,
    /// Warning threshold (0.0-1.0). Default: 0.8 (80%).
    pub warning_threshold: f64,
}

/// Quota status for a user/org.
#[derive(Debug, Clone)]
pub struct QuotaStatus {
    pub scope_name: String,
    pub max_tokens: u64,
    pub used_tokens: u64,
    pub remaining: u64,
    pub pct_used: f64,
    pub enforcement: QuotaEnforcement,
    pub exceeded: bool,
    pub warning: bool,
}

/// Quota manager: tracks usage against quota definitions.
pub struct QuotaManager {
    /// scope_name → quota definition.
    quotas: RwLock<HashMap<String, QuotaDefinition>>,
    /// "{scope_name}:{grantee_id}" → tokens used in current period.
    usage: RwLock<HashMap<String, u64>>,
}

impl QuotaManager {
    pub fn new() -> Self {
        Self {
            quotas: RwLock::new(HashMap::new()),
            usage: RwLock::new(HashMap::new()),
        }
    }

    /// Define or update a quota for a scope.
    pub fn define_quota(&self, quota: QuotaDefinition) {
        let mut quotas = self.quotas.write().unwrap_or_else(|p| p.into_inner());
        quotas.insert(quota.scope_name.clone(), quota);
    }

    /// Remove a quota definition.
    pub fn remove_quota(&self, scope_name: &str) -> bool {
        let mut quotas = self.quotas.write().unwrap_or_else(|p| p.into_inner());
        quotas.remove(scope_name).is_some()
    }

    /// Record token usage against a quota.
    pub fn record_usage(&self, scope_name: &str, grantee_id: &str, tokens: u64) {
        let key = format!("{scope_name}:{grantee_id}");
        let mut usage = self.usage.write().unwrap_or_else(|p| p.into_inner());
        *usage.entry(key).or_insert(0) += tokens;
    }

    /// Check if a request should be allowed based on quota.
    ///
    /// Returns `Ok(())` if allowed, `Err` with quota status if blocked.
    pub fn check_quota(
        &self,
        scope_name: &str,
        grantee_id: &str,
        additional_tokens: u64,
    ) -> Result<(), QuotaStatus> {
        let quotas = self.quotas.read().unwrap_or_else(|p| p.into_inner());
        let Some(quota) = quotas.get(scope_name) else {
            return Ok(()); // No quota defined → allow.
        };

        let key = format!("{scope_name}:{grantee_id}");
        let usage = self.usage.read().unwrap_or_else(|p| p.into_inner());
        let used = *usage.get(&key).unwrap_or(&0);
        let projected = used + additional_tokens;

        let pct = if quota.max_tokens > 0 {
            used as f64 / quota.max_tokens as f64
        } else {
            0.0
        };

        let status = QuotaStatus {
            scope_name: scope_name.into(),
            max_tokens: quota.max_tokens,
            used_tokens: used,
            remaining: quota.max_tokens.saturating_sub(used),
            pct_used: pct,
            enforcement: quota.enforcement,
            exceeded: projected > quota.max_tokens,
            warning: pct >= quota.warning_threshold,
        };

        if status.warning && !status.exceeded {
            warn!(
                scope = %scope_name,
                grantee = %grantee_id,
                pct = format!("{:.0}%", pct * 100.0),
                "quota warning threshold reached"
            );
        }

        if status.exceeded {
            match quota.enforcement {
                QuotaEnforcement::Hard => return Err(status),
                QuotaEnforcement::Soft => {
                    warn!(scope = %scope_name, "quota exceeded (soft enforcement — allowing)");
                }
                QuotaEnforcement::Throttle => {
                    // Caller should reduce rate limit.
                    info!(scope = %scope_name, "quota exceeded — throttling");
                }
                QuotaEnforcement::Overage => {
                    info!(scope = %scope_name, "quota exceeded — overage billing");
                }
            }
        }

        Ok(())
    }

    /// Get quota status for a user/org.
    pub fn get_status(&self, scope_name: &str, grantee_id: &str) -> Option<QuotaStatus> {
        let quotas = self.quotas.read().unwrap_or_else(|p| p.into_inner());
        let quota = quotas.get(scope_name)?;

        let key = format!("{scope_name}:{grantee_id}");
        let usage = self.usage.read().unwrap_or_else(|p| p.into_inner());
        let used = *usage.get(&key).unwrap_or(&0);

        let pct = if quota.max_tokens > 0 {
            used as f64 / quota.max_tokens as f64
        } else {
            0.0
        };

        Some(QuotaStatus {
            scope_name: scope_name.into(),
            max_tokens: quota.max_tokens,
            used_tokens: used,
            remaining: quota.max_tokens.saturating_sub(used),
            pct_used: pct,
            enforcement: quota.enforcement,
            exceeded: used > quota.max_tokens,
            warning: pct >= quota.warning_threshold,
        })
    }

    /// List all quota definitions.
    pub fn list_quotas(&self) -> Vec<QuotaDefinition> {
        let quotas = self.quotas.read().unwrap_or_else(|p| p.into_inner());
        quotas.values().cloned().collect()
    }

    /// Reset usage counters for a new billing period.
    pub fn reset_period(&self, scope_name: &str) {
        let prefix = format!("{scope_name}:");
        let mut usage = self.usage.write().unwrap_or_else(|p| p.into_inner());
        usage.retain(|k, _| !k.starts_with(&prefix));
    }
}

impl Default for QuotaManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hard_quota_blocks() {
        let mgr = QuotaManager::new();
        mgr.define_quota(QuotaDefinition {
            scope_name: "free".into(),
            max_tokens: 100,
            period_secs: 86400,
            enforcement: QuotaEnforcement::Hard,
            warning_threshold: 0.8,
        });

        // Use 90 tokens.
        mgr.record_usage("free", "u1", 90);
        assert!(mgr.check_quota("free", "u1", 5).is_ok());

        // Try to use 20 more → exceeds 100.
        assert!(mgr.check_quota("free", "u1", 20).is_err());
    }

    #[test]
    fn soft_quota_allows() {
        let mgr = QuotaManager::new();
        mgr.define_quota(QuotaDefinition {
            scope_name: "free".into(),
            max_tokens: 100,
            period_secs: 86400,
            enforcement: QuotaEnforcement::Soft,
            warning_threshold: 0.8,
        });

        mgr.record_usage("free", "u1", 200);
        assert!(mgr.check_quota("free", "u1", 1).is_ok()); // Soft = allow.
    }

    #[test]
    fn no_quota_allows_all() {
        let mgr = QuotaManager::new();
        assert!(mgr.check_quota("nonexistent", "u1", 999999).is_ok());
    }

    #[test]
    fn quota_status() {
        let mgr = QuotaManager::new();
        mgr.define_quota(QuotaDefinition {
            scope_name: "pro".into(),
            max_tokens: 1000,
            period_secs: 86400,
            enforcement: QuotaEnforcement::Hard,
            warning_threshold: 0.8,
        });
        mgr.record_usage("pro", "u1", 500);

        let status = mgr.get_status("pro", "u1").unwrap();
        assert_eq!(status.used_tokens, 500);
        assert_eq!(status.remaining, 500);
        assert!(!status.exceeded);
        assert!(!status.warning);
    }

    #[test]
    fn reset_period_clears() {
        let mgr = QuotaManager::new();
        mgr.record_usage("free", "u1", 100);
        mgr.reset_period("free");

        let usage = mgr.usage.read().unwrap();
        assert!(!usage.contains_key("free:u1"));
    }
}
