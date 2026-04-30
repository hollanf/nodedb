//! Tenant ceilings: hard limits that even superusers respect.
//!
//! Ceilings are guardrails that cannot be bypassed by any role.
//! Only a break-glass key can modify ceilings.
//! Audit log deletion is always forbidden.

use std::collections::HashMap;
use std::sync::RwLock;

use serde::{Deserialize, Serialize};
use tracing::info;

/// A ceiling definition for a tenant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantCeiling {
    pub tenant_id: u64,
    /// Maximum number of collections allowed.
    pub max_collections: u64,
    /// Maximum storage in bytes. 0 = unlimited.
    pub max_storage_bytes: u64,
    /// Minimum audit retention in days. Audit entries younger than this
    /// cannot be deleted even by superuser.
    pub audit_min_retention_days: u32,
}

/// Ceiling store: enforced limits that superusers cannot bypass.
pub struct CeilingStore {
    /// tenant_id → ceiling.
    ceilings: RwLock<HashMap<u64, TenantCeiling>>,
}

impl CeilingStore {
    pub fn new() -> Self {
        Self {
            ceilings: RwLock::new(HashMap::new()),
        }
    }

    /// Define or update a ceiling for a tenant.
    ///
    /// In production, this should only be callable with a break-glass key.
    pub fn define(&self, ceiling: TenantCeiling) {
        let tid = ceiling.tenant_id;
        let mut ceilings = self.ceilings.write().unwrap_or_else(|p| p.into_inner());
        info!(
            tenant_id = tid,
            max_collections = ceiling.max_collections,
            max_storage_bytes = ceiling.max_storage_bytes,
            audit_min_retention_days = ceiling.audit_min_retention_days,
            "ceiling defined"
        );
        ceilings.insert(tid, ceiling);
    }

    /// Get the ceiling for a tenant (if defined).
    pub fn get(&self, tenant_id: u64) -> Option<TenantCeiling> {
        let ceilings = self.ceilings.read().unwrap_or_else(|p| p.into_inner());
        ceilings.get(&tenant_id).cloned()
    }

    /// Check if creating a new collection would exceed the ceiling.
    pub fn check_collection_limit(&self, tenant_id: u64, current_count: u64) -> crate::Result<()> {
        let ceilings = self.ceilings.read().unwrap_or_else(|p| p.into_inner());
        if let Some(c) = ceilings.get(&tenant_id)
            && c.max_collections > 0
            && current_count >= c.max_collections
        {
            return Err(crate::Error::RejectedAuthz {
                tenant_id: crate::types::TenantId::new(tenant_id),
                resource: format!("ceiling exceeded: max_collections = {}", c.max_collections),
            });
        }
        Ok(())
    }

    /// Check if adding storage would exceed the ceiling.
    pub fn check_storage_limit(
        &self,
        tenant_id: u64,
        current_bytes: u64,
        additional_bytes: u64,
    ) -> crate::Result<()> {
        let ceilings = self.ceilings.read().unwrap_or_else(|p| p.into_inner());
        if let Some(c) = ceilings.get(&tenant_id)
            && c.max_storage_bytes > 0
            && current_bytes + additional_bytes > c.max_storage_bytes
        {
            return Err(crate::Error::RejectedAuthz {
                tenant_id: crate::types::TenantId::new(tenant_id),
                resource: format!(
                    "ceiling exceeded: max_storage = {} bytes",
                    c.max_storage_bytes
                ),
            });
        }
        Ok(())
    }

    /// Check if an audit entry is protected by the minimum retention ceiling.
    /// Returns `true` if the entry CANNOT be deleted.
    pub fn is_audit_protected(&self, tenant_id: u64, entry_age_days: u32) -> bool {
        let ceilings = self.ceilings.read().unwrap_or_else(|p| p.into_inner());
        if let Some(c) = ceilings.get(&tenant_id) {
            return entry_age_days < c.audit_min_retention_days;
        }
        false
    }

    /// Always returns true — audit log deletion is categorically forbidden.
    /// This is a ceiling enforcement: even superuser cannot DELETE/TRUNCATE audit.
    pub fn is_audit_deletion_forbidden() -> bool {
        true
    }

    /// List all defined ceilings.
    pub fn list(&self) -> Vec<TenantCeiling> {
        let ceilings = self.ceilings.read().unwrap_or_else(|p| p.into_inner());
        ceilings.values().cloned().collect()
    }
}

impl Default for CeilingStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Parse storage size strings like "1TiB", "100GiB", "500MiB" to bytes.
pub fn parse_storage_size(s: &str) -> Option<u64> {
    let s = s.trim();
    if let Some(n) = s.strip_suffix("TiB") {
        n.trim()
            .parse::<u64>()
            .ok()
            .map(|n| n * 1024 * 1024 * 1024 * 1024)
    } else if let Some(n) = s.strip_suffix("GiB") {
        n.trim().parse::<u64>().ok().map(|n| n * 1024 * 1024 * 1024)
    } else if let Some(n) = s.strip_suffix("MiB") {
        n.trim().parse::<u64>().ok().map(|n| n * 1024 * 1024)
    } else {
        s.parse::<u64>().ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ceiling_enforcement() {
        let store = CeilingStore::new();
        store.define(TenantCeiling {
            tenant_id: 1,
            max_collections: 10,
            max_storage_bytes: 1024 * 1024 * 1024,
            audit_min_retention_days: 365,
        });

        assert!(store.check_collection_limit(1, 5).is_ok());
        assert!(store.check_collection_limit(1, 10).is_err());
        assert!(store.check_collection_limit(2, 999).is_ok()); // No ceiling for tenant 2.
    }

    #[test]
    fn audit_always_forbidden() {
        assert!(CeilingStore::is_audit_deletion_forbidden());
    }

    #[test]
    fn parse_sizes() {
        assert_eq!(parse_storage_size("1TiB"), Some(1024 * 1024 * 1024 * 1024));
        assert_eq!(parse_storage_size("100GiB"), Some(100 * 1024 * 1024 * 1024));
        assert_eq!(parse_storage_size("500MiB"), Some(500 * 1024 * 1024));
    }
}
