//! Per-tenant WASM quota enforcement.
//!
//! Separates the enforcement lifecycle into two steps that bracket execution:
//!
//! 1. [`WasmQuotaEnforcer::check`] — pre-flight before WASM runs. Rejects if the
//!    tenant has hit their invocation count limit.
//! 2. [`WasmQuotaEnforcer::record`] — post-execution after wasmtime returns. Records
//!    actual fuel consumed and rejects if the cumulative fuel quota is exceeded.
//!
//! Per-invocation fuel limits (hard execution caps) are handled separately by
//! wasmtime's fuel metering in [`super::fuel`]. This enforcer tracks accumulated
//! usage across invocations for tenant-level quota enforcement.

use std::collections::HashMap;
use std::sync::Mutex;

/// Quota limits applied per tenant.
///
/// `None` means unlimited for that dimension.
#[derive(Debug, Clone, Default)]
pub struct QuotaLimits {
    /// Maximum total invocations (across all functions, all time). `None` = unlimited.
    pub max_invocations: Option<u64>,
    /// Maximum total fuel consumed (across all functions, all time). `None` = unlimited.
    pub max_fuel: Option<u64>,
}

/// Per-function usage counters.
#[derive(Debug, Clone, Default)]
pub struct FunctionUsage {
    /// Total committed invocations.
    pub invocations: u64,
    /// Total fuel consumed across all committed invocations.
    pub fuel_consumed: u64,
}

/// Per-tenant WASM quota enforcer.
///
/// Thread-safe. Shared across all DataFusion UDF invocations on the Control Plane.
pub struct WasmQuotaEnforcer {
    /// (tenant_id, function_name) → accumulated usage.
    usage: Mutex<HashMap<(u64, String), FunctionUsage>>,
    /// Default limits when no per-tenant override is set.
    default_limits: QuotaLimits,
    /// Per-tenant limit overrides (set via admin DDL or config).
    tenant_limits: Mutex<HashMap<u64, QuotaLimits>>,
}

impl WasmQuotaEnforcer {
    pub fn new(default_limits: QuotaLimits) -> Self {
        Self {
            usage: Mutex::new(HashMap::new()),
            default_limits,
            tenant_limits: Mutex::new(HashMap::new()),
        }
    }

    /// Override the quota limits for a specific tenant.
    pub fn set_tenant_limits(&self, tenant_id: u64, limits: QuotaLimits) {
        self.tenant_limits
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(tenant_id, limits);
    }

    /// Pre-flight check — call this **before** executing WASM.
    ///
    /// Rejects if the tenant has reached their maximum invocation count.
    /// Does not modify any counters.
    pub fn check(&self, tenant_id: u64, function_name: &str) -> crate::Result<()> {
        let limits = self.limits_for(tenant_id);
        let Some(max_inv) = limits.max_invocations else {
            return Ok(());
        };

        let map = self.usage.lock().unwrap_or_else(|p| p.into_inner());
        let current = map
            .get(&(tenant_id, function_name.to_string()))
            .map(|u| u.invocations)
            .unwrap_or(0);

        if current >= max_inv {
            return Err(crate::Error::ExecutionLimitExceeded {
                detail: format!(
                    "WASM UDF '{function_name}' invocation quota ({max_inv}) exceeded for tenant {tenant_id}"
                ),
            });
        }
        Ok(())
    }

    /// Post-execution recording — call this **after** WASM returns successfully.
    ///
    /// Records the invocation and fuel consumed. Rejects if the cumulative fuel
    /// quota is exceeded, returning an error that the caller should surface to
    /// the client (the invocation already happened but will not be billed again).
    pub fn record(
        &self,
        tenant_id: u64,
        function_name: &str,
        fuel_consumed: u64,
    ) -> crate::Result<()> {
        let limits = self.limits_for(tenant_id);

        let mut map = self.usage.lock().unwrap_or_else(|p| p.into_inner());
        let entry = map
            .entry((tenant_id, function_name.to_string()))
            .or_default();

        if let Some(max_fuel) = limits.max_fuel {
            let new_total = entry.fuel_consumed.saturating_add(fuel_consumed);
            if new_total > max_fuel {
                return Err(crate::Error::ExecutionLimitExceeded {
                    detail: format!(
                        "WASM UDF '{function_name}' fuel quota ({max_fuel}) exceeded for tenant {tenant_id}"
                    ),
                });
            }
        }

        entry.invocations += 1;
        entry.fuel_consumed += fuel_consumed;
        Ok(())
    }

    /// Get usage for a specific (tenant, function) pair.
    pub fn get_usage(&self, tenant_id: u64, function_name: &str) -> FunctionUsage {
        let map = self.usage.lock().unwrap_or_else(|p| p.into_inner());
        map.get(&(tenant_id, function_name.to_string()))
            .cloned()
            .unwrap_or_default()
    }

    /// Get all usage entries for a tenant.
    pub fn get_tenant_usage(&self, tenant_id: u64) -> Vec<(String, FunctionUsage)> {
        let map = self.usage.lock().unwrap_or_else(|p| p.into_inner());
        map.iter()
            .filter(|((tid, _), _)| *tid == tenant_id)
            .map(|((_, name), usage)| (name.clone(), usage.clone()))
            .collect()
    }

    /// Reset all usage counters (for periodic window reset or testing).
    pub fn clear(&self) {
        self.usage.lock().unwrap_or_else(|p| p.into_inner()).clear();
    }

    fn limits_for(&self, tenant_id: u64) -> QuotaLimits {
        self.tenant_limits
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&tenant_id)
            .cloned()
            .unwrap_or_else(|| self.default_limits.clone())
    }
}

impl Default for WasmQuotaEnforcer {
    fn default() -> Self {
        Self::new(QuotaLimits::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unlimited() -> WasmQuotaEnforcer {
        WasmQuotaEnforcer::default()
    }

    fn with_limits(max_inv: Option<u64>, max_fuel: Option<u64>) -> WasmQuotaEnforcer {
        WasmQuotaEnforcer::new(QuotaLimits {
            max_invocations: max_inv,
            max_fuel,
        })
    }

    #[test]
    fn unlimited_always_passes() {
        let e = unlimited();
        assert!(e.check(1, "add").is_ok());
        assert!(e.record(1, "add", 1_000_000).is_ok());
        assert!(e.check(1, "add").is_ok());
    }

    #[test]
    fn invocation_quota_enforced_on_check() {
        let e = with_limits(Some(2), None);
        e.record(1, "f", 100).unwrap();
        e.record(1, "f", 100).unwrap();
        // Two invocations recorded — next check must reject.
        assert!(e.check(1, "f").is_err());
    }

    #[test]
    fn fuel_quota_enforced_on_record() {
        let e = with_limits(None, Some(500));
        e.record(1, "f", 300).unwrap();
        // 300 + 300 = 600 > 500 → reject.
        assert!(e.record(1, "f", 300).is_err());
    }

    #[test]
    fn fuel_within_limit_passes() {
        let e = with_limits(None, Some(1000));
        assert!(e.record(1, "f", 400).is_ok());
        assert!(e.record(1, "f", 400).is_ok());
        // 800 total, still under 1000.
        assert!(e.check(1, "f").is_ok());
    }

    #[test]
    fn tenant_isolation() {
        let e = with_limits(Some(1), None);
        e.record(1, "f", 100).unwrap();
        // Tenant 1 is at quota, tenant 2 is unaffected.
        assert!(e.check(1, "f").is_err());
        assert!(e.check(2, "f").is_ok());
    }

    #[test]
    fn per_tenant_override() {
        let e = with_limits(Some(1), None);
        e.set_tenant_limits(
            42,
            QuotaLimits {
                max_invocations: Some(100),
                max_fuel: None,
            },
        );
        // Tenant 42 has a higher limit.
        for _ in 0..50 {
            e.record(42, "f", 0).unwrap();
        }
        assert!(e.check(42, "f").is_ok());
    }

    #[test]
    fn usage_query_correct() {
        let e = unlimited();
        e.record(1, "add", 500).unwrap();
        e.record(1, "add", 300).unwrap();
        e.record(1, "mul", 100).unwrap();

        let add = e.get_usage(1, "add");
        assert_eq!(add.invocations, 2);
        assert_eq!(add.fuel_consumed, 800);

        let mul = e.get_usage(1, "mul");
        assert_eq!(mul.invocations, 1);
        assert_eq!(mul.fuel_consumed, 100);
    }

    #[test]
    fn tenant_usage_list() {
        let e = unlimited();
        e.record(1, "a", 10).unwrap();
        e.record(1, "b", 20).unwrap();
        e.record(2, "c", 30).unwrap();

        let t1 = e.get_tenant_usage(1);
        assert_eq!(t1.len(), 2);
        let t2 = e.get_tenant_usage(2);
        assert_eq!(t2.len(), 1);
    }

    #[test]
    fn clear_resets_counters() {
        let e = with_limits(Some(1), None);
        e.record(1, "f", 100).unwrap();
        assert!(e.check(1, "f").is_err());
        e.clear();
        assert!(e.check(1, "f").is_ok());
    }
}
