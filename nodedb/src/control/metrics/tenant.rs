//! Per-tenant quota usage tracking.

/// Per-tenant resource quota usage.
#[derive(Debug)]
pub struct TenantQuotaMetrics {
    pub tenant_id: u64,
    pub memory_bytes_used: u64,
    pub memory_bytes_limit: u64,
    pub storage_bytes_used: u64,
    pub storage_bytes_limit: u64,
    pub qps_current: u64,
    pub qps_limit: u64,
    pub connections_active: u64,
    pub connections_limit: u64,
}

impl TenantQuotaMetrics {
    /// Whether any quota is exceeded.
    pub fn is_over_quota(&self) -> bool {
        (self.memory_bytes_limit > 0 && self.memory_bytes_used > self.memory_bytes_limit)
            || (self.storage_bytes_limit > 0 && self.storage_bytes_used > self.storage_bytes_limit)
            || (self.qps_limit > 0 && self.qps_current > self.qps_limit)
            || (self.connections_limit > 0 && self.connections_active > self.connections_limit)
    }

    /// Utilization as a percentage (0–100) of the most constrained resource.
    pub fn max_utilization_pct(&self) -> u8 {
        let mut max = 0.0f64;
        if self.memory_bytes_limit > 0 {
            max = max.max(self.memory_bytes_used as f64 / self.memory_bytes_limit as f64);
        }
        if self.storage_bytes_limit > 0 {
            max = max.max(self.storage_bytes_used as f64 / self.storage_bytes_limit as f64);
        }
        if self.qps_limit > 0 {
            max = max.max(self.qps_current as f64 / self.qps_limit as f64);
        }
        (max * 100.0).min(100.0) as u8
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tenant_quota_check() {
        let q = TenantQuotaMetrics {
            tenant_id: 1,
            memory_bytes_used: 800,
            memory_bytes_limit: 1000,
            storage_bytes_used: 500,
            storage_bytes_limit: 1000,
            qps_current: 50,
            qps_limit: 100,
            connections_active: 5,
            connections_limit: 10,
        };
        assert!(!q.is_over_quota());
        assert_eq!(q.max_utilization_pct(), 80);

        let over = TenantQuotaMetrics {
            memory_bytes_used: 1100,
            ..q
        };
        assert!(over.is_over_quota());
    }
}
