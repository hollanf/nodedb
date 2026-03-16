//! Metrics export for the memory governor.
//!
//! Provides jemalloc introspection to report actual RSS, mapped memory,
//! and arena statistics alongside the governor's logical budget tracking.

/// System memory statistics from jemalloc.
#[derive(Debug, Clone)]
pub struct SystemMemoryStats {
    /// Resident Set Size — actual physical memory used.
    pub rss_bytes: usize,

    /// Total bytes allocated by the application.
    pub allocated_bytes: usize,

    /// Total bytes in active pages (mapped and potentially dirty).
    pub active_bytes: usize,

    /// Total bytes mapped by the allocator (may exceed active).
    pub mapped_bytes: usize,

    /// Total bytes retained in the allocator's caches.
    pub retained_bytes: usize,
}

impl SystemMemoryStats {
    /// Query jemalloc for current system memory statistics.
    ///
    /// Returns `None` if jemalloc introspection is unavailable.
    pub fn query() -> Option<Self> {
        // Trigger a stats epoch refresh.
        let _ = tikv_jemalloc_ctl::epoch::advance();

        let allocated = tikv_jemalloc_ctl::stats::allocated::read().ok()?;
        let active = tikv_jemalloc_ctl::stats::active::read().ok()?;
        let mapped = tikv_jemalloc_ctl::stats::mapped::read().ok()?;
        let retained = tikv_jemalloc_ctl::stats::retained::read().ok()?;
        let resident = tikv_jemalloc_ctl::stats::resident::read().ok()?;

        Some(Self {
            rss_bytes: resident,
            allocated_bytes: allocated,
            active_bytes: active,
            mapped_bytes: mapped,
            retained_bytes: retained,
        })
    }
}
