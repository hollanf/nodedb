//! WAL slab pinning budget: per-core memory limit for frozen WAL slabs.
//!
//! WriteEvent payloads (`new_value` / `old_value`) are `Arc<[u8]>` references
//! to frozen WAL slab memory. As long as any consumer holds an Arc to a slab,
//! that slab cannot be recycled. If a slow consumer falls behind, it pins
//! arbitrarily large amounts of WAL memory.
//!
//! This budget enforces a per-core limit (default 128 MB). When exceeded,
//! the slowest consumer (highest slab-pin estimate) is forcibly shed:
//! suspended and its held Arcs dropped. The shed consumer recovers via
//! WAL Catchup Mode (mmap reads from disk, no slab pinning).
//!
//! **No heap copying under pressure.** Copying payloads to heap would spike
//! CPU during the same memory pressure that caused the overflow, creating
//! a death spiral. Shedding is the correct response.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// Fallback slab pinning budget per core when system memory cannot be detected.
const FALLBACK_BUDGET_PER_CORE: u64 = 128 * 1024 * 1024; // 128 MB

/// Fraction of total system memory allocated to Event Plane slab budgets.
/// The total slab budget is `system_memory * SLAB_MEMORY_FRACTION`, then
/// divided evenly across cores.
const SLAB_MEMORY_FRACTION: f64 = 0.10; // 10% of system RAM

/// Minimum slab budget per core (prevents starvation on tiny systems).
const MIN_BUDGET_PER_CORE: u64 = 32 * 1024 * 1024; // 32 MB

/// Maximum slab budget per core (prevents excessive pinning on large systems).
const MAX_BUDGET_PER_CORE: u64 = 1024 * 1024 * 1024; // 1 GB

/// Per-consumer slab-pin accounting.
///
/// Tracks the estimated bytes of WAL slab memory pinned by Arc<[u8]>
/// references held in the consumer's processing pipeline.
pub struct ConsumerSlabAccount {
    /// Estimated bytes currently pinned by this consumer.
    pinned_bytes: AtomicU64,
    /// Whether this consumer has been shed (forcibly suspended).
    shed: AtomicBool,
    /// Core ID (for logging).
    core_id: usize,
}

impl ConsumerSlabAccount {
    pub fn new(core_id: usize) -> Self {
        Self {
            pinned_bytes: AtomicU64::new(0),
            shed: AtomicBool::new(false),
            core_id,
        }
    }

    /// Record that the consumer received events with this total payload size.
    pub fn add_pinned(&self, bytes: u64) {
        self.pinned_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record that the consumer released events (processed and dropped Arcs).
    ///
    /// Caller must not release more than was added. In dev builds, a
    /// debug_assert fires on over-release to catch accounting bugs.
    pub fn release_pinned(&self, bytes: u64) {
        let current = self.pinned_bytes.load(Ordering::Relaxed);
        debug_assert!(
            bytes <= current,
            "slab over-release: releasing {bytes} but only {current} pinned (core {})",
            self.core_id,
        );
        self.pinned_bytes
            .fetch_sub(bytes.min(current), Ordering::Relaxed);
    }

    /// Reset pinned bytes to zero (after entering WAL Catchup Mode — all Arcs dropped).
    pub fn reset(&self) {
        self.pinned_bytes.store(0, Ordering::Relaxed);
    }

    /// Current estimated pinned bytes.
    pub fn pinned_bytes(&self) -> u64 {
        self.pinned_bytes.load(Ordering::Relaxed)
    }

    /// Whether this consumer has been shed.
    pub fn is_shed(&self) -> bool {
        self.shed.load(Ordering::Relaxed)
    }

    /// Mark this consumer as shed (called by the budget enforcer).
    pub fn mark_shed(&self) {
        self.shed.store(true, Ordering::Relaxed);
    }

    /// Clear the shed flag (called after consumer enters WAL Catchup Mode).
    pub fn clear_shed(&self) {
        self.shed.store(false, Ordering::Relaxed);
    }

    /// Core ID.
    pub fn core_id(&self) -> usize {
        self.core_id
    }
}

/// Per-core slab pinning budget.
///
/// Shared across all consumers. The `check_and_shed()` method identifies
/// the slowest consumer (highest pinned bytes) when the total exceeds
/// the budget, and marks it for shedding.
pub struct SlabBudget {
    /// Maximum allowed pinned slab bytes per core.
    limit: u64,
    /// Total sheds performed (monotonic counter).
    total_sheds: AtomicU64,
}

impl SlabBudget {
    /// Create a slab budget with the fallback per-core limit (128 MB).
    pub fn new() -> Self {
        Self {
            limit: FALLBACK_BUDGET_PER_CORE,
            total_sheds: AtomicU64::new(0),
        }
    }

    /// Create a slab budget auto-tuned from system memory.
    ///
    /// Allocates `SLAB_MEMORY_FRACTION` (10%) of total system RAM, divided
    /// evenly across `num_cores`. Clamped to [32 MB, 1 GB] per core.
    /// Falls back to 128 MB if system memory cannot be detected.
    pub fn for_cores(num_cores: usize) -> Self {
        let cores = num_cores.max(1) as u64;
        let limit = match detect_system_memory_bytes() {
            Some(total_mem) => {
                let total_slab = (total_mem as f64 * SLAB_MEMORY_FRACTION) as u64;
                let per_core = total_slab / cores;
                per_core.clamp(MIN_BUDGET_PER_CORE, MAX_BUDGET_PER_CORE)
            }
            None => FALLBACK_BUDGET_PER_CORE,
        };

        tracing::info!(
            limit_mb = limit / (1024 * 1024),
            num_cores,
            "slab budget auto-tuned from system memory"
        );

        Self {
            limit,
            total_sheds: AtomicU64::new(0),
        }
    }

    pub fn with_limit(limit: u64) -> Self {
        Self {
            limit,
            total_sheds: AtomicU64::new(0),
        }
    }

    /// Check if the total pinned bytes across consumers exceeds the budget.
    /// If so, shed the slowest consumer (highest pinned bytes).
    ///
    /// Returns the core_id of the shed consumer, or None if within budget.
    pub fn check_and_shed(&self, accounts: &[&ConsumerSlabAccount]) -> Option<usize> {
        let total: u64 = accounts.iter().map(|a| a.pinned_bytes()).sum();

        if total <= self.limit {
            return None;
        }

        // Find the consumer with the highest pinned bytes (slowest).
        let slowest = accounts
            .iter()
            .filter(|a| !a.is_shed()) // Don't re-shed already-shed consumers.
            .max_by_key(|a| a.pinned_bytes())?;

        if slowest.pinned_bytes() == 0 {
            return None; // All consumers are caught up — budget exceeded by other allocations.
        }

        slowest.mark_shed();
        self.total_sheds.fetch_add(1, Ordering::Relaxed);

        tracing::warn!(
            core_id = slowest.core_id(),
            pinned_mb = slowest.pinned_bytes() / (1024 * 1024),
            total_mb = total / (1024 * 1024),
            limit_mb = self.limit / (1024 * 1024),
            "slab budget exceeded — shedding slowest consumer"
        );

        Some(slowest.core_id())
    }

    /// Budget limit in bytes.
    pub fn limit(&self) -> u64 {
        self.limit
    }

    /// Total sheds performed since startup.
    pub fn total_sheds(&self) -> u64 {
        self.total_sheds.load(Ordering::Relaxed)
    }
}

impl Default for SlabBudget {
    fn default() -> Self {
        Self::new()
    }
}

/// Detect total system memory in bytes by reading `/proc/meminfo`.
///
/// Returns `None` if the file cannot be read or parsed (non-Linux, containers
/// with restricted procfs, etc.). Callers should fall back to a hardcoded default.
fn detect_system_memory_bytes() -> Option<u64> {
    let contents = std::fs::read_to_string("/proc/meminfo").ok()?;
    for line in contents.lines() {
        if let Some(rest) = line.strip_prefix("MemTotal:") {
            // Format: "MemTotal:       16384000 kB"
            let kb_str = rest.trim().strip_suffix("kB")?.trim();
            let kb: u64 = kb_str.parse().ok()?;
            return Some(kb * 1024);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn within_budget_no_shed() {
        let budget = SlabBudget::with_limit(1000);
        let a0 = ConsumerSlabAccount::new(0);
        let a1 = ConsumerSlabAccount::new(1);
        a0.add_pinned(200);
        a1.add_pinned(300);

        assert!(budget.check_and_shed(&[&a0, &a1]).is_none());
    }

    #[test]
    fn over_budget_sheds_slowest() {
        let budget = SlabBudget::with_limit(1000);
        let a0 = ConsumerSlabAccount::new(0);
        let a1 = ConsumerSlabAccount::new(1);
        a0.add_pinned(400);
        a1.add_pinned(700); // Total 1100 > 1000.

        let shed = budget.check_and_shed(&[&a0, &a1]);
        assert_eq!(shed, Some(1)); // Consumer 1 is slowest.
        assert!(a1.is_shed());
        assert!(!a0.is_shed());
        assert_eq!(budget.total_sheds(), 1);
    }

    #[test]
    fn already_shed_not_re_shed() {
        let budget = SlabBudget::with_limit(1000);
        let a0 = ConsumerSlabAccount::new(0);
        let a1 = ConsumerSlabAccount::new(1);
        a0.add_pinned(600);
        a1.add_pinned(700);
        a1.mark_shed(); // Already shed.

        // Should shed a0 instead (a1 is already shed).
        let shed = budget.check_and_shed(&[&a0, &a1]);
        assert_eq!(shed, Some(0));
    }

    #[test]
    fn release_pinned_bytes() {
        let account = ConsumerSlabAccount::new(0);
        account.add_pinned(500);
        assert_eq!(account.pinned_bytes(), 500);
        account.release_pinned(200);
        assert_eq!(account.pinned_bytes(), 300);
        account.reset();
        assert_eq!(account.pinned_bytes(), 0);
    }

    #[test]
    fn shed_and_clear() {
        let account = ConsumerSlabAccount::new(0);
        assert!(!account.is_shed());
        account.mark_shed();
        assert!(account.is_shed());
        account.clear_shed();
        assert!(!account.is_shed());
    }

    #[test]
    fn default_budget_128mb() {
        let budget = SlabBudget::new();
        assert_eq!(budget.limit(), 128 * 1024 * 1024);
    }

    #[test]
    fn for_cores_auto_tunes() {
        let budget = SlabBudget::for_cores(4);
        // On any real system, the limit should be within [32 MB, 1 GB].
        assert!(budget.limit() >= MIN_BUDGET_PER_CORE);
        assert!(budget.limit() <= MAX_BUDGET_PER_CORE);
    }

    #[test]
    fn for_cores_zero_treated_as_one() {
        let budget = SlabBudget::for_cores(0);
        assert!(budget.limit() >= MIN_BUDGET_PER_CORE);
    }

    #[test]
    fn detect_system_memory() {
        // On Linux, /proc/meminfo should exist and return > 0.
        if cfg!(target_os = "linux") {
            let mem = detect_system_memory_bytes();
            assert!(mem.is_some(), "should detect memory on Linux");
            assert!(mem.unwrap() > 0);
        }
    }
}
