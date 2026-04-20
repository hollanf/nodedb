//! Collection-lifecycle retention configuration.
//!
//! Controls how long a soft-deleted collection (status `is_active =
//! false`, produced by `DROP COLLECTION`) sits in the catalog before
//! the Event-Plane GC sweeper proposes its `PurgeCollection`. During
//! this window `UNDROP COLLECTION` can restore the collection with
//! zero data loss; past the window, storage is reclaimed and UNDROP
//! fails with "retention elapsed".

use serde::{Deserialize, Serialize};

/// System-wide retention + GC settings.
///
/// Example TOML:
///
/// ```toml
/// [retention]
/// deactivated_collection_retention_days = 7
/// gc_sweep_interval_secs = 60
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionSettings {
    /// Default retention window (days) for soft-deleted collections.
    /// Per-tenant `tenant_config.deactivated_collection_retention_days`
    /// overrides this when set. `0` means "purge immediately on next
    /// sweep" — supported, but strongly discouraged outside tests.
    /// Default: 7.
    #[serde(default = "default_deactivated_collection_retention_days")]
    pub deactivated_collection_retention_days: u32,

    /// How often the Event-Plane collection-GC sweeper evaluates
    /// soft-deleted collections (seconds). Each pass scans
    /// `_system.collections` for `is_active = false` rows whose
    /// `deactivated_at + retention_window < now`, and proposes
    /// `CatalogEntry::PurgeCollection` for each.
    /// Default: 60.
    #[serde(default = "default_gc_sweep_interval_secs")]
    pub gc_sweep_interval_secs: u64,
}

impl Default for RetentionSettings {
    fn default() -> Self {
        Self {
            deactivated_collection_retention_days: default_deactivated_collection_retention_days(),
            gc_sweep_interval_secs: default_gc_sweep_interval_secs(),
        }
    }
}

impl RetentionSettings {
    pub fn sweep_interval(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.gc_sweep_interval_secs)
    }

    pub fn retention_window(&self) -> std::time::Duration {
        std::time::Duration::from_secs(
            u64::from(self.deactivated_collection_retention_days) * 24 * 60 * 60,
        )
    }
}

fn default_deactivated_collection_retention_days() -> u32 {
    7
}

fn default_gc_sweep_interval_secs() -> u64 {
    60
}
