//! `DroppedCollection` — row shape for `_system.dropped_collections`.
//!
//! Returned by `NodeDb::list_dropped_collections`. Each row describes
//! one soft-deleted collection within its retention window (the
//! `StoredCollection` redb row is still present with `is_active =
//! false`). Once the retention window elapses, the sweeper hard-deletes
//! the row and it disappears from this list.

use serde::{Deserialize, Serialize};

/// A soft-deleted collection awaiting either `UNDROP` or retention-driven
/// hard deletion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DroppedCollection {
    /// Tenant that owns this collection.
    pub tenant_id: u64,
    /// Collection name (unique per tenant while soft-deleted).
    pub name: String,
    /// Preserved owner at the time of `DROP` — the user who can
    /// `UNDROP` without superuser/tenant_admin elevation. Empty
    /// string if the owner row could not be resolved at the time
    /// of the catalog read.
    pub owner: String,
    /// Engine / collection-type slug resolved from
    /// `StoredCollection.collection_type.as_str()`. One of
    /// `"document"`, `"strict"`, `"columnar"`, `"timeseries"`,
    /// `"columnar:spatial"`, `"kv"`. Drives operator dashboards that
    /// need to group pending-purge rows by engine.
    pub engine_type: String,
    /// Wall-clock nanoseconds when `DROP COLLECTION` was committed
    /// (from `stored.modification_hlc.wall_ns`).
    pub deactivated_at_ns: u64,
    /// Wall-clock nanoseconds at which the retention window elapses
    /// and the sweeper will hard-delete this row. Derived from
    /// `deactivated_at_ns + retention_window`.
    pub retention_expires_at_ns: u64,
}

impl DroppedCollection {
    /// Whether the retention window has already elapsed as of `now_ns`.
    /// Rows with `is_expired(now_ns) == true` are candidates for the
    /// next sweeper pass; they may appear in the list briefly before
    /// the sweeper runs.
    pub fn is_expired(&self, now_ns: u64) -> bool {
        now_ns >= self.retention_expires_at_ns
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_expired_strictly_on_or_past_window() {
        let d = DroppedCollection {
            tenant_id: 1u64,
            name: "orders".into(),
            owner: "admin".into(),
            engine_type: "document".into(),
            deactivated_at_ns: 100,
            retention_expires_at_ns: 200,
        };
        assert!(!d.is_expired(199));
        assert!(d.is_expired(200));
        assert!(d.is_expired(300));
    }

    #[test]
    fn serde_roundtrip() {
        let d = DroppedCollection {
            tenant_id: 7u64,
            name: "test".into(),
            owner: "alice".into(),
            engine_type: "timeseries".into(),
            deactivated_at_ns: 1_700_000_000_000_000_000,
            retention_expires_at_ns: 1_700_604_800_000_000_000,
        };
        let json = serde_json::to_string(&d).unwrap();
        let back: DroppedCollection = serde_json::from_str(&json).unwrap();
        assert_eq!(d, back);
    }
}
