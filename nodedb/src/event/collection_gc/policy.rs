//! Retention-window resolution for the collection GC sweeper.
//!
//! Resolution order (highest precedence first):
//!
//! 1. Per-tenant `tenant_config.deactivated_collection_retention_days`
//!    — an operator override set via `ALTER TENANT ... SET ...`.
//! 2. System-wide `server.retention.deactivated_collection_retention_days`.
//!
//! The resolver is pure (no I/O); the sweeper supplies the inputs.

use std::time::Duration;

use crate::control::security::catalog::StoredCollection;

/// Outcome of evaluating a single soft-deleted collection against its
/// retention policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PurgeDecision {
    /// Retention has elapsed; propose `PurgeCollection`.
    Purge,
    /// Still within retention; skip this sweep, re-evaluate next tick.
    Wait { remaining: Duration },
    /// Stored row is active (defensive: sweeper should have filtered
    /// these out, but treat as not-purgable for safety).
    NotDeactivated,
}

/// Resolve whether `coll` should be purged given the `now` wall-clock
/// time (Unix-epoch nanoseconds) and the effective retention window.
pub fn resolve_retention(
    coll: &StoredCollection,
    now_ns: u64,
    retention: Duration,
) -> PurgeDecision {
    if coll.is_active {
        return PurgeDecision::NotDeactivated;
    }
    let deactivated_at_ns = coll.modification_hlc.wall_ns;
    let retention_ns = retention.as_nanos() as u64;
    let purge_at = deactivated_at_ns.saturating_add(retention_ns);
    if now_ns >= purge_at {
        PurgeDecision::Purge
    } else {
        PurgeDecision::Wait {
            remaining: Duration::from_nanos(purge_at - now_ns),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::Hlc;

    fn coll(is_active: bool, wall_ns: u64) -> StoredCollection {
        let mut c = StoredCollection::new(1, "c", "u");
        c.is_active = is_active;
        c.modification_hlc = Hlc::new(wall_ns, 0);
        c
    }

    #[test]
    fn active_collection_is_never_purgable() {
        let c = coll(true, 0);
        assert_eq!(
            resolve_retention(&c, u64::MAX, Duration::ZERO),
            PurgeDecision::NotDeactivated
        );
    }

    #[test]
    fn soft_deleted_past_retention_is_purgable() {
        let c = coll(false, 1_000_000_000); // 1s
        let decision = resolve_retention(
            &c,
            2_000_000_000 + Duration::from_secs(5).as_nanos() as u64,
            Duration::from_secs(5),
        );
        assert_eq!(decision, PurgeDecision::Purge);
    }

    #[test]
    fn soft_deleted_within_retention_waits() {
        let c = coll(false, 1_000_000_000);
        let decision = resolve_retention(
            &c,
            1_000_000_000 + Duration::from_secs(1).as_nanos() as u64,
            Duration::from_secs(5),
        );
        match decision {
            PurgeDecision::Wait { remaining } => {
                assert!(remaining <= Duration::from_secs(4));
                assert!(remaining > Duration::from_secs(3));
            }
            other => panic!("expected Wait, got {other:?}"),
        }
    }

    #[test]
    fn zero_retention_is_immediately_purgable() {
        let c = coll(false, 1_000);
        assert_eq!(
            resolve_retention(&c, 1_000, Duration::ZERO),
            PurgeDecision::Purge
        );
    }
}
