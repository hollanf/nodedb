//! Async post-apply dispatch function.
//!
//! Spawns per-variant tokio tasks for `CatalogEntry` mutations that need
//! async follow-up on **every node** (leader and followers). The match is
//! exhaustive by design — adding a new `CatalogEntry` variant without
//! wiring an async branch (even if that branch is `()`) is a compile
//! error. Best-effort per variant: failures log and drop.

use std::sync::Arc;

use crate::control::catalog_entry::entry::CatalogEntry;
use crate::control::state::SharedState;

use super::collection;

/// Spawn the async post-apply side effects of `entry`. Runs on
/// **every node** (leader and followers) so each node's local Data
/// Plane observes catalog mutations symmetrically. Cluster-global
/// side effects (audit emit, raft re-proposals) gate on
/// `shared.is_metadata_leader()` inside individual dispatchers, not
/// on whether the function runs at all.
pub fn spawn_post_apply_async_side_effects(
    entry: CatalogEntry,
    shared: Arc<SharedState>,
    raft_index: u64,
) {
    match entry {
        CatalogEntry::PutCollection(stored) => {
            tokio::spawn(async move {
                collection::put_async(*stored, shared).await;
            });
        }
        CatalogEntry::PurgeCollection { tenant_id, name } => {
            tokio::spawn(async move {
                collection::purge_async(tenant_id, name, raft_index, shared).await;
            });
        }
        // ── Variants with no async side effect today ─────────────────────────
        // Listed explicitly (no `_ => {}`) so the compiler forces a decision
        // when a new variant is added. Note: `DeleteTrigger`,
        // `DeleteChangeStream`, and `DeleteMaterializedView` handle their
        // per-node in-memory teardown synchronously via
        // `apply_post_apply_side_effects_sync` (which also runs on every
        // node); they have no additional async work today.
        CatalogEntry::DeactivateCollection { .. }
        | CatalogEntry::PutSequence(_)
        | CatalogEntry::DeleteSequence { .. }
        | CatalogEntry::PutSequenceState(_)
        | CatalogEntry::PutTrigger(_)
        | CatalogEntry::DeleteTrigger { .. }
        | CatalogEntry::PutFunction(_)
        | CatalogEntry::DeleteFunction { .. }
        | CatalogEntry::PutProcedure(_)
        | CatalogEntry::DeleteProcedure { .. }
        | CatalogEntry::PutSchedule(_)
        | CatalogEntry::DeleteSchedule { .. }
        | CatalogEntry::PutChangeStream(_)
        | CatalogEntry::DeleteChangeStream { .. }
        | CatalogEntry::PutUser(_)
        | CatalogEntry::DeactivateUser { .. }
        | CatalogEntry::PutRole(_)
        | CatalogEntry::DeleteRole { .. }
        | CatalogEntry::PutApiKey(_)
        | CatalogEntry::RevokeApiKey { .. }
        | CatalogEntry::PutMaterializedView(_)
        | CatalogEntry::DeleteMaterializedView { .. }
        | CatalogEntry::PutTenant(_)
        | CatalogEntry::DeleteTenant { .. }
        | CatalogEntry::PutRlsPolicy(_)
        | CatalogEntry::DeleteRlsPolicy { .. }
        | CatalogEntry::PutPermission(_)
        | CatalogEntry::DeletePermission { .. }
        | CatalogEntry::PutOwner(_)
        | CatalogEntry::DeleteOwner { .. } => {
            let _ = shared;
            let _ = raft_index;
        }
    }
}
