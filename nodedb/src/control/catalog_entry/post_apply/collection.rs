//! Collection post-apply side effects.

use std::sync::Arc;

use tracing::debug;

use crate::control::security::catalog::{StoredCollection, StoredOwner};
use crate::control::state::SharedState;

/// Synchronous half of `PutCollection` post-apply: install the owner
/// record into the in-memory `PermissionStore`. Called inline by the
/// metadata applier BEFORE the applied-index watcher bump so readers
/// of `applied_index` observe the ownership consistently.
pub fn put_owner_sync(stored: &StoredCollection, shared: Arc<SharedState>) {
    // Replicate the owner record on every node so cluster-wide
    // `is_owner` / `check` evaluations succeed. Handlers no longer
    // call `set_owner` directly — ownership is entirely a side
    // effect of the parent `PutCollection` apply.
    shared.permissions.install_replicated_owner(&StoredOwner {
        object_type: "collection".into(),
        object_name: stored.name.clone(),
        tenant_id: stored.tenant_id,
        owner_username: stored.owner.clone(),
    });
}

/// Asynchronous half: dispatch a `Register` request to this node's
/// Data Plane so the first cross-node INSERT doesn't need to
/// rediscover the storage mode. Spawned as a best-effort task —
/// correctness does not depend on it completing before the
/// `applied_index` watcher bumps, only performance does.
pub async fn put_async(stored: StoredCollection, shared: Arc<SharedState>) {
    crate::control::server::pgwire::ddl::collection::create::dispatch_register_from_stored(
        &shared, &stored,
    )
    .await;
    debug!(
        collection = %stored.name,
        "catalog_entry: Register dispatched to local Data Plane"
    );
}

pub fn deactivate(tenant_id: u32, name: String, _shared: Arc<SharedState>) {
    // Ownership is intentionally preserved on soft-delete. The
    // primary `StoredCollection` record is kept for audit / undrop
    // (see `CatalogEntry::DeactivateCollection`); removing the
    // in-memory owner entry would split truth from the preserved
    // primary row's `stored.owner` field and force any future
    // UNDROP to be admin-only. `is_owner` returning true for a
    // soft-deleted collection is the correct semantics: the former
    // owner remains the rightful restorer. Hard deletion of the
    // collection (not wired today) would clear both halves via
    // `delete_parent_owner` in the applier.
    debug!(
        collection = %name,
        tenant = tenant_id,
        "catalog_entry: DeactivateCollection post-apply (owner retained for undrop; Data Plane Unregister deferred)"
    );
}
