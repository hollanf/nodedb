//! Apply Collection catalog entries to `SystemCatalog` redb.

use tracing::{debug, warn};

use crate::control::security::catalog::auth_types::object_type;
use crate::control::security::catalog::{StoredCollection, SystemCatalog};

pub fn put(stored: &StoredCollection, catalog: &SystemCatalog) {
    if let Err(e) = catalog.put_collection(stored) {
        warn!(
            collection = %stored.name,
            tenant = stored.tenant_id,
            error = %e,
            "catalog_entry: put_collection failed"
        );
    }
    super::owner::put_parent_owner(
        object_type::COLLECTION,
        stored.tenant_id,
        &stored.name,
        &stored.owner,
        catalog,
    );
}

pub fn deactivate(tenant_id: u32, name: &str, catalog: &SystemCatalog) {
    match catalog.get_collection(tenant_id, name) {
        Ok(Some(mut stored)) => {
            stored.is_active = false;
            if let Err(e) = catalog.put_collection(&stored) {
                warn!(
                    collection = %name,
                    tenant = tenant_id,
                    error = %e,
                    "catalog_entry: deactivate_collection put failed"
                );
            }
        }
        Ok(None) => {
            debug!(
                collection = %name,
                tenant = tenant_id,
                "catalog_entry: deactivate on missing collection (fresh follower)"
            );
        }
        Err(e) => warn!(
            collection = %name,
            tenant = tenant_id,
            error = %e,
            "catalog_entry: deactivate_collection get failed"
        ),
    }
    // Intentionally preserve the `StoredOwner` row on soft-delete.
    // The primary `StoredCollection` record is kept for audit and
    // undrop (see `CatalogEntry::DeactivateCollection` doc and
    // `ddl/collection/drop.rs`). Stripping the owner row would
    // split truth from the preserved primary row whose
    // `stored.owner` is still populated, and would break any
    // future `UNDROP COLLECTION` by requiring admin to restore
    // ownership that was still knowable from the primary. Hard
    // deletion of the collection (not wired today) would remove
    // both halves via `delete_parent_owner`.
}
