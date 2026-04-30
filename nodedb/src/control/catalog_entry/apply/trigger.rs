//! Apply Trigger catalog entries to `SystemCatalog` redb.

use tracing::warn;

use crate::control::security::catalog::SystemCatalog;
use crate::control::security::catalog::auth_types::object_type;
use crate::control::security::catalog::trigger_types::StoredTrigger;

pub fn put(stored: &StoredTrigger, catalog: &SystemCatalog) {
    if let Err(e) = catalog.put_trigger(stored) {
        warn!(
            trigger = %stored.name,
            tenant = stored.tenant_id,
            error = %e,
            "catalog_entry: put_trigger failed"
        );
    }
    super::owner::put_parent_owner(
        object_type::TRIGGER,
        stored.tenant_id,
        &stored.name,
        &stored.owner,
        catalog,
    );
}

pub fn delete(tenant_id: u64, name: &str, catalog: &SystemCatalog) {
    if let Err(e) = catalog.delete_trigger(tenant_id, name) {
        warn!(
            trigger = %name,
            tenant = tenant_id,
            error = %e,
            "catalog_entry: delete_trigger failed"
        );
    }
    super::owner::delete_parent_owner(object_type::TRIGGER, tenant_id, name, catalog);
}
