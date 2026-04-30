//! Apply Schedule catalog entries to `SystemCatalog` redb.

use tracing::warn;

use crate::control::security::catalog::SystemCatalog;
use crate::control::security::catalog::auth_types::object_type;
use crate::event::scheduler::types::ScheduleDef;

pub fn put(stored: &ScheduleDef, catalog: &SystemCatalog) {
    if let Err(e) = catalog.put_schedule(stored) {
        warn!(
            schedule = %stored.name,
            tenant = stored.tenant_id,
            error = %e,
            "catalog_entry: put_schedule failed"
        );
    }
    super::owner::put_parent_owner(
        object_type::SCHEDULE,
        stored.tenant_id,
        &stored.name,
        &stored.owner,
        catalog,
    );
}

pub fn delete(tenant_id: u64, name: &str, catalog: &SystemCatalog) {
    if let Err(e) = catalog.delete_schedule(tenant_id, name) {
        warn!(
            schedule = %name,
            tenant = tenant_id,
            error = %e,
            "catalog_entry: delete_schedule failed"
        );
    }
    super::owner::delete_parent_owner(object_type::SCHEDULE, tenant_id, name, catalog);
}
