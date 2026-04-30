//! Apply Function catalog entries to `SystemCatalog` redb.

use tracing::warn;

use crate::control::security::catalog::SystemCatalog;
use crate::control::security::catalog::auth_types::object_type;
use crate::control::security::catalog::function_types::StoredFunction;

pub fn put(stored: &StoredFunction, catalog: &SystemCatalog) {
    if let Err(e) = catalog.put_function(stored) {
        warn!(
            function = %stored.name,
            tenant = stored.tenant_id,
            error = %e,
            "catalog_entry: put_function failed"
        );
    }
    super::owner::put_parent_owner(
        object_type::FUNCTION,
        stored.tenant_id,
        &stored.name,
        &stored.owner,
        catalog,
    );
}

pub fn delete(tenant_id: u64, name: &str, catalog: &SystemCatalog) {
    if let Err(e) = catalog.delete_function(tenant_id, name) {
        warn!(
            function = %name,
            tenant = tenant_id,
            error = %e,
            "catalog_entry: delete_function failed"
        );
    }
    super::owner::delete_parent_owner(object_type::FUNCTION, tenant_id, name, catalog);
}
