//! Apply tenant catalog entries to `SystemCatalog` redb.

use tracing::warn;

use crate::control::security::catalog::{StoredTenant, SystemCatalog};

pub fn put(stored: &StoredTenant, catalog: &SystemCatalog) {
    if let Err(e) = catalog.put_tenant(stored) {
        warn!(
            tenant = stored.tenant_id,
            name = %stored.name,
            error = %e,
            "catalog_entry: put_tenant failed"
        );
    }
}

pub fn delete(tenant_id: u64, catalog: &SystemCatalog) {
    if let Err(e) = catalog.delete_tenant(tenant_id) {
        warn!(
            tenant = tenant_id,
            error = %e,
            "catalog_entry: delete_tenant failed"
        );
    }
}
