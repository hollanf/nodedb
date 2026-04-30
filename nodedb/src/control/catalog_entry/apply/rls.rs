//! Apply RLS policy catalog entries to `SystemCatalog` redb.

use tracing::warn;

use crate::control::security::catalog::{StoredRlsPolicy, SystemCatalog};

pub fn put(stored: &StoredRlsPolicy, catalog: &SystemCatalog) {
    if let Err(e) = catalog.put_rls_policy(stored) {
        warn!(
            policy = %stored.name,
            collection = %stored.collection,
            tenant = stored.tenant_id,
            error = %e,
            "catalog_entry: put_rls_policy failed"
        );
    }
}

pub fn delete(tenant_id: u64, collection: &str, name: &str, catalog: &SystemCatalog) {
    if let Err(e) = catalog.delete_rls_policy(tenant_id, collection, name) {
        warn!(
            policy = %name,
            collection = %collection,
            tenant = tenant_id,
            error = %e,
            "catalog_entry: delete_rls_policy failed"
        );
    }
}
