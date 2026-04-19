//! Apply MaterializedView catalog entries to `SystemCatalog` redb.

use tracing::warn;

use crate::control::security::catalog::auth_types::object_type;
use crate::control::security::catalog::{StoredMaterializedView, SystemCatalog};

pub fn put(stored: &StoredMaterializedView, catalog: &SystemCatalog) {
    if let Err(e) = catalog.put_materialized_view(stored) {
        warn!(
            view = %stored.name,
            tenant = stored.tenant_id,
            error = %e,
            "catalog_entry: put_materialized_view failed"
        );
    }
    super::owner::put_parent_owner(
        object_type::MATERIALIZED_VIEW,
        stored.tenant_id,
        &stored.name,
        &stored.owner,
        catalog,
    );
}

pub fn delete(tenant_id: u32, name: &str, catalog: &SystemCatalog) {
    if let Err(e) = catalog.delete_materialized_view(tenant_id, name) {
        warn!(
            view = %name,
            tenant = tenant_id,
            error = %e,
            "catalog_entry: delete_materialized_view failed"
        );
    }
    super::owner::delete_parent_owner(object_type::MATERIALIZED_VIEW, tenant_id, name, catalog);
}
