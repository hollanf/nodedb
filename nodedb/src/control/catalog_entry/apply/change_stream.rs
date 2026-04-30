//! Apply ChangeStream catalog entries to `SystemCatalog` redb.

use tracing::warn;

use crate::control::security::catalog::SystemCatalog;
use crate::control::security::catalog::auth_types::object_type;
use crate::event::cdc::stream_def::ChangeStreamDef;

pub fn put(stored: &ChangeStreamDef, catalog: &SystemCatalog) {
    if let Err(e) = catalog.put_change_stream(stored) {
        warn!(
            stream = %stored.name,
            tenant = stored.tenant_id,
            error = %e,
            "catalog_entry: put_change_stream failed"
        );
    }
    super::owner::put_parent_owner(
        object_type::CHANGE_STREAM,
        stored.tenant_id,
        &stored.name,
        &stored.owner,
        catalog,
    );
}

pub fn delete(tenant_id: u64, name: &str, catalog: &SystemCatalog) {
    if let Err(e) = catalog.delete_change_stream(tenant_id, name) {
        warn!(
            stream = %name,
            tenant = tenant_id,
            error = %e,
            "catalog_entry: delete_change_stream failed"
        );
    }
    super::owner::delete_parent_owner(object_type::CHANGE_STREAM, tenant_id, name, catalog);
}
