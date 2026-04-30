//! Apply Sequence catalog entries to `SystemCatalog` redb.

use tracing::warn;

use crate::control::security::catalog::SystemCatalog;
use crate::control::security::catalog::auth_types::object_type;
use crate::control::security::catalog::sequence_types::{SequenceState, StoredSequence};

pub fn put(stored: &StoredSequence, catalog: &SystemCatalog) {
    if let Err(e) = catalog.put_sequence(stored) {
        warn!(
            sequence = %stored.name,
            tenant = stored.tenant_id,
            error = %e,
            "catalog_entry: put_sequence failed"
        );
    }
    super::owner::put_parent_owner(
        object_type::SEQUENCE,
        stored.tenant_id,
        &stored.name,
        &stored.owner,
        catalog,
    );
}

pub fn delete(tenant_id: u64, name: &str, catalog: &SystemCatalog) {
    if let Err(e) = catalog.delete_sequence(tenant_id, name) {
        warn!(
            sequence = %name,
            tenant = tenant_id,
            error = %e,
            "catalog_entry: delete_sequence failed"
        );
    }
    super::owner::delete_parent_owner(object_type::SEQUENCE, tenant_id, name, catalog);
}

pub fn put_state(state: &SequenceState, catalog: &SystemCatalog) {
    if let Err(e) = catalog.put_sequence_state(state) {
        warn!(
            sequence = %state.name,
            tenant = state.tenant_id,
            error = %e,
            "catalog_entry: put_sequence_state failed"
        );
    }
}
