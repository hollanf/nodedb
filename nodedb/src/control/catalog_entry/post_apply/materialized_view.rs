//! MaterializedView post-apply — no in-memory registry to sync.
//! The refresh loop reads the view definition straight from
//! `SystemCatalog` on its next tick.

use std::sync::Arc;

use tracing::debug;

use crate::control::security::catalog::StoredMaterializedView;
use crate::control::state::SharedState;

pub fn put(stored: StoredMaterializedView, _shared: Arc<SharedState>) {
    debug!(
        view = %stored.name,
        tenant = stored.tenant_id,
        "catalog_entry: materialized view upserted (refresh loop will pick it up)"
    );
}

pub fn delete(tenant_id: u32, name: String, _shared: Arc<SharedState>) {
    debug!(
        view = %name,
        tenant = tenant_id,
        "catalog_entry: materialized view removed"
    );
}
