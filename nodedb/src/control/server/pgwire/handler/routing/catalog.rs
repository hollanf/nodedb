//! Plan-cache freshness lookup against the local `SystemCatalog`.

use crate::control::state::SharedState;

/// Look up the current descriptor version for `id` against the
/// local `SystemCatalog`. Used by the plan cache's freshness
/// check — a cached plan is only returned when every recorded
/// `(id, version)` still matches the current catalog.
///
/// Returns `None` if the descriptor has been dropped, the
/// catalog is unavailable, or the descriptor kind is not
/// currently tracked (only `Collection` goes through this
/// path today; other kinds do not land in the plan cache).
pub(super) fn current_descriptor_version(
    state: &SharedState,
    tenant_id: u64,
    id: &nodedb_cluster::DescriptorId,
) -> Option<u64> {
    if id.tenant_id != tenant_id {
        return None;
    }
    let catalog = state.credentials.catalog();
    let catalog = catalog.as_ref()?;
    match id.kind {
        nodedb_cluster::DescriptorKind::Collection => catalog
            .get_collection(tenant_id, &id.name)
            .ok()
            .flatten()
            .filter(|c| c.is_active)
            .map(|c| c.descriptor_version.max(1)),
        _ => None,
    }
}
