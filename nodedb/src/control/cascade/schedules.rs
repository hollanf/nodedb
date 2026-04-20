//! Schedule enumeration for `PurgeCollection` cascade.
//!
//! Schedules may reference a specific target collection via
//! `target_collection: Option<String>`. When `Some`, the schedule
//! becomes invalid on collection hard-delete and must be dropped.
//!
//! Schedules with `target_collection = None` run an opaque SQL body
//! that may or may not reference the collection. We cannot cheaply
//! enumerate those without parsing every body — the orchestrator
//! handles that case via `CASCADE FORCE` (accept the risk and purge
//! anyway; the next scheduler tick will log a failure).

use crate::control::security::catalog::SystemCatalog;

/// Enumerate schedules whose `target_collection` matches
/// `(tenant_id, collection)`. Returns schedule names only, sorted.
/// Opaque-body schedules (`target_collection = None`) are excluded
/// by construction — callers that need them must use CASCADE FORCE.
pub fn find_schedules_referencing(
    catalog: &SystemCatalog,
    tenant_id: u32,
    collection: &str,
) -> crate::Result<Vec<String>> {
    let all = catalog.load_all_schedules()?;
    let mut out: Vec<String> = all
        .into_iter()
        .filter(|s| s.tenant_id == tenant_id && s.target_collection.as_deref() == Some(collection))
        .map(|s| s.name)
        .collect();
    out.sort();
    Ok(out)
}
