//! Row-Level Security policy enumeration for `PurgeCollection` cascade.
//!
//! RLS policies are scoped to a single collection. When the collection
//! is hard-deleted the policies must go too — an orphan policy would
//! sit in the catalog forever and silently short-circuit future
//! queries if a collection with the same name were re-created.

use crate::control::security::catalog::SystemCatalog;

/// Enumerate RLS policies bound to `(tenant_id, collection)`.
/// Returns policy names only.
pub fn find_rls_policies_on(
    catalog: &SystemCatalog,
    tenant_id: u32,
    collection: &str,
) -> crate::Result<Vec<String>> {
    let all = catalog.load_all_rls_policies()?;
    let mut out: Vec<String> = all
        .into_iter()
        .filter(|p| p.tenant_id == tenant_id && p.collection == collection)
        .map(|p| p.name)
        .collect();
    out.sort();
    Ok(out)
}
