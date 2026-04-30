//! Materialized-view enumeration for `PurgeCollection` cascade.
//!
//! An MV's `source` points at the collection whose CDC stream feeds
//! it. When the source is hard-deleted the MV is no longer definable
//! and must be dropped. This walks the MV graph transitively: an MV
//! that sources another MV (which sources the purged collection) is
//! itself a dependent.
//!
//! `MAX_DEPTH` bounds the transitive walk — a legitimate MV DAG will
//! never approach 32 levels, so exceeding it means a cycle (or an
//! adversarial definition) and the orchestrator treats it as an
//! error.

use std::collections::HashSet;

use crate::control::security::catalog::SystemCatalog;

/// Safety bound on MV-of-MV chain depth.
pub const MAX_DEPTH: usize = 32;

/// Enumerate MVs whose source (direct or transitive) is
/// `(tenant_id, root_collection)`. Returns MV names only, sorted.
///
/// Returns `NodeDbError::cascade_cycle` if the MV graph exceeds
/// `MAX_DEPTH` levels — the orchestrator surfaces that up as a purge
/// blocker rather than silently truncating.
pub fn find_mvs_sourcing(
    catalog: &SystemCatalog,
    tenant_id: u64,
    root_collection: &str,
) -> crate::Result<Vec<String>> {
    let all = catalog.load_all_materialized_views()?;

    // Build adjacency: source-name → [mv names]. Only consider the
    // target tenant — MV definitions are tenant-scoped.
    use std::collections::HashMap;
    let mut by_source: HashMap<String, Vec<String>> = HashMap::new();
    for mv in all.iter().filter(|m| m.tenant_id == tenant_id) {
        by_source
            .entry(mv.source.clone())
            .or_default()
            .push(mv.name.clone());
    }

    let mut found: HashSet<String> = HashSet::new();
    let mut frontier: Vec<String> = vec![root_collection.to_string()];
    let mut depth = 0usize;
    while !frontier.is_empty() {
        depth += 1;
        if depth > MAX_DEPTH {
            return Err(crate::Error::CascadeCycle {
                tenant_id,
                root: root_collection.to_string(),
                depth: MAX_DEPTH,
            });
        }
        let mut next: Vec<String> = Vec::new();
        for src in frontier {
            if let Some(mvs) = by_source.get(&src) {
                for mv_name in mvs {
                    if found.insert(mv_name.clone()) {
                        next.push(mv_name.clone());
                    }
                }
            }
        }
        frontier = next;
    }

    let mut out: Vec<String> = found.into_iter().collect();
    out.sort();
    Ok(out)
}
