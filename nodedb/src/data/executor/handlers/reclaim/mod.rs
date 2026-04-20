//! Per-engine collection reclaim handlers.
//!
//! Each file in this module unlinks the persistent on-disk surface
//! for one engine for a single `(tenant, collection)` pair. Called
//! from `execute_unregister_collection` after in-memory state has
//! been evicted but before the JSON summary is built, so the handler
//! picks up per-file byte counts for the `bytes_reclaimed` metric.
//!
//! Engines whose persistent state is either shared-redb (document,
//! document-strict, FTS, graph edges) or per-tenant / in-memory only
//! (KV, CRDT) are documented inline in the parent handler — no
//! separate file unlinks are required. The modules here cover the
//! engines that write per-collection checkpoint or partition files
//! under `{data_dir}/...`.

pub mod sparse_vector;
pub mod spatial;
pub mod timeseries;
pub mod vector;

/// Summary of a single engine's reclaim pass. Byte counts are
/// best-effort — missing files count as 0, IO errors are warn-logged
/// and skipped (idempotent).
#[derive(Debug, Default, Clone, Copy)]
pub struct ReclaimStats {
    pub files_unlinked: u32,
    pub bytes_freed: u64,
}

impl ReclaimStats {
    pub fn merge(&mut self, other: ReclaimStats) {
        self.files_unlinked = self.files_unlinked.saturating_add(other.files_unlinked);
        self.bytes_freed = self.bytes_freed.saturating_add(other.bytes_freed);
    }
}
