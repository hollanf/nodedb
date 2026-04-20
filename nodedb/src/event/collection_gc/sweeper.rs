//! Collection-GC sweeper loop.
//!
//! One long-lived Tokio task per node. On each eval tick (default 60s)
//! it loads every soft-deleted collection from the local catalog and,
//! for those past their retention window, proposes
//! `CatalogEntry::PurgeCollection` through the normal metadata-raft
//! path. The post_apply async dispatch does the rest (per-node
//! tombstone persist + WAL append + quiesce + Data Plane reclaim).
//!
//! Runs everywhere — proposing is idempotent (the leader accepts,
//! followers' proposals forward or race-lose with no harm).

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::config::server::RetentionSettings;
use crate::control::catalog_entry::CatalogEntry;
use crate::control::state::SharedState;

use super::policy::{PurgeDecision, resolve_retention};

/// Owning handle for the sweeper task. Dropping it drops the task's
/// `JoinHandle`; callers that need to shut down cleanly should hold
/// this and `.abort()` explicitly.
#[derive(Debug)]
pub struct CollectionGcSweeper {
    pub handle: JoinHandle<()>,
}

/// Spawn the sweeper on the Tokio runtime.
pub fn spawn_collection_gc(
    shared: Arc<SharedState>,
    settings: RetentionSettings,
) -> CollectionGcSweeper {
    let handle = tokio::spawn(async move { run_loop(shared, settings).await });
    CollectionGcSweeper { handle }
}

async fn run_loop(shared: Arc<SharedState>, settings: RetentionSettings) {
    let interval = settings.sweep_interval();
    let retention = settings.retention_window();
    info!(
        sweep_interval_secs = interval.as_secs(),
        retention_days = settings.deactivated_collection_retention_days,
        "collection-gc sweeper started"
    );

    // One-tick delay before the first sweep so the node has a chance
    // to finish applying startup catalog entries before we evaluate
    // them. No effect on correctness, just avoids a noisy first pass.
    tokio::time::sleep(interval).await;

    loop {
        if let Err(e) = sweep_once(&shared, retention) {
            warn!(error = %e, "collection-gc sweep failed; will retry next tick");
        }
        tokio::time::sleep(interval).await;
    }
}

/// Single sweep pass. Public for testability — callers can drive the
/// sweeper synchronously in tests without waiting on the interval.
pub fn sweep_once(shared: &SharedState, retention: Duration) -> crate::Result<()> {
    let Some(catalog) = shared.credentials.catalog() else {
        // No persistent catalog — nothing to sweep.
        return Ok(());
    };

    let now_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);

    let dropped = catalog.load_dropped_collections()?;
    if dropped.is_empty() {
        return Ok(());
    }

    let mut proposed = 0usize;
    let mut waiting = 0usize;
    for coll in &dropped {
        match resolve_retention(coll, now_ns, retention) {
            PurgeDecision::Purge => {
                let entry = CatalogEntry::PurgeCollection {
                    tenant_id: coll.tenant_id,
                    name: coll.name.clone(),
                };
                match crate::control::metadata_proposer::propose_catalog_entry(shared, &entry) {
                    Ok(_) => {
                        proposed += 1;
                        debug!(
                            tenant = coll.tenant_id,
                            collection = %coll.name,
                            "collection-gc: proposed PurgeCollection"
                        );
                    }
                    Err(e) => {
                        // Likely not-leader on this node — that's fine;
                        // the leader's own sweeper will handle it. Log
                        // at debug, not warn, to avoid alerting on
                        // expected behavior.
                        debug!(
                            tenant = coll.tenant_id,
                            collection = %coll.name,
                            error = %e,
                            "collection-gc: propose failed (expected on follower)"
                        );
                    }
                }
            }
            PurgeDecision::Wait { .. } => waiting += 1,
            PurgeDecision::NotDeactivated => {}
        }
    }

    if proposed > 0 || waiting > 0 {
        info!(
            proposed,
            waiting,
            total = dropped.len(),
            "collection-gc sweep complete"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    //! End-to-end exercise of the sweep pass against an in-memory
    //! catalog is deferred to
    //! `tests/collection_gc_retention.rs` (needs full SharedState
    //! fixture). The pure policy logic is covered in
    //! `super::policy::tests`.
}
