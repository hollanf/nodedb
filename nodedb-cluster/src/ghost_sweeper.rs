//! Periodic ghost edge anti-entropy sweeper.
//!
//! Runs as a background task that periodically sweeps all ghost tables
//! to purge stale ghost stubs. This prevents unbounded ghost accumulation
//! after shard migrations.
//!
//! Sweep interval: configurable, default 30 minutes.
//! Sweep logic: for each ghost stub, check refcount (zero = purge fast path)
//! then verify against the target shard if needed.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tracing::{debug, info};

use crate::ghost::{GhostTable, SweepVerdict};

/// Default sweep interval: 30 minutes.
pub const DEFAULT_SWEEP_INTERVAL: Duration = Duration::from_secs(30 * 60);

/// Ghost sweeper configuration.
pub struct GhostSweeperConfig {
    /// How often to run the sweep.
    pub interval: Duration,
}

impl Default for GhostSweeperConfig {
    fn default() -> Self {
        Self {
            interval: DEFAULT_SWEEP_INTERVAL,
        }
    }
}

/// Run the ghost sweeper as a blocking loop (call from a dedicated thread or
/// tokio::spawn_blocking). Sweeps all ghost tables periodically.
///
/// `ghost_tables` is a map of vshard_id → GhostTable. Each vShard on this
/// node has its own ghost table tracking migrated-away nodes.
///
/// `verify_fn` checks whether a ghost should be purged by querying the
/// target shard. In single-node mode, this always returns `Purge` (no
/// remote shards to check). In cluster mode, it sends an RPC.
pub fn run_sweep_loop<V>(
    ghost_tables: Arc<Mutex<Vec<(u16, GhostTable)>>>,
    config: GhostSweeperConfig,
    verify_fn: V,
    shutdown: Arc<std::sync::atomic::AtomicBool>,
) where
    V: Fn(&str, u16) -> SweepVerdict + Send + 'static,
{
    info!(
        interval_secs = config.interval.as_secs(),
        "ghost sweeper started"
    );

    loop {
        std::thread::sleep(config.interval);

        if shutdown.load(std::sync::atomic::Ordering::Relaxed) {
            info!("ghost sweeper shutting down");
            break;
        }

        let mut tables = match ghost_tables.lock() {
            Ok(t) => t,
            Err(poisoned) => poisoned.into_inner(),
        };

        let mut total_purged = 0;
        let mut total_checked = 0;

        for (vshard_id, table) in tables.iter_mut() {
            if table.is_empty() {
                continue;
            }
            let report = table.sweep(|node_id, target_shard| verify_fn(node_id, target_shard));
            total_purged += report.purged;
            total_checked += report.checked;

            if report.purged > 0 {
                debug!(
                    vshard = vshard_id,
                    purged = report.purged,
                    remaining = table.len(),
                    "ghost sweep for vshard"
                );
            }
        }

        if total_checked > 0 {
            info!(
                checked = total_checked,
                purged = total_purged,
                "ghost sweep cycle complete"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ghost::GhostStub;

    #[test]
    fn sweep_purges_zero_refcount() {
        let mut table = GhostTable::new();
        let mut stub = GhostStub::new("old-node".into(), 5, 1);
        stub.refcount = 0;
        table.insert(stub);

        let tables = Arc::new(Mutex::new(vec![(0u16, table)]));

        // Run one sweep manually.
        {
            let mut locked = tables.lock().unwrap();
            for (_, t) in locked.iter_mut() {
                t.sweep(|_, _| SweepVerdict::Keep);
            }
        }

        let locked = tables.lock().unwrap();
        assert!(
            locked[0].1.is_empty(),
            "zero-refcount ghost should be purged"
        );
    }
}
