//! TPC (Thread-per-Core) runtime for Data Plane cores.
//!
//! Replaces the naive `sleep(50µs)` busy-poll with an eventfd-driven wake
//! mechanism. Each core thread:
//!
//! 1. Pins itself to a dedicated jemalloc arena (zero allocator contention).
//! 2. Blocks on `libc::poll(eventfd)` when idle (zero CPU waste).
//! 3. Wakes instantly when the Control Plane signals via `EventFdNotifier`.
//! 4. Processes all pending requests in a tight loop, then re-parks.

use std::path::Path;
use std::thread::JoinHandle;

use tracing::{info, warn};

use crate::data::eventfd::{EventFd, EventFdNotifier};
use crate::data::executor::core_loop::CoreLoop;

use synapsedb_bridge::buffer::{Consumer, Producer};

use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};

/// Maximum idle poll timeout in milliseconds.
///
/// Even without signals, cores wake periodically to run maintenance
/// (e.g., deferred retry polling, metrics flush).
const IDLE_POLL_TIMEOUT_MS: i32 = 100;

/// Spawn a Data Plane core on a dedicated OS thread with TPC isolation.
///
/// Returns the `JoinHandle` and the `EventFdNotifier` that the Control Plane
/// uses to wake this core after pushing a request into the SPSC queue.
pub fn spawn_core(
    core_id: usize,
    request_rx: Consumer<BridgeRequest>,
    response_tx: Producer<BridgeResponse>,
    data_dir: &Path,
) -> std::io::Result<(JoinHandle<()>, EventFdNotifier)> {
    let data_dir = data_dir.to_path_buf();

    // Create eventfd and extract notifier before moving EventFd to core thread.
    let efd = EventFd::new().map_err(std::io::Error::other)?;
    let notifier = efd.notifier();

    let handle = std::thread::Builder::new()
        .name(format!("data-core-{core_id}"))
        .spawn(move || {
            // 1. Pin to dedicated jemalloc arena.
            match synapsedb_mem::arena::pin_thread_arena(core_id as u32) {
                Ok(arena) => info!(core_id, arena, "pinned to jemalloc arena"),
                Err(e) => warn!(core_id, error = %e, "failed to pin jemalloc arena, continuing with default"),
            }

            // 2. Open engines.
            let mut core = CoreLoop::open(core_id, request_rx, response_tx, &data_dir)
                .expect("failed to open CoreLoop engines");

            info!(core_id, "data plane core started (eventfd-driven)");

            // 3. Event loop: poll → drain → tick → repeat.
            loop {
                // Block until signaled or timeout.
                efd.poll_wait(IDLE_POLL_TIMEOUT_MS);

                // Drain all accumulated signals.
                while efd.drain() > 0 {}

                // Process all pending requests.
                loop {
                    let processed = core.tick();
                    if processed == 0 {
                        break;
                    }
                }
            }
        })?;

    Ok((handle, notifier))
}
