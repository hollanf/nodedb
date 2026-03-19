//! TPC (Thread-per-Core) runtime for Data Plane cores.
//!
//! Replaces the naive `sleep(50µs)` busy-poll with an eventfd-driven wake
//! mechanism. Each core thread:
//!
//! 1. Pins itself to a dedicated jemalloc arena (zero allocator contention).
//! 2. Blocks on `libc::poll(eventfd)` when idle (zero CPU waste).
//! 3. Wakes instantly when the Control Plane signals via `EventFdNotifier`.
//! 4. Processes all pending requests in a tight loop, then re-parks.
//!
//! # Panic Isolation
//!
//! Every `core.tick()` invocation is wrapped in `catch_unwind`. A panic in
//! any engine execution (bad index, arithmetic overflow, corrupted data)
//! is caught without killing the core thread. The faulting request receives
//! an `INTERNAL_ERROR` response, and the core continues serving subsequent
//! requests. A health watchdog tracks consecutive panics: if the threshold
//! is exceeded, the core stops accepting new work and logs an alert.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::Path;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Instant;

use tracing::{error, info, warn};

use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
use crate::bridge::envelope::{ErrorCode, Response, Status};
use crate::data::eventfd::{EventFd, EventFdNotifier};
use crate::data::executor::core_loop::CoreLoop;

use nodedb_bridge::buffer::{Consumer, Producer};

/// Maximum idle poll timeout in milliseconds.
///
/// Even without signals, cores wake periodically to run maintenance
/// (e.g., deferred retry polling, metrics flush).
const IDLE_POLL_TIMEOUT_MS: i32 = 100;

/// Maximum consecutive panics before the core enters degraded mode.
///
/// Degraded mode drains and rejects all incoming requests with
/// `ErrorCode::CoreDegraded` instead of executing them. This prevents
/// a poison-pill request from hot-looping through catch_unwind.
const MAX_CONSECUTIVE_PANICS: u32 = 3;

/// Window in which consecutive panics are counted. If more than
/// `MAX_CONSECUTIVE_PANICS` occur within this window, the core degrades.
/// Panics separated by more than this duration reset the counter.
const PANIC_WINDOW_SECS: u64 = 60;

/// Tracks core health across panics for the watchdog.
struct CoreHealthWatchdog {
    /// Number of panics in the current window.
    consecutive_panics: u32,
    /// Timestamp of the first panic in the current window.
    window_start: Option<Instant>,
    /// Whether this core has been marked degraded.
    degraded: bool,
}

impl CoreHealthWatchdog {
    fn new() -> Self {
        Self {
            consecutive_panics: 0,
            window_start: None,
            degraded: false,
        }
    }

    /// Record a panic. Returns `true` if the core should enter degraded mode.
    fn record_panic(&mut self) -> bool {
        let now = Instant::now();

        // Reset window if the previous panic was outside the window.
        if let Some(start) = self.window_start {
            if now.duration_since(start).as_secs() > PANIC_WINDOW_SECS {
                self.consecutive_panics = 0;
                self.window_start = Some(now);
            }
        } else {
            self.window_start = Some(now);
        }

        self.consecutive_panics += 1;

        if self.consecutive_panics >= MAX_CONSECUTIVE_PANICS {
            self.degraded = true;
        }

        self.degraded
    }

    /// Record a successful tick (no panic). Resets the consecutive counter.
    fn record_success(&mut self) {
        if self.consecutive_panics > 0 {
            self.consecutive_panics = 0;
            self.window_start = None;
        }
    }

    fn is_degraded(&self) -> bool {
        self.degraded
    }
}

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
            match nodedb_mem::arena::pin_thread_arena(core_id as u32) {
                Ok(arena) => info!(core_id, arena, "pinned to jemalloc arena"),
                Err(e) => warn!(core_id, error = %e, "failed to pin jemalloc arena, continuing with default"),
            }

            // 2. Open engines.
            let mut core = CoreLoop::open(core_id, request_rx, response_tx, &data_dir)
                .expect("failed to open CoreLoop engines");

            info!(core_id, "data plane core started (eventfd-driven)");

            let mut watchdog = CoreHealthWatchdog::new();

            // 3. Event loop: poll → drain → tick → repeat.
            loop {
                // Block until signaled or timeout.
                efd.poll_wait(IDLE_POLL_TIMEOUT_MS);

                // Drain all accumulated signals.
                while efd.drain() > 0 {}

                // If degraded, drain and reject all pending requests.
                if watchdog.is_degraded() {
                    drain_and_reject(&mut core, core_id);
                    continue;
                }

                // Process all pending requests with panic isolation.
                loop {
                    // catch_unwind requires FnOnce: &mut core is not UnwindSafe
                    // by default, but we explicitly opt in. The CoreLoop state
                    // may be partially inconsistent after a panic (e.g., a
                    // half-inserted HNSW node), but:
                    //   - Reads are safe: stale or partial data is better than dead core.
                    //   - Writes: the WAL ensures crash consistency on replay;
                    //     a panicked write was never acknowledged to the client.
                    //   - The watchdog degrades the core before repeated panics
                    //     can compound corruption.
                    let result =
                        catch_unwind(AssertUnwindSafe(|| core.tick()));

                    match result {
                        Ok(0) => break, // No more pending requests.
                        Ok(_) => {
                            watchdog.record_success();
                        }
                        Err(panic_payload) => {
                            // Extract panic message for logging.
                            let msg = panic_message(&panic_payload);
                            error!(
                                core_id,
                                panic_count = watchdog.consecutive_panics + 1,
                                message = %msg,
                                "data plane core caught panic during tick"
                            );

                            let is_degraded = watchdog.record_panic();
                            if is_degraded {
                                error!(
                                    core_id,
                                    threshold = MAX_CONSECUTIVE_PANICS,
                                    window_secs = PANIC_WINDOW_SECS,
                                    "core entered DEGRADED mode — rejecting all requests"
                                );
                                drain_and_reject(&mut core, core_id);
                            }

                            // The panicked tick may have consumed a request from
                            // the queue without sending a response. The in-flight
                            // request's oneshot channel in RequestTracker will
                            // time out on the Control Plane side (deadline expiry),
                            // which is the correct behavior — the client sees
                            // DEADLINE_EXCEEDED rather than hanging forever.

                            break; // Exit inner loop; re-enter poll_wait.
                        }
                    }
                }
            }
        })?;

    Ok((handle, notifier))
}

/// Drain all pending requests from a core's SPSC queue and send back
/// `CoreDegraded` error responses. Used when the watchdog has flagged
/// the core as unhealthy.
fn drain_and_reject(core: &mut CoreLoop, core_id: usize) {
    core.drain_requests();
    while let Some(task) = core.task_queue.pop_front() {
        let response = Response {
            request_id: task.request_id(),
            status: Status::Error,
            attempt: 1,
            partial: false,
            payload: Arc::from([].as_slice()),
            watermark_lsn: core.watermark,
            error_code: Some(ErrorCode::Internal {
                detail: format!("core-{core_id} is degraded after repeated panics"),
            }),
        };
        if let Err(e) = core
            .response_tx
            .try_push(BridgeResponse { inner: response })
        {
            warn!(core_id, error = %e, "failed to send degraded-rejection response");
        }
    }
}

/// Extract a human-readable message from a panic payload.
fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "non-string panic payload".to_string()
    }
}
