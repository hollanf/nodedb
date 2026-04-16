//! Bounded job dispatcher for the scheduler.
//!
//! Three compounding hazards in the original `scheduler_loop`:
//!   * no shutdown observation inside spawned jobs — long-running bodies
//!     delay teardown and outlive `SharedState`;
//!   * no concurrency cap — schedules firing at the same minute-mark
//!     (`* * * * *` with `allow_overlap = true`) spawn unbounded parallel
//!     heavy SQL, starving request-path queries;
//!   * no memory ceiling — `execute_job` could materialise unbounded
//!     result sets through `execute_block`.
//!
//! `JobDispatcher` solves all three:
//!   * owns a shutdown `watch` handed to every spawned body;
//!   * owns a `Semaphore` sized to `max_concurrent_jobs` (extra tries fail
//!     fast with `DispatchOutcome::OverBudget`, rather than queueing);
//!   * rejects jobs whose declared result-set size exceeds
//!     `max_result_bytes` before any work is done.

use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::{Semaphore, watch};
use tokio::task::JoinSet;

/// Dispatcher configuration.
#[derive(Debug, Clone)]
pub struct JobDispatcherConfig {
    /// Hard cap on concurrent in-flight jobs. Extra jobs are rejected
    /// with `DispatchOutcome::OverBudget` so the scheduler loop doesn't
    /// unboundedly pile up parallel SQL at minute boundaries.
    pub max_concurrent_jobs: usize,
    /// Declared result-set ceiling in bytes. Jobs spawned via
    /// `try_spawn_with_budget` declare their worst-case size up front and
    /// are rejected if it exceeds this ceiling.
    pub max_result_bytes: u64,
}

/// Outcome of an attempted job spawn.
#[derive(Debug, PartialEq, Eq)]
pub enum DispatchOutcome {
    /// Job was accepted and is running in the background.
    Spawned,
    /// Job was rejected because the concurrency cap or memory ceiling is
    /// full. The caller is expected to log it and move on — the
    /// scheduler's next tick may find capacity.
    OverBudget,
}

pub struct JobDispatcher {
    config: JobDispatcherConfig,
    semaphore: Arc<Semaphore>,
    shutdown_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
    in_flight: Arc<AtomicUsize>,
    tasks: Mutex<JoinSet<()>>,
}

impl JobDispatcher {
    pub fn new(config: JobDispatcherConfig) -> Self {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let semaphore = Arc::new(Semaphore::new(config.max_concurrent_jobs));
        Self {
            config,
            semaphore,
            shutdown_tx,
            shutdown_rx,
            in_flight: Arc::new(AtomicUsize::new(0)),
            tasks: Mutex::new(JoinSet::new()),
        }
    }

    /// Try to spawn a job. Accepts a closure receiving a shutdown
    /// receiver the job can `.await` on to learn when to stop early.
    pub fn try_spawn<F, Fut>(&self, job: F) -> DispatchOutcome
    where
        F: FnOnce(watch::Receiver<bool>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), String>> + Send + 'static,
    {
        let Ok(permit) = Arc::clone(&self.semaphore).try_acquire_owned() else {
            return DispatchOutcome::OverBudget;
        };

        let shutdown_rx = self.shutdown_rx.clone();
        let in_flight = Arc::clone(&self.in_flight);
        in_flight.fetch_add(1, Ordering::Relaxed);

        let mut tasks = self.tasks.lock().unwrap_or_else(|p| p.into_inner());
        tasks.spawn(async move {
            let _ = job(shutdown_rx).await;
            drop(permit);
            in_flight.fetch_sub(1, Ordering::Relaxed);
        });
        DispatchOutcome::Spawned
    }

    /// Like `try_spawn` but also enforces the configured memory ceiling
    /// against the caller-declared worst-case result size.
    pub fn try_spawn_with_budget<F, Fut>(
        &self,
        declared_result_bytes: u64,
        job: F,
    ) -> DispatchOutcome
    where
        F: FnOnce(watch::Receiver<bool>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), String>> + Send + 'static,
    {
        if declared_result_bytes > self.config.max_result_bytes {
            return DispatchOutcome::OverBudget;
        }
        self.try_spawn(job)
    }

    /// Number of jobs currently in flight.
    pub fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::Relaxed)
    }

    /// Signal shutdown to every in-flight job, abort any that haven't
    /// finished, and wait for the dispatcher to drain.
    pub async fn shutdown_and_drain(&self) {
        let _ = self.shutdown_tx.send(true);
        let mut tasks = {
            let mut guard = self.tasks.lock().unwrap_or_else(|p| p.into_inner());
            std::mem::take(&mut *guard)
        };
        tasks.abort_all();
        while let Some(_res) = tasks.join_next().await {}
        self.in_flight.store(0, Ordering::Relaxed);
    }

    /// Subscribe to the dispatcher's shutdown signal. Used by the
    /// scheduler loop to observe the same bus that jobs observe.
    pub fn shutdown_receiver(&self) -> watch::Receiver<bool> {
        self.shutdown_rx.clone()
    }
}
