//! Webhook manager: spawns and stops delivery tasks per stream.
//!
//! On startup, scans all change streams with webhook config and spawns
//! delivery tasks. On CREATE/DROP CHANGE STREAM with webhook config,
//! dynamically starts/stops tasks.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use tokio::sync::watch;
use tracing::{debug, info};

use crate::control::state::SharedState;

use super::delivery::spawn_delivery_task;

/// Manages webhook delivery tasks for all webhook-enabled streams.
pub struct WebhookManager {
    /// Running delivery tasks, keyed by (tenant_id, stream_name).
    tasks: Mutex<HashMap<(u64, String), tokio::task::JoinHandle<()>>>,
    /// Shared shutdown receiver (cloned for each task).
    shutdown_rx: watch::Receiver<bool>,
    /// Shared state reference, set once after SharedState construction.
    state: OnceLock<Arc<SharedState>>,
}

impl WebhookManager {
    pub fn new(shutdown_rx: watch::Receiver<bool>) -> Self {
        Self {
            tasks: Mutex::new(HashMap::new()),
            shutdown_rx,
            state: OnceLock::new(),
        }
    }

    /// Set the shared state reference (called once during startup).
    pub fn set_state(&self, state: Arc<SharedState>) {
        let _ = self.state.set(state);
    }

    /// Start a delivery task for a specific stream.
    pub fn start_task(
        &self,
        tenant_id: u64,
        stream_name: &str,
        config: super::types::WebhookConfig,
    ) {
        let state = match self.state.get() {
            Some(s) => Arc::clone(s),
            None => {
                tracing::warn!("webhook manager: state not set, cannot start task");
                return;
            }
        };

        let key = (tenant_id, stream_name.to_string());
        let mut tasks = self.tasks.lock().unwrap_or_else(|p| p.into_inner());

        if tasks.contains_key(&key) {
            debug!(
                stream = stream_name,
                "webhook delivery task already running, skipping"
            );
            return;
        }

        let handle = spawn_delivery_task(
            state,
            tenant_id,
            stream_name.to_string(),
            config,
            self.shutdown_rx.clone(),
        );

        info!(stream = stream_name, "webhook delivery task spawned");
        tasks.insert(key, handle);
    }

    /// Stop a delivery task for a specific stream (on DROP CHANGE STREAM).
    pub fn stop_task(&self, tenant_id: u64, stream_name: &str) {
        let key = (tenant_id, stream_name.to_string());
        let mut tasks = self.tasks.lock().unwrap_or_else(|p| p.into_inner());

        if let Some(handle) = tasks.remove(&key) {
            handle.abort();
            info!(stream = stream_name, "webhook delivery task stopped");
        }
    }

    /// Number of active delivery tasks.
    pub fn active_count(&self) -> usize {
        let tasks = self.tasks.lock().unwrap_or_else(|p| p.into_inner());
        tasks.len()
    }
}

impl Drop for WebhookManager {
    fn drop(&mut self) {
        let tasks = self.tasks.get_mut().unwrap_or_else(|p| p.into_inner());
        for (_, handle) in tasks.drain() {
            handle.abort();
        }
        debug!("webhook manager dropped, all delivery tasks aborted");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_manager_has_no_tasks() {
        let (_tx, rx) = watch::channel(false);
        let mgr = WebhookManager::new(rx);
        assert_eq!(mgr.active_count(), 0);
    }

    #[test]
    fn start_without_state_logs_warning() {
        let (_tx, rx) = watch::channel(false);
        let mgr = WebhookManager::new(rx);
        // start_task without set_state should not panic.
        mgr.start_task(
            1,
            "test_stream",
            super::super::types::WebhookConfig::default(),
        );
        assert_eq!(mgr.active_count(), 0); // No task started (state not set).
    }

    #[test]
    fn stop_nonexistent_task_is_noop() {
        let (_tx, rx) = watch::channel(false);
        let mgr = WebhookManager::new(rx);
        mgr.stop_task(1, "nonexistent"); // Should not panic.
        assert_eq!(mgr.active_count(), 0);
    }

    #[test]
    fn drop_aborts_all() {
        let (_tx, rx) = watch::channel(false);
        let mgr = WebhookManager::new(rx);
        drop(mgr); // Should not panic.
    }
}
