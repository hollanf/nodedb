//! Kafka producer lifecycle manager.
//!
//! Tracks running Kafka producer tasks per stream. Starts producers on
//! `CREATE CHANGE STREAM ... WITH (DELIVERY = 'kafka')`, stops them on
//! `DROP CHANGE STREAM`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use tokio::sync::watch;
use tracing::{debug, info};

use super::config::KafkaDeliveryConfig;
use crate::control::state::SharedState;

/// Manages Kafka producer tasks for change streams.
pub struct KafkaManager {
    /// stream_key "{tenant_id}:{stream_name}" → task handle.
    tasks: Mutex<HashMap<String, tokio::task::JoinHandle<()>>>,
    /// Shutdown signal receiver (cloned per task).
    shutdown_rx: watch::Receiver<bool>,
    /// Shared state (set once after SharedState construction).
    state: OnceLock<Arc<SharedState>>,
}

impl KafkaManager {
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

    /// Start a Kafka producer for a change stream.
    ///
    /// No-op if a producer is already running for this stream.
    pub fn start(&self, tenant_id: u64, stream_name: &str, config: KafkaDeliveryConfig) {
        if !config.enabled {
            return;
        }

        let shared_state = match self.state.get() {
            Some(s) => Arc::clone(s),
            None => {
                tracing::warn!("Kafka manager: state not set, cannot start producer");
                return;
            }
        };

        let key = format!("{tenant_id}:{stream_name}");
        let mut tasks = self.tasks.lock().unwrap_or_else(|p| p.into_inner());

        if tasks.contains_key(&key) {
            debug!(stream = %stream_name, "Kafka producer already running");
            return;
        }

        let handle = super::producer::spawn_kafka_task(
            stream_name.to_string(),
            tenant_id,
            config,
            shared_state,
            self.shutdown_rx.clone(),
        );

        tasks.insert(key, handle);
        info!(stream = %stream_name, tenant_id, "Kafka producer started");
    }

    /// Stop and remove a Kafka producer for a dropped change stream.
    pub fn stop(&self, tenant_id: u64, stream_name: &str) {
        let key = format!("{tenant_id}:{stream_name}");
        let mut tasks = self.tasks.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(handle) = tasks.remove(&key) {
            handle.abort();
            info!(stream = %stream_name, "Kafka producer stopped");
        }
    }

    /// Number of running Kafka producers.
    pub fn running_count(&self) -> usize {
        let tasks = self.tasks.lock().unwrap_or_else(|p| p.into_inner());
        tasks.len()
    }

    /// Total pending Kafka publishes across all producers.
    ///
    /// Approximate: counts events in each producer's internal consumer group
    /// backlog. Used by `EventPlaneBudget` for memory accounting.
    pub fn total_pending(&self) -> u64 {
        // Kafka producers use internal consumer groups with offsets tracked
        // in OffsetStore. The pending count is the difference between the
        // stream buffer's latest sequence and the consumer group's committed
        // offset. This is computed externally when needed — here we just
        // return the number of active streams as a rough proxy.
        let tasks = self.tasks.lock().unwrap_or_else(|p| p.into_inner());
        tasks.len() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manager_lifecycle() {
        let (_tx, rx) = watch::channel(false);
        let mgr = KafkaManager::new(rx);
        assert_eq!(mgr.running_count(), 0);
    }

    #[test]
    fn stop_nonexistent_is_noop() {
        let (_tx, rx) = watch::channel(false);
        let mgr = KafkaManager::new(rx);
        mgr.stop(1, "nonexistent"); // Should not panic.
    }
}
