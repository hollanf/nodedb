//! In-memory sequence registry with lock-free counters.
//!
//! Loaded from catalog on startup. Provides nextval/currval/setval operations.
//! State is persisted back to the catalog on checkpoint/shutdown.

use std::collections::HashMap;
use std::sync::RwLock;

use crate::control::security::catalog::sequence_types::{SequenceState, StoredSequence};
use crate::control::security::catalog::types::SystemCatalog;

use super::types::{SequenceError, SequenceHandle};

/// In-memory registry of all sequences, keyed by `"{tenant_id}:{name}"`.
///
/// Loaded from the system catalog on startup. `nextval` operates on lock-free
/// atomic counters — the RwLock is only held during create/drop/alter (DDL).
pub struct SequenceRegistry {
    /// Sequences keyed by `"{tenant_id}:{name}"`.
    sequences: RwLock<HashMap<String, SequenceHandle>>,
}

impl SequenceRegistry {
    pub fn new() -> Self {
        Self {
            sequences: RwLock::new(HashMap::new()),
        }
    }

    /// Load all sequences from the catalog on startup.
    pub fn load_from_catalog(&self, catalog: &SystemCatalog) {
        let all_defs = match catalog.load_all_sequences() {
            Ok(defs) => defs,
            Err(e) => {
                tracing::warn!(error = %e, "failed to load sequences from catalog");
                return;
            }
        };

        let mut map = self.sequences.write().unwrap_or_else(|p| p.into_inner());
        for def in all_defs {
            let key = registry_key(def.tenant_id, &def.name);
            // Load persisted state if available.
            let state = catalog
                .get_sequence_state(def.tenant_id, &def.name)
                .ok()
                .flatten();
            map.insert(key, SequenceHandle::new(def, state));
        }
    }

    /// Create a new sequence. Returns error if it already exists.
    pub fn create(&self, def: StoredSequence) -> Result<(), SequenceError> {
        let key = registry_key(def.tenant_id, &def.name);
        let mut map = self.sequences.write().unwrap_or_else(|p| p.into_inner());

        if map.contains_key(&key) {
            return Err(SequenceError::AlreadyExists {
                name: def.name.clone(),
            });
        }

        map.insert(key, SequenceHandle::new(def, None));
        Ok(())
    }

    /// Remove a sequence. Returns error if not found.
    pub fn remove(&self, tenant_id: u32, name: &str) -> Result<(), SequenceError> {
        let key = registry_key(tenant_id, name);
        let mut map = self.sequences.write().unwrap_or_else(|p| p.into_inner());

        if map.remove(&key).is_none() {
            return Err(SequenceError::NotFound {
                name: name.to_string(),
            });
        }
        Ok(())
    }

    /// Get the next value from a sequence (lock-free on the hot path).
    pub fn nextval(&self, tenant_id: u32, name: &str) -> Result<i64, SequenceError> {
        let key = registry_key(tenant_id, name);
        let map = self.sequences.read().unwrap_or_else(|p| p.into_inner());

        let handle = map.get(&key).ok_or_else(|| SequenceError::NotFound {
            name: name.to_string(),
        })?;

        handle.nextval()
    }

    /// Get N values from a sequence in one atomic batch.
    pub fn nextval_batch(
        &self,
        tenant_id: u32,
        name: &str,
        n: usize,
    ) -> Result<Vec<i64>, SequenceError> {
        let key = registry_key(tenant_id, name);
        let map = self.sequences.read().unwrap_or_else(|p| p.into_inner());
        let handle = map.get(&key).ok_or_else(|| SequenceError::NotFound {
            name: name.to_string(),
        })?;
        handle.nextval_batch(n)
    }

    /// Get the current value (last nextval result on this node).
    pub fn currval(&self, tenant_id: u32, name: &str) -> Result<i64, SequenceError> {
        let key = registry_key(tenant_id, name);
        let map = self.sequences.read().unwrap_or_else(|p| p.into_inner());

        let handle = map.get(&key).ok_or_else(|| SequenceError::NotFound {
            name: name.to_string(),
        })?;

        handle.currval()
    }

    /// Set the counter to a specific value.
    pub fn setval(&self, tenant_id: u32, name: &str, value: i64) -> Result<i64, SequenceError> {
        let key = registry_key(tenant_id, name);
        let map = self.sequences.read().unwrap_or_else(|p| p.into_inner());

        let handle = map.get(&key).ok_or_else(|| SequenceError::NotFound {
            name: name.to_string(),
        })?;

        handle.setval(value)
    }

    /// Restart a sequence at a new value (ALTER SEQUENCE ... RESTART WITH).
    pub fn restart(
        &self,
        tenant_id: u32,
        name: &str,
        restart_value: i64,
    ) -> Result<(), SequenceError> {
        let key = registry_key(tenant_id, name);
        let map = self.sequences.read().unwrap_or_else(|p| p.into_inner());

        let handle = map.get(&key).ok_or_else(|| SequenceError::NotFound {
            name: name.to_string(),
        })?;

        handle.setval(restart_value)?;
        Ok(())
    }

    /// List all sequences for a tenant. Returns (name, current_value, is_called).
    pub fn list(&self, tenant_id: u32) -> Vec<(String, i64, bool)> {
        let prefix = format!("{tenant_id}:");
        let map = self.sequences.read().unwrap_or_else(|p| p.into_inner());

        map.iter()
            .filter(|(k, _)| k.starts_with(&prefix))
            .map(|(_, handle)| {
                (
                    handle.def.name.clone(),
                    handle.current_value(),
                    handle.is_called(),
                )
            })
            .collect()
    }

    /// Persist all sequence states to the catalog (for checkpoint/shutdown).
    pub fn persist_all(&self, catalog: &SystemCatalog) {
        let map = self.sequences.read().unwrap_or_else(|p| p.into_inner());

        for (_, handle) in map.iter() {
            let state = SequenceState {
                tenant_id: handle.def.tenant_id,
                name: handle.def.name.clone(),
                current_value: handle.current_value(),
                is_called: handle.is_called(),
                epoch: handle.def.epoch,
            };
            if let Err(e) = catalog.put_sequence_state(&state) {
                tracing::warn!(
                    sequence = %handle.def.name,
                    error = %e,
                    "failed to persist sequence state"
                );
            }
        }
    }

    /// Check if a sequence exists.
    pub fn exists(&self, tenant_id: u32, name: &str) -> bool {
        let key = registry_key(tenant_id, name);
        let map = self.sequences.read().unwrap_or_else(|p| p.into_inner());
        map.contains_key(&key)
    }

    /// Get a sequence definition (for SHOW SEQUENCES detail).
    pub fn get_def(&self, tenant_id: u32, name: &str) -> Option<StoredSequence> {
        let key = registry_key(tenant_id, name);
        let map = self.sequences.read().unwrap_or_else(|p| p.into_inner());
        map.get(&key).map(|h| h.def.clone())
    }
}

impl Default for SequenceRegistry {
    fn default() -> Self {
        Self::new()
    }
}

fn registry_key(tenant_id: u32, name: &str) -> String {
    format!("{tenant_id}:{name}")
}
