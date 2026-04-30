//! In-memory registry of durable topics.

use std::collections::HashMap;
use std::sync::RwLock;

use super::types::TopicDef;

/// In-memory topic registry.
pub struct EpTopicRegistry {
    by_name: RwLock<HashMap<(u64, String), TopicDef>>,
}

impl EpTopicRegistry {
    pub fn new() -> Self {
        Self {
            by_name: RwLock::new(HashMap::new()),
        }
    }

    pub fn register(&self, def: TopicDef) {
        let key = (def.tenant_id, def.name.clone());
        let mut map = self.by_name.write().unwrap_or_else(|p| p.into_inner());
        map.insert(key, def);
    }

    pub fn unregister(&self, tenant_id: u64, name: &str) -> bool {
        let key = (tenant_id, name.to_string());
        let mut map = self.by_name.write().unwrap_or_else(|p| p.into_inner());
        map.remove(&key).is_some()
    }

    pub fn get(&self, tenant_id: u64, name: &str) -> Option<TopicDef> {
        let key = (tenant_id, name.to_string());
        let map = self.by_name.read().unwrap_or_else(|p| p.into_inner());
        map.get(&key).cloned()
    }

    pub fn list_for_tenant(&self, tenant_id: u64) -> Vec<TopicDef> {
        let map = self.by_name.read().unwrap_or_else(|p| p.into_inner());
        map.values()
            .filter(|t| t.tenant_id == tenant_id)
            .cloned()
            .collect()
    }

    pub fn load_from_catalog(
        &self,
        catalog: &crate::control::security::catalog::types::SystemCatalog,
    ) {
        let topics = match catalog.load_all_ep_topics() {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(error = %e, "failed to load topics from catalog");
                return;
            }
        };
        if topics.is_empty() {
            return;
        }
        let mut map = self.by_name.write().unwrap_or_else(|p| p.into_inner());
        for topic in topics {
            let key = (topic.tenant_id, topic.name.clone());
            map.insert(key, topic);
        }
        tracing::info!(count = map.len(), "loaded topics from catalog");
    }
}

impl Default for EpTopicRegistry {
    fn default() -> Self {
        Self::new()
    }
}
