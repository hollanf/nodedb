//! Per-client shape registry: tracks which shapes each connected client
//! is subscribed to and evaluates mutations against active shapes.

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::Instant;

use tracing::{debug, info};

use super::definition::{ShapeDefinition, ShapeId};

/// Per-session shape subscription state.
struct ClientShapes {
    /// Active shape subscriptions: shape_id → definition.
    shapes: HashMap<ShapeId, ShapeDefinition>,
    /// Tenant ID.
    tenant_id: u32,
    /// When shapes were last modified.
    last_modified: Instant,
}

/// Registry of all active shape subscriptions across all sync sessions.
///
/// Thread-safe (RwLock): mutations are evaluated against shapes from
/// the WAL tail loop (single writer), while subscribe/unsubscribe
/// comes from sync sessions (multiple writers).
pub struct ShapeRegistry {
    /// Per-session shapes: session_id → ClientShapes.
    sessions: RwLock<HashMap<String, ClientShapes>>,
}

impl Default for ShapeRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ShapeRegistry {
    pub fn new() -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
        }
    }

    /// Register a shape subscription for a session.
    pub fn subscribe(&self, session_id: &str, tenant_id: u32, shape: ShapeDefinition) {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        let client = sessions
            .entry(session_id.to_string())
            .or_insert_with(|| ClientShapes {
                shapes: HashMap::new(),
                tenant_id,
                last_modified: Instant::now(),
            });
        info!(
            session = session_id,
            shape_id = %shape.shape_id,
            "shape subscribed"
        );
        client.shapes.insert(shape.shape_id.clone(), shape);
        client.last_modified = Instant::now();
    }

    /// Unsubscribe a shape for a session.
    pub fn unsubscribe(&self, session_id: &str, shape_id: &str) -> bool {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        if let Some(client) = sessions.get_mut(session_id) {
            let removed = client.shapes.remove(shape_id).is_some();
            if removed {
                debug!(session = session_id, shape_id, "shape unsubscribed");
                client.last_modified = Instant::now();
            }
            removed
        } else {
            false
        }
    }

    /// Remove all shapes for a session (disconnect cleanup).
    pub fn remove_session(&self, session_id: &str) {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        if let Some(client) = sessions.remove(session_id) {
            info!(
                session = session_id,
                shapes = client.shapes.len(),
                "session shapes removed"
            );
        }
    }

    /// Get all shape IDs for a session.
    pub fn shapes_for_session(&self, session_id: &str) -> Vec<ShapeId> {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        sessions
            .get(session_id)
            .map(|c| c.shapes.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Evaluate a mutation against all active shapes.
    ///
    /// Returns a list of `(session_id, shape_id)` pairs for shapes that
    /// match the mutation. The caller then pushes ShapeDelta messages
    /// to the matching sessions.
    pub fn evaluate_mutation(
        &self,
        tenant_id: u32,
        collection: &str,
        doc_id: &str,
    ) -> Vec<(String, ShapeId)> {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        let mut matches = Vec::new();

        for (session_id, client) in sessions.iter() {
            if client.tenant_id != tenant_id {
                continue;
            }
            for (shape_id, shape) in &client.shapes {
                if shape.could_match(collection, doc_id) {
                    matches.push((session_id.clone(), shape_id.clone()));
                }
            }
        }

        matches
    }

    /// Get the session ID for a session's shape state.
    pub fn session_info(&self, session_id: &str) -> Option<(u32, usize)> {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        sessions
            .get(session_id)
            .map(|c| (c.tenant_id, c.shapes.len()))
    }

    /// Total active shape subscriptions across all sessions.
    pub fn total_shapes(&self) -> usize {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        sessions.values().map(|c| c.shapes.len()).sum()
    }

    /// Number of sessions with active shapes.
    pub fn active_sessions(&self) -> usize {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        sessions.len()
    }

    /// Get a shape definition by session + shape_id.
    pub fn get_shape(&self, session_id: &str, shape_id: &str) -> Option<ShapeDefinition> {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        sessions
            .get(session_id)
            .and_then(|c| c.shapes.get(shape_id).cloned())
    }

    /// Export all shapes for persistence (serializable snapshot).
    pub fn export_all(&self) -> Vec<(String, u32, ShapeDefinition)> {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        let mut result = Vec::new();
        for (session_id, client) in sessions.iter() {
            for shape in client.shapes.values() {
                result.push((session_id.clone(), client.tenant_id, shape.clone()));
            }
        }
        result
    }

    /// Import shapes from a persisted snapshot (called on startup).
    pub fn import(&self, shapes: Vec<(String, u32, ShapeDefinition)>) {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        for (session_id, tenant_id, shape) in shapes {
            let client = sessions.entry(session_id).or_insert_with(|| ClientShapes {
                shapes: HashMap::new(),
                tenant_id,
                last_modified: Instant::now(),
            });
            client.shapes.insert(shape.shape_id.clone(), shape);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::definition::ShapeType;
    use super::*;

    fn make_doc_shape(id: &str, collection: &str) -> ShapeDefinition {
        ShapeDefinition {
            shape_id: id.into(),
            tenant_id: 1,
            shape_type: ShapeType::Document {
                collection: collection.into(),
                predicate: Vec::new(),
            },
            description: format!("all {collection}"),
        }
    }

    #[test]
    fn subscribe_and_query() {
        let reg = ShapeRegistry::new();
        reg.subscribe("s1", 1, make_doc_shape("sh1", "orders"));
        reg.subscribe("s1", 1, make_doc_shape("sh2", "users"));

        assert_eq!(reg.total_shapes(), 2);
        assert_eq!(reg.active_sessions(), 1);
        assert_eq!(reg.shapes_for_session("s1").len(), 2);
    }

    #[test]
    fn evaluate_mutation_matches() {
        let reg = ShapeRegistry::new();
        reg.subscribe("s1", 1, make_doc_shape("sh1", "orders"));
        reg.subscribe("s2", 1, make_doc_shape("sh2", "orders"));
        reg.subscribe("s3", 2, make_doc_shape("sh3", "orders")); // Different tenant.

        let matches = reg.evaluate_mutation(1, "orders", "o1");
        assert_eq!(matches.len(), 2); // s1 and s2, not s3 (wrong tenant).
    }

    #[test]
    fn unsubscribe() {
        let reg = ShapeRegistry::new();
        reg.subscribe("s1", 1, make_doc_shape("sh1", "orders"));
        assert_eq!(reg.total_shapes(), 1);

        assert!(reg.unsubscribe("s1", "sh1"));
        assert_eq!(reg.total_shapes(), 0);

        assert!(!reg.unsubscribe("s1", "sh1")); // Already removed.
    }

    #[test]
    fn remove_session() {
        let reg = ShapeRegistry::new();
        reg.subscribe("s1", 1, make_doc_shape("sh1", "orders"));
        reg.subscribe("s1", 1, make_doc_shape("sh2", "users"));

        reg.remove_session("s1");
        assert_eq!(reg.total_shapes(), 0);
        assert_eq!(reg.active_sessions(), 0);
    }

    #[test]
    fn no_match_wrong_collection() {
        let reg = ShapeRegistry::new();
        reg.subscribe("s1", 1, make_doc_shape("sh1", "orders"));

        let matches = reg.evaluate_mutation(1, "users", "u1");
        assert!(matches.is_empty());
    }
}
