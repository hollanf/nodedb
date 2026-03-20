//! CRDT state management backed by loro.
//!
//! Each `CrdtState` wraps a `LoroDoc` representing one tenant/namespace's
//! state. Collections within the doc are `LoroMap` instances keyed by row ID,
//! where each row is itself a `LoroMap` of field→value.

use loro::{LoroDoc, LoroMap, LoroValue, ValueOrContainer};

use crate::error::{CrdtError, Result};

/// A CRDT state for a single tenant/namespace.
pub struct CrdtState {
    doc: LoroDoc,
    peer_id: u64,
}

impl CrdtState {
    /// Create a new empty state for the given peer.
    pub fn new(peer_id: u64) -> Result<Self> {
        let doc = LoroDoc::new();
        doc.set_peer_id(peer_id)
            .map_err(|e| CrdtError::Loro(format!("failed to set peer_id {peer_id}: {e}")))?;
        Ok(Self { doc, peer_id })
    }

    /// Insert or update a row in a collection.
    pub fn upsert(
        &self,
        collection: &str,
        row_id: &str,
        fields: &[(&str, LoroValue)],
    ) -> Result<()> {
        let coll = self.doc.get_map(collection);
        let row_container = coll
            .insert_container(row_id, LoroMap::new())
            .map_err(|e| CrdtError::Loro(e.to_string()))?;
        for (field, value) in fields {
            row_container
                .insert(field, value.clone())
                .map_err(|e| CrdtError::Loro(e.to_string()))?;
        }
        Ok(())
    }

    /// Delete a row from a collection.
    pub fn delete(&self, collection: &str, row_id: &str) -> Result<()> {
        let coll = self.doc.get_map(collection);
        coll.delete(row_id)
            .map_err(|e| CrdtError::Loro(e.to_string()))?;
        Ok(())
    }

    /// Read a single row's fields as a `LoroValue::Map`.
    ///
    /// Returns the deep value of the row (all nested containers resolved),
    /// or `None` if the row does not exist.
    pub fn read_row(&self, collection: &str, row_id: &str) -> Option<LoroValue> {
        let path = format!("{collection}/{row_id}");
        Some(self.doc.get_by_str_path(&path)?.get_deep_value())
    }

    /// Check if a row exists in a collection.
    pub fn row_exists(&self, collection: &str, row_id: &str) -> bool {
        let coll = self.doc.get_map(collection);
        coll.get(row_id).is_some()
    }

    /// Get all row IDs in a collection.
    pub fn row_ids(&self, collection: &str) -> Vec<String> {
        let coll = self.doc.get_map(collection);
        coll.keys().map(|k| k.to_string()).collect()
    }

    /// Check if a value exists for the given field across all rows in a collection.
    /// Used for UNIQUE constraint checking.
    pub fn field_value_exists(&self, collection: &str, field: &str, value: &LoroValue) -> bool {
        let coll = self.doc.get_map(collection);
        for key in coll.keys() {
            let path = format!("{collection}/{key}/{field}");
            if let Some(voc) = self.doc.get_by_str_path(&path) {
                let field_val = match voc {
                    ValueOrContainer::Value(v) => v,
                    ValueOrContainer::Container(_) => {
                        continue;
                    }
                };
                if &field_val == value {
                    return true;
                }
            }
        }
        false
    }

    /// Export the current state as bytes for sync.
    pub fn export_snapshot(&self) -> Result<Vec<u8>> {
        self.doc
            .export(loro::ExportMode::Snapshot)
            .map_err(|e| CrdtError::Loro(format!("snapshot export failed: {e}")))
    }

    /// Import remote updates.
    pub fn import(&self, data: &[u8]) -> Result<()> {
        self.doc
            .import(data)
            .map_err(|e| CrdtError::DeltaApplyFailed(e.to_string()))?;
        Ok(())
    }

    /// Get the underlying LoroDoc for advanced operations.
    pub fn doc(&self) -> &LoroDoc {
        &self.doc
    }

    /// Peer ID of this state.
    pub fn peer_id(&self) -> u64 {
        self.peer_id
    }

    /// Compact the CRDT history by replacing the internal LoroDoc with a
    /// shallow snapshot.
    ///
    /// A shallow snapshot contains the current state but discards the
    /// full operation history. This is the CRDT equivalent of WAL
    /// truncation after checkpoint.
    ///
    /// After compaction:
    /// - All current state is preserved (reads return same values).
    /// - New deltas can still be applied and merged.
    /// - Historical operations before the snapshot point are gone.
    /// - Peers that sync after compaction receive a full snapshot
    ///   instead of incremental deltas (acceptable for long-offline peers).
    ///
    /// Call this periodically (e.g., every 30 minutes or when memory
    /// pressure exceeds threshold) to prevent unbounded history growth.
    pub fn compact_history(&mut self) -> Result<()> {
        // Export a shallow snapshot at the current frontiers.
        let frontiers = self.doc.oplog_frontiers();
        let snapshot = self
            .doc
            .export(loro::ExportMode::shallow_snapshot(&frontiers))
            .map_err(|e| CrdtError::Loro(format!("shallow snapshot export: {e}")))?;

        // Replace the doc with a fresh one loaded from the snapshot.
        let new_doc = LoroDoc::new();
        new_doc
            .set_peer_id(self.peer_id)
            .map_err(|e| CrdtError::Loro(format!("failed to set peer_id on compacted doc: {e}")))?;
        new_doc
            .import(&snapshot)
            .map_err(|e| CrdtError::Loro(format!("shallow snapshot import: {e}")))?;

        self.doc = new_doc;
        Ok(())
    }

    /// Estimated memory usage of the CRDT state (bytes).
    ///
    /// Includes operation history, current state, and internal caches.
    /// Use this to decide when to trigger `compact_history()`.
    pub fn estimated_memory_bytes(&self) -> usize {
        // Loro doesn't expose a direct memory metric.
        // Use snapshot size as a proxy — it's proportional to state size.
        // This is not precise but good enough for pressure monitoring.
        self.doc
            .export(loro::ExportMode::Snapshot)
            .map(|s| s.len())
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upsert_and_check_existence() {
        let state = CrdtState::new(1).unwrap();
        state
            .upsert(
                "users",
                "user-1",
                &[
                    ("name", LoroValue::String("Alice".into())),
                    ("email", LoroValue::String("alice@example.com".into())),
                ],
            )
            .unwrap();

        assert!(state.row_exists("users", "user-1"));
        assert!(!state.row_exists("users", "user-2"));
    }

    #[test]
    fn delete_row() {
        let state = CrdtState::new(1).unwrap();
        state
            .upsert(
                "users",
                "user-1",
                &[("name", LoroValue::String("Alice".into()))],
            )
            .unwrap();

        assert!(state.row_exists("users", "user-1"));
        state.delete("users", "user-1").unwrap();
        assert!(!state.row_exists("users", "user-1"));
    }

    #[test]
    fn row_ids_listing() {
        let state = CrdtState::new(1).unwrap();
        state
            .upsert("users", "a", &[("x", LoroValue::I64(1))])
            .unwrap();
        state
            .upsert("users", "b", &[("x", LoroValue::I64(2))])
            .unwrap();

        let mut ids = state.row_ids("users");
        ids.sort();
        assert_eq!(ids, vec!["a", "b"]);
    }

    #[test]
    fn field_value_uniqueness_check() {
        let state = CrdtState::new(1).unwrap();
        state
            .upsert(
                "users",
                "u1",
                &[("email", LoroValue::String("alice@example.com".into()))],
            )
            .unwrap();

        assert!(state.field_value_exists(
            "users",
            "email",
            &LoroValue::String("alice@example.com".into())
        ));
        assert!(!state.field_value_exists(
            "users",
            "email",
            &LoroValue::String("bob@example.com".into())
        ));
    }

    #[test]
    fn compact_history_preserves_state() {
        let mut state = CrdtState::new(1).unwrap();
        // Create some state with history.
        state
            .upsert(
                "users",
                "u1",
                &[("name", LoroValue::String("Alice".into()))],
            )
            .unwrap();
        state
            .upsert("users", "u2", &[("name", LoroValue::String("Bob".into()))])
            .unwrap();
        // Update to create more history.
        state
            .upsert(
                "users",
                "u1",
                &[("name", LoroValue::String("Alice Updated".into()))],
            )
            .unwrap();

        // Compact.
        state.compact_history().unwrap();

        // State should be preserved after compaction.
        assert!(state.row_exists("users", "u1"));
        assert!(state.row_exists("users", "u2"));

        // New operations should still work.
        state
            .upsert(
                "users",
                "u3",
                &[("name", LoroValue::String("Carol".into()))],
            )
            .unwrap();
        assert!(state.row_exists("users", "u3"));
    }

    #[test]
    fn estimated_memory_grows_with_data() {
        let state = CrdtState::new(1).unwrap();
        let before = state.estimated_memory_bytes();

        for i in 0..100 {
            state
                .upsert(
                    "items",
                    &format!("item-{i}"),
                    &[("value", LoroValue::I64(i))],
                )
                .unwrap();
        }

        let after = state.estimated_memory_bytes();
        assert!(
            after > before,
            "memory should grow: before={before}, after={after}"
        );
    }

    #[test]
    fn snapshot_roundtrip() {
        let state1 = CrdtState::new(1).unwrap();
        state1
            .upsert("users", "u1", &[("name", LoroValue::String("Bob".into()))])
            .unwrap();

        let snapshot = state1.export_snapshot().unwrap();

        let state2 = CrdtState::new(2).unwrap();
        state2.import(&snapshot).unwrap();

        assert!(state2.row_exists("users", "u1"));
    }
}
