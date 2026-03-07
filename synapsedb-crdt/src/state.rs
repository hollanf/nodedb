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
    pub fn new(peer_id: u64) -> Self {
        let doc = LoroDoc::new();
        doc.set_peer_id(peer_id).unwrap();
        Self { doc, peer_id }
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
    pub fn export_snapshot(&self) -> Vec<u8> {
        self.doc.export(loro::ExportMode::Snapshot).unwrap()
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upsert_and_check_existence() {
        let state = CrdtState::new(1);
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
        let state = CrdtState::new(1);
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
        let state = CrdtState::new(1);
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
        let state = CrdtState::new(1);
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
    fn snapshot_roundtrip() {
        let state1 = CrdtState::new(1);
        state1
            .upsert("users", "u1", &[("name", LoroValue::String("Bob".into()))])
            .unwrap();

        let snapshot = state1.export_snapshot();

        let state2 = CrdtState::new(2);
        state2.import(&snapshot).unwrap();

        assert!(state2.row_exists("users", "u1"));
    }
}
