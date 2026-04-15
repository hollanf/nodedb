//! CRDT engine operations dispatched to the Data Plane.

/// CRDT engine physical operations.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum CrdtOp {
    /// CRDT state read for a document.
    Read {
        collection: String,
        document_id: String,
    },

    /// CRDT delta application (write path).
    Apply {
        collection: String,
        document_id: String,
        delta: Vec<u8>,
        peer_id: u64,
        /// Per-mutation unique ID for deduplication and compensation tracking.
        mutation_id: u64,
    },

    /// Set conflict resolution policy for a CRDT collection (DDL).
    SetPolicy {
        collection: String,
        /// JSON-serialized `CollectionPolicy` from nodedb-crdt.
        policy_json: String,
    },

    /// Read a document at a specific historical version.
    /// Returns the document state as JSON bytes.
    ReadAtVersion {
        collection: String,
        document_id: String,
        /// JSON-serialized `HashMap<String, i64>` of {peer_id_hex: counter}.
        version_vector_json: String,
    },

    /// Get the current oplog version vector for a tenant's CRDT state.
    /// Returns version vector as JSON string.
    GetVersionVector,

    /// Export oplog delta from a version to current.
    /// Returns raw Loro delta bytes.
    ExportDelta {
        /// JSON-serialized version vector to start from.
        from_version_json: String,
    },

    /// Restore a document to a historical version (forward mutation).
    /// Returns the delta bytes for the restore operation.
    RestoreToVersion {
        collection: String,
        document_id: String,
        /// JSON-serialized version vector of the target version.
        target_version_json: String,
    },

    /// Compact history at a specific version.
    CompactAtVersion {
        /// JSON-serialized version vector. Oplog before this is discarded.
        target_version_json: String,
    },

    // ─── Block Document (LoroList) Operations ───────────────────────
    /// Insert a block (LoroMap) into a document's block list.
    /// `fields_json` contains the block's fields as a JSON object.
    ListInsert {
        collection: String,
        document_id: String,
        list_path: String,
        index: usize,
        fields_json: String,
    },

    /// Delete a block from a document's block list.
    ListDelete {
        collection: String,
        document_id: String,
        list_path: String,
        index: usize,
    },

    /// Move a block within a document's block list (reorder).
    ListMove {
        collection: String,
        document_id: String,
        list_path: String,
        from_index: usize,
        to_index: usize,
    },
}
