/// Serializable snapshot of a tenant's Data Plane state.
///
/// Shared between Control Plane (backup/restore DDL) and Data Plane
/// (snapshot creation/restoration). Lives in `types` to avoid
/// cross-plane module visibility leaks.
///
/// Every engine stores its data as `Vec<(String, Vec<u8>)>` — a list
/// of `(key, serialized_value)` pairs. This uniform format allows
/// engines to evolve their internal storage without breaking the
/// snapshot format (the key/value semantics are engine-defined).
#[derive(serde::Serialize, serde::Deserialize, Default)]
pub struct TenantDataSnapshot {
    /// Sparse engine documents: `[("{tid}:{collection}:{doc_id}", value_bytes), ...]`
    pub documents: Vec<(String, Vec<u8>)>,
    /// Sparse engine index entries: `[("{tid}:{collection}:{field}:{value}:{doc_id}", []), ...]`
    pub indexes: Vec<(String, Vec<u8>)>,
    /// Graph edges: `[("{tid}:{src}\x00{label}\x00{tid}:{dst}", properties), ...]`
    pub edges: Vec<(String, Vec<u8>)>,
    /// Vector collections: `[("{tid}:{collection}", serialized_vectors_msgpack), ...]`
    /// Each value is a MessagePack-serialized list of `(vector_id, f32_data, doc_id)`.
    /// HNSW graph is NOT serialized — it's rebuilt on restore from raw vectors.
    pub vectors: Vec<(String, Vec<u8>)>,
    /// KV tables: `[("{tid}:{collection}", serialized_entries_msgpack), ...]`
    /// Each value is a MessagePack-serialized list of `(key_bytes, value_bytes, expire_at_ms)`.
    pub kv_tables: Vec<(String, Vec<u8>)>,
    /// CRDT state: `[("{tid}", loro_export_bytes), ...]`
    pub crdt_state: Vec<(String, Vec<u8>)>,
    /// Timeseries memtable data: `[("{tid}:{collection}", serialized_columns_msgpack), ...]`
    pub timeseries: Vec<(String, Vec<u8>)>,
}
