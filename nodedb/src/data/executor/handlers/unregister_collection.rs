//! Collection-scoped purge handler.
//!
//! Reclaims storage for a single `(tenant_id, collection)` pair
//! across every engine on this Data Plane core. Dispatched by the
//! Control Plane via `MetaOp::UnregisterCollection` after the
//! metadata-raft commit of `CatalogEntry::PurgeCollection`.
//!
//! Runs on **every node** (leader and followers) — each node's Data
//! Plane reclaims its own local storage symmetrically with the
//! metadata row removal.
//!
//! Idempotent: safe to re-run after partial completion. Missing
//! in-memory state is a no-op; missing files are a no-op.
//!
//! # Current coverage
//!
//! In-memory, tuple-keyed state is reclaimed here (retain filters on
//! maps keyed by `(TenantId, collection_name)` or
//! `(TenantId, collection_name, ...)`). This covers the vector, KV,
//! timeseries, spatial, columnar, CRDT, cache, doc-config, chain-hash,
//! and sparse-vector-index maps.
//!
//! Persistent, redb-backed engines (sparse documents, inverted index,
//! graph edges) are reclaimed here via collection-scoped purge methods
//! on each store.

use tracing::{info, warn};

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::types::TenantId;

impl CoreLoop {
    /// Purge collection-scoped data on this core.
    pub(in crate::data::executor) fn execute_unregister_collection(
        &mut self,
        task: &ExecutionTask,
        tenant_id: u32,
        collection: &str,
        purge_lsn: u64,
    ) -> Response {
        info!(
            core = self.core_id,
            tenant_id, collection, purge_lsn, "starting collection purge"
        );
        let tid = TenantId::new(tenant_id);
        let coll = collection.to_string();

        // ── Persistent engines (redb-backed, collection-scoped range drop) ──

        // Sparse engine: documents + secondary indexes.
        let (docs_removed, idxs_removed) =
            match self.sparse.delete_all_for_collection(tenant_id, collection) {
                Ok(counts) => counts,
                Err(e) => {
                    warn!(tenant_id, collection, error = %e, "sparse collection purge failed");
                    (0, 0)
                }
            };

        // Inverted index: postings + doc_lengths + stats + segments.
        let inv_removed = match self.inverted.purge_collection(tid, collection) {
            Ok(n) => n,
            Err(e) => {
                warn!(tenant_id, collection, error = %e, "inverted collection purge failed");
                0
            }
        };

        // Graph edge store: remove all edges scoped to this collection.
        let edges_removed = match self.edge_store.purge_collection(tid, collection) {
            Ok(n) => n,
            Err(e) => {
                warn!(tenant_id, collection, error = %e, "graph edge collection purge failed");
                0
            }
        };
        // The CSR in-memory index is collection-agnostic. Stale edges will
        // be absent from the next CSR rebuild (which reads from EdgeStore).
        self.csr.drop_collection(tid, collection);

        // ── In-memory, tuple-keyed state (reclaimable today) ─────────────────

        // Vector engine.
        let vec_removed = {
            let key = (tid, coll.clone());
            let mut r = 0;
            if self.vector_collections.remove(&key).is_some() {
                r += 1;
            }
            self.vector_params.remove(&key);
            self.index_configs.remove(&key);
            self.ivf_indexes.remove(&key);
            r
        };

        // Timeseries engine.
        let ts_removed = {
            let key = (tid, coll.clone());
            let mut r = 0;
            if self.columnar_memtables.remove(&key).is_some() {
                r += 1;
            }
            self.ts_registries.remove(&key);
            self.ts_max_ingested_lsn.remove(&key);
            self.ts_last_value_caches.remove(&key);
            r
        };

        // Spatial indexes.
        let spatial_removed = {
            let before = self.spatial_indexes.len();
            self.spatial_indexes
                .retain(|(t, c, _), _| !(*t == tid && c == &coll));
            self.spatial_doc_map
                .retain(|(t, c, _, _), _| !(*t == tid && c == &coll));
            before - self.spatial_indexes.len()
        };

        // Columnar engine state (per-core, tuple-keyed).
        self.columnar_engines
            .retain(|(t, c), _| !(*t == tid && c == &coll));
        self.columnar_flushed_segments
            .retain(|(t, c), _| !(*t == tid && c == &coll));

        // Sparse vector indexes (tuple key: tenant, collection, field).
        self.sparse_vector_indexes
            .retain(|(t, c, _), _| !(*t == tid && c == &coll));

        // KV engine: drop this collection's hash table + indexes.
        let kv_removed = self.kv_engine.purge_collection(tenant_id, collection);

        // CRDT engine: clear rows for this collection in the tenant state.
        let crdt_rows_removed = match self.crdt_engines.get_mut(&tid) {
            Some(engine) => match engine.purge_collection(collection) {
                Ok(n) => n,
                Err(e) => {
                    warn!(tenant_id, collection, error = %e, "crdt collection purge failed");
                    0
                }
            },
            None => 0,
        };

        // Doc cache: evict entries for this collection.
        self.doc_cache.evict_collection(tenant_id, collection);

        // Doc configs + chain hashes + aggregate cache.
        self.doc_configs
            .retain(|(t, c), _| !(*t == tid && c == &coll));
        self.chain_hashes
            .retain(|(t, c), _| !(*t == tid && c == &coll));
        self.aggregate_cache
            .retain(|(t, c), _| !(*t == tid && c == &coll));

        info!(
            core = self.core_id,
            tenant_id,
            collection,
            purge_lsn,
            docs_removed,
            idxs_removed,
            inv_removed,
            kv_removed,
            crdt_rows_removed,
            vec_removed,
            ts_removed,
            spatial_removed,
            edges_removed,
            "collection purge reclaim complete"
        );

        let summary = serde_json::json!({
            "tenant_id": tenant_id,
            "collection": collection,
            "purge_lsn": purge_lsn,
            "documents_removed": docs_removed,
            "indexes_removed": idxs_removed,
            "inverted_entries_removed": inv_removed,
            "kv_tables_removed": kv_removed,
            "crdt_rows_removed": crdt_rows_removed,
            "vector_indexes_removed": vec_removed,
            "timeseries_removed": ts_removed,
            "spatial_removed": spatial_removed,
            "edges_removed": edges_removed,
        });

        match crate::data::executor::response_codec::encode_json(&summary) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(_) => self.response_ok(task),
        }
    }
}
