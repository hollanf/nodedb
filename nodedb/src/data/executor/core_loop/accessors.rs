//! Small accessor impls on `CoreLoop`: scan-quiesce install + acquire,
//! test-only inspection helpers. Extracted from `mod.rs` to keep the
//! main file under the 500-LOC ceiling while leaving the struct
//! definition in one place.

use std::sync::Arc;

#[cfg(test)]
use crate::types::TenantId;

use super::CoreLoop;

impl CoreLoop {
    /// Install the shared scan-quiesce registry. Called once by the
    /// server bootstrap in `main.rs` after `SharedState::open`.
    pub fn set_quiesce(
        &mut self,
        quiesce: std::sync::Arc<crate::bridge::quiesce::CollectionQuiesce>,
    ) {
        self.quiesce = Some(quiesce);
    }

    /// Acquire a scan guard for `(tid, collection)`. Returns `Ok(None)`
    /// if no quiesce registry is installed (e.g. in tests) — callers
    /// treat that as "scan unconditionally". Returns `Err(Response)`
    /// carrying a `NodeDbError::collection_draining` error code when
    /// a drain is in progress against the collection.
    pub(in crate::data::executor) fn acquire_scan_guard(
        &self,
        task: &crate::data::executor::task::ExecutionTask,
        tid: u64,
        collection: &str,
    ) -> Result<Option<crate::bridge::quiesce::ScanGuard>, crate::bridge::envelope::Response> {
        let Some(q) = self.quiesce.as_ref() else {
            return Ok(None);
        };
        match q.try_start_scan(tid, collection) {
            Ok(g) => Ok(Some(g)),
            Err(_) => Err(self.response_error(
                task,
                crate::bridge::envelope::ErrorCode::CollectionDraining {
                    collection: collection.to_string(),
                },
            )),
        }
    }

    /// Install the encryption key used for vector checkpoint at-rest encryption.
    ///
    /// Called by the server bootstrap after opening the WAL key. When set,
    /// `checkpoint_vector_indexes` encrypts checkpoint files and
    /// `load_vector_checkpoints` refuses plaintext ones.
    pub fn set_vector_checkpoint_kek(&mut self, kek: nodedb_wal::crypto::WalEncryptionKey) {
        self.vector_checkpoint_kek = Some(kek);
    }

    /// Install the encryption key used for spatial checkpoint at-rest encryption.
    ///
    /// When set, `checkpoint_spatial_indexes` encrypts checkpoint files and
    /// `load_spatial_checkpoints` refuses plaintext ones.
    pub fn set_spatial_checkpoint_kek(&mut self, kek: nodedb_wal::crypto::WalEncryptionKey) {
        self.spatial_checkpoint_kek = Some(kek);
    }

    /// Install the encryption key used for columnar segment at-rest encryption.
    ///
    /// When set, columnar segment flushes produce AES-256-GCM encrypted SEGC
    /// envelopes and the segment reader refuses to load plaintext segments.
    pub fn set_columnar_segment_kek(&mut self, kek: nodedb_wal::crypto::WalEncryptionKey) {
        self.columnar_segment_kek = Some(kek);
    }

    /// Install the encryption key used for array segment at-rest encryption.
    ///
    /// When set, array segment flushes produce AES-256-GCM encrypted SEGA
    /// envelopes and the segment handle refuses to load plaintext segments.
    pub fn set_array_segment_kek(&mut self, kek: nodedb_wal::crypto::WalEncryptionKey) {
        self.array_engine.set_kek(kek.clone());
        self.array_segment_kek = Some(kek);
    }

    /// Returns the current SPSC drain batch size.
    ///
    /// Useful for observability and integration-level pressure tests that
    /// verify the governor correctly throttles the read depth.
    pub fn spsc_read_depth(&self) -> usize {
        self.spsc_read_depth
    }

    /// Returns whether new SPSC reads are suspended due to Emergency pressure.
    ///
    /// Useful for observability and integration-level pressure tests that
    /// verify the governor correctly gates the drain path.
    pub fn pressure_suspend_reads(&self) -> bool {
        self.pressure_suspend_reads
    }

    /// Returns the configured baseline SPSC drain depth (the value restored
    /// after pressure normalizes).  Exposed so integration tests can assert
    /// throttled depths relative to the baseline without hard-coding the value.
    pub fn spsc_read_depth_normal() -> usize {
        crate::data::executor::core_loop::pressure::SPSC_READ_DEPTH_NORMAL
    }

    /// Install the encryption key for timeseries columnar segment files.
    ///
    /// When set, `flush_ts_collection` wraps each output file in a `SEGT`
    /// AES-256-GCM envelope and readers refuse to load plaintext segment files.
    pub fn set_ts_segment_kek(&mut self, kek: nodedb_wal::crypto::WalEncryptionKey) {
        self.ts_segment_kek = Some(kek);
    }

    /// Install the shared quarantine registry.
    ///
    /// Called once by the server bootstrap after `SharedState::open`.
    pub fn set_quarantine_registry(
        &mut self,
        registry: std::sync::Arc<crate::storage::quarantine::QuarantineRegistry>,
    ) {
        self.inverted.set_quarantine_registry(Arc::clone(&registry));
        self.quarantine_registry = Some(registry);
    }

    /// Set the last timeseries ingest timestamp (for testing idle flush).
    pub fn set_last_ts_ingest(&mut self, value: Option<std::time::Instant>) {
        self.last_ts_ingest = value;
    }

    /// Test accessor: schema version for a strict-mode collection in `doc_configs`.
    ///
    /// Returns `None` if the collection is not registered on this core or is not
    /// in strict (Binary Tuple) storage mode.  Used by schema-visibility barrier
    /// integration tests to confirm every core has applied a schema ALTER.
    #[cfg(test)]
    pub fn schema_version_for_collection(&self, tid: u64, collection: &str) -> Option<u32> {
        let key = (TenantId::new(tid), collection.to_string());
        let config = self.doc_configs.get(&key)?;
        match &config.storage_mode {
            crate::bridge::physical_plan::StorageMode::Strict { schema } => Some(schema.version),
            crate::bridge::physical_plan::StorageMode::Schemaless => None,
        }
    }

    /// Test accessor: row count in a columnar memtable.
    #[cfg(test)]
    pub fn columnar_memtable_row_count(&self, tid: u64, collection: &str) -> u64 {
        let key = (TenantId::new(tid), collection.to_string());
        self.columnar_memtables
            .get(&key)
            .map(|mt| mt.row_count())
            .unwrap_or(0)
    }

    /// Test accessor: total row count across all partitions in a timeseries registry.
    #[cfg(test)]
    pub fn ts_registry_row_count(&self, tid: u64, collection: &str) -> u64 {
        let key = (TenantId::new(tid), collection.to_string());
        self.ts_registries
            .get(&key)
            .map(|reg| {
                let range = nodedb_types::timeseries::TimeRange::new(0, i64::MAX);
                reg.query_partitions(&range)
                    .iter()
                    .map(|e| e.meta.row_count)
                    .sum()
            })
            .unwrap_or(0)
    }
}
