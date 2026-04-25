use nodedb_types::{Surrogate, SurrogateBitmap};

use super::types::{EnforcementOptions, RegisteredIndex, StorageMode, UpdateValue};

/// Document engine physical operations (schemaless + strict + DML).
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum DocumentOp {
    /// Point lookup by document ID.
    PointGet {
        collection: String,
        document_id: String,
        /// Stable cross-engine identity bound to `(collection, document_id)`
        /// in the catalog. The handler hex-encodes this to compute the
        /// substrate row key; user-PK strings are not used for storage
        /// addressing on the document path.
        surrogate: Surrogate,
        /// Raw primary-key bytes, used by follower-side WAL decode to
        /// re-derive the surrogate via the catalog rev table when the
        /// physical plan is reconstructed from the WAL stream.
        pk_bytes: Vec<u8>,
        /// RLS post-fetch filters (serialized `Vec<ScanFilter>`).
        /// If non-empty, the Data Plane evaluates these after fetching
        /// the document. Returns `NOT_FOUND` on denial (no info leak).
        /// Injected by the Control Plane planner from RLS policies.
        #[allow(clippy::doc_markdown)]
        rls_filters: Vec<u8>,
        /// `FOR SYSTEM_TIME AS OF <ms>` cutoff. `None` = current state.
        /// Honored only by bitemporal collections; the planner rejects
        /// temporal point-gets on non-bitemporal collections.
        system_as_of_ms: Option<i64>,
        /// `FOR VALID_TIME CONTAINS <ms>` filter.
        valid_at_ms: Option<i64>,
    },

    /// Point write: insert/update a document.
    ///
    /// This variant is unconditional-overwrite (upsert semantics). Use
    /// [`DocumentOp::PointInsert`] for SQL `INSERT` where duplicate PKs must
    /// raise `unique_violation`.
    PointPut {
        collection: String,
        document_id: String,
        value: Vec<u8>,
        /// Catalog-bound identity for `(collection, document_id)`.
        /// Hex-encoded by the handler to compute the substrate row key.
        surrogate: Surrogate,
        /// Raw primary-key bytes, used by follower-side WAL decode to
        /// re-derive the surrogate via the catalog rev table.
        pk_bytes: Vec<u8>,
    },

    /// Point insert: write one document, fail on duplicate primary key.
    ///
    /// When `if_absent` is true the handler silently skips conflicts
    /// (`INSERT ... ON CONFLICT DO NOTHING`). When false, a duplicate
    /// primary key raises a unique-violation error.
    ///
    /// Separate from [`DocumentOp::PointPut`] because the write path must
    /// probe the existence of `document_id` inside the same write txn as
    /// the insert — conflating the two routed `INSERT` to silent upsert.
    PointInsert {
        collection: String,
        document_id: String,
        value: Vec<u8>,
        if_absent: bool,
        /// Stable cross-engine identity assigned by the CP-side
        /// `SurrogateAssigner` from `(collection, document_id_bytes)`.
        /// `Surrogate::ZERO` is reserved as a sentinel and only appears
        /// in test fixtures.
        surrogate: Surrogate,
    },

    /// Point delete: remove a document.
    PointDelete {
        collection: String,
        document_id: String,
        /// Catalog-bound identity for `(collection, document_id)`. The
        /// handler hex-encodes this for the substrate row key.
        surrogate: Surrogate,
        /// Raw primary-key bytes for follower WAL decode rebind.
        pk_bytes: Vec<u8>,
    },

    /// Point update: read-modify-write with field-level changes.
    PointUpdate {
        collection: String,
        document_id: String,
        /// Catalog-bound identity for `(collection, document_id)`. The
        /// handler hex-encodes this for the substrate row key.
        surrogate: Surrogate,
        /// Raw primary-key bytes for follower WAL decode rebind.
        pk_bytes: Vec<u8>,
        /// Field name → assignment RHS (literal bytes or row-scope expression).
        updates: Vec<(String, UpdateValue)>,
        /// If true, return the post-update document as payload (for RETURNING clause).
        returning: bool,
    },

    /// Full collection scan with filtering, sorting, and pagination.
    Scan {
        collection: String,
        limit: usize,
        offset: usize,
        sort_keys: Vec<(String, bool)>,
        /// Filter predicates serialized as JSON.
        filters: Vec<u8>,
        distinct: bool,
        projection: Vec<String>,
        /// Serialized `Vec<ComputedColumn>`.
        computed_columns: Vec<u8>,
        /// Serialized `Vec<WindowFuncSpec>`.
        window_functions: Vec<u8>,
        /// `FOR SYSTEM_TIME AS OF <ms>` cutoff. `None` = current state.
        /// Honored only by collections registered with bitemporal storage;
        /// the planner rejects temporal scans on non-bitemporal collections
        /// at SQL plan time, so the handler trusts this field.
        system_as_of_ms: Option<i64>,
        /// `FOR VALID_TIME CONTAINS <ms>` filter. `None` = no filter.
        valid_at_ms: Option<i64>,
        /// Optional surrogate prefilter injected by a cross-engine sub-plan.
        /// When present, the scan skips rows whose surrogate is absent from
        /// this bitmap. `None` = no prefilter; full collection is scanned.
        #[serde(default)]
        #[msgpack(default)]
        prefilter: Option<SurrogateBitmap>,
    },

    /// Batch insert documents in a single redb transaction.
    BatchInsert {
        collection: String,
        /// (document_id, value_bytes) pairs.
        documents: Vec<(String, Vec<u8>)>,
    },

    /// Range scan on a sparse/metadata index.
    RangeScan {
        collection: String,
        field: String,
        lower: Option<Vec<u8>>,
        upper: Option<Vec<u8>>,
        limit: usize,
    },

    /// Register collection with secondary indexes and storage mode (DDL).
    Register {
        collection: String,
        /// Full secondary-index specs (name, path, unique, case_insensitive,
        /// state). Replaces the old `Vec<String>` path-only payload so the
        /// write handler can enforce UNIQUE and skip Building indexes.
        indexes: Vec<RegisteredIndex>,
        crdt_enabled: bool,
        /// Storage encoding mode. Determines how documents are serialized.
        storage_mode: StorageMode,
        /// Collection enforcement options propagated from catalog (boxed to reduce enum size).
        enforcement: Box<EnforcementOptions>,
        /// Bitemporal storage: every write becomes a new version keyed by
        /// `system_from_ms`; reads use the versioned table and Ceiling
        /// resolver.
        bitemporal: bool,
    },

    /// Lookup documents by secondary index value.
    IndexLookup {
        collection: String,
        path: String,
        value: String,
    },

    /// Fetch full document rows via a secondary index.
    ///
    /// Emitted from `SqlPlan::DocumentIndexLookup` for SELECT queries where
    /// the WHERE clause has an equality predicate on an indexed field. The
    /// handler resolves doc IDs via `sparse.index_lookup`, fetches each
    /// document, applies any remaining filters + projection, and emits
    /// scan-compatible row output via `response_codec`.
    ///
    /// Sort / distinct / window functions are handled by the planner
    /// falling back to a full scan — the planner only emits this variant
    /// when none of those are present.
    IndexedFetch {
        collection: String,
        /// Indexed field path (e.g. `$.email`).
        path: String,
        /// Equality lookup value. COLLATE NOCASE rewrites normalize to
        /// lowercase before emission, so the handler does not need to.
        value: String,
        /// Remaining post-filters (serialized `Vec<ScanFilter>`).
        filters: Vec<u8>,
        /// Column names to include in each row (empty = all fields).
        projection: Vec<String>,
        limit: usize,
        offset: usize,
    },

    /// Drop all secondary index entries for a field.
    DropIndex { collection: String, field: String },

    /// Backfill a secondary index from existing collection documents.
    ///
    /// Emitted by CREATE INDEX on a collection that already has rows.
    /// The handler scans every document, extracts the indexed value, and
    /// writes sparse-index entries — atomically detecting UNIQUE
    /// violations along the way. Running this inside a single write
    /// transaction is intentional: it mirrors Postgres's blocking CREATE
    /// INDEX lock semantics and guarantees the index is consistent when
    /// the Ready flip commits.
    BackfillIndex {
        collection: String,
        /// JSON-path-like field (e.g. `$.email`).
        path: String,
        is_array: bool,
        unique: bool,
        case_insensitive: bool,
        /// Partial-index predicate (raw SQL text of the `WHERE` body)
        /// or `None` for full indexes. Rows where the predicate is
        /// false are skipped — not indexed, not UNIQUE-checked.
        #[serde(default)]
        predicate: Option<String>,
    },

    /// Truncate: delete ALL documents in a collection.
    /// If `restart_identity` is true, sequences attached to this collection's
    /// fields are reset to their start value after truncation.
    Truncate {
        collection: String,
        restart_identity: bool,
    },

    /// Estimate count via HLL cardinality stats.
    EstimateCount { collection: String, field: String },

    /// INSERT ... SELECT: copy documents from source to target.
    InsertSelect {
        target_collection: String,
        source_collection: String,
        source_filters: Vec<u8>,
        source_limit: usize,
    },

    /// Upsert: insert or merge. When `on_conflict_updates` is non-empty,
    /// the conflict branch evaluates those assignments against the
    /// *existing* document instead of merging the inserted value —
    /// the `INSERT ... ON CONFLICT DO UPDATE SET ...` path.
    Upsert {
        collection: String,
        document_id: String,
        value: Vec<u8>,
        on_conflict_updates: Vec<(String, UpdateValue)>,
        /// Stable cross-engine identity assigned by the CP-side
        /// `SurrogateAssigner`. `Surrogate::ZERO` only in test fixtures.
        surrogate: Surrogate,
    },

    /// Bulk update: scan + apply field updates to all matches.
    BulkUpdate {
        collection: String,
        filters: Vec<u8>,
        updates: Vec<(String, UpdateValue)>,
        /// If true, return updated documents as JSON array payload (for RETURNING clause).
        returning: bool,
    },

    /// Bulk delete: scan + delete all matches.
    BulkDelete {
        collection: String,
        filters: Vec<u8>,
    },
}
