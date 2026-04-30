//! SqlPlan intermediate representation and supporting types.

use crate::fts_types::FtsQuery;
use crate::temporal::TemporalScope;
use crate::types_array;
use crate::types_expr::{SqlExpr, SqlPayloadAtom, SqlValue};
pub use nodedb_types::vector_distance::DistanceMetric;

use super::filter::Filter;
use super::query::{
    AggregateExpr, EngineType, JoinType, Projection, SortKey, SpatialPredicate, WindowSpec,
};

/// Cross-engine prefilter for `SqlPlan::VectorSearch`: the array slice
/// runs first, its matching cells' surrogates form a bitmap that gates
/// the HNSW candidate set.
#[derive(Debug, Clone)]
pub struct NdArrayPrefilter {
    /// Array name (resolved against the catalog).
    pub array_name: String,
    /// Slice predicate (per-dim ranges).
    pub slice: types_array::ArraySliceAst,
}

/// Knobs the vector planner exposes via SQL.
///
/// All fields default to `None` / sensible defaults, in which case the
/// executor falls back to the collection's configured quantization and
/// `ef_search` heuristic.
///
/// Parsed from an optional JSON-string third argument to
/// `vector_distance(field, query, '{"quantization":"rabitq","oversample":3}')`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct VectorAnnOptions {
    pub quantization: Option<VectorQuantization>,
    pub oversample: Option<u8>,
    pub query_dim: Option<u32>,
    pub meta_token_budget: Option<u8>,
    /// Override `ef_search`; falls back to `2 * top_k` when None.
    pub ef_search_override: Option<usize>,
    /// Target recall used with the cost model to escalate from coarse to
    /// fine quantization.
    pub target_recall: Option<f32>,
}

/// Quantization choices exposed at SQL level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorQuantization {
    None,
    Sq8,
    Pq,
    RaBitQ,
    Bbq,
    Binary,
    Ternary,
    Opq,
}

impl VectorAnnOptions {
    /// Convert into the runtime mirror used by `nodedb-types`.
    pub fn to_runtime(&self) -> nodedb_types::VectorAnnOptions {
        nodedb_types::VectorAnnOptions {
            quantization: self.quantization.map(|q| match q {
                VectorQuantization::None => nodedb_types::VectorQuantization::None,
                VectorQuantization::Sq8 => nodedb_types::VectorQuantization::Sq8,
                VectorQuantization::Pq => nodedb_types::VectorQuantization::Pq,
                VectorQuantization::RaBitQ => nodedb_types::VectorQuantization::RaBitQ,
                VectorQuantization::Bbq => nodedb_types::VectorQuantization::Bbq,
                VectorQuantization::Binary => nodedb_types::VectorQuantization::Binary,
                VectorQuantization::Ternary => nodedb_types::VectorQuantization::Ternary,
                VectorQuantization::Opq => nodedb_types::VectorQuantization::Opq,
            }),
            oversample: self.oversample,
            query_dim: self.query_dim,
            meta_token_budget: self.meta_token_budget,
            ef_search_override: self.ef_search_override,
            target_recall: self.target_recall,
        }
    }
}

impl VectorQuantization {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Sq8 => "sq8",
            Self::Pq => "pq",
            Self::RaBitQ => "rabitq",
            Self::Bbq => "bbq",
            Self::Binary => "binary",
            Self::Ternary => "ternary",
            Self::Opq => "opq",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "none" => Some(Self::None),
            "sq8" => Some(Self::Sq8),
            "pq" => Some(Self::Pq),
            "rabitq" => Some(Self::RaBitQ),
            "bbq" => Some(Self::Bbq),
            "binary" => Some(Self::Binary),
            "ternary" => Some(Self::Ternary),
            "opq" => Some(Self::Opq),
            _ => None,
        }
    }
}

/// The top-level plan produced by the SQL planner.
#[derive(Debug, Clone)]
pub enum SqlPlan {
    // ── Constant ──
    /// Query with no FROM clause: SELECT 1, SELECT 'hello' AS name, etc.
    /// Produces a single row with evaluated constant expressions.
    ConstantResult {
        columns: Vec<String>,
        values: Vec<SqlValue>,
    },

    // ── Reads ──
    Scan {
        collection: String,
        alias: Option<String>,
        engine: EngineType,
        filters: Vec<Filter>,
        projection: Vec<Projection>,
        sort_keys: Vec<SortKey>,
        limit: Option<usize>,
        offset: usize,
        distinct: bool,
        window_functions: Vec<WindowSpec>,
        /// Bitemporal qualifier extracted from `FOR SYSTEM_TIME` /
        /// `FOR VALID_TIME`. Default when the scan is current-state.
        temporal: TemporalScope,
    },
    PointGet {
        collection: String,
        alias: Option<String>,
        engine: EngineType,
        key_column: String,
        key_value: SqlValue,
    },
    /// Document fetch via a secondary index: equality predicate on an
    /// indexed field. The executor performs an index lookup to resolve
    /// matching document IDs, reads each document, and applies any
    /// remaining filters, projection, sort, and limit.
    ///
    /// Emitted by `document_schemaless::plan_scan` /
    /// `document_strict::plan_scan` when the WHERE clause contains a
    /// single equality predicate on a `Ready` indexed field. Any
    /// additional predicates fall through as post-filters.
    DocumentIndexLookup {
        collection: String,
        alias: Option<String>,
        engine: EngineType,
        /// Indexed field path used for the lookup.
        field: String,
        /// Equality value from the WHERE clause.
        value: SqlValue,
        /// Remaining filters after extracting the equality used for lookup.
        filters: Vec<Filter>,
        projection: Vec<Projection>,
        sort_keys: Vec<SortKey>,
        limit: Option<usize>,
        offset: usize,
        distinct: bool,
        window_functions: Vec<WindowSpec>,
        /// Whether the chosen index is COLLATE NOCASE — the executor
        /// lowercases the lookup value before probing.
        case_insensitive: bool,
        /// Bitemporal qualifier — mirrors `Scan::temporal`. Document
        /// engines must honor it at the Ceiling stage.
        temporal: TemporalScope,
    },
    RangeScan {
        collection: String,
        field: String,
        lower: Option<SqlValue>,
        upper: Option<SqlValue>,
        limit: usize,
    },

    // ── Writes ──
    Insert {
        collection: String,
        engine: EngineType,
        rows: Vec<Vec<(String, SqlValue)>>,
        /// Column defaults from schema: `(column_name, default_expr)`.
        /// Used to auto-generate values for missing columns (e.g. `id` with `UUID_V7`).
        column_defaults: Vec<(String, String)>,
        /// `ON CONFLICT DO NOTHING` semantics: when true, duplicate-PK rows
        /// are silently skipped instead of raising `unique_violation`. Plain
        /// `INSERT` (no `ON CONFLICT` clause) sets this to `false`.
        if_absent: bool,
    },
    /// KV INSERT: key and value are fundamentally separate.
    /// Each entry is `(key, value_columns)`.
    KvInsert {
        collection: String,
        entries: Vec<(SqlValue, Vec<(String, SqlValue)>)>,
        /// TTL in seconds (0 = no expiry). Extracted from `ttl` column if present.
        ttl_secs: u64,
        /// INSERT-vs-UPSERT distinction. `KvOp::Put` is a Redis-SET-style
        /// upsert by design; to honor SQL `INSERT` semantics the planner must
        /// tell the converter whether a duplicate key should raise (plain
        /// `INSERT`, `Insert`), be silently skipped (`ON CONFLICT DO NOTHING`,
        /// `InsertIfAbsent`), or overwrite (`UPSERT` / `ON CONFLICT DO
        /// UPDATE`, `Put`).
        intent: KvInsertIntent,
        /// `ON CONFLICT (key) DO UPDATE SET field = expr` assignments, carried
        /// through when `intent == Put` via the ON-CONFLICT-DO-UPDATE path.
        /// Empty for plain UPSERT (whole-value overwrite) and for INSERT
        /// variants.
        on_conflict_updates: Vec<(String, SqlExpr)>,
    },
    /// UPSERT: insert or merge if document exists.
    Upsert {
        collection: String,
        engine: EngineType,
        rows: Vec<Vec<(String, SqlValue)>>,
        column_defaults: Vec<(String, String)>,
        /// `ON CONFLICT (...) DO UPDATE SET field = expr` assignments.
        /// When empty, upsert is a plain merge: new columns overwrite existing.
        /// When non-empty, the engine applies these per-row against the
        /// *existing* document instead of merging the inserted values.
        on_conflict_updates: Vec<(String, SqlExpr)>,
    },
    InsertSelect {
        target: String,
        source: Box<SqlPlan>,
        limit: usize,
    },
    Update {
        collection: String,
        engine: EngineType,
        assignments: Vec<(String, SqlExpr)>,
        filters: Vec<Filter>,
        target_keys: Vec<SqlValue>,
        returning: bool,
    },
    Delete {
        collection: String,
        engine: EngineType,
        filters: Vec<Filter>,
        target_keys: Vec<SqlValue>,
    },
    Truncate {
        collection: String,
        restart_identity: bool,
    },

    // ── Joins ──
    Join {
        left: Box<SqlPlan>,
        right: Box<SqlPlan>,
        on: Vec<(String, String)>,
        join_type: JoinType,
        condition: Option<SqlExpr>,
        limit: usize,
        /// Post-join projection: column names to keep (empty = all columns).
        projection: Vec<Projection>,
        /// Post-join filters (from WHERE clause).
        filters: Vec<Filter>,
    },

    // ── Aggregation ──
    Aggregate {
        input: Box<SqlPlan>,
        group_by: Vec<SqlExpr>,
        aggregates: Vec<AggregateExpr>,
        having: Vec<Filter>,
        limit: usize,
    },

    // ── Timeseries ──
    TimeseriesScan {
        collection: String,
        time_range: (i64, i64),
        bucket_interval_ms: i64,
        group_by: Vec<String>,
        aggregates: Vec<AggregateExpr>,
        filters: Vec<Filter>,
        projection: Vec<Projection>,
        gap_fill: String,
        limit: usize,
        tiered: bool,
        /// Bitemporal system-time / valid-time scope. Only non-default
        /// on collections created `WITH BITEMPORAL`; `TimeseriesRules::plan_scan`
        /// rejects temporal scopes otherwise.
        temporal: TemporalScope,
    },
    TimeseriesIngest {
        collection: String,
        rows: Vec<Vec<(String, SqlValue)>>,
    },

    // ── Search (first-class) ──
    VectorSearch {
        collection: String,
        field: String,
        query_vector: Vec<f32>,
        top_k: usize,
        ef_search: usize,
        /// Distance metric requested by the query operator (`<->`, `<=>`, `<#>`).
        /// Overrides the collection-default metric at search time.
        metric: DistanceMetric,
        filters: Vec<Filter>,
        /// Optional cross-engine prefilter: when set, the ND-array slice
        /// runs first and its output cells' surrogates form a bitmap that
        /// gates the HNSW candidate set. Set by the planner when an
        /// `ORDER BY vector_distance(...) LIMIT k` query is JOINed against
        /// `NDARRAY_SLICE(...)`. The convert layer lowers this to
        /// `VectorOp::Search { inline_prefilter_plan: Some(ArrayOp::SurrogateBitmapScan) }`.
        array_prefilter: Option<NdArrayPrefilter>,
        /// ANN knobs parsed from the optional third JSON-string argument
        /// to `vector_distance(field, query, '{...}')`.
        ann_options: VectorAnnOptions,
        /// When `true`, the projection contains only the surrogate/PK column
        /// and/or `vector_distance(...)` — no payload fields. The Data Plane
        /// can skip the document-body fetch entirely for vector-primary
        /// collections. Always `false` for non-vector-primary collections
        /// (document body is the primary result).
        skip_payload_fetch: bool,
        /// Predicates against payload-indexed columns on a vector-primary
        /// collection. Each atom is `Eq(field, value)`, `In(field, values)`,
        /// or `Range(field, ...)`. The convert layer translates SqlValue →
        /// nodedb_types::Value and emits them as
        /// `VectorOp::Search::payload_filters`. The Data Plane intersects
        /// the resulting bitmap with the HNSW candidate set via the
        /// per-collection `PayloadIndexSet::pre_filter`.
        payload_filters: Vec<SqlPayloadAtom>,
    },
    MultiVectorSearch {
        collection: String,
        query_vector: Vec<f32>,
        top_k: usize,
        ef_search: usize,
    },
    TextSearch {
        collection: String,
        /// Structured FTS query.  Use `FtsQuery::Plain { text, fuzzy }` for
        /// simple keyword search.  `FtsQuery::And/Or/Prefix` are supported;
        /// `FtsQuery::Phrase` and `FtsQuery::Not` are represented but rejected
        /// by the executor with `Unsupported`.
        query: FtsQuery,
        top_k: usize,
        filters: Vec<Filter>,
    },
    HybridSearch {
        collection: String,
        query_vector: Vec<f32>,
        query_text: String,
        top_k: usize,
        ef_search: usize,
        vector_weight: f32,
        fuzzy: bool,
    },
    SpatialScan {
        collection: String,
        field: String,
        predicate: SpatialPredicate,
        query_geometry: nodedb_types::geometry::Geometry,
        distance_meters: f64,
        attribute_filters: Vec<Filter>,
        limit: usize,
        projection: Vec<Projection>,
    },

    // ── Composite ──
    Union {
        inputs: Vec<SqlPlan>,
        distinct: bool,
    },
    Intersect {
        left: Box<SqlPlan>,
        right: Box<SqlPlan>,
        all: bool,
    },
    Except {
        left: Box<SqlPlan>,
        right: Box<SqlPlan>,
        all: bool,
    },
    RecursiveScan {
        collection: String,
        base_filters: Vec<Filter>,
        recursive_filters: Vec<Filter>,
        /// Equi-join link for tree-traversal recursion:
        /// `(collection_field, working_table_field)`.
        /// e.g. `("parent_id", "id")` means each iteration finds rows
        /// where `collection.parent_id` matches a `working_table.id`.
        join_link: Option<(String, String)>,
        max_iterations: usize,
        distinct: bool,
        limit: usize,
    },

    /// Non-recursive CTE: execute each definition, then the outer query.
    Cte {
        /// CTE definitions: `(name, subquery_plan)`.
        definitions: Vec<(String, SqlPlan)>,
        /// The outer query that references CTE names.
        outer: Box<SqlPlan>,
    },

    // ── Array (ND sparse) ─────────────────────────────────────
    /// `CREATE ARRAY <name> DIMS (...) ATTRS (...) TILE_EXTENTS (...)`.
    /// AST is engine-agnostic — the Origin converter builds the typed
    /// `nodedb_array::ArraySchema` and persists the catalog row.
    CreateArray {
        name: String,
        dims: Vec<types_array::ArrayDimAst>,
        attrs: Vec<types_array::ArrayAttrAst>,
        tile_extents: Vec<i64>,
        cell_order: types_array::ArrayCellOrderAst,
        tile_order: types_array::ArrayTileOrderAst,
        /// Hilbert-prefix bits for vShard routing (1–16, default 8).
        prefix_bits: u8,
        /// Audit-retention horizon in milliseconds. `None` = non-bitemporal.
        audit_retain_ms: Option<u64>,
        /// Compliance floor for `audit_retain_ms`. `None` = no floor.
        minimum_audit_retain_ms: Option<u64>,
    },
    /// `DROP ARRAY [IF EXISTS] <name>` — pure Control-Plane catalog
    /// mutation. Per-core array store cleanup happens lazily.
    DropArray { name: String, if_exists: bool },
    /// `ALTER NDARRAY <name> SET (audit_retain_ms = N, ...)`.
    ///
    /// Double-`Option` semantics for each diff field:
    /// - `None`          = key was absent from SET clause → field unchanged.
    /// - `Some(None)`    = key present with value `NULL` → field set to NULL.
    /// - `Some(Some(v))` = key present with integer value → field set to v.
    AlterArray {
        name: String,
        /// New value for `audit_retain_ms`. `Some(None)` unregisters
        /// the array from the bitemporal retention registry.
        audit_retain_ms: Option<Option<i64>>,
        /// New value for `minimum_audit_retain_ms`. Cannot be NULL.
        minimum_audit_retain_ms: Option<u64>,
    },
    /// `INSERT INTO ARRAY <name> COORDS (...) VALUES (...) [, ...]`.
    InsertArray {
        name: String,
        rows: Vec<types_array::ArrayInsertRow>,
    },
    /// `DELETE FROM ARRAY <name> WHERE COORDS IN ((...), (...))`.
    DeleteArray {
        name: String,
        coords: Vec<Vec<types_array::ArrayCoordLiteral>>,
    },
    /// `SELECT * FROM NDARRAY_SLICE(name, {dim:[lo,hi],..}, [attrs], limit)`.
    NdArraySlice {
        name: String,
        slice: types_array::ArraySliceAst,
        /// Attribute names. Empty = all attrs.
        attr_projection: Vec<String>,
        /// 0 = unlimited.
        limit: u32,
        /// Bitemporal qualifier. When both axes are `None` / `Any`, the Data
        /// Plane returns the live (current) state — the default fast path.
        /// Populated from `AS OF SYSTEM TIME` / `AS OF VALID TIME` clauses.
        temporal: TemporalScope,
    },
    /// `SELECT * FROM NDARRAY_PROJECT(name, [attrs])`.
    NdArrayProject {
        name: String,
        /// Attribute names. Must be non-empty.
        attr_projection: Vec<String>,
    },
    /// `SELECT * FROM NDARRAY_AGG(name, attr, reducer [, group_by_dim])`.
    NdArrayAgg {
        name: String,
        attr: String,
        reducer: types_array::ArrayReducerAst,
        /// `None` = scalar fold; `Some(name)` = group by that dim.
        group_by_dim: Option<String>,
        /// Bitemporal qualifier. When both axes are `None` / `Any`, the Data
        /// Plane aggregates against the live (current) state — the default
        /// fast path. Populated from `AS OF SYSTEM TIME` / `AS OF VALID TIME`.
        temporal: TemporalScope,
    },
    /// `SELECT * FROM NDARRAY_ELEMENTWISE(left, right, op, attr)`.
    NdArrayElementwise {
        left: String,
        right: String,
        op: types_array::ArrayBinaryOpAst,
        attr: String,
    },
    /// `SELECT NDARRAY_FLUSH(name)` — returns one row `{result: BOOL}`.
    NdArrayFlush { name: String },
    /// `SELECT NDARRAY_COMPACT(name)` — returns one row `{result: BOOL}`.
    NdArrayCompact { name: String },

    // ── Vector-primary ──────────────────────────────────────────────────
    /// INSERT into a vector-primary collection.
    ///
    /// Emitted by the planner instead of the generic `Insert` variant when the
    /// target collection has `primary = PrimaryEngine::Vector`. The Data Plane
    /// routes each row through `VectorOp::DirectUpsert`, bypassing full-document
    /// MessagePack encoding.
    VectorPrimaryInsert {
        collection: String,
        /// Vector column name (matches `VectorPrimaryConfig::vector_field`).
        /// Plumbed to `VectorOp::DirectUpsert` so the Data Plane keys its
        /// HNSW index by `(tid, collection, field)` — the same key the SELECT
        /// path uses.
        field: String,
        /// Collection-level quantization. Applied via `set_quantization` on
        /// the first DirectUpsert so subsequent seals trigger codec-dispatch
        /// rebuilds against the configured codec.
        quantization: nodedb_types::VectorQuantization,
        /// Payload field names that get equality bitmap indexes. Registered
        /// via `payload.add_index` on the first DirectUpsert.
        payload_indexes: Vec<(String, nodedb_types::PayloadIndexKind)>,
        rows: Vec<VectorPrimaryRow>,
    },
}

/// A single row for a vector-primary INSERT.
///
/// The surrogate is allocated by the Control Plane before the op reaches
/// the Data Plane; the Data Plane only stores the binding.
#[derive(Debug, Clone)]
pub struct VectorPrimaryRow {
    /// Global surrogate allocated by the Control Plane (`Surrogate::ZERO`
    /// is a sentinel meaning "not yet assigned").
    pub surrogate: nodedb_types::Surrogate,
    /// FP32 vector extracted from the vector-field column.
    pub vector: Vec<f32>,
    /// Payload fields (non-vector columns that may feed bitmap indexes).
    pub payload_fields: std::collections::HashMap<String, SqlValue>,
}

/// INSERT-vs-UPSERT intent carried on `SqlPlan::KvInsert`.
///
/// The KV engine's `KvOp::Put` is a Redis-SET-style upsert: write wins
/// unconditionally. SQL requires `INSERT` to raise `unique_violation`
/// on duplicate keys, so the plan must carry the caller's intent through
/// to the Data Plane where the hash-index existence probe happens.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KvInsertIntent {
    /// Plain `INSERT`: duplicate key raises `SQLSTATE 23505`.
    Insert,
    /// `INSERT ... ON CONFLICT DO NOTHING`: duplicate key is a no-op.
    InsertIfAbsent,
    /// `UPSERT` / `INSERT ... ON CONFLICT (key) DO UPDATE` / RESP `SET`:
    /// duplicate key overwrites. Also the shape used by the RESP SET path.
    Put,
}
