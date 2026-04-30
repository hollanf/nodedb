//! TextFields — per-request wire fields encoded as a MsgPack map.
//!
//! # Wire format
//!
//! TextFields is encoded as a MsgPack **map** whose keys are `u16` numeric
//! field IDs starting at 1. Fields whose value is `None` are **omitted**
//! entirely (compact encoding). The decoder ignores unknown keys, so new
//! fields can be added to newer servers without breaking older clients
//! (forward compatibility).
//!
//! # Field ID policy
//!
//! * IDs are assigned sequentially starting at 1.
//! * Deleted fields are **tombstoned**: the ID is never recycled. Add a
//!   comment `// ID N: TOMBSTONE — <field_name> deleted <date>` to mark
//!   retired IDs.
//! * The field ID table below is the authoritative source of truth.
//!
//! # Field ID table
//!
//! ```text
//!  1  auth
//!  2  sql
//!  3  key
//!  4  value
//!  5  collection
//!  6  document_id
//!  7  data
//!  8  query_vector
//!  9  top_k
//! 10  field
//! 11  limit
//! 12  delta
//! 13  peer_id
//! 14  vector_top_k
//! 15  edge_label
//! 16  direction
//! 17  expansion_depth
//! 18  final_top_k
//! 19  vector_k
//! 20  graph_k
//! 21  vector_field
//! 22  start_node
//! 23  end_node
//! 24  depth
//! 25  from_node
//! 26  to_node
//! 27  edge_type
//! 28  properties
//! 29  query_text
//! 30  vector_weight
//! 31  fuzzy
//! 32  ef_search
//! 33  field_name
//! 34  lower_bound
//! 35  upper_bound
//! 36  mutation_id
//! 37  vectors
//! 38  documents
//! 39  query_geometry
//! 40  spatial_predicate
//! 41  distance_meters
//! 42  payload
//! 43  format
//! 44  time_range_start
//! 45  time_range_end
//! 46  bucket_interval
//! 47  ttl_ms
//! 48  cursor
//! 49  match_pattern
//! 50  keys
//! 51  entries
//! 52  fields
//! 53  incr_delta
//! 54  incr_float_delta
//! 55  expected
//! 56  new_value
//! 57  index_name
//! 58  sort_columns
//! 59  key_column
//! 60  window_type
//! 61  window_timestamp_column
//! 62  window_start_ms
//! 63  window_end_ms
//! 64  top_k_count
//! 65  score_min
//! 66  score_max
//! 67  updates
//! 68  filters
//! 69  vector
//! 70  vector_id
//! 71  policy
//! 72  algorithm
//! 73  match_query
//! 74  algo_params
//! 75  index_paths
//! 76  source_collection
//! 77  field_position
//! 78  backfill
//! 79  m
//! 80  ef_construction
//! 81  metric
//! 82  index_type
//! ```

use serde::{Deserialize, Serialize};

use super::auth::AuthMethod;
use super::batch::{BatchDocument, BatchVector};
use crate::json_msgpack::JsonValue;

// ─── Numeric field-ID constants ────────────────────────────────────

const FID_AUTH: u16 = 1;
const FID_SQL: u16 = 2;
const FID_KEY: u16 = 3;
const FID_VALUE: u16 = 4;
const FID_COLLECTION: u16 = 5;
const FID_DOCUMENT_ID: u16 = 6;
const FID_DATA: u16 = 7;
const FID_QUERY_VECTOR: u16 = 8;
const FID_TOP_K: u16 = 9;
const FID_FIELD: u16 = 10;
const FID_LIMIT: u16 = 11;
const FID_DELTA: u16 = 12;
const FID_PEER_ID: u16 = 13;
const FID_VECTOR_TOP_K: u16 = 14;
const FID_EDGE_LABEL: u16 = 15;
const FID_DIRECTION: u16 = 16;
const FID_EXPANSION_DEPTH: u16 = 17;
const FID_FINAL_TOP_K: u16 = 18;
const FID_VECTOR_K: u16 = 19;
const FID_GRAPH_K: u16 = 20;
const FID_VECTOR_FIELD: u16 = 21;
const FID_START_NODE: u16 = 22;
const FID_END_NODE: u16 = 23;
const FID_DEPTH: u16 = 24;
const FID_FROM_NODE: u16 = 25;
const FID_TO_NODE: u16 = 26;
const FID_EDGE_TYPE: u16 = 27;
const FID_PROPERTIES: u16 = 28;
const FID_QUERY_TEXT: u16 = 29;
const FID_VECTOR_WEIGHT: u16 = 30;
const FID_FUZZY: u16 = 31;
const FID_EF_SEARCH: u16 = 32;
const FID_FIELD_NAME: u16 = 33;
const FID_LOWER_BOUND: u16 = 34;
const FID_UPPER_BOUND: u16 = 35;
const FID_MUTATION_ID: u16 = 36;
const FID_VECTORS: u16 = 37;
const FID_DOCUMENTS: u16 = 38;
const FID_QUERY_GEOMETRY: u16 = 39;
const FID_SPATIAL_PREDICATE: u16 = 40;
const FID_DISTANCE_METERS: u16 = 41;
const FID_PAYLOAD: u16 = 42;
const FID_FORMAT: u16 = 43;
const FID_TIME_RANGE_START: u16 = 44;
const FID_TIME_RANGE_END: u16 = 45;
const FID_BUCKET_INTERVAL: u16 = 46;
const FID_TTL_MS: u16 = 47;
const FID_CURSOR: u16 = 48;
const FID_MATCH_PATTERN: u16 = 49;
const FID_KEYS: u16 = 50;
const FID_ENTRIES: u16 = 51;
const FID_FIELDS: u16 = 52;
const FID_INCR_DELTA: u16 = 53;
const FID_INCR_FLOAT_DELTA: u16 = 54;
const FID_EXPECTED: u16 = 55;
const FID_NEW_VALUE: u16 = 56;
const FID_INDEX_NAME: u16 = 57;
const FID_SORT_COLUMNS: u16 = 58;
const FID_KEY_COLUMN: u16 = 59;
const FID_WINDOW_TYPE: u16 = 60;
const FID_WINDOW_TIMESTAMP_COLUMN: u16 = 61;
const FID_WINDOW_START_MS: u16 = 62;
const FID_WINDOW_END_MS: u16 = 63;
const FID_TOP_K_COUNT: u16 = 64;
const FID_SCORE_MIN: u16 = 65;
const FID_SCORE_MAX: u16 = 66;
const FID_UPDATES: u16 = 67;
const FID_FILTERS: u16 = 68;
const FID_VECTOR: u16 = 69;
const FID_VECTOR_ID: u16 = 70;
const FID_POLICY: u16 = 71;
const FID_ALGORITHM: u16 = 72;
const FID_MATCH_QUERY: u16 = 73;
const FID_ALGO_PARAMS: u16 = 74;
const FID_INDEX_PATHS: u16 = 75;
const FID_SOURCE_COLLECTION: u16 = 76;
const FID_FIELD_POSITION: u16 = 77;
const FID_BACKFILL: u16 = 78;
const FID_M: u16 = 79;
const FID_EF_CONSTRUCTION: u16 = 80;
const FID_METRIC: u16 = 81;
const FID_INDEX_TYPE: u16 = 82;

// ─── TextFields struct ─────────────────────────────────────────────

/// Catch-all text fields used by most operations.
///
/// Each operation uses a subset; unused fields default to `None`/empty.
///
/// Wire format: MsgPack map with `u16` numeric field IDs. `None` fields
/// are omitted. Unknown keys are ignored by the decoder (forward-compat).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TextFields {
    // ── Auth ─────────────────────────────────────────────────
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth: Option<AuthMethod>,

    // ── SQL/DDL/Explain/Set/Show/Reset/CopyFrom ─────────────
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,

    // ── Collection + document targeting ──────────────────────
    #[serde(skip_serializing_if = "Option::is_none")]
    pub collection: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub document_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Vec<u8>>,

    // ── Vector search ────────────────────────────────────────
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_vector: Option<Vec<f32>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_k: Option<u32>,

    // ── Range scan ───────────────────────────────────────────
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,

    // ── CRDT ─────────────────────────────────────────────────
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delta: Option<Vec<u8>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub peer_id: Option<u64>,

    // ── Graph RAG fusion ─────────────────────────────────────
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_top_k: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub edge_label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub direction: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expansion_depth: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub final_top_k: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_k: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub graph_k: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_field: Option<String>,

    // ── Graph ops ────────────────────────────────────────────
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_node: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_node: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub depth: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from_node: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to_node: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub edge_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<serde_json::Value>,

    // ── Text/Hybrid search ───────────────────────────────────
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_weight: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fuzzy: Option<bool>,

    // ── Vector search tuning ────────────────────────────────
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ef_search: Option<u32>,
    /// Named vector field (for multi-field vector collections).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field_name: Option<String>,

    // ── Range scan bounds ───────────────────────────────────
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lower_bound: Option<Vec<u8>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upper_bound: Option<Vec<u8>>,

    // ── CRDT dedup ──────────────────────────────────────────
    /// Monotonic mutation ID for CRDT delta deduplication.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mutation_id: Option<u64>,

    // ── Batch operations ─────────────────────────────────────
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vectors: Option<Vec<BatchVector>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub documents: Option<Vec<BatchDocument>>,

    // ── Spatial scan ───────────────────────────────────────────
    /// Query geometry as GeoJSON bytes (for SpatialScan).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_geometry: Option<Vec<u8>>,
    /// Spatial predicate name: "dwithin", "contains", "intersects", "within".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spatial_predicate: Option<String>,
    /// Distance threshold in meters (for ST_DWithin).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub distance_meters: Option<f64>,

    // ── Timeseries ───────────────────────────────────────────
    /// ILP payload bytes (for TimeseriesIngest).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<Vec<u8>>,
    /// Ingest format (default: "ilp").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
    /// Time range start (epoch ms, for TimeseriesScan).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_range_start: Option<i64>,
    /// Time range end (epoch ms, for TimeseriesScan).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_range_end: Option<i64>,
    /// Bucket interval string for time_bucket aggregation (e.g., "5m").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bucket_interval: Option<String>,

    // ── KV advanced ─────────────────────────────────────────
    /// TTL in milliseconds (for KvExpire, KvBatchPut).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttl_ms: Option<u64>,
    /// Cursor bytes for KvScan pagination.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<Vec<u8>>,
    /// Glob pattern for KvScan key matching.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub match_pattern: Option<String>,
    /// Multiple keys for BatchGet / Delete.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keys: Option<Vec<Vec<u8>>>,
    /// Key-value entries for BatchPut: [(key, value), ...].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entries: Option<Vec<(Vec<u8>, Vec<u8>)>>,
    /// Field names for FieldGet.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields: Option<Vec<String>>,

    // ── KV atomic operations ────────────────────────────────
    /// Integer delta for KvIncr.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub incr_delta: Option<i64>,
    /// Float delta for KvIncrFloat.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub incr_float_delta: Option<f64>,
    /// Expected value for KvCas.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected: Option<Vec<u8>>,
    /// New value for KvCas / KvGetSet.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_value: Option<Vec<u8>>,

    // ── KV sorted index operations ──────────────────────────
    /// Sorted index name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_name: Option<String>,
    /// Sort columns: [(column_name, direction), ...].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort_columns: Option<Vec<(String, String)>>,
    /// Primary key column for sorted index.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_column: Option<String>,
    /// Window type for sorted index: "none", "daily", "weekly", "monthly", "custom".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub window_type: Option<String>,
    /// Timestamp column for windowed sorted index.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub window_timestamp_column: Option<String>,
    /// Custom window start (ms since epoch).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub window_start_ms: Option<u64>,
    /// Custom window end (ms since epoch).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub window_end_ms: Option<u64>,
    /// Top-K value for SortedIndexTopK.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_k_count: Option<u32>,
    /// Score min for SortedIndexRange (encoded bytes).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score_min: Option<Vec<u8>>,
    /// Score max for SortedIndexRange (encoded bytes).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score_max: Option<Vec<u8>>,

    // ── Document advanced ───────────────────────────────────
    /// Field-level updates: [(field_name, value_bytes), ...].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updates: Option<Vec<(String, Vec<u8>)>>,
    /// Serialized filter predicates (MessagePack).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filters: Option<Vec<u8>>,

    // ── Vector advanced ─────────────────────────────────────
    /// Single vector embedding (for VectorInsert).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector: Option<Vec<f32>>,
    /// Vector ID for deletion. Wire type is u64 to accommodate future range
    /// expansion; the server narrows to u32 via the surrogate space check.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_id: Option<u64>,

    // ── Collection policy ────────────────────────────────────
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policy: Option<serde_json::Value>,

    // ── Graph algorithm/match ───────────────────────────────
    /// Algorithm name for GraphAlgo (e.g., "pagerank", "wcc", "sssp").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub algorithm: Option<String>,
    /// Cypher-subset MATCH query string for GraphMatch.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub match_query: Option<String>,
    /// Algorithm-specific parameters (JSON object).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub algo_params: Option<serde_json::Value>,

    // ── Document DDL ────────────────────────────────────────
    /// Index paths for DocumentRegister.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_paths: Option<Vec<String>>,
    /// Source collection for InsertSelect.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_collection: Option<String>,

    // ── KV DDL ──────────────────────────────────────────────
    /// Field position in tuple for KvRegisterIndex.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field_position: Option<u64>,
    /// Whether to backfill existing keys on index creation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backfill: Option<bool>,

    // ── Vector DDL ──────────────────────────────────────────
    /// HNSW M parameter (max connections per layer).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub m: Option<u16>,
    /// HNSW ef_construction parameter.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ef_construction: Option<u16>,
    /// Distance metric name ("cosine", "euclidean", "dot").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metric: Option<String>,
    /// Index type ("hnsw", "hnsw_pq", "ivf_pq").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_type: Option<String>,
}

// ─── Skip helper ──────────────────────────────────────────────────

/// Consume and discard the next MsgPack value from `reader`.
///
/// Uses the "try-and-back-up" property of zerompk readers: a failed
/// typed read restores the format byte, so we can try each type in
/// order without permanently advancing the cursor.
///
/// This is used for forward-compat: unknown field IDs are skipped
/// rather than rejected.
fn skip_msgpack_value<'de, R: zerompk::Read<'de>>(reader: &mut R) -> zerompk::Result<()> {
    // Nil
    if reader.read_nil().is_ok() {
        return Ok(());
    }
    // Boolean
    if reader.read_boolean().is_ok() {
        return Ok(());
    }
    // Signed integers (handles neg fixint, int8/16/32/64)
    if reader.read_i64().is_ok() {
        return Ok(());
    }
    // Unsigned integers (handles uint8/16/32/64 and pos fixint that i64 may miss)
    if reader.read_u64().is_ok() {
        return Ok(());
    }
    // Floats
    if reader.read_f32().is_ok() {
        return Ok(());
    }
    if reader.read_f64().is_ok() {
        return Ok(());
    }
    // String
    if reader.read_string().is_ok() {
        return Ok(());
    }
    // Binary
    if reader.read_binary().is_ok() {
        return Ok(());
    }
    // Array: read length then skip each element
    if let Ok(len) = reader.read_array_len() {
        for _ in 0..len {
            skip_msgpack_value(reader)?;
        }
        return Ok(());
    }
    // Map: read length then skip each key-value pair
    if let Ok(len) = reader.read_map_len() {
        for _ in 0..len {
            skip_msgpack_value(reader)?; // key
            skip_msgpack_value(reader)?; // value
        }
        return Ok(());
    }
    Err(zerompk::Error::BufferTooSmall)
}

// ─── Helper: count present fields ─────────────────────────────────

impl TextFields {
    fn present_field_count(&self) -> usize {
        let mut n = 0usize;
        if self.auth.is_some() {
            n += 1;
        }
        if self.sql.is_some() {
            n += 1;
        }
        if self.key.is_some() {
            n += 1;
        }
        if self.value.is_some() {
            n += 1;
        }
        if self.collection.is_some() {
            n += 1;
        }
        if self.document_id.is_some() {
            n += 1;
        }
        if self.data.is_some() {
            n += 1;
        }
        if self.query_vector.is_some() {
            n += 1;
        }
        if self.top_k.is_some() {
            n += 1;
        }
        if self.field.is_some() {
            n += 1;
        }
        if self.limit.is_some() {
            n += 1;
        }
        if self.delta.is_some() {
            n += 1;
        }
        if self.peer_id.is_some() {
            n += 1;
        }
        if self.vector_top_k.is_some() {
            n += 1;
        }
        if self.edge_label.is_some() {
            n += 1;
        }
        if self.direction.is_some() {
            n += 1;
        }
        if self.expansion_depth.is_some() {
            n += 1;
        }
        if self.final_top_k.is_some() {
            n += 1;
        }
        if self.vector_k.is_some() {
            n += 1;
        }
        if self.graph_k.is_some() {
            n += 1;
        }
        if self.vector_field.is_some() {
            n += 1;
        }
        if self.start_node.is_some() {
            n += 1;
        }
        if self.end_node.is_some() {
            n += 1;
        }
        if self.depth.is_some() {
            n += 1;
        }
        if self.from_node.is_some() {
            n += 1;
        }
        if self.to_node.is_some() {
            n += 1;
        }
        if self.edge_type.is_some() {
            n += 1;
        }
        if self.properties.is_some() {
            n += 1;
        }
        if self.query_text.is_some() {
            n += 1;
        }
        if self.vector_weight.is_some() {
            n += 1;
        }
        if self.fuzzy.is_some() {
            n += 1;
        }
        if self.ef_search.is_some() {
            n += 1;
        }
        if self.field_name.is_some() {
            n += 1;
        }
        if self.lower_bound.is_some() {
            n += 1;
        }
        if self.upper_bound.is_some() {
            n += 1;
        }
        if self.mutation_id.is_some() {
            n += 1;
        }
        if self.vectors.is_some() {
            n += 1;
        }
        if self.documents.is_some() {
            n += 1;
        }
        if self.query_geometry.is_some() {
            n += 1;
        }
        if self.spatial_predicate.is_some() {
            n += 1;
        }
        if self.distance_meters.is_some() {
            n += 1;
        }
        if self.payload.is_some() {
            n += 1;
        }
        if self.format.is_some() {
            n += 1;
        }
        if self.time_range_start.is_some() {
            n += 1;
        }
        if self.time_range_end.is_some() {
            n += 1;
        }
        if self.bucket_interval.is_some() {
            n += 1;
        }
        if self.ttl_ms.is_some() {
            n += 1;
        }
        if self.cursor.is_some() {
            n += 1;
        }
        if self.match_pattern.is_some() {
            n += 1;
        }
        if self.keys.is_some() {
            n += 1;
        }
        if self.entries.is_some() {
            n += 1;
        }
        if self.fields.is_some() {
            n += 1;
        }
        if self.incr_delta.is_some() {
            n += 1;
        }
        if self.incr_float_delta.is_some() {
            n += 1;
        }
        if self.expected.is_some() {
            n += 1;
        }
        if self.new_value.is_some() {
            n += 1;
        }
        if self.index_name.is_some() {
            n += 1;
        }
        if self.sort_columns.is_some() {
            n += 1;
        }
        if self.key_column.is_some() {
            n += 1;
        }
        if self.window_type.is_some() {
            n += 1;
        }
        if self.window_timestamp_column.is_some() {
            n += 1;
        }
        if self.window_start_ms.is_some() {
            n += 1;
        }
        if self.window_end_ms.is_some() {
            n += 1;
        }
        if self.top_k_count.is_some() {
            n += 1;
        }
        if self.score_min.is_some() {
            n += 1;
        }
        if self.score_max.is_some() {
            n += 1;
        }
        if self.updates.is_some() {
            n += 1;
        }
        if self.filters.is_some() {
            n += 1;
        }
        if self.vector.is_some() {
            n += 1;
        }
        if self.vector_id.is_some() {
            n += 1;
        }
        if self.policy.is_some() {
            n += 1;
        }
        if self.algorithm.is_some() {
            n += 1;
        }
        if self.match_query.is_some() {
            n += 1;
        }
        if self.algo_params.is_some() {
            n += 1;
        }
        if self.index_paths.is_some() {
            n += 1;
        }
        if self.source_collection.is_some() {
            n += 1;
        }
        if self.field_position.is_some() {
            n += 1;
        }
        if self.backfill.is_some() {
            n += 1;
        }
        if self.m.is_some() {
            n += 1;
        }
        if self.ef_construction.is_some() {
            n += 1;
        }
        if self.metric.is_some() {
            n += 1;
        }
        if self.index_type.is_some() {
            n += 1;
        }
        n
    }
}

// ─── Helper: write optional field ─────────────────────────────────

macro_rules! write_opt_field {
    ($writer:expr, $id:expr, $val:expr) => {
        if let Some(ref v) = $val {
            $writer.write_u16($id)?;
            v.write($writer)?;
        }
    };
    // variant for types that need JsonValue wrapping
    (json $writer:expr, $id:expr, $val:expr) => {
        if let Some(ref v) = $val {
            $writer.write_u16($id)?;
            JsonValue(v.clone()).write($writer)?;
        }
    };
}

// ─── ToMessagePack ─────────────────────────────────────────────────

impl zerompk::ToMessagePack for TextFields {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        writer.write_map_len(self.present_field_count())?;

        write_opt_field!(writer, FID_AUTH, self.auth);
        write_opt_field!(writer, FID_SQL, self.sql);
        write_opt_field!(writer, FID_KEY, self.key);
        write_opt_field!(writer, FID_VALUE, self.value);
        write_opt_field!(writer, FID_COLLECTION, self.collection);
        write_opt_field!(writer, FID_DOCUMENT_ID, self.document_id);
        write_opt_field!(writer, FID_DATA, self.data);
        write_opt_field!(writer, FID_QUERY_VECTOR, self.query_vector);
        write_opt_field!(writer, FID_TOP_K, self.top_k);
        write_opt_field!(writer, FID_FIELD, self.field);
        write_opt_field!(writer, FID_LIMIT, self.limit);
        write_opt_field!(writer, FID_DELTA, self.delta);
        write_opt_field!(writer, FID_PEER_ID, self.peer_id);
        write_opt_field!(writer, FID_VECTOR_TOP_K, self.vector_top_k);
        write_opt_field!(writer, FID_EDGE_LABEL, self.edge_label);
        write_opt_field!(writer, FID_DIRECTION, self.direction);
        write_opt_field!(writer, FID_EXPANSION_DEPTH, self.expansion_depth);
        write_opt_field!(writer, FID_FINAL_TOP_K, self.final_top_k);
        write_opt_field!(writer, FID_VECTOR_K, self.vector_k);
        write_opt_field!(writer, FID_GRAPH_K, self.graph_k);
        write_opt_field!(writer, FID_VECTOR_FIELD, self.vector_field);
        write_opt_field!(writer, FID_START_NODE, self.start_node);
        write_opt_field!(writer, FID_END_NODE, self.end_node);
        write_opt_field!(writer, FID_DEPTH, self.depth);
        write_opt_field!(writer, FID_FROM_NODE, self.from_node);
        write_opt_field!(writer, FID_TO_NODE, self.to_node);
        write_opt_field!(writer, FID_EDGE_TYPE, self.edge_type);
        write_opt_field!(json writer, FID_PROPERTIES, self.properties);
        write_opt_field!(writer, FID_QUERY_TEXT, self.query_text);
        write_opt_field!(writer, FID_VECTOR_WEIGHT, self.vector_weight);
        write_opt_field!(writer, FID_FUZZY, self.fuzzy);
        write_opt_field!(writer, FID_EF_SEARCH, self.ef_search);
        write_opt_field!(writer, FID_FIELD_NAME, self.field_name);
        write_opt_field!(writer, FID_LOWER_BOUND, self.lower_bound);
        write_opt_field!(writer, FID_UPPER_BOUND, self.upper_bound);
        write_opt_field!(writer, FID_MUTATION_ID, self.mutation_id);
        write_opt_field!(writer, FID_VECTORS, self.vectors);
        write_opt_field!(writer, FID_DOCUMENTS, self.documents);
        write_opt_field!(writer, FID_QUERY_GEOMETRY, self.query_geometry);
        write_opt_field!(writer, FID_SPATIAL_PREDICATE, self.spatial_predicate);
        write_opt_field!(writer, FID_DISTANCE_METERS, self.distance_meters);
        write_opt_field!(writer, FID_PAYLOAD, self.payload);
        write_opt_field!(writer, FID_FORMAT, self.format);
        write_opt_field!(writer, FID_TIME_RANGE_START, self.time_range_start);
        write_opt_field!(writer, FID_TIME_RANGE_END, self.time_range_end);
        write_opt_field!(writer, FID_BUCKET_INTERVAL, self.bucket_interval);
        write_opt_field!(writer, FID_TTL_MS, self.ttl_ms);
        write_opt_field!(writer, FID_CURSOR, self.cursor);
        write_opt_field!(writer, FID_MATCH_PATTERN, self.match_pattern);
        write_opt_field!(writer, FID_KEYS, self.keys);
        write_opt_field!(writer, FID_ENTRIES, self.entries);
        write_opt_field!(writer, FID_FIELDS, self.fields);
        write_opt_field!(writer, FID_INCR_DELTA, self.incr_delta);
        write_opt_field!(writer, FID_INCR_FLOAT_DELTA, self.incr_float_delta);
        write_opt_field!(writer, FID_EXPECTED, self.expected);
        write_opt_field!(writer, FID_NEW_VALUE, self.new_value);
        write_opt_field!(writer, FID_INDEX_NAME, self.index_name);
        write_opt_field!(writer, FID_SORT_COLUMNS, self.sort_columns);
        write_opt_field!(writer, FID_KEY_COLUMN, self.key_column);
        write_opt_field!(writer, FID_WINDOW_TYPE, self.window_type);
        write_opt_field!(
            writer,
            FID_WINDOW_TIMESTAMP_COLUMN,
            self.window_timestamp_column
        );
        write_opt_field!(writer, FID_WINDOW_START_MS, self.window_start_ms);
        write_opt_field!(writer, FID_WINDOW_END_MS, self.window_end_ms);
        write_opt_field!(writer, FID_TOP_K_COUNT, self.top_k_count);
        write_opt_field!(writer, FID_SCORE_MIN, self.score_min);
        write_opt_field!(writer, FID_SCORE_MAX, self.score_max);
        write_opt_field!(writer, FID_UPDATES, self.updates);
        write_opt_field!(writer, FID_FILTERS, self.filters);
        write_opt_field!(writer, FID_VECTOR, self.vector);
        write_opt_field!(writer, FID_VECTOR_ID, self.vector_id);
        write_opt_field!(json writer, FID_POLICY, self.policy);
        write_opt_field!(writer, FID_ALGORITHM, self.algorithm);
        write_opt_field!(writer, FID_MATCH_QUERY, self.match_query);
        write_opt_field!(json writer, FID_ALGO_PARAMS, self.algo_params);
        write_opt_field!(writer, FID_INDEX_PATHS, self.index_paths);
        write_opt_field!(writer, FID_SOURCE_COLLECTION, self.source_collection);
        write_opt_field!(writer, FID_FIELD_POSITION, self.field_position);
        write_opt_field!(writer, FID_BACKFILL, self.backfill);
        write_opt_field!(writer, FID_M, self.m);
        write_opt_field!(writer, FID_EF_CONSTRUCTION, self.ef_construction);
        write_opt_field!(writer, FID_METRIC, self.metric);
        write_opt_field!(writer, FID_INDEX_TYPE, self.index_type);

        Ok(())
    }
}

// ─── FromMessagePack ───────────────────────────────────────────────

impl<'a> zerompk::FromMessagePack<'a> for TextFields {
    fn read<R: zerompk::Read<'a>>(reader: &mut R) -> zerompk::Result<Self> {
        let map_len = reader.read_map_len()?;
        let mut out = TextFields::default();

        for _ in 0..map_len {
            // Keys are always u16 field IDs
            let id = reader.read_u16()?;
            match id {
                FID_AUTH => {
                    out.auth = Some(AuthMethod::read(reader)?);
                }
                FID_SQL => {
                    out.sql = Some(reader.read_string()?.into_owned());
                }
                FID_KEY => {
                    out.key = Some(reader.read_string()?.into_owned());
                }
                FID_VALUE => {
                    out.value = Some(reader.read_string()?.into_owned());
                }
                FID_COLLECTION => {
                    out.collection = Some(reader.read_string()?.into_owned());
                }
                FID_DOCUMENT_ID => {
                    out.document_id = Some(reader.read_string()?.into_owned());
                }
                FID_DATA => {
                    out.data = Some(reader.read_binary()?.into_owned());
                }
                FID_QUERY_VECTOR => {
                    out.query_vector = Some(Vec::<f32>::read(reader)?);
                }
                FID_TOP_K => {
                    out.top_k = Some(reader.read_u32()?);
                }
                FID_FIELD => {
                    out.field = Some(reader.read_string()?.into_owned());
                }
                FID_LIMIT => {
                    out.limit = Some(reader.read_u64()?);
                }
                FID_DELTA => {
                    out.delta = Some(reader.read_binary()?.into_owned());
                }
                FID_PEER_ID => {
                    out.peer_id = Some(reader.read_u64()?);
                }
                FID_VECTOR_TOP_K => {
                    out.vector_top_k = Some(reader.read_u32()?);
                }
                FID_EDGE_LABEL => {
                    out.edge_label = Some(reader.read_string()?.into_owned());
                }
                FID_DIRECTION => {
                    out.direction = Some(reader.read_string()?.into_owned());
                }
                FID_EXPANSION_DEPTH => {
                    out.expansion_depth = Some(reader.read_u32()?);
                }
                FID_FINAL_TOP_K => {
                    out.final_top_k = Some(reader.read_u32()?);
                }
                FID_VECTOR_K => {
                    out.vector_k = Some(reader.read_f64()?);
                }
                FID_GRAPH_K => {
                    out.graph_k = Some(reader.read_f64()?);
                }
                FID_VECTOR_FIELD => {
                    out.vector_field = Some(reader.read_string()?.into_owned());
                }
                FID_START_NODE => {
                    out.start_node = Some(reader.read_string()?.into_owned());
                }
                FID_END_NODE => {
                    out.end_node = Some(reader.read_string()?.into_owned());
                }
                FID_DEPTH => {
                    out.depth = Some(reader.read_u32()?);
                }
                FID_FROM_NODE => {
                    out.from_node = Some(reader.read_string()?.into_owned());
                }
                FID_TO_NODE => {
                    out.to_node = Some(reader.read_string()?.into_owned());
                }
                FID_EDGE_TYPE => {
                    out.edge_type = Some(reader.read_string()?.into_owned());
                }
                FID_PROPERTIES => {
                    out.properties = Some(JsonValue::read(reader)?.0);
                }
                FID_QUERY_TEXT => {
                    out.query_text = Some(reader.read_string()?.into_owned());
                }
                FID_VECTOR_WEIGHT => {
                    out.vector_weight = Some(reader.read_f64()?);
                }
                FID_FUZZY => {
                    out.fuzzy = Some(reader.read_boolean()?);
                }
                FID_EF_SEARCH => {
                    out.ef_search = Some(reader.read_u32()?);
                }
                FID_FIELD_NAME => {
                    out.field_name = Some(reader.read_string()?.into_owned());
                }
                FID_LOWER_BOUND => {
                    out.lower_bound = Some(reader.read_binary()?.into_owned());
                }
                FID_UPPER_BOUND => {
                    out.upper_bound = Some(reader.read_binary()?.into_owned());
                }
                FID_MUTATION_ID => {
                    out.mutation_id = Some(reader.read_u64()?);
                }
                FID_VECTORS => {
                    out.vectors = Some(Vec::<BatchVector>::read(reader)?);
                }
                FID_DOCUMENTS => {
                    out.documents = Some(Vec::<BatchDocument>::read(reader)?);
                }
                FID_QUERY_GEOMETRY => {
                    out.query_geometry = Some(reader.read_binary()?.into_owned());
                }
                FID_SPATIAL_PREDICATE => {
                    out.spatial_predicate = Some(reader.read_string()?.into_owned());
                }
                FID_DISTANCE_METERS => {
                    out.distance_meters = Some(reader.read_f64()?);
                }
                FID_PAYLOAD => {
                    out.payload = Some(reader.read_binary()?.into_owned());
                }
                FID_FORMAT => {
                    out.format = Some(reader.read_string()?.into_owned());
                }
                FID_TIME_RANGE_START => {
                    out.time_range_start = Some(reader.read_i64()?);
                }
                FID_TIME_RANGE_END => {
                    out.time_range_end = Some(reader.read_i64()?);
                }
                FID_BUCKET_INTERVAL => {
                    out.bucket_interval = Some(reader.read_string()?.into_owned());
                }
                FID_TTL_MS => {
                    out.ttl_ms = Some(reader.read_u64()?);
                }
                FID_CURSOR => {
                    out.cursor = Some(reader.read_binary()?.into_owned());
                }
                FID_MATCH_PATTERN => {
                    out.match_pattern = Some(reader.read_string()?.into_owned());
                }
                FID_KEYS => {
                    out.keys = Some(Vec::<Vec<u8>>::read(reader)?);
                }
                FID_ENTRIES => {
                    out.entries = Some(Vec::<(Vec<u8>, Vec<u8>)>::read(reader)?);
                }
                FID_FIELDS => {
                    out.fields = Some(Vec::<String>::read(reader)?);
                }
                FID_INCR_DELTA => {
                    out.incr_delta = Some(reader.read_i64()?);
                }
                FID_INCR_FLOAT_DELTA => {
                    out.incr_float_delta = Some(reader.read_f64()?);
                }
                FID_EXPECTED => {
                    out.expected = Some(reader.read_binary()?.into_owned());
                }
                FID_NEW_VALUE => {
                    out.new_value = Some(reader.read_binary()?.into_owned());
                }
                FID_INDEX_NAME => {
                    out.index_name = Some(reader.read_string()?.into_owned());
                }
                FID_SORT_COLUMNS => {
                    out.sort_columns = Some(Vec::<(String, String)>::read(reader)?);
                }
                FID_KEY_COLUMN => {
                    out.key_column = Some(reader.read_string()?.into_owned());
                }
                FID_WINDOW_TYPE => {
                    out.window_type = Some(reader.read_string()?.into_owned());
                }
                FID_WINDOW_TIMESTAMP_COLUMN => {
                    out.window_timestamp_column = Some(reader.read_string()?.into_owned());
                }
                FID_WINDOW_START_MS => {
                    out.window_start_ms = Some(reader.read_u64()?);
                }
                FID_WINDOW_END_MS => {
                    out.window_end_ms = Some(reader.read_u64()?);
                }
                FID_TOP_K_COUNT => {
                    out.top_k_count = Some(reader.read_u32()?);
                }
                FID_SCORE_MIN => {
                    out.score_min = Some(reader.read_binary()?.into_owned());
                }
                FID_SCORE_MAX => {
                    out.score_max = Some(reader.read_binary()?.into_owned());
                }
                FID_UPDATES => {
                    out.updates = Some(Vec::<(String, Vec<u8>)>::read(reader)?);
                }
                FID_FILTERS => {
                    out.filters = Some(reader.read_binary()?.into_owned());
                }
                FID_VECTOR => {
                    out.vector = Some(Vec::<f32>::read(reader)?);
                }
                FID_VECTOR_ID => {
                    out.vector_id = Some(reader.read_u64()?);
                }
                FID_POLICY => {
                    out.policy = Some(JsonValue::read(reader)?.0);
                }
                FID_ALGORITHM => {
                    out.algorithm = Some(reader.read_string()?.into_owned());
                }
                FID_MATCH_QUERY => {
                    out.match_query = Some(reader.read_string()?.into_owned());
                }
                FID_ALGO_PARAMS => {
                    out.algo_params = Some(JsonValue::read(reader)?.0);
                }
                FID_INDEX_PATHS => {
                    out.index_paths = Some(Vec::<String>::read(reader)?);
                }
                FID_SOURCE_COLLECTION => {
                    out.source_collection = Some(reader.read_string()?.into_owned());
                }
                FID_FIELD_POSITION => {
                    out.field_position = Some(reader.read_u64()?);
                }
                FID_BACKFILL => {
                    out.backfill = Some(reader.read_boolean()?);
                }
                FID_M => {
                    out.m = Some(reader.read_u16()?);
                }
                FID_EF_CONSTRUCTION => {
                    out.ef_construction = Some(reader.read_u16()?);
                }
                FID_METRIC => {
                    out.metric = Some(reader.read_string()?.into_owned());
                }
                FID_INDEX_TYPE => {
                    out.index_type = Some(reader.read_string()?.into_owned());
                }
                // Unknown field ID — skip the value for forward compatibility.
                _ => {
                    skip_msgpack_value(reader)?;
                }
            }
        }

        Ok(out)
    }
}

// ─── Tests ─────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(tf: &TextFields) -> TextFields {
        let bytes = zerompk::to_msgpack_vec(tf).expect("encode failed");
        zerompk::from_msgpack(&bytes).expect("decode failed")
    }

    /// Only present (non-None) fields are written to the map.
    #[test]
    fn textfields_present_only_roundtrip() {
        let tf = TextFields {
            sql: Some("SELECT 1".into()),
            collection: Some("docs".into()),
            top_k: Some(42),
            ..Default::default()
        };
        let bytes = zerompk::to_msgpack_vec(&tf).expect("encode");
        // Map should contain exactly 3 entries (sql, collection, top_k)
        let decoded: TextFields = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(decoded.sql.as_deref(), Some("SELECT 1"));
        assert_eq!(decoded.collection.as_deref(), Some("docs"));
        assert_eq!(decoded.top_k, Some(42));
        // All other fields should be None
        assert!(decoded.auth.is_none());
        assert!(decoded.query_vector.is_none());
        assert!(decoded.ef_search.is_none());
    }

    /// Unknown field IDs in the map are skipped without error.
    #[test]
    fn textfields_unknown_field_tolerance() {
        // Build a TextFields map then inject an unknown field ID (9999) before known fields.
        let known = TextFields {
            sql: Some("SELECT 2".into()),
            top_k: Some(10),
            ..Default::default()
        };
        let known_bytes = zerompk::to_msgpack_vec(&known).expect("encode known");
        // Verify 2 entries are present by checking known_bytes starts with fixmap(2)
        // MsgPack fixmap with 2 entries: 0x82
        assert_eq!(known_bytes[0], 0x82, "expected fixmap with 2 entries");

        // Manually construct a MsgPack map with 3 entries:
        //   9999 => "future_value" (unknown)
        //   FID_SQL (2) => "SELECT 2"
        //   FID_TOP_K (9) => 10
        let mut buf = Vec::new();
        // fixmap with 3 entries
        buf.push(0x83u8);
        // key: 9999 as uint16 (0xCD 0x27 0x0F)
        buf.extend_from_slice(&[0xcd, 0x27, 0x0f]);
        // value: fixstr "future_value"
        let fv = b"future_value";
        buf.push(0xa0 | fv.len() as u8);
        buf.extend_from_slice(fv);
        // key: 2 (FID_SQL) as positive fixint
        buf.push(2u8);
        // value: fixstr "SELECT 2"
        let sv = b"SELECT 2";
        buf.push(0xa0 | sv.len() as u8);
        buf.extend_from_slice(sv);
        // key: 9 (FID_TOP_K) as positive fixint
        buf.push(9u8);
        // value: 10 as positive fixint
        buf.push(10u8);

        let decoded: TextFields = zerompk::from_msgpack(&buf).expect("decode with unknown field");
        assert_eq!(decoded.sql.as_deref(), Some("SELECT 2"));
        assert_eq!(decoded.top_k, Some(10));
    }

    /// Full roundtrip including all width-changed fields.
    #[test]
    fn narrow_widths_at_max_roundtrip() {
        let tf = TextFields {
            top_k: Some(u32::MAX),
            vector_top_k: Some(u32::MAX),
            final_top_k: Some(u32::MAX),
            expansion_depth: Some(u32::MAX),
            depth: Some(u32::MAX),
            ef_search: Some(u32::MAX),
            m: Some(u16::MAX),
            ef_construction: Some(u16::MAX),
            ..Default::default()
        };
        let decoded = roundtrip(&tf);
        assert_eq!(decoded.top_k, Some(u32::MAX));
        assert_eq!(decoded.vector_top_k, Some(u32::MAX));
        assert_eq!(decoded.final_top_k, Some(u32::MAX));
        assert_eq!(decoded.expansion_depth, Some(u32::MAX));
        assert_eq!(decoded.depth, Some(u32::MAX));
        assert_eq!(decoded.ef_search, Some(u32::MAX));
        assert_eq!(decoded.m, Some(u16::MAX));
        assert_eq!(decoded.ef_construction, Some(u16::MAX));
    }

    /// vector_id wire type is u64.
    #[test]
    fn vector_id_roundtrip_at_u32_max() {
        let tf = TextFields {
            vector_id: Some(u32::MAX as u64),
            ..Default::default()
        };
        let decoded = roundtrip(&tf);
        assert_eq!(decoded.vector_id, Some(u32::MAX as u64));
    }
}
