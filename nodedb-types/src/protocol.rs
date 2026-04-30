//! Native binary wire protocol types.
//!
//! Shared between the server (`nodedb`) and the client (`nodedb-client`).
//! All types derive `Serialize`/`Deserialize` for MessagePack encoding.

use serde::{Deserialize, Serialize};

use crate::value::Value;

// ─── Operation Codes ────────────────────────────────────────────────

/// Operation codes for the native binary protocol.
///
/// Encoded as a single `u8` in both the MessagePack frame and JSON frame
/// (e.g. `{"op":3}` for `Status`). The `#[serde(try_from = "u8", into = "u8")]`
/// attribute makes JSON encoding consistent with the numeric opcode values.
///
/// `#[non_exhaustive]` — new operation codes may be added as engines grow.
/// Wire-side unknown-op handling is covered by `Unknown(u8)` (T2-05);
/// this attribute adds Rust API hygiene.
#[non_exhaustive]
#[repr(u8)]
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[serde(try_from = "u8", into = "u8")]
#[msgpack(c_enum)]
pub enum OpCode {
    // ── Auth & session ──────────────────────────────────────────
    Auth = 0x01,
    Ping = 0x02,
    /// Report startup/readiness status. Returns the current startup phase
    /// and whether the node is healthy. Does not require authentication.
    Status = 0x03,

    // ── Data operations (direct Data Plane dispatch) ────────────
    PointGet = 0x10,
    PointPut = 0x11,
    PointDelete = 0x12,
    VectorSearch = 0x13,
    RangeScan = 0x14,
    CrdtRead = 0x15,
    CrdtApply = 0x16,
    GraphRagFusion = 0x17,
    AlterCollectionPolicy = 0x18,

    // ── SQL & DDL ───────────────────────────────────────────────
    Sql = 0x20,
    Ddl = 0x21,
    Explain = 0x22,
    CopyFrom = 0x23,

    // ── Session parameters ──────────────────────────────────────
    Set = 0x30,
    Show = 0x31,
    Reset = 0x32,

    // ── Transaction control ─────────────────────────────────────
    Begin = 0x40,
    Commit = 0x41,
    Rollback = 0x42,

    // ── Graph operations (direct Data Plane dispatch) ───────────
    GraphHop = 0x50,
    GraphNeighbors = 0x51,
    GraphPath = 0x52,
    GraphSubgraph = 0x53,
    EdgePut = 0x54,
    EdgeDelete = 0x55,
    GraphAlgo = 0x56,
    GraphMatch = 0x57,

    // ── Spatial operations (direct Data Plane dispatch) ────────
    SpatialScan = 0x19,

    // ── Timeseries operations (direct Data Plane dispatch) ──────
    TimeseriesScan = 0x1A,
    TimeseriesIngest = 0x1B,

    // ── Search operations (direct Data Plane dispatch) ──────────
    TextSearch = 0x60,
    HybridSearch = 0x61,

    // ── Batch operations ────────────────────────────────────────
    VectorBatchInsert = 0x70,
    DocumentBatchInsert = 0x71,

    // ── KV advanced operations ──────────────────────────────────
    KvScan = 0x72,
    KvExpire = 0x73,
    KvPersist = 0x74,
    KvGetTtl = 0x75,
    KvBatchGet = 0x76,
    KvBatchPut = 0x77,
    KvFieldGet = 0x78,
    KvFieldSet = 0x79,

    // ── Document advanced operations ────────────────────────────
    DocumentUpdate = 0x7A,
    DocumentScan = 0x7B,
    DocumentUpsert = 0x7C,
    DocumentBulkUpdate = 0x7D,
    DocumentBulkDelete = 0x7E,

    // ── Vector advanced operations ──────────────────────────────
    VectorInsert = 0x7F,
    VectorMultiSearch = 0x80,
    VectorDelete = 0x81,

    // ── Columnar operations ─────────────────────────────────────
    ColumnarScan = 0x82,
    ColumnarInsert = 0x83,

    // ── Query operations ────────────────────────────────────────
    RecursiveScan = 0x84,

    // ── Document DDL operations ─────────────────────────────────
    DocumentTruncate = 0x85,
    DocumentEstimateCount = 0x86,
    DocumentInsertSelect = 0x87,
    DocumentRegister = 0x88,
    DocumentDropIndex = 0x89,

    // ── KV DDL operations ───────────────────────────────────────
    KvRegisterIndex = 0x8A,
    KvDropIndex = 0x8B,
    KvTruncate = 0x8C,

    // ── Vector DDL operations ───────────────────────────────────
    VectorSetParams = 0x8D,

    // ── KV atomic operations ───────────────────────────────────
    KvIncr = 0x8E,
    KvIncrFloat = 0x8F,
    KvCas = 0x90,
    KvGetSet = 0x91,

    // ── KV sorted index operations ─────────────────────────────
    KvRegisterSortedIndex = 0x92,
    KvDropSortedIndex = 0x93,
    KvSortedIndexRank = 0x94,
    KvSortedIndexTopK = 0x95,
    KvSortedIndexRange = 0x96,
    KvSortedIndexCount = 0x97,
    KvSortedIndexScore = 0x98,
}

impl OpCode {
    /// Returns true if this operation is a write that requires WAL append.
    pub fn is_write(&self) -> bool {
        matches!(
            self,
            OpCode::PointPut
                | OpCode::PointDelete
                | OpCode::CrdtApply
                | OpCode::EdgePut
                | OpCode::EdgeDelete
                | OpCode::VectorBatchInsert
                | OpCode::DocumentBatchInsert
                | OpCode::AlterCollectionPolicy
                | OpCode::TimeseriesIngest
                | OpCode::KvExpire
                | OpCode::KvPersist
                | OpCode::KvBatchPut
                | OpCode::KvFieldSet
                | OpCode::DocumentUpdate
                | OpCode::DocumentUpsert
                | OpCode::DocumentBulkUpdate
                | OpCode::DocumentBulkDelete
                | OpCode::VectorInsert
                | OpCode::VectorDelete
                | OpCode::ColumnarInsert
                | OpCode::DocumentTruncate
                | OpCode::DocumentInsertSelect
                | OpCode::DocumentRegister
                | OpCode::DocumentDropIndex
                | OpCode::KvRegisterIndex
                | OpCode::KvDropIndex
                | OpCode::KvTruncate
                | OpCode::VectorSetParams
                | OpCode::KvIncr
                | OpCode::KvIncrFloat
                | OpCode::KvCas
                | OpCode::KvGetSet
                | OpCode::KvRegisterSortedIndex
                | OpCode::KvDropSortedIndex
        )
    }
}

impl From<OpCode> for u8 {
    fn from(op: OpCode) -> u8 {
        op as u8
    }
}

/// Error returned when decoding an unknown `OpCode` byte from the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("unknown OpCode byte: 0x{0:02X}")]
pub struct UnknownOpCode(pub u8);

impl TryFrom<u8> for OpCode {
    type Error = UnknownOpCode;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x01 => Ok(OpCode::Auth),
            0x02 => Ok(OpCode::Ping),
            0x03 => Ok(OpCode::Status),
            0x10 => Ok(OpCode::PointGet),
            0x11 => Ok(OpCode::PointPut),
            0x12 => Ok(OpCode::PointDelete),
            0x13 => Ok(OpCode::VectorSearch),
            0x14 => Ok(OpCode::RangeScan),
            0x15 => Ok(OpCode::CrdtRead),
            0x16 => Ok(OpCode::CrdtApply),
            0x17 => Ok(OpCode::GraphRagFusion),
            0x18 => Ok(OpCode::AlterCollectionPolicy),
            0x19 => Ok(OpCode::SpatialScan),
            0x1A => Ok(OpCode::TimeseriesScan),
            0x1B => Ok(OpCode::TimeseriesIngest),
            0x20 => Ok(OpCode::Sql),
            0x21 => Ok(OpCode::Ddl),
            0x22 => Ok(OpCode::Explain),
            0x23 => Ok(OpCode::CopyFrom),
            0x30 => Ok(OpCode::Set),
            0x31 => Ok(OpCode::Show),
            0x32 => Ok(OpCode::Reset),
            0x40 => Ok(OpCode::Begin),
            0x41 => Ok(OpCode::Commit),
            0x42 => Ok(OpCode::Rollback),
            0x50 => Ok(OpCode::GraphHop),
            0x51 => Ok(OpCode::GraphNeighbors),
            0x52 => Ok(OpCode::GraphPath),
            0x53 => Ok(OpCode::GraphSubgraph),
            0x54 => Ok(OpCode::EdgePut),
            0x55 => Ok(OpCode::EdgeDelete),
            0x56 => Ok(OpCode::GraphAlgo),
            0x57 => Ok(OpCode::GraphMatch),
            0x60 => Ok(OpCode::TextSearch),
            0x61 => Ok(OpCode::HybridSearch),
            0x70 => Ok(OpCode::VectorBatchInsert),
            0x71 => Ok(OpCode::DocumentBatchInsert),
            0x72 => Ok(OpCode::KvScan),
            0x73 => Ok(OpCode::KvExpire),
            0x74 => Ok(OpCode::KvPersist),
            0x75 => Ok(OpCode::KvGetTtl),
            0x76 => Ok(OpCode::KvBatchGet),
            0x77 => Ok(OpCode::KvBatchPut),
            0x78 => Ok(OpCode::KvFieldGet),
            0x79 => Ok(OpCode::KvFieldSet),
            0x7A => Ok(OpCode::DocumentUpdate),
            0x7B => Ok(OpCode::DocumentScan),
            0x7C => Ok(OpCode::DocumentUpsert),
            0x7D => Ok(OpCode::DocumentBulkUpdate),
            0x7E => Ok(OpCode::DocumentBulkDelete),
            0x7F => Ok(OpCode::VectorInsert),
            0x80 => Ok(OpCode::VectorMultiSearch),
            0x81 => Ok(OpCode::VectorDelete),
            0x82 => Ok(OpCode::ColumnarScan),
            0x83 => Ok(OpCode::ColumnarInsert),
            0x84 => Ok(OpCode::RecursiveScan),
            0x85 => Ok(OpCode::DocumentTruncate),
            0x86 => Ok(OpCode::DocumentEstimateCount),
            0x87 => Ok(OpCode::DocumentInsertSelect),
            0x88 => Ok(OpCode::DocumentRegister),
            0x89 => Ok(OpCode::DocumentDropIndex),
            0x8A => Ok(OpCode::KvRegisterIndex),
            0x8B => Ok(OpCode::KvDropIndex),
            0x8C => Ok(OpCode::KvTruncate),
            0x8D => Ok(OpCode::VectorSetParams),
            0x8E => Ok(OpCode::KvIncr),
            0x8F => Ok(OpCode::KvIncrFloat),
            0x90 => Ok(OpCode::KvCas),
            0x91 => Ok(OpCode::KvGetSet),
            0x92 => Ok(OpCode::KvRegisterSortedIndex),
            0x93 => Ok(OpCode::KvDropSortedIndex),
            0x94 => Ok(OpCode::KvSortedIndexRank),
            0x95 => Ok(OpCode::KvSortedIndexTopK),
            0x96 => Ok(OpCode::KvSortedIndexRange),
            0x97 => Ok(OpCode::KvSortedIndexCount),
            0x98 => Ok(OpCode::KvSortedIndexScore),
            other => Err(UnknownOpCode(other)),
        }
    }
}

// ─── Response Status ────────────────────────────────────────────────

/// Status code in response frames.
///
/// `#[non_exhaustive]` — additional status codes (e.g. `Throttled`, `Redirect`)
/// may be added as the protocol evolves.
#[non_exhaustive]
#[repr(u8)]
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(c_enum)]
pub enum ResponseStatus {
    /// Request completed successfully.
    Ok = 0,
    /// Partial result — more chunks follow with the same `seq`.
    Partial = 1,
    /// Request failed — see `error` field.
    Error = 2,
}

// ─── Auth ───────────────────────────────────────────────────────────

/// Authentication method in an `Auth` request.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
#[serde(tag = "method", rename_all = "snake_case")]
#[non_exhaustive]
pub enum AuthMethod {
    Trust {
        #[serde(default = "default_username")]
        username: String,
    },
    Password {
        username: String,
        password: String,
    },
    ApiKey {
        token: String,
    },
}

fn default_username() -> String {
    "admin".into()
}

/// Successful auth response payload.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct AuthResponse {
    pub username: String,
    pub tenant_id: u64,
}

// ─── Request Frame ──────────────────────────────────────────────────

/// A request sent from client to server over the native protocol.
///
/// Serialized as MessagePack. The `op` field selects the handler,
/// `seq` correlates request to response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NativeRequest {
    /// Operation code.
    pub op: OpCode,
    /// Client-assigned sequence number for request/response correlation.
    pub seq: u64,
    /// Operation-specific fields (flattened into the same map).
    #[serde(flatten)]
    pub fields: RequestFields,
}

impl zerompk::ToMessagePack for NativeRequest {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        writer.write_array_len(3)?;
        self.op.write(writer)?;
        writer.write_u64(self.seq)?;
        self.fields.write(writer)
    }
}

impl<'a> zerompk::FromMessagePack<'a> for NativeRequest {
    fn read<R: zerompk::Read<'a>>(reader: &mut R) -> zerompk::Result<Self> {
        let len = reader.read_array_len()?;
        if len != 3 {
            return Err(zerompk::Error::ArrayLengthMismatch {
                expected: 3,
                actual: len,
            });
        }
        let op = OpCode::read(reader)?;
        let seq = reader.read_u64()?;
        let fields = RequestFields::read(reader)?;
        Ok(Self { op, seq, fields })
    }
}

/// Operation-specific request fields.
///
/// Each variant carries only the fields needed for that operation.
/// Unknown fields are silently ignored during deserialization.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
#[serde(untagged)]
#[non_exhaustive]
pub enum RequestFields {
    /// Auth, SQL, DDL, Explain, Set, Show, Reset, Begin, Commit, Rollback,
    /// CopyFrom, Ping — all use a subset of these text fields.
    Text(TextFields),
}

/// Catch-all text fields used by most operations.
///
/// Each operation uses a subset; unused fields default to `None`/empty.
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
    pub top_k: Option<u64>,

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
    pub vector_top_k: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub edge_label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub direction: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expansion_depth: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub final_top_k: Option<u64>,
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
    pub depth: Option<u64>,
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
    pub ef_search: Option<u64>,
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
    /// Vector ID for deletion.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_id: Option<u32>,

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
    pub m: Option<u64>,
    /// HNSW ef_construction parameter.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ef_construction: Option<u64>,
    /// Distance metric name ("cosine", "euclidean", "dot").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metric: Option<String>,
    /// Index type ("hnsw", "hnsw_pq", "ivf_pq").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_type: Option<String>,
}

impl zerompk::ToMessagePack for TextFields {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        use crate::json_msgpack::JsonValue;
        writer.write_array_len(81)?;
        self.auth.write(writer)?;
        self.sql.write(writer)?;
        self.key.write(writer)?;
        self.value.write(writer)?;
        self.collection.write(writer)?;
        self.document_id.write(writer)?;
        self.data.write(writer)?;
        self.query_vector.write(writer)?;
        self.top_k.write(writer)?;
        self.field.write(writer)?;
        self.limit.write(writer)?;
        self.delta.write(writer)?;
        self.peer_id.write(writer)?;
        self.vector_top_k.write(writer)?;
        self.edge_label.write(writer)?;
        self.direction.write(writer)?;
        self.expansion_depth.write(writer)?;
        self.final_top_k.write(writer)?;
        self.vector_k.write(writer)?;
        self.graph_k.write(writer)?;
        self.vector_field.write(writer)?;
        self.start_node.write(writer)?;
        self.end_node.write(writer)?;
        self.depth.write(writer)?;
        self.from_node.write(writer)?;
        self.to_node.write(writer)?;
        self.edge_type.write(writer)?;
        self.properties
            .as_ref()
            .map(|v| JsonValue(v.clone()))
            .write(writer)?;
        self.query_text.write(writer)?;
        self.vector_weight.write(writer)?;
        self.fuzzy.write(writer)?;
        self.ef_search.write(writer)?;
        self.field_name.write(writer)?;
        self.lower_bound.write(writer)?;
        self.upper_bound.write(writer)?;
        self.mutation_id.write(writer)?;
        self.vectors.write(writer)?;
        self.documents.write(writer)?;
        self.query_geometry.write(writer)?;
        self.spatial_predicate.write(writer)?;
        self.distance_meters.write(writer)?;
        self.payload.write(writer)?;
        self.format.write(writer)?;
        self.time_range_start.write(writer)?;
        self.time_range_end.write(writer)?;
        self.bucket_interval.write(writer)?;
        self.ttl_ms.write(writer)?;
        self.cursor.write(writer)?;
        self.match_pattern.write(writer)?;
        self.keys.write(writer)?;
        self.entries.write(writer)?;
        self.fields.write(writer)?;
        self.incr_delta.write(writer)?;
        self.incr_float_delta.write(writer)?;
        self.expected.write(writer)?;
        self.new_value.write(writer)?;
        self.index_name.write(writer)?;
        self.sort_columns.write(writer)?;
        self.key_column.write(writer)?;
        self.window_type.write(writer)?;
        self.window_timestamp_column.write(writer)?;
        self.window_start_ms.write(writer)?;
        self.window_end_ms.write(writer)?;
        self.top_k_count.write(writer)?;
        self.score_min.write(writer)?;
        self.score_max.write(writer)?;
        self.updates.write(writer)?;
        self.filters.write(writer)?;
        self.vector.write(writer)?;
        self.vector_id.write(writer)?;
        self.policy
            .as_ref()
            .map(|v| JsonValue(v.clone()))
            .write(writer)?;
        self.algorithm.write(writer)?;
        self.match_query.write(writer)?;
        self.algo_params
            .as_ref()
            .map(|v| JsonValue(v.clone()))
            .write(writer)?;
        self.index_paths.write(writer)?;
        self.source_collection.write(writer)?;
        self.field_position.write(writer)?;
        self.backfill.write(writer)?;
        self.m.write(writer)?;
        self.ef_construction.write(writer)?;
        self.metric.write(writer)?;
        self.index_type.write(writer)
    }
}

impl<'a> zerompk::FromMessagePack<'a> for TextFields {
    fn read<R: zerompk::Read<'a>>(reader: &mut R) -> zerompk::Result<Self> {
        use crate::json_msgpack::JsonValue;
        let len = reader.read_array_len()?;
        if len != 81 {
            return Err(zerompk::Error::ArrayLengthMismatch {
                expected: 81,
                actual: len,
            });
        }
        Ok(Self {
            auth: Option::<AuthMethod>::read(reader)?,
            sql: Option::<String>::read(reader)?,
            key: Option::<String>::read(reader)?,
            value: Option::<String>::read(reader)?,
            collection: Option::<String>::read(reader)?,
            document_id: Option::<String>::read(reader)?,
            data: Option::<Vec<u8>>::read(reader)?,
            query_vector: Option::<Vec<f32>>::read(reader)?,
            top_k: Option::<u64>::read(reader)?,
            field: Option::<String>::read(reader)?,
            limit: Option::<u64>::read(reader)?,
            delta: Option::<Vec<u8>>::read(reader)?,
            peer_id: Option::<u64>::read(reader)?,
            vector_top_k: Option::<u64>::read(reader)?,
            edge_label: Option::<String>::read(reader)?,
            direction: Option::<String>::read(reader)?,
            expansion_depth: Option::<u64>::read(reader)?,
            final_top_k: Option::<u64>::read(reader)?,
            vector_k: Option::<f64>::read(reader)?,
            graph_k: Option::<f64>::read(reader)?,
            vector_field: Option::<String>::read(reader)?,
            start_node: Option::<String>::read(reader)?,
            end_node: Option::<String>::read(reader)?,
            depth: Option::<u64>::read(reader)?,
            from_node: Option::<String>::read(reader)?,
            to_node: Option::<String>::read(reader)?,
            edge_type: Option::<String>::read(reader)?,
            properties: Option::<JsonValue>::read(reader)?.map(|v| v.0),
            query_text: Option::<String>::read(reader)?,
            vector_weight: Option::<f64>::read(reader)?,
            fuzzy: Option::<bool>::read(reader)?,
            ef_search: Option::<u64>::read(reader)?,
            field_name: Option::<String>::read(reader)?,
            lower_bound: Option::<Vec<u8>>::read(reader)?,
            upper_bound: Option::<Vec<u8>>::read(reader)?,
            mutation_id: Option::<u64>::read(reader)?,
            vectors: Option::<Vec<BatchVector>>::read(reader)?,
            documents: Option::<Vec<BatchDocument>>::read(reader)?,
            query_geometry: Option::<Vec<u8>>::read(reader)?,
            spatial_predicate: Option::<String>::read(reader)?,
            distance_meters: Option::<f64>::read(reader)?,
            payload: Option::<Vec<u8>>::read(reader)?,
            format: Option::<String>::read(reader)?,
            time_range_start: Option::<i64>::read(reader)?,
            time_range_end: Option::<i64>::read(reader)?,
            bucket_interval: Option::<String>::read(reader)?,
            ttl_ms: Option::<u64>::read(reader)?,
            cursor: Option::<Vec<u8>>::read(reader)?,
            match_pattern: Option::<String>::read(reader)?,
            keys: Option::<Vec<Vec<u8>>>::read(reader)?,
            entries: Option::<Vec<(Vec<u8>, Vec<u8>)>>::read(reader)?,
            fields: Option::<Vec<String>>::read(reader)?,
            incr_delta: Option::<i64>::read(reader)?,
            incr_float_delta: Option::<f64>::read(reader)?,
            expected: Option::<Vec<u8>>::read(reader)?,
            new_value: Option::<Vec<u8>>::read(reader)?,
            index_name: Option::<String>::read(reader)?,
            sort_columns: Option::<Vec<(String, String)>>::read(reader)?,
            key_column: Option::<String>::read(reader)?,
            window_type: Option::<String>::read(reader)?,
            window_timestamp_column: Option::<String>::read(reader)?,
            window_start_ms: Option::<u64>::read(reader)?,
            window_end_ms: Option::<u64>::read(reader)?,
            top_k_count: Option::<u32>::read(reader)?,
            score_min: Option::<Vec<u8>>::read(reader)?,
            score_max: Option::<Vec<u8>>::read(reader)?,
            updates: Option::<Vec<(String, Vec<u8>)>>::read(reader)?,
            filters: Option::<Vec<u8>>::read(reader)?,
            vector: Option::<Vec<f32>>::read(reader)?,
            vector_id: Option::<u32>::read(reader)?,
            policy: Option::<JsonValue>::read(reader)?.map(|v| v.0),
            algorithm: Option::<String>::read(reader)?,
            match_query: Option::<String>::read(reader)?,
            algo_params: Option::<JsonValue>::read(reader)?.map(|v| v.0),
            index_paths: Option::<Vec<String>>::read(reader)?,
            source_collection: Option::<String>::read(reader)?,
            field_position: Option::<u64>::read(reader)?,
            backfill: Option::<bool>::read(reader)?,
            m: Option::<u64>::read(reader)?,
            ef_construction: Option::<u64>::read(reader)?,
            metric: Option::<String>::read(reader)?,
            index_type: Option::<String>::read(reader)?,
        })
    }
}

/// A single vector in a batch insert.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchVector {
    pub id: String,
    pub embedding: Vec<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

impl zerompk::ToMessagePack for BatchVector {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        use crate::json_msgpack::JsonValue;
        writer.write_array_len(3)?;
        writer.write_string(&self.id)?;
        self.embedding.write(writer)?;
        self.metadata
            .as_ref()
            .map(|v| JsonValue(v.clone()))
            .write(writer)
    }
}

impl<'a> zerompk::FromMessagePack<'a> for BatchVector {
    fn read<R: zerompk::Read<'a>>(reader: &mut R) -> zerompk::Result<Self> {
        use crate::json_msgpack::JsonValue;
        let len = reader.read_array_len()?;
        if len != 3 {
            return Err(zerompk::Error::ArrayLengthMismatch {
                expected: 3,
                actual: len,
            });
        }
        let id = reader.read_string()?.into_owned();
        let embedding = Vec::<f32>::read(reader)?;
        let metadata = Option::<JsonValue>::read(reader)?.map(|v| v.0);
        Ok(Self {
            id,
            embedding,
            metadata,
        })
    }
}

/// A single document in a batch insert.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchDocument {
    pub id: String,
    pub fields: serde_json::Value,
}

impl zerompk::ToMessagePack for BatchDocument {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        use crate::json_msgpack::JsonValue;
        writer.write_array_len(2)?;
        writer.write_string(&self.id)?;
        JsonValue(self.fields.clone()).write(writer)
    }
}

impl<'a> zerompk::FromMessagePack<'a> for BatchDocument {
    fn read<R: zerompk::Read<'a>>(reader: &mut R) -> zerompk::Result<Self> {
        use crate::json_msgpack::JsonValue;
        let len = reader.read_array_len()?;
        if len != 2 {
            return Err(zerompk::Error::ArrayLengthMismatch {
                expected: 2,
                actual: len,
            });
        }
        let id = reader.read_string()?.into_owned();
        let fields = JsonValue::read(reader)?.0;
        Ok(Self { id, fields })
    }
}

// ─── Response Frame ─────────────────────────────────────────────────

/// A response sent from server to client over the native protocol.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct NativeResponse {
    /// Echoed from the request for correlation.
    pub seq: u64,
    /// Execution outcome.
    pub status: ResponseStatus,
    /// Column names (for query results).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub columns: Option<Vec<String>>,
    /// Row data (for query results). Each row is a Vec of Values.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows: Option<Vec<Vec<Value>>>,
    /// Number of rows affected (for writes).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows_affected: Option<u64>,
    /// WAL LSN watermark at time of computation.
    pub watermark_lsn: u64,
    /// Error details (if status == Error).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorPayload>,
    /// Auth response (if op == Auth and status == Ok).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth: Option<AuthResponse>,
    /// Advisory warnings (e.g. password expiry grace period, must_change_password).
    /// Empty in the common case; `#[serde(default)]` and `#[msgpack(default)]`
    /// keep this additive and backward-compatible with older clients.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[msgpack(default)]
    pub warnings: Vec<String>,
}

/// Error details in a response.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct ErrorPayload {
    /// SQLSTATE-style error code (e.g., "42P01" for undefined table).
    pub code: String,
    /// Human-readable error message.
    pub message: String,
}

impl NativeResponse {
    /// Create a successful response with no data.
    pub fn ok(seq: u64) -> Self {
        Self {
            seq,
            status: ResponseStatus::Ok,
            columns: None,
            rows: None,
            rows_affected: None,
            watermark_lsn: 0,
            error: None,
            auth: None,
            warnings: Vec::new(),
        }
    }

    /// Create a successful response from a `QueryResult`.
    pub fn from_query_result(seq: u64, qr: crate::result::QueryResult, lsn: u64) -> Self {
        Self {
            seq,
            status: ResponseStatus::Ok,
            columns: Some(qr.columns),
            rows: Some(qr.rows),
            rows_affected: Some(qr.rows_affected),
            watermark_lsn: lsn,
            error: None,
            auth: None,
            warnings: Vec::new(),
        }
    }

    /// Create an error response.
    pub fn error(seq: u64, code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            seq,
            status: ResponseStatus::Error,
            columns: None,
            rows: None,
            rows_affected: None,
            watermark_lsn: 0,
            error: Some(ErrorPayload {
                code: code.into(),
                message: message.into(),
            }),
            auth: None,
            warnings: Vec::new(),
        }
    }

    /// Create an auth success response.
    pub fn auth_ok(seq: u64, username: String, tenant_id: u64) -> Self {
        Self {
            seq,
            status: ResponseStatus::Ok,
            columns: None,
            rows: None,
            rows_affected: None,
            watermark_lsn: 0,
            error: None,
            auth: Some(AuthResponse {
                username,
                tenant_id,
            }),
            warnings: Vec::new(),
        }
    }

    /// Create a response with a single "status" column and one row.
    pub fn status_row(seq: u64, message: impl Into<String>) -> Self {
        Self {
            seq,
            status: ResponseStatus::Ok,
            columns: Some(vec!["status".into()]),
            rows: Some(vec![vec![Value::String(message.into())]]),
            rows_affected: Some(1),
            watermark_lsn: 0,
            error: None,
            auth: None,
            warnings: Vec::new(),
        }
    }
}

// ─── Protocol Constants ─────────────────────────────────────────────

/// Maximum frame payload size (16 MiB).
pub const MAX_FRAME_SIZE: u32 = 16 * 1024 * 1024;

/// Length of the frame header (4-byte big-endian u32 payload length).
pub const FRAME_HEADER_LEN: usize = 4;

/// Default server port for the native protocol.
pub const DEFAULT_NATIVE_PORT: u16 = 6433;

/// Current native protocol version advertised in `HelloFrame`.
pub const PROTO_VERSION: u16 = 1;

// ─── Capability Bits ────────────────────────────────────────────────

/// Capability bit: server supports streaming (partial-response chunking).
///
/// When set, the client may observe `ResponseStatus::Partial` chunks on
/// long-running queries instead of a single final response.
pub const CAP_STREAMING: u64 = 1 << 0;

/// Capability bit: server supports GraphRAG fusion (`GraphRagFusion` opcode).
pub const CAP_GRAPHRAG: u64 = 1 << 1;

/// Capability bit: server supports full-text search opcodes (`TextSearch`, `HybridSearch`).
pub const CAP_FTS: u64 = 1 << 2;

/// Capability bit: server supports CRDT sync (`CrdtRead`, `CrdtApply`).
pub const CAP_CRDT: u64 = 1 << 3;

/// Capability bit: server supports spatial operations (`SpatialScan`).
pub const CAP_SPATIAL: u64 = 1 << 4;

/// Capability bit: server supports timeseries operations (`TimeseriesScan`, `TimeseriesIngest`).
pub const CAP_TIMESERIES: u64 = 1 << 5;

/// Capability bit: server supports columnar scan (`ColumnarScan`, `ColumnarInsert`).
pub const CAP_COLUMNAR: u64 = 1 << 6;

// ─── Per-Operation Limits ───────────────────────────────────────────

/// Per-operation capability limits negotiated during the connection handshake.
///
/// The server announces its effective caps in `HelloAckFrame`. Clients must
/// respect these limits on every request; the server enforces them server-side
/// and returns `LimitExceeded` for violations.
///
/// All limits are `Option<u32>` — `None` means "no server-side cap" for that
/// dimension. A client that does not receive a `Limits` (pre-T2-08 server)
/// should treat all limits as `None`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Limits {
    /// Maximum vector embedding dimensionality.
    pub max_vector_dim: Option<u32>,
    /// Maximum `top_k` / `k` for vector and text-search operations.
    pub max_top_k: Option<u32>,
    /// Maximum row limit for scan operations.
    pub max_scan_limit: Option<u32>,
    /// Maximum number of items in a batch insert.
    pub max_batch_size: Option<u32>,
    /// Maximum CRDT delta payload in bytes.
    pub max_crdt_delta_bytes: Option<u32>,
    /// Maximum query text length in bytes.
    pub max_query_text_bytes: Option<u32>,
    /// Maximum graph traversal depth.
    pub max_graph_depth: Option<u32>,
}

// ─── Handshake Frames ───────────────────────────────────────────────

/// First frame sent by the client immediately after the TCP connection is
/// established. Carries the client's protocol version range and capability
/// bits so the server can choose the highest mutually-supported version.
///
/// Wire layout (big-endian, length-prefixed by the standard 4-byte header):
/// ```text
/// [magic: 4 bytes] [proto_min: u16] [proto_max: u16] [capabilities: u64]
/// ```
/// `magic` is `0x4E44_4248` ("NDBH" — NodeDB Hello).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HelloFrame {
    /// Client's minimum supported protocol version.
    pub proto_min: u16,
    /// Client's maximum supported protocol version.
    pub proto_max: u16,
    /// Bitfield of capabilities the client understands.
    pub capabilities: u64,
}

/// Magic bytes for `HelloFrame`: b"NDBH".
pub const HELLO_MAGIC: u32 = 0x4E44_4248;

impl HelloFrame {
    /// Wire size: 4-byte magic + 2 + 2 + 8 = 16 bytes.
    pub const WIRE_SIZE: usize = 16;

    /// Encode into a fixed-size byte array.
    pub fn encode(&self) -> [u8; Self::WIRE_SIZE] {
        let mut buf = [0u8; Self::WIRE_SIZE];
        buf[0..4].copy_from_slice(&HELLO_MAGIC.to_be_bytes());
        buf[4..6].copy_from_slice(&self.proto_min.to_be_bytes());
        buf[6..8].copy_from_slice(&self.proto_max.to_be_bytes());
        buf[8..16].copy_from_slice(&self.capabilities.to_be_bytes());
        buf
    }

    /// Decode from raw bytes. Returns `None` if magic does not match.
    pub fn decode(buf: &[u8; Self::WIRE_SIZE]) -> Option<Self> {
        let magic = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        if magic != HELLO_MAGIC {
            return None;
        }
        let proto_min = u16::from_be_bytes([buf[4], buf[5]]);
        let proto_max = u16::from_be_bytes([buf[6], buf[7]]);
        let capabilities = u64::from_be_bytes([
            buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15],
        ]);
        Some(Self {
            proto_min,
            proto_max,
            capabilities,
        })
    }
}

/// Server's response to a `HelloFrame`. Carries the negotiated version,
/// server's capability bits, server version string, and per-op limits.
///
/// Wire layout (big-endian, length-prefixed):
/// ```text
/// [magic: 4 bytes] [proto_version: u16] [capabilities: u64]
/// [server_version_len: u8] [server_version: N bytes]
/// [limits_present: u8]
/// if limits_present:
///   [max_vector_dim_present: u8] [max_vector_dim: u32]
///   [max_top_k_present: u8]      [max_top_k: u32]
///   [max_scan_limit_present: u8] [max_scan_limit: u32]
///   [max_batch_size_present: u8] [max_batch_size: u32]
///   [max_crdt_delta_bytes_present: u8] [max_crdt_delta_bytes: u32]
///   [max_query_text_bytes_present: u8] [max_query_text_bytes: u32]
///   [max_graph_depth_present: u8] [max_graph_depth: u32]
/// ```
/// `magic` is `0x4E44_4241` ("NDBA" — NodeDB Ack).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HelloAckFrame {
    /// The protocol version the server chose (server.proto_max clamped to client range).
    pub proto_version: u16,
    /// Bitfield of capabilities the server supports.
    pub capabilities: u64,
    /// Human-readable server version string (e.g. "0.1.0-dev").
    pub server_version: String,
    /// Per-op limits announced by the server.
    pub limits: Limits,
}

/// Magic bytes for `HelloAckFrame`: b"NDBA".
pub const HELLO_ACK_MAGIC: u32 = 0x4E44_4241;

impl HelloAckFrame {
    /// Encode into a `Vec<u8>`.
    pub fn encode(&self) -> Vec<u8> {
        let sv = self.server_version.as_bytes();
        let sv_len = sv.len().min(255) as u8;

        // Base: 4 (magic) + 2 (version) + 8 (caps) + 1 (sv_len) + sv_len
        let mut buf = Vec::with_capacity(15 + sv_len as usize + 1 + 7 * 5);
        buf.extend_from_slice(&HELLO_ACK_MAGIC.to_be_bytes());
        buf.extend_from_slice(&self.proto_version.to_be_bytes());
        buf.extend_from_slice(&self.capabilities.to_be_bytes());
        buf.push(sv_len);
        buf.extend_from_slice(&sv[..sv_len as usize]);

        // Limits section.
        buf.push(1u8); // limits_present = true
        encode_limit_field(&mut buf, self.limits.max_vector_dim);
        encode_limit_field(&mut buf, self.limits.max_top_k);
        encode_limit_field(&mut buf, self.limits.max_scan_limit);
        encode_limit_field(&mut buf, self.limits.max_batch_size);
        encode_limit_field(&mut buf, self.limits.max_crdt_delta_bytes);
        encode_limit_field(&mut buf, self.limits.max_query_text_bytes);
        encode_limit_field(&mut buf, self.limits.max_graph_depth);

        buf
    }

    /// Decode from a byte slice. Returns `None` on magic mismatch or truncation.
    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.len() < 15 {
            return None;
        }
        let magic = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        if magic != HELLO_ACK_MAGIC {
            return None;
        }
        let proto_version = u16::from_be_bytes([data[4], data[5]]);
        let capabilities = u64::from_be_bytes([
            data[6], data[7], data[8], data[9], data[10], data[11], data[12], data[13],
        ]);
        let sv_len = data[14] as usize;
        let sv_end = 15 + sv_len;
        if data.len() < sv_end {
            return None;
        }
        let server_version = String::from_utf8_lossy(&data[15..sv_end]).into_owned();

        let mut limits = Limits::default();
        if data.len() > sv_end && data[sv_end] == 1 {
            let mut pos = sv_end + 1;
            limits.max_vector_dim = decode_limit_field(data, &mut pos);
            limits.max_top_k = decode_limit_field(data, &mut pos);
            limits.max_scan_limit = decode_limit_field(data, &mut pos);
            limits.max_batch_size = decode_limit_field(data, &mut pos);
            limits.max_crdt_delta_bytes = decode_limit_field(data, &mut pos);
            limits.max_query_text_bytes = decode_limit_field(data, &mut pos);
            limits.max_graph_depth = decode_limit_field(data, &mut pos);
        }

        Some(Self {
            proto_version,
            capabilities,
            server_version,
            limits,
        })
    }
}

fn encode_limit_field(buf: &mut Vec<u8>, val: Option<u32>) {
    match val {
        Some(v) => {
            buf.push(1u8);
            buf.extend_from_slice(&v.to_be_bytes());
        }
        None => {
            buf.push(0u8);
            buf.extend_from_slice(&0u32.to_be_bytes());
        }
    }
}

fn decode_limit_field(data: &[u8], pos: &mut usize) -> Option<u32> {
    if *pos + 5 > data.len() {
        return None;
    }
    let present = data[*pos];
    let value = u32::from_be_bytes([
        data[*pos + 1],
        data[*pos + 2],
        data[*pos + 3],
        data[*pos + 4],
    ]);
    *pos += 5;
    if present == 1 { Some(value) } else { None }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opcode_repr() {
        assert_eq!(OpCode::Auth as u8, 0x01);
        assert_eq!(OpCode::Sql as u8, 0x20);
        assert_eq!(OpCode::Begin as u8, 0x40);
        assert_eq!(OpCode::GraphHop as u8, 0x50);
        assert_eq!(OpCode::TextSearch as u8, 0x60);
        assert_eq!(OpCode::VectorBatchInsert as u8, 0x70);
    }

    #[test]
    fn opcode_is_write() {
        assert!(OpCode::PointPut.is_write());
        assert!(OpCode::PointDelete.is_write());
        assert!(OpCode::CrdtApply.is_write());
        assert!(OpCode::EdgePut.is_write());
        assert!(!OpCode::PointGet.is_write());
        assert!(!OpCode::Sql.is_write());
        assert!(!OpCode::VectorSearch.is_write());
        assert!(!OpCode::Ping.is_write());
    }

    #[test]
    fn response_status_repr() {
        assert_eq!(ResponseStatus::Ok as u8, 0);
        assert_eq!(ResponseStatus::Partial as u8, 1);
        assert_eq!(ResponseStatus::Error as u8, 2);
    }

    #[test]
    fn native_response_ok() {
        let r = NativeResponse::ok(42);
        assert_eq!(r.seq, 42);
        assert_eq!(r.status, ResponseStatus::Ok);
        assert!(r.error.is_none());
    }

    #[test]
    fn native_response_error() {
        let r = NativeResponse::error(1, "42P01", "collection not found");
        assert_eq!(r.status, ResponseStatus::Error);
        let e = r.error.unwrap();
        assert_eq!(e.code, "42P01");
        assert_eq!(e.message, "collection not found");
    }

    #[test]
    fn native_response_from_query_result() {
        let qr = crate::result::QueryResult {
            columns: vec!["id".into(), "name".into()],
            rows: vec![vec![
                Value::String("u1".into()),
                Value::String("Alice".into()),
            ]],
            rows_affected: 0,
        };
        let r = NativeResponse::from_query_result(5, qr, 100);
        assert_eq!(r.seq, 5);
        assert_eq!(r.watermark_lsn, 100);
        assert_eq!(r.columns.as_ref().unwrap().len(), 2);
        assert_eq!(r.rows.as_ref().unwrap().len(), 1);
    }

    #[test]
    fn native_response_status_row() {
        let r = NativeResponse::status_row(3, "OK");
        assert_eq!(r.columns.as_ref().unwrap(), &["status"]);
        assert_eq!(r.rows.as_ref().unwrap()[0][0].as_str(), Some("OK"));
    }

    #[test]
    fn msgpack_roundtrip_request() {
        let req = NativeRequest {
            op: OpCode::Sql,
            seq: 1,
            fields: RequestFields::Text(TextFields {
                sql: Some("SELECT 1".into()),
                ..Default::default()
            }),
        };
        let bytes = zerompk::to_msgpack_vec(&req).unwrap();
        let decoded: NativeRequest = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(decoded.op, OpCode::Sql);
        assert_eq!(decoded.seq, 1);
    }

    #[test]
    fn msgpack_roundtrip_response() {
        let resp = NativeResponse::from_query_result(
            7,
            crate::result::QueryResult {
                columns: vec!["x".into()],
                rows: vec![vec![Value::Integer(42)]],
                rows_affected: 0,
            },
            99,
        );
        let bytes = zerompk::to_msgpack_vec(&resp).unwrap();
        let decoded: NativeResponse = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(decoded.seq, 7);
        assert_eq!(decoded.watermark_lsn, 99);
        assert_eq!(decoded.rows.unwrap()[0][0].as_i64(), Some(42));
    }

    #[test]
    fn auth_method_variants() {
        let trust = AuthMethod::Trust {
            username: "admin".into(),
        };
        let bytes = zerompk::to_msgpack_vec(&trust).unwrap();
        let decoded: AuthMethod = zerompk::from_msgpack(&bytes).unwrap();
        match decoded {
            AuthMethod::Trust { username } => assert_eq!(username, "admin"),
            _ => panic!("expected Trust variant"),
        }

        let pw = AuthMethod::Password {
            username: "user".into(),
            password: "secret".into(),
        };
        let bytes = zerompk::to_msgpack_vec(&pw).unwrap();
        let decoded: AuthMethod = zerompk::from_msgpack(&bytes).unwrap();
        match decoded {
            AuthMethod::Password { username, password } => {
                assert_eq!(username, "user");
                assert_eq!(password, "secret");
            }
            _ => panic!("expected Password variant"),
        }
    }

    #[test]
    fn hello_frame_roundtrip() {
        let frame = HelloFrame {
            proto_min: 1,
            proto_max: 3,
            capabilities: CAP_STREAMING | CAP_GRAPHRAG | CAP_FTS,
        };
        let buf = frame.encode();
        let decoded = HelloFrame::decode(&buf).expect("decode failed");
        assert_eq!(decoded, frame);
    }

    #[test]
    fn hello_frame_bad_magic() {
        let mut buf = HelloFrame {
            proto_min: 1,
            proto_max: 1,
            capabilities: 0,
        }
        .encode();
        buf[0] = 0xFF; // corrupt magic
        assert!(HelloFrame::decode(&buf).is_none());
    }

    #[test]
    fn hello_ack_frame_roundtrip_all_limits() {
        let frame = HelloAckFrame {
            proto_version: 1,
            capabilities: CAP_STREAMING | CAP_CRDT,
            server_version: "0.1.0-dev".into(),
            limits: Limits {
                max_vector_dim: Some(1536),
                max_top_k: Some(1000),
                max_scan_limit: Some(10_000),
                max_batch_size: Some(512),
                max_crdt_delta_bytes: Some(1 << 20),
                max_query_text_bytes: Some(4096),
                max_graph_depth: Some(16),
            },
        };
        let enc = frame.encode();
        let decoded = HelloAckFrame::decode(&enc).expect("decode failed");
        assert_eq!(decoded, frame);
    }

    #[test]
    fn hello_ack_frame_roundtrip_some_limits() {
        let frame = HelloAckFrame {
            proto_version: 1,
            capabilities: 0,
            server_version: "1.0.0".into(),
            limits: Limits {
                max_vector_dim: Some(768),
                max_top_k: None,
                max_scan_limit: None,
                max_batch_size: None,
                max_crdt_delta_bytes: None,
                max_query_text_bytes: None,
                max_graph_depth: None,
            },
        };
        let enc = frame.encode();
        let decoded = HelloAckFrame::decode(&enc).expect("decode failed");
        assert_eq!(decoded, frame);
    }

    #[test]
    fn hello_ack_frame_roundtrip_no_limits() {
        let frame = HelloAckFrame {
            proto_version: 1,
            capabilities: CAP_STREAMING,
            server_version: "0.2.0".into(),
            limits: Limits::default(),
        };
        let enc = frame.encode();
        let decoded = HelloAckFrame::decode(&enc).expect("decode failed");
        assert_eq!(decoded, frame);
    }

    #[test]
    fn hello_ack_bad_magic() {
        let frame = HelloAckFrame {
            proto_version: 1,
            capabilities: 0,
            server_version: "x".into(),
            limits: Limits::default(),
        };
        let mut enc = frame.encode();
        enc[0] = 0xFF;
        assert!(HelloAckFrame::decode(&enc).is_none());
    }

    #[test]
    fn cap_bits_non_overlapping() {
        let all = CAP_STREAMING
            | CAP_GRAPHRAG
            | CAP_FTS
            | CAP_CRDT
            | CAP_SPATIAL
            | CAP_TIMESERIES
            | CAP_COLUMNAR;
        // Each bit should be set exactly once — count of set bits equals number of caps.
        assert_eq!(all.count_ones(), 7);
    }
}
