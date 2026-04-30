//! Static argument type specifications for all built-in functions.
//!
//! Each constant defines the accepted argument types per position.
//! An empty `accepted` slice means any type is accepted (wildcard).
//! `variadic: true` on the last entry means the argument repeats.

use nodedb_types::columnar::ColumnType;

use super::registry::ArgTypeSpec;

// ── Shorthands ──────────────────────────────────────────────────────────────

const ANY: &[ColumnType] = &[];

const NUMERIC: &[ColumnType] = &[
    ColumnType::Int64,
    ColumnType::Float64,
    ColumnType::Decimal {
        precision: 38,
        scale: 10,
    },
];

const TEXT: &[ColumnType] = &[ColumnType::String];

const FLOAT64_ONLY: &[ColumnType] = &[ColumnType::Float64];

const INT64_ONLY: &[ColumnType] = &[ColumnType::Int64];

const VECTOR_ONLY: &[ColumnType] = &[ColumnType::Vector(0)];

const GEOMETRY_ONLY: &[ColumnType] = &[ColumnType::Geometry];

const TIMESTAMP_TYPES: &[ColumnType] = &[ColumnType::Timestamp, ColumnType::Timestamptz];

// ── Helper constructors ──────────────────────────────────────────────────────

pub(super) const fn any(name: &'static str) -> ArgTypeSpec {
    ArgTypeSpec {
        name,
        accepted: ANY,
        variadic: false,
    }
}

pub(super) const fn any_variadic(name: &'static str) -> ArgTypeSpec {
    ArgTypeSpec {
        name,
        accepted: ANY,
        variadic: true,
    }
}

pub(super) const fn typed(name: &'static str, accepted: &'static [ColumnType]) -> ArgTypeSpec {
    ArgTypeSpec {
        name,
        accepted,
        variadic: false,
    }
}

pub(super) const fn typed_variadic(
    name: &'static str,
    accepted: &'static [ColumnType],
) -> ArgTypeSpec {
    ArgTypeSpec {
        name,
        accepted,
        variadic: true,
    }
}

// ── Standard aggregates ──────────────────────────────────────────────────────

/// count(*) / count(expr)
pub static COUNT_ARGS: &[ArgTypeSpec] = &[any_variadic("expr")];

pub static SUM_ARGS: &[ArgTypeSpec] = &[typed("expr", NUMERIC)];

pub static AVG_ARGS: &[ArgTypeSpec] = &[typed("expr", NUMERIC)];

pub static MIN_ARGS: &[ArgTypeSpec] = &[any("expr")];

pub static MAX_ARGS: &[ArgTypeSpec] = &[any("expr")];

// ── Standard window ──────────────────────────────────────────────────────────

pub static NO_ARGS: &[ArgTypeSpec] = &[];

pub static LAG_LEAD_ARGS: &[ArgTypeSpec] =
    &[any("expr"), typed("offset", INT64_ONLY), any("default")];

pub static FIRST_LAST_VALUE_ARGS: &[ArgTypeSpec] = &[any("expr")];

pub static NTH_VALUE_ARGS: &[ArgTypeSpec] = &[any("expr"), typed("n", INT64_ONLY)];

// ── Vector search ────────────────────────────────────────────────────────────

pub static VECTOR_DISTANCE_ARGS: &[ArgTypeSpec] = &[
    typed("column", VECTOR_ONLY),
    typed("query", VECTOR_ONLY),
    any("metric"),
];

pub static MULTI_VECTOR_SEARCH_ARGS: &[ArgTypeSpec] = &[any("query"), any("options")];

pub static MULTI_VECTOR_SCORE_ARGS: &[ArgTypeSpec] =
    &[any("col1"), any("col2"), typed("query", VECTOR_ONLY)];

pub static SPARSE_SCORE_ARGS: &[ArgTypeSpec] =
    &[any("col"), any("query"), typed("boost", FLOAT64_ONLY)];

// ── Text search ───────────────────────────────────────────────────────────────

pub static BM25_SCORE_ARGS: &[ArgTypeSpec] = &[any("column"), typed("query", TEXT)];

pub static SEARCH_SCORE_ARGS: &[ArgTypeSpec] = &[any("column"), typed("query", TEXT)];

pub static TEXT_MATCH_ARGS: &[ArgTypeSpec] = &[any("column"), typed("query", TEXT), any("options")];

// ── Hybrid search ─────────────────────────────────────────────────────────────

pub static RRF_SCORE_ARGS: &[ArgTypeSpec] = &[
    typed("rank1", INT64_ONLY),
    typed("rank2", INT64_ONLY),
    typed("k1", INT64_ONLY),
    typed("k2", INT64_ONLY),
];

// ── Spatial ───────────────────────────────────────────────────────────────────

pub static SPATIAL_3_ARGS: &[ArgTypeSpec] = &[
    typed("geom1", GEOMETRY_ONLY),
    typed("geom2", GEOMETRY_ONLY),
    typed("distance", FLOAT64_ONLY),
];

pub static SPATIAL_2_ARGS: &[ArgTypeSpec] =
    &[typed("geom1", GEOMETRY_ONLY), typed("geom2", GEOMETRY_ONLY)];

pub static ST_POINT_ARGS: &[ArgTypeSpec] = &[typed("x", FLOAT64_ONLY), typed("y", FLOAT64_ONLY)];

// ── Timeseries ────────────────────────────────────────────────────────────────

pub static TIME_BUCKET_ARGS: &[ArgTypeSpec] = &[any("interval"), typed("ts", TIMESTAMP_TYPES)];

pub static TS_PERCENTILE_ARGS: &[ArgTypeSpec] = &[any("expr"), typed("percentile", FLOAT64_ONLY)];

pub static TS_STDDEV_ARGS: &[ArgTypeSpec] = &[any("expr")];

pub static TS_CORRELATE_ARGS: &[ArgTypeSpec] = &[any("col1"), any("col2")];

pub static TS_WINDOW_1_ARGS: &[ArgTypeSpec] = &[any("expr")];

pub static TS_MOVING_AVG_ARGS: &[ArgTypeSpec] = &[any("expr"), typed("window", INT64_ONLY)];

pub static TS_EMA_ARGS: &[ArgTypeSpec] = &[any("expr"), typed("alpha", FLOAT64_ONLY)];

pub static TS_LAG_LEAD_ARGS: &[ArgTypeSpec] =
    &[any("expr"), typed("offset", INT64_ONLY), any("default")];

// ── Approximate aggregates ────────────────────────────────────────────────────

pub static APPROX_COUNT_DISTINCT_ARGS: &[ArgTypeSpec] = &[any("expr")];

pub static APPROX_PERCENTILE_ARGS: &[ArgTypeSpec] =
    &[any("expr"), typed("percentile", FLOAT64_ONLY)];

pub static APPROX_TOPK_ARGS: &[ArgTypeSpec] = &[any("expr"), typed("k", INT64_ONLY)];

pub static APPROX_COUNT_ARGS: &[ArgTypeSpec] = &[any("expr")];

// ── Document helpers ──────────────────────────────────────────────────────────

pub static DOC_GET_ARGS: &[ArgTypeSpec] = &[any("doc"), typed("path", TEXT), any("default")];

pub static DOC_EXISTS_ARGS: &[ArgTypeSpec] = &[any("doc"), typed("path", TEXT)];

pub static DOC_ARRAY_CONTAINS_ARGS: &[ArgTypeSpec] =
    &[any("doc"), typed("path", TEXT), any("value")];

pub static NAV_ARGS: &[ArgTypeSpec] = &[any("doc"), typed("path", TEXT)];

// ── Utility ───────────────────────────────────────────────────────────────────

pub static NDB_CHUNK_TEXT_ARGS: &[ArgTypeSpec] = &[
    typed("text", TEXT),
    typed("chunk_size", INT64_ONLY),
    typed("overlap", INT64_ONLY),
];

// ── Standard scalar ───────────────────────────────────────────────────────────

pub static COALESCE_ARGS: &[ArgTypeSpec] = &[any_variadic("expr")];

pub static NULLIF_ARGS: &[ArgTypeSpec] = &[any("expr1"), any("expr2")];

pub static MATH_1_ARGS: &[ArgTypeSpec] = &[typed("expr", NUMERIC)];

pub static ROUND_ARGS: &[ArgTypeSpec] = &[typed("expr", NUMERIC), typed("scale", INT64_ONLY)];

pub static STRING_1_ARGS: &[ArgTypeSpec] = &[typed("expr", TEXT)];

pub static LENGTH_ARGS: &[ArgTypeSpec] = &[typed("expr", TEXT)];

pub static SUBSTRING_ARGS: &[ArgTypeSpec] = &[
    typed("expr", TEXT),
    typed("start", INT64_ONLY),
    typed("length", INT64_ONLY),
];

pub static CONCAT_ARGS: &[ArgTypeSpec] = &[typed_variadic("expr", TEXT)];

pub static REPLACE_ARGS: &[ArgTypeSpec] =
    &[typed("expr", TEXT), typed("from", TEXT), typed("to", TEXT)];

pub static MAKE_ARRAY_ARGS: &[ArgTypeSpec] = &[any_variadic("expr")];

// ── PostgreSQL JSON operators ─────────────────────────────────────────────────

pub static PG_JSON_2_ARGS: &[ArgTypeSpec] = &[any("json_col"), any("key")];

pub static PG_JSON_BOOL_2_ARGS: &[ArgTypeSpec] = &[any("json_col"), any("operand")];

// ── SQL/JSON standard functions ───────────────────────────────────────────────

pub static JSON_VALUE_ARGS: &[ArgTypeSpec] = &[any("json_col"), typed("path", TEXT)];

pub static JSON_QUERY_ARGS: &[ArgTypeSpec] = &[any("json_col"), typed("path", TEXT)];

pub static JSON_EXISTS_ARGS: &[ArgTypeSpec] = &[any("json_col"), typed("path", TEXT)];

// ── PostgreSQL FTS operators ──────────────────────────────────────────────────

pub static PG_FTS_MATCH_ARGS: &[ArgTypeSpec] = &[any("tsvector"), any("tsquery")];

pub static PG_TO_TSVECTOR_ARGS: &[ArgTypeSpec] = &[typed("config", TEXT), typed("document", TEXT)];

pub static PG_TO_TSQUERY_ARGS: &[ArgTypeSpec] = &[typed("config", TEXT), typed("query", TEXT)];

pub static PG_TS_RANK_ARGS: &[ArgTypeSpec] = &[
    any("tsvector"),
    any("tsquery"),
    typed("weights", FLOAT64_ONLY),
    typed("normalization", INT64_ONLY),
];

pub static PG_TS_HEADLINE_ARGS: &[ArgTypeSpec] = &[
    typed("config", TEXT),
    any("document"),
    any("tsquery"),
    typed("options", TEXT),
];

// ── Array engine ──────────────────────────────────────────────────────────────

pub static NDARRAY_SLICE_ARGS: &[ArgTypeSpec] = &[
    typed("name", TEXT),
    any("slice_obj"),
    any("attrs"),
    typed("limit", INT64_ONLY),
];

pub static NDARRAY_PROJECT_ARGS: &[ArgTypeSpec] = &[typed("name", TEXT), any("attrs")];

pub static NDARRAY_AGG_ARGS: &[ArgTypeSpec] = &[
    typed("name", TEXT),
    typed("attr", TEXT),
    typed("reducer", TEXT),
    typed("group_by_dim", INT64_ONLY),
];

pub static NDARRAY_ELEMENTWISE_ARGS: &[ArgTypeSpec] = &[
    typed("left", TEXT),
    typed("right", TEXT),
    typed("op", TEXT),
    typed("attr", TEXT),
];

pub static NDARRAY_MAINT_ARGS: &[ArgTypeSpec] = &[typed("name", TEXT)];
