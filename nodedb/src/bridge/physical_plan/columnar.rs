//! Columnar engine base operations dispatched to the Data Plane.
//!
//! `ColumnarOp` is the base for all columnar-profile collections:
//! - **Plain columnar**: analytics collections without time semantics.
//! - **Timeseries**: extends with `time_range` + `bucket_interval_ms` (via `TimeseriesOp`).
//! - **Spatial**: extends with R-tree + OGC predicates (via `SpatialOp`).
//!
//! All profiles share the same `ColumnarMemtable` → `SegmentWriter` infrastructure.

/// Base columnar physical operations.
#[derive(Debug, Clone)]
pub enum ColumnarOp {
    /// Read rows from columnar memtable + segments.
    ///
    /// Applies filters, projects columns, respects limit.
    /// No time-range semantics — that's `TimeseriesOp::Scan`.
    Scan {
        collection: String,
        projection: Vec<String>,
        limit: usize,
        filters: Vec<u8>,
        rls_filters: Vec<u8>,
    },

    /// Insert rows into a columnar memtable.
    ///
    /// Accepts JSON or MessagePack payload. The memtable is created on
    /// first insert with schema inferred from the payload.
    Insert {
        collection: String,
        /// Row data. Format determined by `format` field.
        payload: Vec<u8>,
        /// "json" for JSON array of objects, "msgpack" for MessagePack,
        /// "ilp" for InfluxDB Line Protocol (delegated to timeseries path).
        format: String,
    },
}
