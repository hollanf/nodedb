//! Engine identification for memory budget tracking.

/// Identifies a subsystem that owns a memory budget.
///
/// Each engine operates within its allocated memory ceiling. The governor
/// tracks allocations per engine and rejects requests that would exceed
/// the budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EngineId {
    /// HNSW vector index, distance computation buffers, quantized caches.
    Vector,

    /// redb B-Tree, splintr inverted index, schema metadata.
    Sparse,

    /// loro CRDT state, merge buffers, operation logs.
    Crdt,

    /// Gorilla-encoded memtables, Zstd dictionaries, log buffers.
    Timeseries,

    /// DataFusion query execution: sorts, aggregates, hash tables.
    Query,

    /// WAL write buffers, group commit staging.
    Wal,

    /// SPSC bridge buffers, slab allocator, envelope staging.
    Bridge,
}

impl EngineId {
    /// All known engine identifiers (for iteration in the governor).
    pub const ALL: &'static [EngineId] = &[
        EngineId::Vector,
        EngineId::Sparse,
        EngineId::Crdt,
        EngineId::Timeseries,
        EngineId::Query,
        EngineId::Wal,
        EngineId::Bridge,
    ];
}

impl std::fmt::Display for EngineId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EngineId::Vector => write!(f, "vector"),
            EngineId::Sparse => write!(f, "sparse"),
            EngineId::Crdt => write!(f, "crdt"),
            EngineId::Timeseries => write!(f, "timeseries"),
            EngineId::Query => write!(f, "query"),
            EngineId::Wal => write!(f, "wal"),
            EngineId::Bridge => write!(f, "bridge"),
        }
    }
}
