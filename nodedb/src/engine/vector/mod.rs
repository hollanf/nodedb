//! Vector engine — all types re-exported from the shared `nodedb-vector` crate.
//!
//! Origin uses the `collection` feature which enables SIMD, IVF-PQ, mmap,
//! and the full VectorCollection segment lifecycle.

pub use nodedb_vector::adaptive_filter;
pub use nodedb_vector::batch_distance;
pub use nodedb_vector::builder;
pub use nodedb_vector::collection;
pub use nodedb_vector::flat;
pub use nodedb_vector::index_config;
pub use nodedb_vector::ivf;
pub use nodedb_vector::mmap_segment;
pub use nodedb_vector::quantize;

pub mod distance {
    pub use nodedb_vector::distance::*;
}

pub mod hnsw {
    pub use nodedb_vector::hnsw::build;
    pub use nodedb_vector::hnsw::checkpoint;
    pub use nodedb_vector::hnsw::graph;
    pub use nodedb_vector::hnsw::search;
    pub use nodedb_vector::hnsw::*;
}

pub use nodedb_vector::{
    BuildComplete, BuildRequest, BuildSender, CompleteReceiver, DistanceMetric, FilterStrategy,
    FilterThresholds, FlatIndex, HnswIndex, HnswParams, IndexConfig, IndexType, IvfPqIndex,
    IvfPqParams, SearchResult, Sq8Codec, StorageTier, VectorCollection, adaptive_search,
    estimate_selectivity, select_strategy,
};

pub mod sparse;
