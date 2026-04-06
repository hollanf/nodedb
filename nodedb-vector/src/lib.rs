pub mod batch_distance;
pub mod distance;
pub mod error;
pub mod hnsw;
pub mod quantize;

pub use distance::DistanceMetric;
pub use error::VectorError;
pub use hnsw::{HnswIndex, HnswParams, SearchResult};
pub use quantize::Sq8Codec;

// Origin-only modules (always compiled for native targets).
pub mod adaptive_filter;
pub mod flat;
pub mod index_config;

// IVF-PQ index (large datasets).
#[cfg(feature = "ivf")]
pub mod ivf;

// NVMe mmap tier (requires libc).
#[cfg(feature = "collection")]
pub mod mmap_segment;

// Background HNSW builder thread.
#[cfg(feature = "collection")]
pub mod builder;

// Full VectorCollection with segment lifecycle.
#[cfg(feature = "collection")]
pub mod collection;

// Re-exports for feature-gated types.
pub use adaptive_filter::{
    FilterStrategy, FilterThresholds, adaptive_search, estimate_selectivity, select_strategy,
};
#[cfg(feature = "collection")]
pub use builder::{BuildSender, CompleteReceiver};
#[cfg(feature = "collection")]
pub use collection::{BuildComplete, BuildRequest, StorageTier, VectorCollection};
pub use flat::FlatIndex;
pub use index_config::{IndexConfig, IndexType};
#[cfg(feature = "ivf")]
pub use ivf::{IvfPqIndex, IvfPqParams};
