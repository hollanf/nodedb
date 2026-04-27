//! Vector quantization codec family — binary, ternary, 4-bit scalar, residual,
//! and unified layout types shared across HNSW hot-path, IVF-PQ, and BBQ.

pub mod bbq;
pub mod codec;
pub mod hamming;
pub mod layout;
pub mod opq;
pub mod opq_kmeans;
pub mod opq_rotation;
pub mod rabitq;
pub mod ternary;
