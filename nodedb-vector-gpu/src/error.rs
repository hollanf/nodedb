//! Error types for the GPU crate.

#[derive(Debug, thiserror::Error)]
pub enum GpuError {
    /// The `cuvs` feature was not enabled at compile time.
    #[error("nodedb-vector-gpu was compiled without the `cuvs` feature")]
    NotCompiled,

    /// CUDA runtime not available at execution time.
    #[error("CUDA runtime unavailable: {detail}")]
    CudaUnavailable { detail: String },

    /// Internal error inside the GPU build path.
    #[error("internal GPU error: {detail}")]
    Internal { detail: String },
}
