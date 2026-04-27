//! cuVS-based Vamana index build.
//!
//! Offloads the brute-force k-NN portion of the randomized→pruned pipeline
//! to NVIDIA tensor cores. The resulting graph is then served via the CPU
//! Vamana implementation in `nodedb-vector` (build-on-GPU / serve-on-CPU).

use crate::error::GpuError;

#[derive(Debug, Clone)]
pub struct GpuVamanaBuildOptions {
    pub r: usize,
    pub alpha: f32,
    pub l_build: usize,
    /// GPU device index (0 for the first device).
    pub device: u32,
}

impl Default for GpuVamanaBuildOptions {
    fn default() -> Self {
        Self {
            r: 64,
            alpha: 1.2,
            l_build: 100,
            device: 0,
        }
    }
}

/// Stub: returns [`GpuError::NotCompiled`] when the `cuvs` feature is off.
///
/// When enabled, would compute a Vamana adjacency matrix on the GPU using
/// cuVS's `vamana_build` kernel and return adjacency + entry point.
#[cfg(feature = "cuvs")]
pub fn build_vamana_on_gpu(
    _vectors: &[Vec<f32>],
    _options: &GpuVamanaBuildOptions,
) -> Result<GpuBuildOutput, GpuError> {
    // Real implementation requires linking against the CUDA driver and
    // cuVS shared library; out of scope for this skeleton.
    Err(GpuError::Internal {
        detail: "cuvs integration not yet implemented".into(),
    })
}

#[cfg(not(feature = "cuvs"))]
pub fn build_vamana_on_gpu(
    _vectors: &[Vec<f32>],
    _options: &GpuVamanaBuildOptions,
) -> Result<GpuBuildOutput, GpuError> {
    Err(GpuError::NotCompiled)
}

#[derive(Debug)]
pub struct GpuBuildOutput {
    pub adjacency: Vec<Vec<u32>>,
    pub entry: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(not(feature = "cuvs"))]
    fn build_vamana_returns_not_compiled_without_cuvs_feature() {
        let vecs = vec![vec![1.0_f32, 0.0], vec![0.0, 1.0]];
        let opts = GpuVamanaBuildOptions::default();
        let result = build_vamana_on_gpu(&vecs, &opts);
        assert!(
            matches!(result, Err(GpuError::NotCompiled)),
            "expected NotCompiled, got {:?}",
            result
        );
    }

    #[test]
    #[cfg(feature = "cuvs")]
    fn build_vamana_returns_internal_with_cuvs_feature() {
        let vecs = vec![vec![1.0_f32, 0.0], vec![0.0, 1.0]];
        let opts = GpuVamanaBuildOptions::default();
        let result = build_vamana_on_gpu(&vecs, &opts);
        assert!(matches!(result, Err(GpuError::Internal { .. })));
    }
}
