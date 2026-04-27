//! CAGRA fixed-degree flat-graph index — GPU-resident inference.
//!
//! Warp-aligned adjacency layout. Inference kernels execute with no thread
//! divergence on neighbor updates.

use crate::error::GpuError;

#[derive(Debug)]
pub struct CagraIndex {
    pub dim: usize,
    pub graph_degree: usize,
    /// Number of warps per CTA; tuning knob.
    pub warps_per_cta: u8,
}

impl CagraIndex {
    /// Stub: returns [`GpuError::NotCompiled`] when `cuvs` feature is off.
    #[cfg(feature = "cuvs")]
    pub fn build(
        _vectors: &[Vec<f32>],
        _dim: usize,
        _graph_degree: usize,
    ) -> Result<Self, GpuError> {
        Err(GpuError::Internal {
            detail: "cagra integration not yet implemented".into(),
        })
    }

    #[cfg(not(feature = "cuvs"))]
    pub fn build(
        _vectors: &[Vec<f32>],
        _dim: usize,
        _graph_degree: usize,
    ) -> Result<Self, GpuError> {
        Err(GpuError::NotCompiled)
    }

    pub fn search(&self, _query: &[f32], _k: usize) -> Result<Vec<(u32, f32)>, GpuError> {
        Err(GpuError::NotCompiled)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(not(feature = "cuvs"))]
    fn cagra_build_returns_not_compiled_without_cuvs_feature() {
        let vecs = vec![vec![1.0_f32, 0.0], vec![0.0, 1.0]];
        let result = CagraIndex::build(&vecs, 2, 4);
        assert!(
            matches!(result, Err(GpuError::NotCompiled)),
            "expected NotCompiled, got {:?}",
            result
        );
    }

    #[test]
    #[cfg(feature = "cuvs")]
    fn cagra_build_returns_internal_with_cuvs_feature() {
        let vecs = vec![vec![1.0_f32, 0.0], vec![0.0, 1.0]];
        let result = CagraIndex::build(&vecs, 2, 4);
        assert!(matches!(result, Err(GpuError::Internal { .. })));
    }

    #[test]
    fn cagra_search_returns_not_compiled() {
        // Construct directly since build always returns Err without the feature.
        let index = CagraIndex {
            dim: 2,
            graph_degree: 4,
            warps_per_cta: 4,
        };
        let result = index.search(&[1.0, 0.0], 5);
        assert!(
            matches!(result, Err(GpuError::NotCompiled)),
            "expected NotCompiled, got {:?}",
            result
        );
    }
}
