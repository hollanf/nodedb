//! RAM budget enforcement and mmap spillover for `VectorCollection`.
//!
//! Tracks per-collection vector memory usage and spills sealed segment
//! FP32 vectors to NVMe (L1 tier) via mmap when budget is exceeded.
//! SQ8 quantized data and HNSW graph structures remain in RAM for
//! cache-friendly traversal.

use crate::storage::tier::StorageTier;

use super::collection::VectorCollection;
use super::hnsw::HnswIndex;
use super::mmap_segment::MmapVectorSegment;

impl VectorCollection {
    /// Set the data directory for mmap segment files.
    pub fn set_data_dir(&mut self, dir: std::path::PathBuf) {
        self.data_dir = Some(dir);
    }

    /// Set the RAM budget for vector data (FP32 in sealed segments).
    pub fn set_ram_budget(&mut self, bytes: usize) {
        self.ram_budget_bytes = bytes;
    }

    /// Estimate current RAM usage for vector data (FP32 in HNSW nodes).
    /// SQ8 data is small enough to always fit; only FP32 is budget-sensitive.
    pub fn ram_usage_bytes(&self) -> usize {
        let bytes_per_vector = self.dim * std::mem::size_of::<f32>();
        let growing = self.growing.len() * bytes_per_vector;
        let building: usize = self
            .building
            .iter()
            .map(|b| b.flat.len() * bytes_per_vector)
            .sum();
        let sealed_ram: usize = self
            .sealed
            .iter()
            .filter(|s| s.tier == StorageTier::L0Ram)
            .map(|s| s.index.len() * bytes_per_vector)
            .sum();
        growing + building + sealed_ram
    }

    /// Whether the RAM budget is exceeded (new segments should spill to mmap).
    pub fn is_budget_exceeded(&self) -> bool {
        self.ram_budget_bytes > 0 && self.ram_usage_bytes() >= self.ram_budget_bytes
    }

    /// Number of segments that fell back to mmap due to budget exhaustion.
    pub fn mmap_fallback_count(&self) -> u32 {
        self.mmap_fallback_count
    }

    /// Number of currently active mmap segments.
    pub fn mmap_segment_count(&self) -> u32 {
        self.mmap_segment_count
    }

    /// Determine storage tier and optionally create mmap segment for a completed build.
    ///
    /// If the RAM budget is exceeded and a data directory is configured, writes
    /// FP32 vectors to an mmap file on NVMe. Returns the tier and optional mmap handle.
    pub(super) fn resolve_tier_for_build(
        &mut self,
        segment_id: u32,
        index: &HnswIndex,
    ) -> (StorageTier, Option<MmapVectorSegment>) {
        if !self.is_budget_exceeded() {
            return (StorageTier::L0Ram, None);
        }

        let Some(dir) = &self.data_dir else {
            return (StorageTier::L0Ram, None);
        };

        let seg_path = dir.join(format!("seg-{segment_id}.vseg"));
        // Extract vectors from HNSW index for mmap file.
        let refs: Vec<Vec<f32>> = (0..index.len())
            .filter_map(|i| index.get_vector(i as u32).map(|v| v.to_vec()))
            .collect();
        let ref_slices: Vec<&[f32]> = refs.iter().map(|v| v.as_slice()).collect();

        match MmapVectorSegment::create(&seg_path, self.dim, &ref_slices) {
            Ok(mmap) => {
                self.mmap_fallback_count += 1;
                self.mmap_segment_count += 1;
                tracing::info!(
                    segment_id,
                    vectors = index.len(),
                    path = %seg_path.display(),
                    "vector segment spilled to mmap (L1 NVMe)"
                );
                (StorageTier::L1Nvme, Some(mmap))
            }
            Err(e) => {
                tracing::warn!(
                    segment_id,
                    error = %e,
                    "mmap fallback failed, keeping vectors in RAM"
                );
                (StorageTier::L0Ram, None)
            }
        }
    }
}
