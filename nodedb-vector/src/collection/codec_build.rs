//! Methods on `VectorCollection` for building the collection-level
//! codec-dispatch index (RaBitQ, BBQ).
//!
//! Split from `lifecycle.rs` to keep that file under the 500-line limit.
//! All `impl VectorCollection` blocks in `collection/` extend the same type.

use super::codec_dispatch::{CollectionCodec, build_collection_codec};
use super::lifecycle::VectorCollection;

impl VectorCollection {
    /// Collect all live FP32 vectors from every segment (growing, building,
    /// and sealed) in insertion order. Used to train the collection-level
    /// codec-dispatch index.
    pub(crate) fn gather_all_vectors_fp32(&self) -> Vec<Vec<f32>> {
        let total = self.len();
        let mut out = Vec::with_capacity(total);

        for i in 0..self.growing.len() as u32 {
            if let Some(v) = self.growing.get_vector(i) {
                out.push(v.to_vec());
            }
        }

        for seg in &self.building {
            for i in 0..seg.flat.len() as u32 {
                if let Some(v) = seg.flat.get_vector(i) {
                    out.push(v.to_vec());
                }
            }
        }

        for seg in &self.sealed {
            let n = seg.index.len();
            for i in 0..n as u32 {
                if !seg.index.is_deleted(i)
                    && let Some(v) = seg.index.get_vector(i)
                {
                    out.push(v.to_vec());
                }
            }
        }

        out
    }

    /// Build a codec-dispatched index over all current vectors using the
    /// requested quantization. Replaces any existing dispatch index for
    /// this collection. Idempotent.
    ///
    /// Returns a reference to the new index, or `None` if the quantization
    /// tag is not supported (falls back to per-segment Sq8/PQ paths) or there
    /// are no vectors to train on.
    pub fn build_codec_dispatch(&mut self, quantization: &str) -> Option<&CollectionCodec> {
        let vectors = self.gather_all_vectors_fp32();
        let dim = self.dim;
        let m = self.params.m;
        let ef_construction = self.params.ef_construction;
        let seed = 42_u64;
        self.codec_dispatch =
            build_collection_codec(quantization, &vectors, dim, m, ef_construction, seed);
        self.codec_dispatch.as_ref()
    }
}
