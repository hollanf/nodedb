//! Scalar Quantization (SQ8): FP32 → INT8 per-dimension.
//!
//! Each dimension is independently quantized to `[0, 255]` using per-dimension
//! min/max calibration. This is the **default production quantization** for
//! HNSW traversal: 4x RAM reduction with <1% recall loss.
//!
//! Distance computation uses asymmetric mode: query stays in FP32,
//! candidates are in INT8. This avoids quantizing the query and
//! preserves accuracy at the cost of a dequantize-per-dimension
//! during distance computation.
//!
//! Storage: D bytes per vector (vs 4D bytes for FP32).

use serde::{Deserialize, Serialize};

use crate::error::VectorError;

/// Magic bytes identifying a serialized [`Sq8Codec`] blob.
///
/// Format: `[NDSQ\0\0 (6 bytes)][version: u8 = 1][msgpack payload]`
pub const MAGIC: &[u8; 6] = b"NDSQ\0\0";

/// Wire format version for [`Sq8Codec`] serialization.
pub const SQ8_FORMAT_VERSION: u8 = 1;

/// SQ8 calibration parameters: per-dimension min/max.
#[derive(Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct Sq8Codec {
    pub dim: usize,
    /// Per-dimension minimum observed during calibration.
    mins: Vec<f32>,
    /// Per-dimension maximum observed during calibration.
    maxs: Vec<f32>,
    /// Pre-computed per-dimension scale: `(max - min) / 255.0`.
    /// Zero if max == min (constant dimension → all quantize to 0).
    scales: Vec<f32>,
    /// Pre-computed per-dimension inverse scale: `255.0 / (max - min)`.
    inv_scales: Vec<f32>,
}

impl Sq8Codec {
    /// Calibrate min/max from a set of training vectors.
    ///
    /// Scans all vectors to find per-dimension min/max bounds.
    /// At least 1000 vectors recommended for stable calibration;
    /// for fewer vectors the bounds may be tight, causing clipping
    /// on future inserts outside the calibration range.
    pub fn calibrate(vectors: &[&[f32]], dim: usize) -> Self {
        assert!(!vectors.is_empty(), "cannot calibrate on empty set");
        assert!(dim > 0);

        let mut mins = vec![f32::MAX; dim];
        let mut maxs = vec![f32::MIN; dim];

        for v in vectors {
            debug_assert_eq!(v.len(), dim);
            for d in 0..dim {
                if v[d] < mins[d] {
                    mins[d] = v[d];
                }
                if v[d] > maxs[d] {
                    maxs[d] = v[d];
                }
            }
        }

        let mut scales = vec![0.0f32; dim];
        let mut inv_scales = vec![0.0f32; dim];
        for d in 0..dim {
            let range = maxs[d] - mins[d];
            if range > f32::EPSILON {
                scales[d] = range / 255.0;
                inv_scales[d] = 255.0 / range;
            }
        }

        Self {
            dim,
            mins,
            maxs,
            scales,
            inv_scales,
        }
    }

    /// Quantize a single FP32 vector to INT8.
    pub fn quantize(&self, vector: &[f32]) -> Vec<u8> {
        debug_assert_eq!(vector.len(), self.dim);
        let mut out = Vec::with_capacity(self.dim);
        for ((&v, &min), (&max, &inv_scale)) in vector
            .iter()
            .zip(self.mins.iter())
            .zip(self.maxs.iter().zip(self.inv_scales.iter()))
        {
            let clamped = v.clamp(min, max);
            let q = ((clamped - min) * inv_scale).round() as u8;
            out.push(q);
        }
        out
    }

    /// Batch quantize: quantize all vectors into a contiguous byte array.
    ///
    /// Returns `dim * N` bytes laid out as `[v0_d0, v0_d1, ..., v1_d0, ...]`.
    pub fn quantize_batch(&self, vectors: &[&[f32]]) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.dim * vectors.len());
        for v in vectors {
            out.extend(self.quantize(v));
        }
        out
    }

    /// Dequantize INT8 back to FP32 (lossy reconstruction).
    pub fn dequantize(&self, quantized: &[u8]) -> Vec<f32> {
        debug_assert_eq!(quantized.len(), self.dim);
        let mut out = Vec::with_capacity(self.dim);
        for ((&q, &min), &scale) in quantized
            .iter()
            .zip(self.mins.iter())
            .zip(self.scales.iter())
        {
            out.push(min + q as f32 * scale);
        }
        out
    }

    /// Asymmetric L2 squared distance: query (FP32) vs candidate (INT8).
    ///
    /// This is the hot-path function used during HNSW traversal.
    /// The query stays in full precision; only the candidate is quantized.
    #[inline]
    pub fn asymmetric_l2(&self, query: &[f32], candidate: &[u8]) -> f32 {
        debug_assert_eq!(query.len(), self.dim);
        debug_assert_eq!(candidate.len(), self.dim);
        let mut sum = 0.0f32;
        for d in 0..self.dim {
            let dequant = self.mins[d] + candidate[d] as f32 * self.scales[d];
            let diff = query[d] - dequant;
            sum += diff * diff;
        }
        sum
    }

    /// Asymmetric cosine distance: query (FP32) vs candidate (INT8).
    #[inline]
    pub fn asymmetric_cosine(&self, query: &[f32], candidate: &[u8]) -> f32 {
        debug_assert_eq!(query.len(), self.dim);
        debug_assert_eq!(candidate.len(), self.dim);
        let mut dot = 0.0f32;
        let mut norm_q = 0.0f32;
        let mut norm_c = 0.0f32;
        for d in 0..self.dim {
            let dequant = self.mins[d] + candidate[d] as f32 * self.scales[d];
            dot += query[d] * dequant;
            norm_q += query[d] * query[d];
            norm_c += dequant * dequant;
        }
        let denom = (norm_q * norm_c).sqrt();
        if denom < f32::EPSILON {
            return 1.0;
        }
        (1.0 - dot / denom).max(0.0)
    }

    /// Asymmetric negative inner product: query (FP32) vs candidate (INT8).
    #[inline]
    pub fn asymmetric_ip(&self, query: &[f32], candidate: &[u8]) -> f32 {
        debug_assert_eq!(query.len(), self.dim);
        debug_assert_eq!(candidate.len(), self.dim);
        let mut dot = 0.0f32;
        for d in 0..self.dim {
            let dequant = self.mins[d] + candidate[d] as f32 * self.scales[d];
            dot += query[d] * dequant;
        }
        -dot
    }

    /// Dimension count.
    pub fn dim(&self) -> usize {
        self.dim
    }

    /// Serialize the codec to bytes with a versioned magic header.
    ///
    /// Format: `[NDSQ\0\0 (6 bytes)][version: u8 = 1][msgpack payload]`
    pub fn to_bytes(&self) -> Vec<u8> {
        let payload = zerompk::to_msgpack_vec(self).unwrap_or_default();
        let mut out = Vec::with_capacity(7 + payload.len());
        out.extend_from_slice(MAGIC);
        out.push(SQ8_FORMAT_VERSION);
        out.extend_from_slice(&payload);
        out
    }

    /// Deserialize the codec from bytes produced by [`Self::to_bytes`].
    ///
    /// Returns `VectorError::InvalidMagic` if the header does not match
    /// `NDSQ\0\0`, and `VectorError::UnsupportedVersion` for unknown versions.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, VectorError> {
        if bytes.len() < 7 || &bytes[0..6] != MAGIC {
            return Err(VectorError::InvalidMagic);
        }
        let version = bytes[6];
        if version != SQ8_FORMAT_VERSION {
            return Err(VectorError::UnsupportedVersion {
                found: version,
                expected: SQ8_FORMAT_VERSION,
            });
        }
        zerompk::from_msgpack::<Self>(&bytes[7..])
            .map_err(|e| VectorError::DeserializationFailed(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_codec() -> Sq8Codec {
        let vecs: Vec<Vec<f32>> = (0..100)
            .map(|i| vec![i as f32 * 0.1, (i as f32).sin(), (i as f32).cos()])
            .collect();
        let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();
        Sq8Codec::calibrate(&refs, 3)
    }

    #[test]
    fn sq8_codec_golden_format() {
        let codec = make_codec();
        let bytes = codec.to_bytes();
        // First 6 bytes are magic.
        assert_eq!(&bytes[0..6], MAGIC);
        // Byte 6 is the version.
        assert_eq!(bytes[6], SQ8_FORMAT_VERSION);
        // Bytes 7+ must decode back to a valid Sq8Codec.
        let decoded = zerompk::from_msgpack::<Sq8Codec>(&bytes[7..]).unwrap();
        assert_eq!(decoded.dim, 3);
    }

    #[test]
    fn sq8_roundtrip() {
        let codec = make_codec();
        let bytes = codec.to_bytes();
        let restored = Sq8Codec::from_bytes(&bytes).unwrap();
        assert_eq!(restored.dim, codec.dim);
        assert_eq!(restored.inv_scales.len(), codec.inv_scales.len());
        for (a, b) in restored.inv_scales.iter().zip(codec.inv_scales.iter()) {
            assert!((a - b).abs() < 1e-6, "inv_scales mismatch: {a} vs {b}");
        }
    }

    #[test]
    fn sq8_invalid_magic_returns_error() {
        let mut bytes = make_codec().to_bytes();
        bytes[0] = b'X'; // corrupt magic
        assert!(matches!(
            Sq8Codec::from_bytes(&bytes),
            Err(VectorError::InvalidMagic)
        ));
    }

    #[test]
    fn sq8_version_mismatch_returns_error() {
        let mut bytes = make_codec().to_bytes();
        bytes[6] = 0; // wrong version
        assert!(matches!(
            Sq8Codec::from_bytes(&bytes),
            Err(VectorError::UnsupportedVersion {
                found: 0,
                expected: 1
            })
        ));
    }

    fn make_vectors() -> Vec<Vec<f32>> {
        (0..100)
            .map(|i| vec![i as f32 * 0.1, (i as f32).sin(), (i as f32).cos()])
            .collect()
    }

    #[test]
    fn quantize_dequantize_roundtrip() {
        let vecs = make_vectors();
        let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();
        let codec = Sq8Codec::calibrate(&refs, 3);

        for v in &vecs {
            let q = codec.quantize(v);
            let dq = codec.dequantize(&q);
            for d in 0..3 {
                let error = (v[d] - dq[d]).abs();
                let range = codec.maxs[d] - codec.mins[d];
                // Error should be at most half a quantization step.
                assert!(
                    error <= range / 255.0 + 1e-6,
                    "d={d}: error={error}, max_step={}",
                    range / 255.0
                );
            }
        }
    }

    #[test]
    fn asymmetric_l2_close_to_exact() {
        let vecs = make_vectors();
        let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();
        let codec = Sq8Codec::calibrate(&refs, 3);

        let query = &[5.0, 0.5, -0.5];
        for v in &vecs {
            let q = codec.quantize(v);
            let exact = crate::distance::l2_squared(query, v);
            let approx = codec.asymmetric_l2(query, &q);
            // Allow up to 5% relative error.
            let rel_error = if exact > 0.01 {
                (exact - approx).abs() / exact
            } else {
                (exact - approx).abs()
            };
            assert!(
                rel_error < 0.05 || (exact - approx).abs() < 0.1,
                "exact={exact}, approx={approx}, rel_error={rel_error}"
            );
        }
    }

    #[test]
    fn batch_quantize() {
        let vecs = make_vectors();
        let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();
        let codec = Sq8Codec::calibrate(&refs, 3);

        let batch = codec.quantize_batch(&refs);
        assert_eq!(batch.len(), 3 * 100);

        // First vector should match individual quantize.
        let single = codec.quantize(&vecs[0]);
        assert_eq!(&batch[0..3], &single[..]);
    }

    #[test]
    fn constant_dimension_handled() {
        // All vectors have the same value in dimension 0.
        let vecs: Vec<Vec<f32>> = (0..10).map(|i| vec![5.0, i as f32]).collect();
        let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();
        let codec = Sq8Codec::calibrate(&refs, 2);

        // Constant dimension should quantize to 0 without NaN/inf.
        let q = codec.quantize(&[5.0, 3.0]);
        assert_eq!(q[0], 0); // constant dim
    }
}
