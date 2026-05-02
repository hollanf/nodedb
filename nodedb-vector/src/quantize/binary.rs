//! Binary Quantization (BQ): sign-bit encoding + Hamming distance.
//!
//! Each dimension is encoded as a single bit: 1 if positive, 0 if negative.
//! 32x compression (D/8 bytes vs 4D bytes for FP32). Best used as a coarse
//! pre-filter: compute Hamming distance to quickly eliminate far candidates
//! before computing exact distances on survivors.
//!
//! Recall loss: 5-10% as a standalone index, but combined with a reranking
//! step on top-K×10 candidates, effective recall loss is 1-3%.

/// Encode an FP32 vector as binary: one bit per dimension.
///
/// Bit layout: bit 0 of byte 0 = dimension 0, bit 1 = dimension 1, etc.
/// `output.len() = ceil(dim / 8)`.
pub fn encode(vector: &[f32]) -> Vec<u8> {
    let num_bytes = vector.len().div_ceil(8);
    let mut bits = vec![0u8; num_bytes];
    for (i, &val) in vector.iter().enumerate() {
        if val > 0.0 {
            bits[i / 8] |= 1 << (i % 8);
        }
    }
    bits
}

/// Batch encode: encode all vectors into contiguous binary representation.
///
/// Returns `ceil(dim/8) * N` bytes.
pub fn encode_batch(vectors: &[&[f32]], dim: usize) -> Vec<u8> {
    let bytes_per = dim.div_ceil(8);
    // no-governor: cold batch binary encode; governed at segment build call site
    let mut out = Vec::with_capacity(bytes_per * vectors.len());
    for v in vectors {
        out.extend(encode(v));
    }
    out
}

/// Hamming distance between two binary-encoded vectors.
///
/// Counts the number of differing bits using `count_ones()` (hardware
/// POPCNT on x86_64).
#[inline]
pub fn hamming_distance(a: &[u8], b: &[u8]) -> u32 {
    debug_assert_eq!(a.len(), b.len());
    let mut dist = 0u32;
    for i in 0..a.len() {
        dist += (a[i] ^ b[i]).count_ones();
    }
    dist
}

/// Hamming distance operating on u64 chunks for better throughput.
///
/// Processes 8 bytes at a time using u64 POPCNT. Falls back to byte-level
/// for the remainder. ~4x faster than byte-level for vectors ≥64 dims.
#[inline]
pub fn hamming_distance_fast(a: &[u8], b: &[u8]) -> u32 {
    debug_assert_eq!(a.len(), b.len());
    let mut dist = 0u32;
    let chunks = a.len() / 8;
    let remainder = a.len() % 8;

    // Process u64 chunks (slice is guaranteed to be 8 bytes by loop bounds).
    for i in 0..chunks {
        let offset = i * 8;
        let mut a_buf = [0u8; 8];
        let mut b_buf = [0u8; 8];
        a_buf.copy_from_slice(&a[offset..offset + 8]);
        b_buf.copy_from_slice(&b[offset..offset + 8]);
        dist += (u64::from_le_bytes(a_buf) ^ u64::from_le_bytes(b_buf)).count_ones();
    }

    // Process remaining bytes.
    let start = chunks * 8;
    for i in 0..remainder {
        dist += (a[start + i] ^ b[start + i]).count_ones();
    }

    dist
}

/// Binary vector size in bytes for a given dimensionality.
pub fn binary_size(dim: usize) -> usize {
    dim.div_ceil(8)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_positive_negative() {
        let v = [1.0, -1.0, 1.0, -1.0, 0.0, 1.0, -0.5, 0.5];
        let bits = encode(&v);
        assert_eq!(bits.len(), 1);
        // bits[0]: dim0=1, dim1=0, dim2=1, dim3=0, dim4=0, dim5=1, dim6=0, dim7=1
        //        = 0b10100101 = 0xA5
        assert_eq!(bits[0], 0b10100101);
    }

    #[test]
    fn hamming_identical_is_zero() {
        let v = [1.0, -1.0, 1.0, 0.5];
        let a = encode(&v);
        let b = encode(&v);
        assert_eq!(hamming_distance(&a, &b), 0);
    }

    #[test]
    fn hamming_opposite_is_dim() {
        let a_vec = [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0];
        let b_vec = [-1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0];
        let a = encode(&a_vec);
        let b = encode(&b_vec);
        assert_eq!(hamming_distance(&a, &b), 8);
    }

    #[test]
    fn hamming_fast_matches_simple() {
        // 128 dimensions = 16 bytes = 2 u64 chunks.
        let a_vec: Vec<f32> = (0..128)
            .map(|i| if i % 3 == 0 { 1.0 } else { -1.0 })
            .collect();
        let b_vec: Vec<f32> = (0..128)
            .map(|i| if i % 5 == 0 { 1.0 } else { -1.0 })
            .collect();
        let a = encode(&a_vec);
        let b = encode(&b_vec);

        let slow = hamming_distance(&a, &b);
        let fast = hamming_distance_fast(&a, &b);
        assert_eq!(slow, fast);
    }

    #[test]
    fn high_dimensional_encoding() {
        // 768 dimensions = 96 bytes.
        let v: Vec<f32> = (0..768).map(|i| (i as f32).sin()).collect();
        let bits = encode(&v);
        assert_eq!(bits.len(), 96);
    }

    #[test]
    fn batch_encode_layout() {
        let v1 = [1.0f32, -1.0, 1.0, -1.0];
        let v2 = [-1.0f32, 1.0, -1.0, 1.0];
        let batch = encode_batch(&[&v1, &v2], 4);
        assert_eq!(batch.len(), 2); // 2 vectors × 1 byte each
        assert_eq!(batch[0], encode(&v1)[0]);
        assert_eq!(batch[1], encode(&v2)[0]);
    }
}
