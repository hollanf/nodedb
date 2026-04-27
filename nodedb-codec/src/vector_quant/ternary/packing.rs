//! Trit packing/unpacking and FP32→ternary quantization.
//!
//! Cold (5 trits/byte, base-3) is the disk-friendly format.
//! Hot (4 trits/byte, 2-bpw) is the SIMD-friendly format.

/// Encode an `i8` trit `{-1,0,+1}` into the unsigned alphabet `{0,1,2}`.
#[inline(always)]
fn trit_to_u8(t: i8) -> u8 {
    match t {
        -1 => 0,
        0 => 1,
        1 => 2,
        _ => 1,
    }
}

/// Decode unsigned alphabet `{0,1,2}` back to `i8` trit `{-1,0,+1}`.
#[inline(always)]
fn u8_to_trit(v: u8) -> i8 {
    match v {
        0 => -1,
        2 => 1,
        _ => 0,
    }
}

/// Pack trits (`i8 ∈ {-1, 0, +1}`) into cold 5-trits-per-byte format.
pub fn pack_cold(trits: &[i8]) -> Vec<u8> {
    let out_len = trits.len().div_ceil(5);
    let mut out = vec![0u8; out_len];
    for (chunk_idx, chunk) in trits.chunks(5).enumerate() {
        let mut byte = 0u8;
        let mut mul = 1u8;
        for &t in chunk {
            byte = byte.wrapping_add(trit_to_u8(t).wrapping_mul(mul));
            mul = mul.wrapping_mul(3);
        }
        out[chunk_idx] = byte;
    }
    out
}

/// Unpack cold 5-trits-per-byte format back to `i8` trits.
pub fn unpack_cold(cold: &[u8], dim: usize) -> Vec<i8> {
    let mut out = Vec::with_capacity(dim);
    'outer: for &byte in cold {
        let mut v = byte;
        for _ in 0..5 {
            if out.len() >= dim {
                break 'outer;
            }
            out.push(u8_to_trit(v % 3));
            v /= 3;
        }
    }
    out
}

/// Pack trits into hot 2-bpw format (4 trits per byte).
///
/// Bit encoding: `0b00=−1`, `0b01=0`, `0b10=+1`. LSB-first within each byte.
pub fn pack_hot(trits: &[i8]) -> Vec<u8> {
    let out_len = trits.len().div_ceil(4);
    let mut out = vec![0u8; out_len];
    for (i, &t) in trits.iter().enumerate() {
        let byte_idx = i / 4;
        let shift = (i % 4) * 2;
        let bits: u8 = match t {
            -1 => 0b00,
            1 => 0b10,
            _ => 0b01,
        };
        out[byte_idx] |= bits << shift;
    }
    out
}

/// Unpack hot 2-bpw format back to `i8` trits.
pub fn unpack_hot(hot: &[u8], dim: usize) -> Vec<i8> {
    let mut out = Vec::with_capacity(dim);
    'outer: for &byte in hot {
        for slot in 0..4 {
            if out.len() >= dim {
                break 'outer;
            }
            let bits = (byte >> (slot * 2)) & 0b11;
            out.push(match bits {
                0b00 => -1,
                0b10 => 1,
                _ => 0,
            });
        }
    }
    out
}

/// Convert cold-packed trits to hot-packed trits.
pub fn cold_to_hot(cold: &[u8], dim: usize) -> Vec<u8> {
    let trits = unpack_cold(cold, dim);
    pack_hot(&trits)
}

/// Quantize a FP32 vector to ternary trits using BitNet absmean scaling.
///
/// Returns `(trits, scale)` where `scale = mean(|v_i|)`.
pub fn quantize(v: &[f32]) -> (Vec<i8>, f32) {
    if v.is_empty() {
        return (Vec::new(), 0.0);
    }
    let scale: f32 = v.iter().map(|x| x.abs()).sum::<f32>() / v.len() as f32;
    let trits = if scale == 0.0 {
        vec![0i8; v.len()]
    } else {
        v.iter()
            .map(|&x| (x / scale).round().clamp(-1.0, 1.0) as i8)
            .collect()
    };
    (trits, scale)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cold_pack_roundtrip_simple() {
        let trits: Vec<i8> = vec![-1, 0, 1, -1, 0, 1, -1];
        let cold = pack_cold(&trits);
        let out = unpack_cold(&cold, trits.len());
        assert_eq!(out, trits);
    }

    #[test]
    fn cold_pack_roundtrip_dim_7() {
        let trits: Vec<i8> = vec![1, -1, 0, 1, -1, 0, 1];
        let cold = pack_cold(&trits);
        assert_eq!(cold.len(), 2);
        assert_eq!(unpack_cold(&cold, 7), trits);
    }

    #[test]
    fn cold_pack_roundtrip_dim_13() {
        let trits: Vec<i8> = vec![1, 0, -1, 1, 0, -1, 1, 0, -1, 1, 0, -1, 1];
        let cold = pack_cold(&trits);
        assert_eq!(cold.len(), 3);
        assert_eq!(unpack_cold(&cold, 13), trits);
    }

    #[test]
    fn hot_pack_roundtrip() {
        let trits: Vec<i8> = vec![-1, 0, 1, -1, 0, 1, -1, 0, 1, -1, 0, 1];
        let hot = pack_hot(&trits);
        assert_eq!(unpack_hot(&hot, trits.len()), trits);
    }

    #[test]
    fn cold_to_hot_preserves_trits() {
        let trits: Vec<i8> = vec![1, -1, 0, 1, -1, 0, 1, -1, 0, 1, -1];
        let cold = pack_cold(&trits);
        let hot = cold_to_hot(&cold, trits.len());
        assert_eq!(unpack_hot(&hot, trits.len()), trits);
    }

    #[test]
    fn cold_to_hot_dim_not_multiple_of_5() {
        for dim in [7usize, 13, 11, 3] {
            let trits: Vec<i8> = (0..dim)
                .map(|i| match i % 3 {
                    0 => 1i8,
                    1 => -1,
                    _ => 0,
                })
                .collect();
            let cold = pack_cold(&trits);
            let hot = cold_to_hot(&cold, dim);
            assert_eq!(unpack_hot(&hot, dim), trits, "mismatch for dim={dim}");
        }
    }

    #[test]
    fn quantize_zeros_gives_all_zero_trits() {
        let v = vec![0.0f32; 16];
        let (trits, scale) = quantize(&v);
        assert_eq!(scale, 0.0);
        assert!(trits.iter().all(|&t| t == 0));
    }
}
