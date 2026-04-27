//! SIMD ternary dot product with runtime CPU-feature dispatch.

#![allow(unsafe_op_in_unsafe_fn)]

use std::sync::OnceLock;

use super::packing::unpack_hot;

/// Scalar fallback ternary dot product.
fn ternary_dot_scalar(a_hot: &[u8], b_hot: &[u8], dim: usize) -> i32 {
    let a = unpack_hot(a_hot, dim);
    let b = unpack_hot(b_hot, dim);
    a.iter()
        .zip(b.iter())
        .map(|(&x, &y)| x as i32 * y as i32)
        .sum()
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f,avx512bw")]
unsafe fn ternary_dot_avx512(a_hot: &[u8], b_hot: &[u8], dim: usize) -> i32 {
    use std::arch::x86_64::*;

    let a_trits = unpack_hot(a_hot, dim);
    let b_trits = unpack_hot(b_hot, dim);

    let len = a_trits.len();
    let chunks = len / 64;
    let mut sum = _mm512_setzero_si512();

    for i in 0..chunks {
        let a_ptr = a_trits.as_ptr().add(i * 64) as *const __m512i;
        let b_ptr = b_trits.as_ptr().add(i * 64) as *const __m512i;
        let a_vec = _mm512_loadu_si512(a_ptr);
        let b_vec = _mm512_loadu_si512(b_ptr);
        let a_lo = _mm512_cvtepi8_epi16(_mm512_castsi512_si256(a_vec));
        let a_hi = _mm512_cvtepi8_epi16(_mm512_extracti64x4_epi64(a_vec, 1));
        let b_lo = _mm512_cvtepi8_epi16(_mm512_castsi512_si256(b_vec));
        let b_hi = _mm512_cvtepi8_epi16(_mm512_extracti64x4_epi64(b_vec, 1));
        let prod_lo = _mm512_mullo_epi16(a_lo, b_lo);
        let prod_hi = _mm512_mullo_epi16(a_hi, b_hi);
        let ones = _mm512_set1_epi16(1);
        let sum_lo = _mm512_madd_epi16(prod_lo, ones);
        let sum_hi = _mm512_madd_epi16(prod_hi, ones);
        sum = _mm512_add_epi32(sum, _mm512_add_epi32(sum_lo, sum_hi));
    }

    let mut acc = _mm512_reduce_add_epi32(sum);
    for i in (chunks * 64)..len {
        acc += a_trits[i] as i32 * b_trits[i] as i32;
    }
    acc
}

#[cfg(target_arch = "aarch64")]
fn ternary_dot_neon(a_hot: &[u8], b_hot: &[u8], dim: usize) -> i32 {
    use std::arch::aarch64::*;

    let a_trits = unpack_hot(a_hot, dim);
    let b_trits = unpack_hot(b_hot, dim);

    let len = a_trits.len();
    let chunks = len / 16;
    let mut acc: i32;

    unsafe {
        let mut sum = vdupq_n_s32(0i32);
        for i in 0..chunks {
            let a_ptr = a_trits.as_ptr().add(i * 16);
            let b_ptr = b_trits.as_ptr().add(i * 16);
            let a_vec = vld1q_s8(a_ptr);
            let b_vec = vld1q_s8(b_ptr);
            let prod = vmulq_s8(a_vec, b_vec);
            let prod_lo = vmovl_s8(vget_low_s8(prod));
            let prod_hi = vmovl_s8(vget_high_s8(prod));
            let prod32_lo = vmovl_s16(vget_low_s16(prod_lo));
            let prod32_hi = vmovl_s16(vget_high_s16(prod_lo));
            let prod32_lo2 = vmovl_s16(vget_low_s16(prod_hi));
            let prod32_hi2 = vmovl_s16(vget_high_s16(prod_hi));
            sum = vaddq_s32(
                sum,
                vaddq_s32(
                    vaddq_s32(prod32_lo, prod32_hi),
                    vaddq_s32(prod32_lo2, prod32_hi2),
                ),
            );
        }
        acc = vaddvq_s32(sum);
        for i in (chunks * 16)..len {
            acc += a_trits[i] as i32 * b_trits[i] as i32;
        }
    }
    acc
}

type DotFn = fn(&[u8], &[u8], usize) -> i32;

static DOT_FN: OnceLock<DotFn> = OnceLock::new();

#[cfg(target_arch = "x86_64")]
fn ternary_dot_avx512_trampoline(a: &[u8], b: &[u8], dim: usize) -> i32 {
    unsafe { ternary_dot_avx512(a, b, dim) }
}

fn resolve() -> DotFn {
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx512f") && std::is_x86_feature_detected!("avx512bw") {
            return ternary_dot_avx512_trampoline;
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        return ternary_dot_neon;
    }
    #[allow(unreachable_code)]
    {
        ternary_dot_scalar
    }
}

/// Compute ternary dot product between two hot-packed byte slices.
#[inline]
pub fn ternary_dot(a_hot: &[u8], b_hot: &[u8], dim: usize) -> i32 {
    let f = DOT_FN.get_or_init(resolve);
    f(a_hot, b_hot, dim)
}

#[cfg(test)]
mod tests {
    use super::super::packing::pack_hot;
    use super::*;

    #[test]
    fn simd_vs_scalar_agreement() {
        let trits_a: Vec<i8> = vec![1, -1, 0, 1, -1, 0, 1, -1, 0, 1, -1, 0, 1, -1, 0, 1];
        let trits_b: Vec<i8> = vec![-1, 1, 0, -1, 1, 0, -1, 1, 0, -1, 1, 0, -1, 1, 0, -1];
        let dim = trits_a.len();
        let hot_a = pack_hot(&trits_a);
        let hot_b = pack_hot(&trits_b);

        let scalar = ternary_dot_scalar(&hot_a, &hot_b, dim);
        let dispatched = ternary_dot(&hot_a, &hot_b, dim);

        let expected: i32 = trits_a
            .iter()
            .zip(trits_b.iter())
            .map(|(&a, &b)| a as i32 * b as i32)
            .sum();
        assert_eq!(scalar, expected);
        assert_eq!(scalar, dispatched);
    }
}
