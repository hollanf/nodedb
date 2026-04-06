//! Runtime SIMD dispatch for vector distance and bitmap operations.
//!
//! Detects CPU features at startup and selects the fastest available
//! kernel for each operation. A single binary supports all targets:
//!
//! - AVX-512 (512-bit, 16 floats/op) — Intel Xeon, AMD Zen 4+
//! - AVX2+FMA (256-bit, 8 floats/op) — most x86_64 since 2013
//! - NEON (128-bit, 4 floats/op) — ARM64 (Graviton, Apple Silicon)
//! - Scalar fallback — auto-vectorized loops

/// Selected SIMD runtime — function pointers to the best available kernels.
pub struct SimdRuntime {
    pub l2_squared: fn(&[f32], &[f32]) -> f32,
    pub cosine_distance: fn(&[f32], &[f32]) -> f32,
    pub neg_inner_product: fn(&[f32], &[f32]) -> f32,
    pub hamming: fn(&[u8], &[u8]) -> u32,
    pub name: &'static str,
}

impl SimdRuntime {
    /// Detect CPU features and select the best kernels.
    pub fn detect() -> Self {
        #[cfg(target_arch = "x86_64")]
        {
            if std::is_x86_feature_detected!("avx512f") {
                return Self {
                    l2_squared: avx512::l2_squared,
                    cosine_distance: avx512::cosine_distance,
                    neg_inner_product: avx512::neg_inner_product,
                    hamming: fast_hamming,
                    name: "avx512",
                };
            }
            if std::is_x86_feature_detected!("avx2") && std::is_x86_feature_detected!("fma") {
                return Self {
                    l2_squared: avx2::l2_squared,
                    cosine_distance: avx2::cosine_distance,
                    neg_inner_product: avx2::neg_inner_product,
                    hamming: fast_hamming,
                    name: "avx2+fma",
                };
            }
        }
        #[cfg(target_arch = "aarch64")]
        {
            return Self {
                l2_squared: neon::l2_squared,
                cosine_distance: neon::cosine_distance,
                neg_inner_product: neon::neg_inner_product,
                hamming: fast_hamming,
                name: "neon",
            };
        }
        #[allow(unreachable_code)]
        Self {
            l2_squared: scalar_l2,
            cosine_distance: scalar_cosine,
            neg_inner_product: scalar_ip,
            hamming: fast_hamming,
            name: "scalar",
        }
    }
}

/// Global SIMD runtime — initialized once, used everywhere.
static RUNTIME: std::sync::OnceLock<SimdRuntime> = std::sync::OnceLock::new();

/// Get the global SIMD runtime (auto-detects on first call).
pub fn runtime() -> &'static SimdRuntime {
    RUNTIME.get_or_init(SimdRuntime::detect)
}

// ── Scalar fallback ──

fn scalar_l2(a: &[f32], b: &[f32]) -> f32 {
    let mut sum = 0.0f32;
    for i in 0..a.len() {
        let d = a[i] - b[i];
        sum += d * d;
    }
    sum
}

fn scalar_cosine(a: &[f32], b: &[f32]) -> f32 {
    let mut dot = 0.0f32;
    let mut na = 0.0f32;
    let mut nb = 0.0f32;
    for i in 0..a.len() {
        dot += a[i] * b[i];
        na += a[i] * a[i];
        nb += b[i] * b[i];
    }
    let denom = (na * nb).sqrt();
    if denom < f32::EPSILON {
        1.0
    } else {
        (1.0 - dot / denom).max(0.0)
    }
}

fn scalar_ip(a: &[f32], b: &[f32]) -> f32 {
    let mut dot = 0.0f32;
    for i in 0..a.len() {
        dot += a[i] * b[i];
    }
    -dot
}

/// Fast Hamming distance using u64 POPCNT (available on all modern CPUs).
fn fast_hamming(a: &[u8], b: &[u8]) -> u32 {
    let mut dist = 0u32;
    let chunks = a.len() / 8;
    for i in 0..chunks {
        let off = i * 8;
        let xa = u64::from_le_bytes([
            a[off],
            a[off + 1],
            a[off + 2],
            a[off + 3],
            a[off + 4],
            a[off + 5],
            a[off + 6],
            a[off + 7],
        ]);
        let xb = u64::from_le_bytes([
            b[off],
            b[off + 1],
            b[off + 2],
            b[off + 3],
            b[off + 4],
            b[off + 5],
            b[off + 6],
            b[off + 7],
        ]);
        dist += (xa ^ xb).count_ones();
    }
    for i in (chunks * 8)..a.len() {
        dist += (a[i] ^ b[i]).count_ones();
    }
    dist
}

// ── AVX2+FMA kernels ──

#[cfg(target_arch = "x86_64")]
mod avx2 {
    pub fn l2_squared(a: &[f32], b: &[f32]) -> f32 {
        // SAFETY: caller verified avx2+fma via is_x86_feature_detected.
        unsafe { l2_squared_impl(a, b) }
    }

    #[target_feature(enable = "avx2,fma")]
    unsafe fn l2_squared_impl(a: &[f32], b: &[f32]) -> f32 {
        unsafe {
            use std::arch::x86_64::*;
            let n = a.len();
            let mut sum = _mm256_setzero_ps();
            let chunks = n / 8;
            for i in 0..chunks {
                let off = i * 8;
                let va = _mm256_loadu_ps(a.as_ptr().add(off));
                let vb = _mm256_loadu_ps(b.as_ptr().add(off));
                let diff = _mm256_sub_ps(va, vb);
                sum = _mm256_fmadd_ps(diff, diff, sum);
            }
            let mut result = hsum256(sum);
            for i in (chunks * 8)..n {
                let d = a[i] - b[i];
                result += d * d;
            }
            result
        }
    }

    pub fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
        unsafe { cosine_impl(a, b) }
    }

    #[target_feature(enable = "avx2,fma")]
    unsafe fn cosine_impl(a: &[f32], b: &[f32]) -> f32 {
        unsafe {
            use std::arch::x86_64::*;
            let n = a.len();
            let mut vdot = _mm256_setzero_ps();
            let mut vna = _mm256_setzero_ps();
            let mut vnb = _mm256_setzero_ps();
            let chunks = n / 8;
            for i in 0..chunks {
                let off = i * 8;
                let va = _mm256_loadu_ps(a.as_ptr().add(off));
                let vb = _mm256_loadu_ps(b.as_ptr().add(off));
                vdot = _mm256_fmadd_ps(va, vb, vdot);
                vna = _mm256_fmadd_ps(va, va, vna);
                vnb = _mm256_fmadd_ps(vb, vb, vnb);
            }
            let mut dot = hsum256(vdot);
            let mut na = hsum256(vna);
            let mut nb = hsum256(vnb);
            for i in (chunks * 8)..n {
                dot += a[i] * b[i];
                na += a[i] * a[i];
                nb += b[i] * b[i];
            }
            let denom = (na * nb).sqrt();
            if denom < f32::EPSILON {
                1.0
            } else {
                (1.0 - dot / denom).max(0.0)
            }
        }
    }

    pub fn neg_inner_product(a: &[f32], b: &[f32]) -> f32 {
        unsafe { ip_impl(a, b) }
    }

    #[target_feature(enable = "avx2,fma")]
    unsafe fn ip_impl(a: &[f32], b: &[f32]) -> f32 {
        unsafe {
            use std::arch::x86_64::*;
            let n = a.len();
            let mut vdot = _mm256_setzero_ps();
            let chunks = n / 8;
            for i in 0..chunks {
                let off = i * 8;
                let va = _mm256_loadu_ps(a.as_ptr().add(off));
                let vb = _mm256_loadu_ps(b.as_ptr().add(off));
                vdot = _mm256_fmadd_ps(va, vb, vdot);
            }
            let mut dot = hsum256(vdot);
            for i in (chunks * 8)..n {
                dot += a[i] * b[i];
            }
            -dot
        }
    }

    /// Horizontal sum of 8 × f32 in a __m256.
    #[target_feature(enable = "avx2")]
    unsafe fn hsum256(v: std::arch::x86_64::__m256) -> f32 {
        use std::arch::x86_64::*;
        let hi = _mm256_extractf128_ps(v, 1);
        let lo = _mm256_castps256_ps128(v);
        let sum128 = _mm_add_ps(lo, hi);
        let shuf = _mm_movehdup_ps(sum128);
        let sums = _mm_add_ps(sum128, shuf);
        let shuf2 = _mm_movehl_ps(sums, sums);
        let sums2 = _mm_add_ss(sums, shuf2);
        _mm_cvtss_f32(sums2)
    }
}

// ── AVX-512 kernels ──

#[cfg(target_arch = "x86_64")]
mod avx512 {
    pub fn l2_squared(a: &[f32], b: &[f32]) -> f32 {
        unsafe { l2_impl(a, b) }
    }

    #[target_feature(enable = "avx512f")]
    unsafe fn l2_impl(a: &[f32], b: &[f32]) -> f32 {
        unsafe {
            use std::arch::x86_64::*;
            let n = a.len();
            let mut sum = _mm512_setzero_ps();
            let chunks = n / 16;
            for i in 0..chunks {
                let off = i * 16;
                let va = _mm512_loadu_ps(a.as_ptr().add(off));
                let vb = _mm512_loadu_ps(b.as_ptr().add(off));
                let diff = _mm512_sub_ps(va, vb);
                sum = _mm512_fmadd_ps(diff, diff, sum);
            }
            let mut result = _mm512_reduce_add_ps(sum);
            for i in (chunks * 16)..n {
                let d = a[i] - b[i];
                result += d * d;
            }
            result
        }
    }

    pub fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
        unsafe { cosine_impl(a, b) }
    }

    #[target_feature(enable = "avx512f")]
    unsafe fn cosine_impl(a: &[f32], b: &[f32]) -> f32 {
        unsafe {
            use std::arch::x86_64::*;
            let n = a.len();
            let mut vdot = _mm512_setzero_ps();
            let mut vna = _mm512_setzero_ps();
            let mut vnb = _mm512_setzero_ps();
            let chunks = n / 16;
            for i in 0..chunks {
                let off = i * 16;
                let va = _mm512_loadu_ps(a.as_ptr().add(off));
                let vb = _mm512_loadu_ps(b.as_ptr().add(off));
                vdot = _mm512_fmadd_ps(va, vb, vdot);
                vna = _mm512_fmadd_ps(va, va, vna);
                vnb = _mm512_fmadd_ps(vb, vb, vnb);
            }
            let mut dot = _mm512_reduce_add_ps(vdot);
            let mut na = _mm512_reduce_add_ps(vna);
            let mut nb = _mm512_reduce_add_ps(vnb);
            for i in (chunks * 16)..n {
                dot += a[i] * b[i];
                na += a[i] * a[i];
                nb += b[i] * b[i];
            }
            let denom = (na * nb).sqrt();
            if denom < f32::EPSILON {
                1.0
            } else {
                (1.0 - dot / denom).max(0.0)
            }
        }
    }

    pub fn neg_inner_product(a: &[f32], b: &[f32]) -> f32 {
        unsafe { ip_impl(a, b) }
    }

    #[target_feature(enable = "avx512f")]
    unsafe fn ip_impl(a: &[f32], b: &[f32]) -> f32 {
        unsafe {
            use std::arch::x86_64::*;
            let n = a.len();
            let mut vdot = _mm512_setzero_ps();
            let chunks = n / 16;
            for i in 0..chunks {
                let off = i * 16;
                let va = _mm512_loadu_ps(a.as_ptr().add(off));
                let vb = _mm512_loadu_ps(b.as_ptr().add(off));
                vdot = _mm512_fmadd_ps(va, vb, vdot);
            }
            let mut dot = _mm512_reduce_add_ps(vdot);
            for i in (chunks * 16)..n {
                dot += a[i] * b[i];
            }
            -dot
        }
    }
}

// ── NEON kernels (ARM64) ──

#[cfg(target_arch = "aarch64")]
mod neon {
    pub fn l2_squared(a: &[f32], b: &[f32]) -> f32 {
        unsafe { l2_impl(a, b) }
    }

    unsafe fn l2_impl(a: &[f32], b: &[f32]) -> f32 {
        use std::arch::aarch64::*;
        let n = a.len();
        let mut sum = vdupq_n_f32(0.0);
        let chunks = n / 4;
        for i in 0..chunks {
            let off = i * 4;
            let va = vld1q_f32(a.as_ptr().add(off));
            let vb = vld1q_f32(b.as_ptr().add(off));
            let diff = vsubq_f32(va, vb);
            sum = vfmaq_f32(sum, diff, diff);
        }
        let mut result = vaddvq_f32(sum);
        for i in (chunks * 4)..n {
            let d = a[i] - b[i];
            result += d * d;
        }
        result
    }

    pub fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
        unsafe { cosine_impl(a, b) }
    }

    unsafe fn cosine_impl(a: &[f32], b: &[f32]) -> f32 {
        use std::arch::aarch64::*;
        let n = a.len();
        let mut vdot = vdupq_n_f32(0.0);
        let mut vna = vdupq_n_f32(0.0);
        let mut vnb = vdupq_n_f32(0.0);
        let chunks = n / 4;
        for i in 0..chunks {
            let off = i * 4;
            let va = vld1q_f32(a.as_ptr().add(off));
            let vb = vld1q_f32(b.as_ptr().add(off));
            vdot = vfmaq_f32(vdot, va, vb);
            vna = vfmaq_f32(vna, va, va);
            vnb = vfmaq_f32(vnb, vb, vb);
        }
        let mut dot = vaddvq_f32(vdot);
        let mut na = vaddvq_f32(vna);
        let mut nb = vaddvq_f32(vnb);
        for i in (chunks * 4)..n {
            dot += a[i] * b[i];
            na += a[i] * a[i];
            nb += b[i] * b[i];
        }
        let denom = (na * nb).sqrt();
        if denom < f32::EPSILON {
            1.0
        } else {
            (1.0 - dot / denom).max(0.0)
        }
    }

    pub fn neg_inner_product(a: &[f32], b: &[f32]) -> f32 {
        unsafe { ip_impl(a, b) }
    }

    unsafe fn ip_impl(a: &[f32], b: &[f32]) -> f32 {
        use std::arch::aarch64::*;
        let n = a.len();
        let mut vdot = vdupq_n_f32(0.0);
        let chunks = n / 4;
        for i in 0..chunks {
            let off = i * 4;
            let va = vld1q_f32(a.as_ptr().add(off));
            let vb = vld1q_f32(b.as_ptr().add(off));
            vdot = vfmaq_f32(vdot, va, vb);
        }
        let mut dot = vaddvq_f32(vdot);
        for i in (chunks * 4)..n {
            dot += a[i] * b[i];
        }
        -dot
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_detects_features() {
        let rt = SimdRuntime::detect();
        // Should detect at least scalar on any platform.
        assert!(!rt.name.is_empty());
        tracing::info!("SIMD runtime: {}", rt.name);
    }

    #[test]
    fn l2_matches_scalar() {
        let rt = runtime();
        let a: Vec<f32> = (0..768).map(|i| (i as f32) * 0.01).collect();
        let b: Vec<f32> = (0..768).map(|i| (i as f32) * 0.01 + 0.001).collect();

        let simd_result = (rt.l2_squared)(&a, &b);
        let scalar_result = scalar_l2(&a, &b);
        assert!(
            (simd_result - scalar_result).abs() < 0.01,
            "simd={simd_result}, scalar={scalar_result}"
        );
    }

    #[test]
    fn cosine_matches_scalar() {
        let rt = runtime();
        let a: Vec<f32> = (0..768).map(|i| (i as f32).sin()).collect();
        let b: Vec<f32> = (0..768).map(|i| (i as f32).cos()).collect();

        let simd_result = (rt.cosine_distance)(&a, &b);
        let scalar_result = scalar_cosine(&a, &b);
        assert!(
            (simd_result - scalar_result).abs() < 0.001,
            "simd={simd_result}, scalar={scalar_result}"
        );
    }

    #[test]
    fn ip_matches_scalar() {
        let rt = runtime();
        let a: Vec<f32> = (0..128).map(|i| (i as f32) * 0.1).collect();
        let b: Vec<f32> = (0..128).map(|i| (i as f32) * 0.2).collect();

        let simd_result = (rt.neg_inner_product)(&a, &b);
        let scalar_result = scalar_ip(&a, &b);
        assert!(
            (simd_result - scalar_result).abs() < 0.1,
            "simd={simd_result}, scalar={scalar_result}"
        );
    }

    #[test]
    fn hamming_matches() {
        let a = vec![0b10101010u8; 16];
        let b = vec![0b01010101u8; 16];
        assert_eq!(fast_hamming(&a, &b), 128); // all 128 bits differ
    }

    #[test]
    fn small_vectors() {
        let rt = runtime();
        // Vectors smaller than SIMD width — tests remainder handling.
        let a = [1.0f32, 2.0, 3.0];
        let b = [4.0f32, 5.0, 6.0];
        let l2 = (rt.l2_squared)(&a, &b);
        assert!((l2 - 27.0).abs() < 0.01); // (3² + 3² + 3²) = 27
    }
}
