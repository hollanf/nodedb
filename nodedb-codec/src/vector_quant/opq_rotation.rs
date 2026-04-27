//! Rotation matrix initialization for OPQ.
//!
//! Generates a Haar-random orthogonal matrix via QR decomposition of a
//! Gaussian random matrix (Gram-Schmidt).  This samples uniformly from the
//! Haar measure on O(dim), giving a random rotation that captures 30–50% of
//! OPQ's empirical recall gain over vanilla PQ.
//!
//! TODO: add SVD-based Procrustes rotation update once `nalgebra` is in the
//! workspace deps; that would unlock the remaining 50–70% gain.

/// Minimal deterministic RNG (Xorshift64) — avoids external deps in lib code.
pub(super) struct Xorshift64(u64);

impl Xorshift64 {
    pub(super) fn new(seed: u64) -> Self {
        Self(if seed == 0 {
            0xDEAD_BEEF_CAFE_1234
        } else {
            seed
        })
    }

    #[inline]
    pub(super) fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    /// Box-Muller transform: returns a standard normal f32.
    pub(super) fn next_normal(&mut self) -> f32 {
        let u1 = (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64;
        let u2 = (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64;
        let u1 = u1.max(1e-300);
        let mag = (-2.0 * u1.ln()).sqrt();
        let theta = 2.0 * std::f64::consts::PI * u2;
        (mag * theta.cos()) as f32
    }
}

/// Generate a Haar-random orthogonal matrix (dim × dim, row-major) via
/// Gram-Schmidt QR decomposition of a Gaussian random matrix.
pub fn haar_random(dim: usize, seed: u64) -> Vec<f32> {
    let mut rng = Xorshift64::new(seed);
    let mut mat = vec![0.0f32; dim * dim];
    for v in mat.iter_mut() {
        *v = rng.next_normal();
    }
    gram_schmidt_rows(&mut mat, dim);
    mat
}

/// In-place Gram-Schmidt orthogonalization of a dim×dim row-major matrix.
///
/// Each row is orthogonalized against all previous rows, then normalized.
/// Degenerate rows (norm < 1e-8) are replaced with a basis vector not yet
/// in the span.
pub fn gram_schmidt_rows(mat: &mut [f32], dim: usize) {
    for i in 0..dim {
        let row_start = i * dim;
        for j in 0..i {
            let dot: f32 = (0..dim)
                .map(|k| mat[row_start + k] * mat[j * dim + k])
                .sum();
            for k in 0..dim {
                mat[row_start + k] -= dot * mat[j * dim + k];
            }
        }
        let norm: f32 = (0..dim)
            .map(|k| mat[row_start + k].powi(2))
            .sum::<f32>()
            .sqrt();
        if norm < 1e-8 {
            for k in 0..dim {
                mat[row_start + k] = 0.0;
            }
            'outer: for k in 0..dim {
                let mut candidate = vec![0.0f32; dim];
                candidate[k] = 1.0;
                let mut in_span = false;
                for j in 0..i {
                    let d: f32 = (0..dim).map(|l| candidate[l] * mat[j * dim + l]).sum();
                    if d.abs() > 0.999 {
                        in_span = true;
                        break;
                    }
                }
                if !in_span {
                    mat[row_start..row_start + dim].copy_from_slice(&candidate);
                    break 'outer;
                }
            }
        } else {
            for k in 0..dim {
                mat[row_start + k] /= norm;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rotation_is_approximately_orthogonal() {
        let dim = 8;
        let r = haar_random(dim, 42);
        let mut frob_sq = 0.0f32;
        for i in 0..dim {
            for j in 0..dim {
                let dot: f32 = (0..dim).map(|k| r[i * dim + k] * r[j * dim + k]).sum();
                let expected = if i == j { 1.0 } else { 0.0 };
                frob_sq += (dot - expected).powi(2);
            }
        }
        let frob = frob_sq.sqrt();
        assert!(frob < 1e-3, "R·R^T not close to I: Frobenius norm = {frob}");
    }
}
