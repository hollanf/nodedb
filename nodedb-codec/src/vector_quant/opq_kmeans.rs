//! Lloyd's k-means for OPQ codebook training.

use super::opq_rotation::Xorshift64;

/// L2 squared distance between two equal-length slices.
#[inline]
pub fn l2_sq(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(x, y)| (x - y).powi(2)).sum()
}

/// Lloyd's k-means clustering.
///
/// Returns `k` centroids of length `sub_dim`, initialized via k-means++.
pub fn lloyd(
    points: &[Vec<f32>],
    sub_dim: usize,
    k: usize,
    iters: usize,
    seed: u64,
) -> Vec<Vec<f32>> {
    let n = points.len();
    if n == 0 || k == 0 {
        return Vec::new();
    }
    let k = k.min(n);

    let mut rng = Xorshift64::new(seed.wrapping_add(0x9E3779B97F4A7C15));
    let mut centroids: Vec<Vec<f32>> = Vec::with_capacity(k);
    centroids.push(points[0].clone());

    let mut min_dists = vec![f32::MAX; n];
    for (i, p) in points.iter().enumerate() {
        min_dists[i] = l2_sq(p, &centroids[0]);
    }

    for _ in 1..k {
        let total: f64 = min_dists.iter().map(|&d| d as f64).sum();
        let chosen = if total < f64::EPSILON {
            0usize
        } else {
            let target = {
                let u = (rng.next_u64() >> 11) as f64 / (1u64 << 53) as f64;
                u * total
            };
            let mut acc = 0.0f64;
            let mut idx = n - 1;
            for (i, &d) in min_dists.iter().enumerate() {
                acc += d as f64;
                if acc >= target {
                    idx = i;
                    break;
                }
            }
            idx
        };
        let new_c = points[chosen].clone();
        for (i, p) in points.iter().enumerate() {
            let d = l2_sq(p, &new_c);
            if d < min_dists[i] {
                min_dists[i] = d;
            }
        }
        centroids.push(new_c);
    }

    let mut assignments = vec![0usize; n];
    for _ in 0..iters {
        let mut changed = false;
        for (i, p) in points.iter().enumerate() {
            let best = (0..k)
                .min_by(|&a, &b| {
                    l2_sq(p, &centroids[a])
                        .partial_cmp(&l2_sq(p, &centroids[b]))
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
                .unwrap_or(0);
            if assignments[i] != best {
                assignments[i] = best;
                changed = true;
            }
        }
        if !changed {
            break;
        }
        let mut sums = vec![vec![0.0f32; sub_dim]; k];
        let mut counts = vec![0usize; k];
        for (i, p) in points.iter().enumerate() {
            let c = assignments[i];
            counts[c] += 1;
            for d in 0..sub_dim {
                sums[c][d] += p[d];
            }
        }
        for c in 0..k {
            if counts[c] > 0 {
                for d in 0..sub_dim {
                    centroids[c][d] = sums[c][d] / counts[c] as f32;
                }
            }
        }
    }
    centroids
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lloyd_separates_two_clusters() {
        // Two tight clusters far apart.
        let mut pts: Vec<Vec<f32>> = Vec::new();
        for i in 0..10 {
            pts.push(vec![i as f32 * 0.01, i as f32 * 0.01]);
        }
        for i in 0..10 {
            pts.push(vec![10.0 + i as f32 * 0.01, 10.0 + i as f32 * 0.01]);
        }
        let centroids = lloyd(&pts, 2, 2, 20, 99);
        assert_eq!(centroids.len(), 2);
        // The two centroids should be separated by more than 5.0.
        let d = l2_sq(&centroids[0], &centroids[1]).sqrt();
        assert!(d > 5.0, "centroids too close: {d}");
    }
}
