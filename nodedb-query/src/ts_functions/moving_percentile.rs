//! Rolling percentile via sliding-window exact computation.
//!
//! For each position, collects all values in the window and computes the
//! exact percentile via sorting + linear interpolation.

use super::percentile::ts_percentile_exact;

/// Compute a rolling percentile over a sliding window.
///
/// `quantile` is clamped to `[0.0, 1.0]`.
/// Returns `None` for positions where fewer than `window` samples are available.
/// NaN values within each window are excluded from computation.
pub fn ts_moving_percentile(values: &[f64], window: usize, quantile: f64) -> Vec<Option<f64>> {
    let n = values.len();
    if n == 0 || window == 0 {
        return vec![None; n];
    }

    let mut result = Vec::with_capacity(n);

    for i in 0..n {
        if i + 1 < window {
            result.push(None);
        } else {
            let start = i + 1 - window;
            let window_vals = &values[start..=i];
            result.push(ts_percentile_exact(window_vals, quantile));
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn median_window_3() {
        let vals = [1.0, 3.0, 5.0, 7.0, 9.0];
        let r = ts_moving_percentile(&vals, 3, 0.5);
        assert!(r[0].is_none());
        assert!(r[1].is_none());
        assert!((r[2].unwrap() - 3.0).abs() < 1e-12); // median of [1,3,5]
        assert!((r[3].unwrap() - 5.0).abs() < 1e-12); // median of [3,5,7]
        assert!((r[4].unwrap() - 7.0).abs() < 1e-12); // median of [5,7,9]
    }

    #[test]
    fn p99_window_5() {
        let vals = [1.0, 2.0, 3.0, 4.0, 100.0];
        let r = ts_moving_percentile(&vals, 5, 0.99);
        // At position 4: p99 of [1,2,3,4,100] should be close to 100.
        assert!(r[4].unwrap() > 90.0);
    }

    #[test]
    fn window_1() {
        let vals = [10.0, 20.0, 30.0];
        let r = ts_moving_percentile(&vals, 1, 0.5);
        // Window size 1: each value is its own median.
        assert!((r[0].unwrap() - 10.0).abs() < 1e-12);
        assert!((r[1].unwrap() - 20.0).abs() < 1e-12);
    }

    #[test]
    fn nan_excluded() {
        let vals = [1.0, f64::NAN, 3.0, 4.0, 5.0];
        let r = ts_moving_percentile(&vals, 3, 0.5);
        // Window [NaN, 3, 4] → NaN excluded → median of [3, 4] = 3.5
        assert!(r[3].is_some());
        assert!((r[3].unwrap() - 3.5).abs() < 1e-12);
    }

    #[test]
    fn empty_and_zero_window() {
        assert!(ts_moving_percentile(&[], 5, 0.5).is_empty());
        assert!(
            ts_moving_percentile(&[1.0], 0, 0.5)
                .iter()
                .all(|v| v.is_none())
        );
    }

    #[test]
    fn window_exceeds_length() {
        let r = ts_moving_percentile(&[1.0, 2.0], 5, 0.5);
        assert!(r.iter().all(|v| v.is_none()));
    }
}
