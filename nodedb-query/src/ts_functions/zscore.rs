//! Z-Score window function for anomaly detection.
//!
//! Computes `(value - mean) / stddev` over a sliding window.
//! Returns the number of standard deviations each value is from the window mean.

use super::stddev::TsStddevAccum;

/// Compute rolling z-score over a sliding window.
///
/// For each position `i`, computes the z-score using the window
/// `[i - window + 1 .. i]`. Returns `None` for positions where
/// fewer than 2 samples are in the window, or where stddev is zero.
///
/// # Edge Cases
/// - If `stddev == 0` (all values identical in window): returns `0.0`.
/// - If `window < 2`: returns all `None` (need at least 2 for stddev).
/// - NaN values in the input are skipped in the accumulator.
pub fn ts_zscore(values: &[f64], window: usize) -> Vec<Option<f64>> {
    let n = values.len();
    if n == 0 || window < 2 {
        return vec![None; n];
    }

    let mut result = Vec::with_capacity(n);

    for i in 0..n {
        if i + 1 < window {
            result.push(None);
            continue;
        }

        let start = i + 1 - window;
        let window_vals = &values[start..=i];

        let mut accum = TsStddevAccum::new();
        accum.update_batch(window_vals);

        match accum.evaluate_population() {
            Some(stddev) if stddev > 0.0 => {
                // Mean is sum / n for the window.
                let mean: f64 = window_vals.iter().filter(|v| !v.is_nan()).sum::<f64>()
                    / window_vals.iter().filter(|v| !v.is_nan()).count() as f64;
                result.push(Some((values[i] - mean) / stddev));
            }
            Some(_) => {
                // stddev == 0: all values identical.
                result.push(Some(0.0));
            }
            None => {
                result.push(None);
            }
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_zscore() {
        // Values: [2, 4, 4, 4, 5, 5, 7, 9]
        // Population stddev = 2.0, mean = 5.0
        // z(9) = (9 - 5) / 2 = 2.0
        let vals = [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];
        let z = ts_zscore(&vals, 8);
        assert!(z[7].is_some());
        assert!((z[7].unwrap() - 2.0).abs() < 1e-10);
    }

    #[test]
    fn window_smaller_than_input() {
        let vals = [10.0, 10.0, 10.0, 20.0, 30.0];
        let z = ts_zscore(&vals, 3);
        // First 2 positions should be None.
        assert!(z[0].is_none());
        assert!(z[1].is_none());
        // Position 2: window [10, 10, 10] → stddev=0 → z=0
        assert!((z[2].unwrap()).abs() < 1e-12);
        // Position 3: window [10, 10, 20] → mean≈13.33, stddev≈4.71 → z(20)≈1.41
        assert!(z[3].is_some());
        assert!(z[3].unwrap() > 1.0);
    }

    #[test]
    fn constant_values_zero_zscore() {
        let vals = [5.0, 5.0, 5.0, 5.0, 5.0];
        let z = ts_zscore(&vals, 3);
        for v in &z[2..] {
            assert!((v.unwrap()).abs() < 1e-12);
        }
    }

    #[test]
    fn window_too_small() {
        let z = ts_zscore(&[1.0, 2.0, 3.0], 1);
        assert!(z.iter().all(|v| v.is_none()));
    }

    #[test]
    fn empty_input() {
        assert!(ts_zscore(&[], 5).is_empty());
    }

    #[test]
    fn nan_handling() {
        let vals = [1.0, f64::NAN, 3.0, 4.0, 5.0];
        let z = ts_zscore(&vals, 3);
        // Window [NaN, 3, 4] at position 3 — NaN skipped, only 2 values.
        assert!(z[3].is_some());
    }
}
