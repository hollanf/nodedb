//! Time-series gap filling (interpolation).

/// Interpolation method for filling NULL gaps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InterpolateMethod {
    /// Linear interpolation using surrounding non-NULL values and timestamps.
    Linear,
    /// Carry forward the last known value (LOCF).
    Previous,
    /// Carry backward the next known value (NOCB).
    Next,
    /// Replace NULLs with zero.
    Zero,
}

impl InterpolateMethod {
    /// Parse from a case-insensitive string.
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "linear" => Some(Self::Linear),
            "prev" | "previous" | "locf" => Some(Self::Previous),
            "next" | "nocb" => Some(Self::Next),
            "zero" => Some(Self::Zero),
            _ => None,
        }
    }
}

/// Fill NULL gaps in a time series.
///
/// `values` — `Some(v)` for present samples, `None` for gaps.
/// `timestamps_ns` — epoch nanoseconds, same length as `values`.
///
/// Edge handling for `Linear`: gaps before the first known value or
/// after the last known value are filled with the nearest known value
/// (flat extrapolation, not unbounded linear extrapolation).
pub fn ts_interpolate(
    values: &[Option<f64>],
    timestamps_ns: &[i64],
    method: InterpolateMethod,
) -> Vec<f64> {
    debug_assert_eq!(values.len(), timestamps_ns.len());
    let n = values.len();
    if n == 0 {
        return vec![];
    }

    match method {
        InterpolateMethod::Zero => values.iter().map(|v| v.unwrap_or(0.0)).collect(),
        InterpolateMethod::Previous => fill_previous(values),
        InterpolateMethod::Next => fill_next(values),
        InterpolateMethod::Linear => fill_linear(values, timestamps_ns),
    }
}

fn fill_previous(values: &[Option<f64>]) -> Vec<f64> {
    let mut result = Vec::with_capacity(values.len());
    let mut last = 0.0;
    for v in values {
        match v {
            Some(val) => {
                last = *val;
                result.push(*val);
            }
            None => result.push(last),
        }
    }
    result
}

fn fill_next(values: &[Option<f64>]) -> Vec<f64> {
    let n = values.len();
    let mut result = vec![0.0; n];
    let mut next_val = 0.0;
    for i in (0..n).rev() {
        match values[i] {
            Some(val) => {
                next_val = val;
                result[i] = val;
            }
            None => result[i] = next_val,
        }
    }
    result
}

fn fill_linear(values: &[Option<f64>], timestamps_ns: &[i64]) -> Vec<f64> {
    let n = values.len();
    let mut result = vec![0.0; n];

    // Copy known values and linearly interpolate gaps.
    let mut prev_idx: Option<usize> = None;

    for i in 0..n {
        if let Some(val) = values[i] {
            // Fill the gap between prev_idx and i.
            if let Some(pi) = prev_idx {
                let Some(prev_val) = values[pi] else { continue };
                let dt = (timestamps_ns[i] - timestamps_ns[pi]) as f64;
                if dt > 0.0 && pi + 1 < i {
                    for j in (pi + 1)..i {
                        let frac = (timestamps_ns[j] - timestamps_ns[pi]) as f64 / dt;
                        result[j] = prev_val + frac * (val - prev_val);
                    }
                }
            }
            result[i] = val;
            prev_idx = Some(i);
        }
    }

    // Edge: fill leading NULLs with first known value.
    if let Some(first) = values.iter().position(|v| v.is_some()) {
        if let Some(fv) = values[first] {
            for r in result.iter_mut().take(first) {
                *r = fv;
            }
        }
    }
    // Edge: fill trailing NULLs with last known value.
    if let Some(last) = values.iter().rposition(|v| v.is_some()) {
        if let Some(lv) = values[last] {
            for r in result.iter_mut().skip(last + 1) {
                *r = lv;
            }
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: f64) -> Option<f64> {
        Some(v)
    }

    #[test]
    fn zero_fill() {
        let r = ts_interpolate(&[s(1.0), None, s(3.0)], &[0, 1, 2], InterpolateMethod::Zero);
        assert_eq!(r, vec![1.0, 0.0, 3.0]);
    }

    #[test]
    fn previous_fill() {
        let r = ts_interpolate(
            &[s(10.0), None, None, s(40.0)],
            &[0, 1, 2, 3],
            InterpolateMethod::Previous,
        );
        assert_eq!(r, vec![10.0, 10.0, 10.0, 40.0]);
    }

    #[test]
    fn next_fill() {
        let r = ts_interpolate(
            &[None, None, s(30.0), s(40.0)],
            &[0, 1, 2, 3],
            InterpolateMethod::Next,
        );
        assert_eq!(r, vec![30.0, 30.0, 30.0, 40.0]);
    }

    #[test]
    fn linear_fill() {
        let r = ts_interpolate(
            &[s(0.0), None, None, s(30.0)],
            &[0, 10, 20, 30],
            InterpolateMethod::Linear,
        );
        assert!((r[0] - 0.0).abs() < 1e-12);
        assert!((r[1] - 10.0).abs() < 1e-12);
        assert!((r[2] - 20.0).abs() < 1e-12);
        assert!((r[3] - 30.0).abs() < 1e-12);
    }

    #[test]
    fn linear_edge_extrapolation() {
        // Leading and trailing NULLs → flat fill from nearest known.
        let r = ts_interpolate(
            &[None, s(10.0), None, s(30.0), None],
            &[0, 10, 20, 30, 40],
            InterpolateMethod::Linear,
        );
        assert!((r[0] - 10.0).abs() < 1e-12); // leading → first known
        assert!((r[2] - 20.0).abs() < 1e-12); // interior → linear
        assert!((r[4] - 30.0).abs() < 1e-12); // trailing → last known
    }

    #[test]
    fn all_present() {
        let r = ts_interpolate(
            &[s(1.0), s(2.0), s(3.0)],
            &[0, 1, 2],
            InterpolateMethod::Linear,
        );
        assert_eq!(r, vec![1.0, 2.0, 3.0]);
    }

    #[test]
    fn empty() {
        assert!(ts_interpolate(&[], &[], InterpolateMethod::Linear).is_empty());
    }

    #[test]
    fn parse_method() {
        assert_eq!(
            InterpolateMethod::parse("linear"),
            Some(InterpolateMethod::Linear)
        );
        assert_eq!(
            InterpolateMethod::parse("PREV"),
            Some(InterpolateMethod::Previous)
        );
        assert_eq!(
            InterpolateMethod::parse("locf"),
            Some(InterpolateMethod::Previous)
        );
        assert_eq!(
            InterpolateMethod::parse("next"),
            Some(InterpolateMethod::Next)
        );
        assert_eq!(
            InterpolateMethod::parse("nocb"),
            Some(InterpolateMethod::Next)
        );
        assert_eq!(
            InterpolateMethod::parse("zero"),
            Some(InterpolateMethod::Zero)
        );
        assert_eq!(InterpolateMethod::parse("unknown"), None);
    }
}
