//! LSN ↔ wall-clock milliseconds interpolator.
//!
//! Bitemporal reads need a `system_from_ms` value for every LSN, but storing
//! wall time next to every write amplifies WAL volume. Instead, the WAL writer
//! emits periodic anchor records (`RecordType::LsnMsAnchor`) and this table
//! interpolates between them linearly.
//!
//! Anchors are strictly monotonic in both LSN and wall-clock time (enforced
//! on insert). Lookup is O(log n) binary search.

use std::cmp::Ordering;

use serde::{Deserialize, Serialize};

use crate::lsn::Lsn;

/// A single anchor point mapping an LSN to a wall-clock millisecond.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct LsnMsAnchor {
    pub lsn: u64,
    pub wall_ms: i64,
}

impl LsnMsAnchor {
    pub const fn new(lsn: u64, wall_ms: i64) -> Self {
        Self { lsn, wall_ms }
    }
}

/// Error produced by the LSN↔ms map.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum LsnMapError {
    /// Attempted to insert an anchor that is not strictly monotonic in LSN or wall time.
    #[error(
        "non-monotonic anchor: prev=(lsn={prev_lsn}, ms={prev_ms}), \
         new=(lsn={new_lsn}, ms={new_ms})"
    )]
    NonMonotonic {
        prev_lsn: u64,
        prev_ms: i64,
        new_lsn: u64,
        new_ms: i64,
    },

    /// The map is empty — cannot interpolate.
    #[error("LSN→ms map is empty")]
    Empty,
}

/// In-memory LSN ↔ wall-ms interpolation table.
///
/// Not `Send + Sync` by itself — callers wrap in whatever concurrency primitive
/// fits their plane (e.g. `Mutex` in Control Plane, per-core cell in Data Plane).
#[derive(Debug, Clone, Default)]
pub struct LsnMsMap {
    anchors: Vec<LsnMsAnchor>,
}

impl LsnMsMap {
    pub const fn new() -> Self {
        Self {
            anchors: Vec::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.anchors.len()
    }

    pub fn is_empty(&self) -> bool {
        self.anchors.is_empty()
    }

    /// Return the anchors (oldest first) — useful for persistence/replay.
    pub fn anchors(&self) -> &[LsnMsAnchor] {
        &self.anchors
    }

    /// Insert an anchor. Must be strictly monotonic relative to the last
    /// anchor on both axes.
    pub fn push(&mut self, anchor: LsnMsAnchor) -> Result<(), LsnMapError> {
        if let Some(&last) = self.anchors.last()
            && (anchor.lsn <= last.lsn || anchor.wall_ms <= last.wall_ms)
        {
            return Err(LsnMapError::NonMonotonic {
                prev_lsn: last.lsn,
                prev_ms: last.wall_ms,
                new_lsn: anchor.lsn,
                new_ms: anchor.wall_ms,
            });
        }
        self.anchors.push(anchor);
        Ok(())
    }

    /// Interpolate the wall-clock millisecond for a given LSN.
    ///
    /// - LSN < first anchor → clamps to first anchor's wall_ms.
    /// - LSN > last anchor → linearly extrapolates using the last two anchors,
    ///   or returns the last anchor's wall_ms if only one anchor exists.
    /// - LSN between two anchors → linear interpolation.
    /// - LSN exactly equals an anchor → that anchor's wall_ms.
    pub fn wall_ms_for_lsn(&self, lsn: Lsn) -> Result<i64, LsnMapError> {
        let target = lsn.as_u64();
        match self.anchors.as_slice() {
            [] => Err(LsnMapError::Empty),
            [only] => Ok(only.wall_ms),
            _ => {
                let search = self.anchors.binary_search_by(|a| a.lsn.cmp(&target));
                match search {
                    Ok(idx) => Ok(self.anchors[idx].wall_ms),
                    Err(idx) => {
                        if idx == 0 {
                            Ok(self.anchors[0].wall_ms)
                        } else if idx >= self.anchors.len() {
                            let n = self.anchors.len();
                            Ok(Self::interpolate(
                                self.anchors[n - 2],
                                self.anchors[n - 1],
                                target,
                            ))
                        } else {
                            Ok(Self::interpolate(
                                self.anchors[idx - 1],
                                self.anchors[idx],
                                target,
                            ))
                        }
                    }
                }
            }
        }
    }

    fn interpolate(lo: LsnMsAnchor, hi: LsnMsAnchor, lsn: u64) -> i64 {
        let lsn_span = hi.lsn.saturating_sub(lo.lsn) as i128;
        if lsn_span == 0 {
            return lo.wall_ms;
        }
        let ms_span = hi.wall_ms as i128 - lo.wall_ms as i128;
        let delta_lsn = (lsn as i128) - (lo.lsn as i128);
        let delta_ms = ms_span * delta_lsn / lsn_span;
        let result = lo.wall_ms as i128 + delta_ms;
        match result.cmp(&(i64::MAX as i128)) {
            Ordering::Greater => i64::MAX,
            _ if result < i64::MIN as i128 => i64::MIN,
            _ => result as i64,
        }
    }
}

/// Convenience wrapper: look up wall-ms for a given LSN using an owning map.
pub fn lsn_to_ms(map: &LsnMsMap, lsn: Lsn) -> Result<i64, LsnMapError> {
    map.wall_ms_for_lsn(lsn)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_map_errors() {
        let m = LsnMsMap::new();
        assert!(matches!(
            m.wall_ms_for_lsn(Lsn::new(5)),
            Err(LsnMapError::Empty)
        ));
    }

    #[test]
    fn single_anchor_returns_its_wall_ms() {
        let mut m = LsnMsMap::new();
        m.push(LsnMsAnchor::new(10, 1_000)).unwrap();
        assert_eq!(m.wall_ms_for_lsn(Lsn::new(5)).unwrap(), 1_000);
        assert_eq!(m.wall_ms_for_lsn(Lsn::new(10)).unwrap(), 1_000);
        assert_eq!(m.wall_ms_for_lsn(Lsn::new(999)).unwrap(), 1_000);
    }

    #[test]
    fn interpolates_between_anchors() {
        let mut m = LsnMsMap::new();
        m.push(LsnMsAnchor::new(0, 1_000)).unwrap();
        m.push(LsnMsAnchor::new(100, 2_000)).unwrap();
        assert_eq!(m.wall_ms_for_lsn(Lsn::new(0)).unwrap(), 1_000);
        assert_eq!(m.wall_ms_for_lsn(Lsn::new(50)).unwrap(), 1_500);
        assert_eq!(m.wall_ms_for_lsn(Lsn::new(100)).unwrap(), 2_000);
    }

    #[test]
    fn clamps_below_first_anchor() {
        let mut m = LsnMsMap::new();
        m.push(LsnMsAnchor::new(100, 5_000)).unwrap();
        m.push(LsnMsAnchor::new(200, 6_000)).unwrap();
        assert_eq!(m.wall_ms_for_lsn(Lsn::new(50)).unwrap(), 5_000);
    }

    #[test]
    fn extrapolates_beyond_last_anchor() {
        let mut m = LsnMsMap::new();
        m.push(LsnMsAnchor::new(0, 0)).unwrap();
        m.push(LsnMsAnchor::new(100, 1_000)).unwrap();
        assert_eq!(m.wall_ms_for_lsn(Lsn::new(150)).unwrap(), 1_500);
    }

    #[test]
    fn non_monotonic_rejected() {
        let mut m = LsnMsMap::new();
        m.push(LsnMsAnchor::new(10, 1_000)).unwrap();
        assert!(matches!(
            m.push(LsnMsAnchor::new(10, 2_000)),
            Err(LsnMapError::NonMonotonic { .. })
        ));
        assert!(matches!(
            m.push(LsnMsAnchor::new(20, 1_000)),
            Err(LsnMapError::NonMonotonic { .. })
        ));
        assert!(matches!(
            m.push(LsnMsAnchor::new(5, 500)),
            Err(LsnMapError::NonMonotonic { .. })
        ));
    }

    #[test]
    fn free_function_matches_method() {
        let mut m = LsnMsMap::new();
        m.push(LsnMsAnchor::new(0, 0)).unwrap();
        m.push(LsnMsAnchor::new(100, 1_000)).unwrap();
        assert_eq!(
            lsn_to_ms(&m, Lsn::new(50)).unwrap(),
            m.wall_ms_for_lsn(Lsn::new(50)).unwrap()
        );
    }

    #[test]
    fn exact_match_binary_search() {
        let mut m = LsnMsMap::new();
        m.push(LsnMsAnchor::new(0, 0)).unwrap();
        m.push(LsnMsAnchor::new(100, 1_000)).unwrap();
        m.push(LsnMsAnchor::new(200, 3_000)).unwrap();
        assert_eq!(m.wall_ms_for_lsn(Lsn::new(100)).unwrap(), 1_000);
        assert_eq!(m.wall_ms_for_lsn(Lsn::new(200)).unwrap(), 3_000);
    }
}
