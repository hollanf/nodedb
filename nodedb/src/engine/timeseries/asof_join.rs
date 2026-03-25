//! ASOF JOIN: temporal join for timeseries data.
//!
//! For each row in the left table, finds the closest preceding (or equal)
//! row in the right table by timestamp. Used for correlating metrics
//! from different sources at the same time points.
//!
//! Algorithm: sorted merge with binary search. Both sides must be sorted
//! by timestamp. For each left row, binary search the right side for
//! the largest timestamp <= left timestamp (within tolerance).

/// A row with a timestamp and associated data.
#[derive(Debug, Clone)]
pub struct TimestampedRow<T: Clone> {
    pub timestamp_ms: i64,
    pub data: T,
}

/// ASOF join result: left row matched with closest right row (if any).
#[derive(Debug, Clone)]
pub struct AsofMatch<L: Clone, R: Clone> {
    pub left: TimestampedRow<L>,
    pub right: Option<TimestampedRow<R>>,
}

/// Perform an ASOF join between two timestamp-sorted sequences.
///
/// For each row in `left`, finds the row in `right` with the largest
/// timestamp that is <= the left row's timestamp AND within `tolerance_ms`
/// (0 = exact match only, i64::MAX = unlimited tolerance).
///
/// Both `left` and `right` MUST be sorted by `timestamp_ms` ascending.
pub fn asof_join<L: Clone, R: Clone>(
    left: &[TimestampedRow<L>],
    right: &[TimestampedRow<R>],
    tolerance_ms: i64,
) -> Vec<AsofMatch<L, R>> {
    let mut results = Vec::with_capacity(left.len());
    let mut right_cursor = 0usize;

    for left_row in left {
        // Advance right cursor to the largest timestamp <= left timestamp.
        while right_cursor < right.len()
            && right[right_cursor].timestamp_ms <= left_row.timestamp_ms
        {
            right_cursor += 1;
        }
        // right_cursor now points past the last valid match.
        // The match candidate is at right_cursor - 1 (if it exists).
        let matched = if right_cursor > 0 {
            let candidate = &right[right_cursor - 1];
            let gap = left_row.timestamp_ms - candidate.timestamp_ms;
            if gap >= 0 && (tolerance_ms == i64::MAX || gap <= tolerance_ms) {
                Some(candidate.clone())
            } else {
                None
            }
        } else {
            None
        };

        results.push(AsofMatch {
            left: left_row.clone(),
            right: matched,
        });
    }

    results
}

/// ASOF join with multiple key columns.
///
/// Groups both sides by key, then performs per-group ASOF join.
/// `key_fn` extracts the grouping key from each row's data.
pub fn asof_join_keyed<L, R, K>(
    left: &[TimestampedRow<L>],
    right: &[TimestampedRow<R>],
    tolerance_ms: i64,
    left_key: impl Fn(&L) -> K,
    right_key: impl Fn(&R) -> K,
) -> Vec<AsofMatch<L, R>>
where
    L: Clone,
    R: Clone,
    K: Eq + std::hash::Hash + Clone,
{
    use std::collections::HashMap;

    // Group right side by key.
    let mut right_groups: HashMap<K, Vec<&TimestampedRow<R>>> = HashMap::new();
    for row in right {
        right_groups
            .entry(right_key(&row.data))
            .or_default()
            .push(row);
    }

    let mut results = Vec::with_capacity(left.len());

    for left_row in left {
        let key = left_key(&left_row.data);
        let matched = if let Some(group) = right_groups.get(&key) {
            // Binary search for largest timestamp <= left timestamp.
            let target = left_row.timestamp_ms;
            let pos = group.partition_point(|r| r.timestamp_ms <= target);
            if pos > 0 {
                let candidate = group[pos - 1];
                let gap = target - candidate.timestamp_ms;
                if tolerance_ms == i64::MAX || gap <= tolerance_ms {
                    Some(candidate.clone())
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        results.push(AsofMatch {
            left: left_row.clone(),
            right: matched,
        });
    }

    results
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(ts: i64, data: &str) -> TimestampedRow<String> {
        TimestampedRow {
            timestamp_ms: ts,
            data: data.to_string(),
        }
    }

    #[test]
    fn basic_asof_join() {
        let left = vec![row(100, "a"), row(200, "b"), row(300, "c")];
        let right = vec![row(90, "x"), row(150, "y"), row(250, "z")];

        let results = asof_join(&left, &right, i64::MAX);
        assert_eq!(results.len(), 3);

        // left 100 → right 90 (closest preceding)
        assert_eq!(results[0].right.as_ref().unwrap().data, "x");
        // left 200 → right 150
        assert_eq!(results[1].right.as_ref().unwrap().data, "y");
        // left 300 → right 250
        assert_eq!(results[2].right.as_ref().unwrap().data, "z");
    }

    #[test]
    fn exact_match() {
        let left = vec![row(100, "a"), row(200, "b")];
        let right = vec![row(100, "x"), row(200, "y")];

        let results = asof_join(&left, &right, 0);
        assert_eq!(results[0].right.as_ref().unwrap().data, "x");
        assert_eq!(results[1].right.as_ref().unwrap().data, "y");
    }

    #[test]
    fn tolerance_window() {
        let left = vec![row(100, "a"), row(200, "b")];
        let right = vec![row(50, "x"), row(180, "y")];

        // Tolerance = 10ms.
        let results = asof_join(&left, &right, 10);
        // left 100 - right 50 = gap 50 > 10 → no match
        assert!(results[0].right.is_none());
        // left 200 - right 180 = gap 20 > 10 → no match
        assert!(results[1].right.is_none());

        // Tolerance = 50ms.
        let results = asof_join(&left, &right, 50);
        assert_eq!(results[0].right.as_ref().unwrap().data, "x"); // gap 50 == 50
        assert_eq!(results[1].right.as_ref().unwrap().data, "y"); // gap 20 <= 50
    }

    #[test]
    fn no_right_rows() {
        let left = vec![row(100, "a")];
        let right: Vec<TimestampedRow<String>> = vec![];
        let results = asof_join(&left, &right, i64::MAX);
        assert!(results[0].right.is_none());
    }

    #[test]
    fn right_all_after_left() {
        let left = vec![row(100, "a")];
        let right = vec![row(200, "x"), row(300, "y")];
        let results = asof_join(&left, &right, i64::MAX);
        // No right row at or before 100.
        assert!(results[0].right.is_none());
    }

    #[test]
    fn keyed_asof_join() {
        let left = vec![
            TimestampedRow {
                timestamp_ms: 100,
                data: ("host-a", 1.0),
            },
            TimestampedRow {
                timestamp_ms: 200,
                data: ("host-b", 2.0),
            },
            TimestampedRow {
                timestamp_ms: 300,
                data: ("host-a", 3.0),
            },
        ];
        let right = vec![
            TimestampedRow {
                timestamp_ms: 90,
                data: ("host-a", 10.0),
            },
            TimestampedRow {
                timestamp_ms: 150,
                data: ("host-b", 20.0),
            },
            TimestampedRow {
                timestamp_ms: 250,
                data: ("host-a", 30.0),
            },
        ];

        let results = asof_join_keyed(&left, &right, i64::MAX, |d| d.0, |d| d.0);
        assert_eq!(results.len(), 3);

        // host-a at 100 → right host-a at 90
        assert_eq!(results[0].right.as_ref().unwrap().data.1, 10.0);
        // host-b at 200 → right host-b at 150
        assert_eq!(results[1].right.as_ref().unwrap().data.1, 20.0);
        // host-a at 300 → right host-a at 250
        assert_eq!(results[2].right.as_ref().unwrap().data.1, 30.0);
    }
}
