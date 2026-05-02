//! R-tree range search and nearest-neighbor queries.

use nodedb_types::BoundingBox;
use std::collections::BinaryHeap;

use super::node::{EntryId, Node, NodeKind, RTreeEntry};

/// Result from nearest-neighbor search.
#[derive(Debug, Clone)]
pub struct NnResult {
    pub entry_id: EntryId,
    pub bbox: BoundingBox,
    /// Minimum distance in degrees (approximate).
    pub distance: f64,
}

/// Recursive range search.
pub(crate) fn search_node<'a>(
    nodes: &'a [Node],
    node_idx: usize,
    query: &BoundingBox,
    results: &mut Vec<&'a RTreeEntry>,
) {
    let node = &nodes[node_idx];
    if !node.bbox.intersects(query) {
        return;
    }
    match &node.kind {
        NodeKind::Leaf { entries } => {
            for entry in entries {
                if entry.bbox.intersects(query) {
                    results.push(entry);
                }
            }
        }
        NodeKind::Internal { children } => {
            for child in children {
                if child.bbox.intersects(query) {
                    search_node(nodes, child.node_idx, query, results);
                }
            }
        }
    }
}

/// Nearest-neighbor search via priority queue (min-heap).
pub(crate) fn nearest(
    nodes: &[Node],
    root: usize,
    query_lng: f64,
    query_lat: f64,
    k: usize,
    is_empty: bool,
) -> Vec<NnResult> {
    if k == 0 || is_empty {
        return Vec::new();
    }

    let mut heap: BinaryHeap<HeapItem> = BinaryHeap::new();
    // no-governor: hot-path kNN search results; bounded by k (query limit, small)
    let mut results: Vec<NnResult> = Vec::with_capacity(k);

    heap.push(HeapItem {
        dist: min_dist_point_bbox(query_lng, query_lat, &nodes[root].bbox),
        node_idx: root,
    });

    while let Some(item) = heap.pop() {
        if results.len() >= k && item.dist > results[k - 1].distance {
            continue;
        }
        let node = &nodes[item.node_idx];
        match &node.kind {
            NodeKind::Internal { children } => {
                for child in children {
                    let d = min_dist_point_bbox(query_lng, query_lat, &child.bbox);
                    if results.len() < k || d <= results[results.len() - 1].distance {
                        heap.push(HeapItem {
                            dist: d,
                            node_idx: child.node_idx,
                        });
                    }
                }
            }
            NodeKind::Leaf { entries } => {
                for entry in entries {
                    let d = min_dist_point_bbox(query_lng, query_lat, &entry.bbox);
                    if results.len() < k || d < results[results.len() - 1].distance {
                        let nn = NnResult {
                            entry_id: entry.id,
                            bbox: entry.bbox,
                            distance: d,
                        };
                        insert_sorted(&mut results, nn, k);
                    }
                }
            }
        }
    }

    results
}

/// Min-heap item for NN traversal.
#[derive(Debug)]
struct HeapItem {
    dist: f64,
    node_idx: usize,
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.dist == other.dist
    }
}
impl Eq for HeapItem {}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse for min-heap (BinaryHeap is max-heap).
        other
            .dist
            .partial_cmp(&self.dist)
            .unwrap_or(std::cmp::Ordering::Equal)
    }
}

/// Minimum distance from point to bbox (in degrees, approximate).
fn min_dist_point_bbox(lng: f64, lat: f64, bbox: &BoundingBox) -> f64 {
    let dlat = if lat < bbox.min_lat {
        bbox.min_lat - lat
    } else if lat > bbox.max_lat {
        lat - bbox.max_lat
    } else {
        0.0
    };

    let dlng = if bbox.crosses_antimeridian() {
        if lng >= bbox.min_lng || lng <= bbox.max_lng {
            0.0
        } else {
            (bbox.min_lng - lng).min(lng - bbox.max_lng).max(0.0)
        }
    } else if lng < bbox.min_lng {
        bbox.min_lng - lng
    } else if lng > bbox.max_lng {
        lng - bbox.max_lng
    } else {
        0.0
    };

    (dlat * dlat + dlng * dlng).sqrt()
}

fn insert_sorted(results: &mut Vec<NnResult>, item: NnResult, k: usize) {
    let pos = results
        .binary_search_by(|r| {
            r.distance
                .partial_cmp(&item.distance)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .unwrap_or_else(|pos| pos);
    results.insert(pos, item);
    if results.len() > k {
        results.truncate(k);
    }
}
