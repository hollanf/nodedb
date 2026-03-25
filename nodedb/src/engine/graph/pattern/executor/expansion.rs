//! Variable-length path expansion and neighbor collection.

use crate::engine::graph::csr::CsrIndex;
use crate::engine::graph::edge_store::Direction;

/// Variable-length path expansion via iterative BFS.
///
/// Returns `(dst_node_id, path_description)` for all nodes reachable
/// in min_hops..=max_hops from source.
pub(super) fn expand_variable_length(
    csr: &CsrIndex,
    source: u32,
    label_filter: Option<&str>,
    direction: Direction,
    min_hops: usize,
    max_hops: usize,
) -> Vec<(u32, String)> {
    let src_name = csr.node_name(source).to_string();
    let mut results = Vec::new();

    let mut frontier: Vec<(u32, String, usize)> = vec![(source, src_name.clone(), 0)];
    let mut _visited = std::collections::HashSet::new();
    _visited.insert(source);

    for depth in 1..=max_hops {
        let mut next_frontier = Vec::new();

        for &(node, ref path, _) in &frontier {
            let neighbors = collect_neighbors(csr, node, label_filter, direction);
            for (_, dst) in neighbors {
                let dst_name = csr.node_name(dst).to_string();
                if path.contains(&dst_name) {
                    continue; // Cycle in this path.
                }

                let new_path = format!("{path}->{dst_name}");

                if depth >= min_hops {
                    results.push((dst, new_path.clone()));
                }

                if depth < max_hops {
                    next_frontier.push((dst, new_path, depth));
                }
            }
        }

        frontier = next_frontier;
        if frontier.is_empty() {
            break;
        }
    }

    results
}

/// Collect neighbor (label_id, node_id) pairs from CSR.
pub(super) fn collect_neighbors(
    csr: &CsrIndex,
    node: u32,
    label_filter: Option<&str>,
    direction: Direction,
) -> Vec<(u16, u32)> {
    let mut neighbors = Vec::new();
    match direction {
        Direction::Out => {
            for (lid, dst) in csr.iter_out_edges(node) {
                if label_filter.is_none() || csr_label_matches(csr, lid, label_filter) {
                    neighbors.push((lid, dst));
                }
            }
        }
        Direction::In => {
            for (lid, src) in csr.iter_in_edges(node) {
                if label_filter.is_none() || csr_label_matches(csr, lid, label_filter) {
                    neighbors.push((lid, src));
                }
            }
        }
        Direction::Both => {
            for (lid, dst) in csr.iter_out_edges(node) {
                if label_filter.is_none() || csr_label_matches(csr, lid, label_filter) {
                    neighbors.push((lid, dst));
                }
            }
            for (lid, src) in csr.iter_in_edges(node) {
                if label_filter.is_none() || csr_label_matches(csr, lid, label_filter) {
                    neighbors.push((lid, src));
                }
            }
        }
    }
    neighbors
}

fn csr_label_matches(csr: &CsrIndex, label_id: u16, filter: Option<&str>) -> bool {
    match filter {
        None => true,
        Some(f) => csr.label_name(label_id) == f,
    }
}
