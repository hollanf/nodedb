use redb::ReadableTable;

use super::store::{
    Direction, EDGES, Edge, EdgeStore, REVERSE_EDGES, edge_key, parse_edge_key, redb_err,
};

/// Default maximum BFS traversal depth. Sourced from `GraphTuning::max_depth`.
pub const DEFAULT_MAX_ALLOWED_DEPTH: usize = 10;
/// Default maximum visited nodes during BFS. Sourced from `GraphTuning::max_visited`.
pub const DEFAULT_MAX_VISITED: usize = 100_000;

impl EdgeStore {
    /// Get all outbound neighbors of a node, optionally filtered by edge label.
    ///
    /// Returns edges sorted by (label, dst_id) due to the composite key ordering.
    pub fn neighbors_out(&self, src: &str, label_filter: Option<&str>) -> crate::Result<Vec<Edge>> {
        let prefix = match label_filter {
            Some(label) => format!("{src}\x00{label}\x00"),
            None => format!("{src}\x00"),
        };

        self.scan_edges_with_prefix(&prefix, |fwd_src, fwd_label, fwd_dst| Edge {
            src_id: fwd_src.to_string(),
            label: fwd_label.to_string(),
            dst_id: fwd_dst.to_string(),
            properties: Vec::new(), // overwritten by scan_edges_with_prefix
        })
    }

    /// Get all inbound neighbors of a node, optionally filtered by edge label.
    pub fn neighbors_in(&self, dst: &str, label_filter: Option<&str>) -> crate::Result<Vec<Edge>> {
        let prefix = match label_filter {
            Some(label) => format!("{dst}\x00{label}\x00"),
            None => format!("{dst}\x00"),
        };

        // Reverse index keys are (dst, label, src), so we scan reverse table
        // and reconstruct edges with src/dst swapped.
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| redb_err("begin_read", e))?;
        let table = read_txn
            .open_table(REVERSE_EDGES)
            .map_err(|e| redb_err("open reverse", e))?;

        let mut edges = Vec::new();
        let range = table
            .range(prefix.as_str()..)
            .map_err(|e| redb_err("range", e))?;

        for entry in range {
            let (key, _) = entry.map_err(|e| redb_err("iter", e))?;
            let key_str = key.value();
            if !key_str.starts_with(&prefix) {
                break;
            }
            if let Some((_rev_dst, rev_label, rev_src)) = parse_edge_key(key_str) {
                edges.push(Edge {
                    src_id: rev_src.to_string(),
                    label: rev_label.to_string(),
                    dst_id: dst.to_string(),
                    properties: Vec::new(),
                });
            }
        }

        // Optionally load properties from forward table.
        if !edges.is_empty() {
            let fwd_table = read_txn
                .open_table(EDGES)
                .map_err(|e| redb_err("open edges", e))?;
            for edge in &mut edges {
                let fwd_key = edge_key(&edge.src_id, &edge.label, &edge.dst_id);
                if let Some(val) = fwd_table
                    .get(fwd_key.as_str())
                    .map_err(|e| redb_err("get props", e))?
                {
                    edge.properties = val.value().to_vec();
                }
            }
        }

        Ok(edges)
    }

    /// Get all neighbors (both directions), optionally filtered by edge label.
    pub fn neighbors(
        &self,
        node: &str,
        label_filter: Option<&str>,
        direction: Direction,
    ) -> crate::Result<Vec<Edge>> {
        match direction {
            Direction::Out => self.neighbors_out(node, label_filter),
            Direction::In => self.neighbors_in(node, label_filter),
            Direction::Both => {
                let mut out = self.neighbors_out(node, label_filter)?;
                let inbound = self.neighbors_in(node, label_filter)?;
                out.extend(inbound);
                Ok(out)
            }
        }
    }

    /// Multi-hop BFS traversal from start nodes.
    ///
    /// Returns all node IDs reachable within `max_depth` hops via edges matching
    /// the optional label filter. Traversal direction is configurable.
    ///
    /// `max_allowed_depth` caps the depth to prevent unbounded memory growth.
    /// Pass `GraphTuning::max_depth` (default 10) from config, or
    /// `DEFAULT_MAX_ALLOWED_DEPTH` when no override is needed.
    ///
    /// `max_visited` caps the visited set to prevent supernode fan-out explosion.
    /// Pass `GraphTuning::max_visited` (default 100_000) from config, or
    /// `DEFAULT_MAX_VISITED` when no override is needed.
    pub fn traverse_bfs(
        &self,
        start_nodes: &[&str],
        label_filter: Option<&str>,
        direction: Direction,
        max_depth: usize,
        max_allowed_depth: usize,
        max_visited: usize,
    ) -> crate::Result<Vec<String>> {
        if max_depth > max_allowed_depth {
            return Err(crate::Error::BadRequest {
                detail: format!(
                    "traverse_bfs: depth {max_depth} exceeds maximum {max_allowed_depth}"
                ),
            });
        }

        use std::collections::{HashSet, VecDeque};

        let mut visited: HashSet<String> = HashSet::new();
        let mut queue: VecDeque<(String, usize)> = VecDeque::new();

        for &node in start_nodes {
            visited.insert(node.to_string());
            queue.push_back((node.to_string(), 0));
        }

        while let Some((node, depth)) = queue.pop_front() {
            if depth >= max_depth || visited.len() >= max_visited {
                continue;
            }

            let edges = self.neighbors(&node, label_filter, direction)?;
            for edge in edges {
                if visited.len() >= max_visited {
                    break;
                }
                let neighbor = if edge.src_id == node {
                    edge.dst_id
                } else {
                    edge.src_id
                };
                if !visited.contains(&neighbor) {
                    visited.insert(neighbor.clone());
                    queue.push_back((neighbor, depth + 1));
                }
            }
        }

        Ok(visited.into_iter().collect())
    }

    /// Scan all forward edges in the store. Used for CSR rebuild on startup.
    pub fn scan_all_edges(&self) -> crate::Result<Vec<Edge>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| redb_err("begin_read", e))?;
        let table = read_txn
            .open_table(EDGES)
            .map_err(|e| redb_err("open edges", e))?;

        let mut edges = Vec::new();
        let range = table.iter().map_err(|e| redb_err("iter", e))?;
        for entry in range {
            let (key, val) = entry.map_err(|e| redb_err("iter", e))?;
            if let Some((src, label, dst)) = parse_edge_key(key.value()) {
                edges.push(Edge {
                    src_id: src.to_string(),
                    label: label.to_string(),
                    dst_id: dst.to_string(),
                    properties: val.value().to_vec(),
                });
            }
        }
        Ok(edges)
    }

    /// Count edges from a source node, optionally filtered by label.
    pub fn out_degree(&self, src: &str, label_filter: Option<&str>) -> crate::Result<usize> {
        Ok(self.neighbors_out(src, label_filter)?.len())
    }

    /// Count edges to a destination node, optionally filtered by label.
    pub fn in_degree(&self, dst: &str, label_filter: Option<&str>) -> crate::Result<usize> {
        Ok(self.neighbors_in(dst, label_filter)?.len())
    }
}
