use std::collections::HashMap;

use super::edge_store::{Direction, EdgeStore};

/// Compressed Sparse Row (CSR) in-memory adjacency index.
///
/// Per TDD §4.4: Hot adjacency data cached as per-shard CSR structures in L0 RAM.
/// CSR provides cache-friendly sequential access during multi-hop traversals.
/// Rebuilt from redb EdgeStore on shard startup, maintained incrementally on writes.
///
/// Internal representation:
/// - `node_to_id`: maps string node IDs to dense integer IDs
/// - `id_to_node`: reverse mapping for result materialization
/// - `out_offsets[i]..out_offsets[i+1]`: range in `out_edges` for node i's outbound edges
/// - `in_offsets[i]..in_offsets[i+1]`: range in `in_edges` for node i's inbound edges
///
/// For incremental updates, we use adjacency lists that compact into CSR on demand.
pub struct CsrIndex {
    node_to_id: HashMap<String, u32>,
    id_to_node: Vec<String>,

    /// Adjacency lists (outbound): node_id → [(label, dst_id)]
    out_adj: Vec<Vec<(String, u32)>>,
    /// Adjacency lists (inbound): node_id → [(label, src_id)]
    in_adj: Vec<Vec<(String, u32)>>,
}

impl Default for CsrIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl CsrIndex {
    pub fn new() -> Self {
        Self {
            node_to_id: HashMap::new(),
            id_to_node: Vec::new(),
            out_adj: Vec::new(),
            in_adj: Vec::new(),
        }
    }

    /// Rebuild the entire CSR index from an EdgeStore.
    ///
    /// Called on shard startup per TDD §4.4.
    pub fn rebuild_from(store: &EdgeStore) -> crate::Result<Self> {
        let mut csr = Self::new();

        // Scan all forward edges from the store.
        let all_edges = store.scan_all_edges()?;
        for edge in &all_edges {
            csr.ensure_node(&edge.src_id);
            csr.ensure_node(&edge.dst_id);
        }
        for edge in &all_edges {
            let src = csr.node_to_id[&edge.src_id];
            let dst = csr.node_to_id[&edge.dst_id];
            csr.out_adj[src as usize].push((edge.label.clone(), dst));
            csr.in_adj[dst as usize].push((edge.label.clone(), src));
        }

        Ok(csr)
    }

    /// Get or create a dense ID for a node.
    fn ensure_node(&mut self, node: &str) -> u32 {
        if let Some(&id) = self.node_to_id.get(node) {
            return id;
        }
        let id = self.id_to_node.len() as u32;
        self.node_to_id.insert(node.to_string(), id);
        self.id_to_node.push(node.to_string());
        self.out_adj.push(Vec::new());
        self.in_adj.push(Vec::new());
        id
    }

    /// Incrementally add an edge. Called after EdgeStore::put_edge().
    pub fn add_edge(&mut self, src: &str, label: &str, dst: &str) {
        let src_id = self.ensure_node(src);
        let dst_id = self.ensure_node(dst);

        // Avoid duplicates.
        let out = &mut self.out_adj[src_id as usize];
        if !out.iter().any(|(l, d)| l == label && *d == dst_id) {
            out.push((label.to_string(), dst_id));
        }
        let inb = &mut self.in_adj[dst_id as usize];
        if !inb.iter().any(|(l, s)| l == label && *s == src_id) {
            inb.push((label.to_string(), src_id));
        }
    }

    /// Incrementally remove an edge. Called after EdgeStore::delete_edge().
    pub fn remove_edge(&mut self, src: &str, label: &str, dst: &str) {
        if let (Some(&src_id), Some(&dst_id)) = (self.node_to_id.get(src), self.node_to_id.get(dst))
        {
            self.out_adj[src_id as usize].retain(|(l, d)| !(l == label && *d == dst_id));
            self.in_adj[dst_id as usize].retain(|(l, s)| !(l == label && *s == src_id));
        }
    }

    /// Get immediate neighbors (1-hop) with optional label filter.
    pub fn neighbors(
        &self,
        node: &str,
        label_filter: Option<&str>,
        direction: Direction,
    ) -> Vec<(String, String)> {
        let Some(&node_id) = self.node_to_id.get(node) else {
            return Vec::new();
        };

        let mut result = Vec::new();

        if matches!(direction, Direction::Out | Direction::Both) {
            for (label, dst_id) in &self.out_adj[node_id as usize] {
                if label_filter.is_none_or(|f| f == label) {
                    result.push((label.clone(), self.id_to_node[*dst_id as usize].clone()));
                }
            }
        }
        if matches!(direction, Direction::In | Direction::Both) {
            for (label, src_id) in &self.in_adj[node_id as usize] {
                if label_filter.is_none_or(|f| f == label) {
                    result.push((label.clone(), self.id_to_node[*src_id as usize].clone()));
                }
            }
        }

        result
    }

    /// BFS traversal over the CSR index. Returns all reachable node IDs within max_depth hops.
    ///
    /// This is the hot path for GRAPH_HOP. All data is in L0 RAM — no disk I/O.
    pub fn traverse_bfs(
        &self,
        start_nodes: &[&str],
        label_filter: Option<&str>,
        direction: Direction,
        max_depth: usize,
    ) -> Vec<String> {
        use std::collections::{HashSet, VecDeque};

        let mut visited: HashSet<u32> = HashSet::new();
        let mut queue: VecDeque<(u32, usize)> = VecDeque::new();

        for &node in start_nodes {
            if let Some(&id) = self.node_to_id.get(node) {
                if visited.insert(id) {
                    queue.push_back((id, 0));
                }
            }
        }

        while let Some((node_id, depth)) = queue.pop_front() {
            if depth >= max_depth {
                continue;
            }

            if matches!(direction, Direction::Out | Direction::Both) {
                for (label, dst_id) in &self.out_adj[node_id as usize] {
                    if label_filter.is_none_or(|f| f == label) && visited.insert(*dst_id) {
                        queue.push_back((*dst_id, depth + 1));
                    }
                }
            }
            if matches!(direction, Direction::In | Direction::Both) {
                for (label, src_id) in &self.in_adj[node_id as usize] {
                    if label_filter.is_none_or(|f| f == label) && visited.insert(*src_id) {
                        queue.push_back((*src_id, depth + 1));
                    }
                }
            }
        }

        visited
            .into_iter()
            .map(|id| self.id_to_node[id as usize].clone())
            .collect()
    }

    /// Shortest path via bidirectional BFS. Returns None if unreachable within max_depth.
    ///
    /// Used by GRAPH_PATH. Runs entirely in L0 RAM.
    pub fn shortest_path(
        &self,
        src: &str,
        dst: &str,
        label_filter: Option<&str>,
        max_depth: usize,
    ) -> Option<Vec<String>> {
        let src_id = *self.node_to_id.get(src)?;
        let dst_id = *self.node_to_id.get(dst)?;

        if src_id == dst_id {
            return Some(vec![src.to_string()]);
        }

        // Forward BFS from src, backward BFS from dst.
        let mut fwd_parent: HashMap<u32, u32> = HashMap::new();
        let mut bwd_parent: HashMap<u32, u32> = HashMap::new();
        fwd_parent.insert(src_id, src_id);
        bwd_parent.insert(dst_id, dst_id);

        let mut fwd_frontier: Vec<u32> = vec![src_id];
        let mut bwd_frontier: Vec<u32> = vec![dst_id];

        for _depth in 0..max_depth {
            // Expand forward.
            let mut next_fwd = Vec::new();
            for &node in &fwd_frontier {
                for (label, neighbor) in &self.out_adj[node as usize] {
                    if label_filter.is_none_or(|f| f == label) {
                        if !fwd_parent.contains_key(neighbor) {
                            fwd_parent.insert(*neighbor, node);
                            next_fwd.push(*neighbor);
                        }
                        if bwd_parent.contains_key(neighbor) {
                            return Some(self.reconstruct_path(
                                *neighbor,
                                &fwd_parent,
                                &bwd_parent,
                            ));
                        }
                    }
                }
            }
            fwd_frontier = next_fwd;

            // Expand backward.
            let mut next_bwd = Vec::new();
            for &node in &bwd_frontier {
                for (label, neighbor) in &self.in_adj[node as usize] {
                    if label_filter.is_none_or(|f| f == label) {
                        if !bwd_parent.contains_key(neighbor) {
                            bwd_parent.insert(*neighbor, node);
                            next_bwd.push(*neighbor);
                        }
                        if fwd_parent.contains_key(neighbor) {
                            return Some(self.reconstruct_path(
                                *neighbor,
                                &fwd_parent,
                                &bwd_parent,
                            ));
                        }
                    }
                }
            }
            bwd_frontier = next_bwd;

            if fwd_frontier.is_empty() && bwd_frontier.is_empty() {
                break;
            }
        }

        None
    }

    fn reconstruct_path(
        &self,
        meeting: u32,
        fwd_parent: &HashMap<u32, u32>,
        bwd_parent: &HashMap<u32, u32>,
    ) -> Vec<String> {
        // Forward half: src -> meeting.
        let mut fwd_path = Vec::new();
        let mut current = meeting;
        loop {
            fwd_path.push(current);
            let parent = fwd_parent[&current];
            if parent == current {
                break;
            }
            current = parent;
        }
        fwd_path.reverse();

        // Backward half: meeting -> dst.
        current = bwd_parent[&meeting];
        if current != meeting {
            loop {
                fwd_path.push(current);
                let parent = bwd_parent[&current];
                if parent == current {
                    break;
                }
                current = parent;
            }
        }

        fwd_path
            .into_iter()
            .map(|id| self.id_to_node[id as usize].clone())
            .collect()
    }

    /// Materialize a subgraph as edge tuples within max_depth from start nodes.
    ///
    /// Used by GRAPH_SUBGRAPH. Returns (src, label, dst) tuples.
    pub fn subgraph(
        &self,
        start_nodes: &[&str],
        label_filter: Option<&str>,
        max_depth: usize,
    ) -> Vec<(String, String, String)> {
        use std::collections::{HashSet, VecDeque};

        let mut visited: HashSet<u32> = HashSet::new();
        let mut queue: VecDeque<(u32, usize)> = VecDeque::new();
        let mut edges = Vec::new();

        for &node in start_nodes {
            if let Some(&id) = self.node_to_id.get(node) {
                if visited.insert(id) {
                    queue.push_back((id, 0));
                }
            }
        }

        while let Some((node_id, depth)) = queue.pop_front() {
            if depth >= max_depth {
                continue;
            }

            for (label, dst_id) in &self.out_adj[node_id as usize] {
                if label_filter.is_none_or(|f| f == label) {
                    edges.push((
                        self.id_to_node[node_id as usize].clone(),
                        label.clone(),
                        self.id_to_node[*dst_id as usize].clone(),
                    ));
                    if visited.insert(*dst_id) {
                        queue.push_back((*dst_id, depth + 1));
                    }
                }
            }
        }

        edges
    }

    pub fn node_count(&self) -> usize {
        self.id_to_node.len()
    }

    pub fn contains_node(&self, node: &str) -> bool {
        self.node_to_id.contains_key(node)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_csr() -> CsrIndex {
        let mut csr = CsrIndex::new();
        // a -KNOWS-> b -KNOWS-> c -KNOWS-> d
        // a -WORKS-> e
        csr.add_edge("a", "KNOWS", "b");
        csr.add_edge("b", "KNOWS", "c");
        csr.add_edge("c", "KNOWS", "d");
        csr.add_edge("a", "WORKS", "e");
        csr
    }

    #[test]
    fn neighbors_out() {
        let csr = make_csr();
        let n = csr.neighbors("a", None, Direction::Out);
        assert_eq!(n.len(), 2);
        let dsts: Vec<&str> = n.iter().map(|(_, d)| d.as_str()).collect();
        assert!(dsts.contains(&"b"));
        assert!(dsts.contains(&"e"));
    }

    #[test]
    fn neighbors_filtered() {
        let csr = make_csr();
        let n = csr.neighbors("a", Some("KNOWS"), Direction::Out);
        assert_eq!(n.len(), 1);
        assert_eq!(n[0].1, "b");
    }

    #[test]
    fn neighbors_in() {
        let csr = make_csr();
        let n = csr.neighbors("b", None, Direction::In);
        assert_eq!(n.len(), 1);
        assert_eq!(n[0].1, "a");
    }

    #[test]
    fn bfs_traversal() {
        let csr = make_csr();
        let mut result = csr.traverse_bfs(&["a"], Some("KNOWS"), Direction::Out, 2);
        result.sort();
        assert_eq!(result, vec!["a", "b", "c"]);
    }

    #[test]
    fn bfs_all_labels() {
        let csr = make_csr();
        let mut result = csr.traverse_bfs(&["a"], None, Direction::Out, 1);
        result.sort();
        assert_eq!(result, vec!["a", "b", "e"]);
    }

    #[test]
    fn bfs_cycle() {
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b");
        csr.add_edge("b", "L", "c");
        csr.add_edge("c", "L", "a");

        let mut result = csr.traverse_bfs(&["a"], None, Direction::Out, 10);
        result.sort();
        assert_eq!(result, vec!["a", "b", "c"]);
    }

    #[test]
    fn shortest_path_direct() {
        let csr = make_csr();
        let path = csr.shortest_path("a", "c", Some("KNOWS"), 5).unwrap();
        assert_eq!(path, vec!["a", "b", "c"]);
    }

    #[test]
    fn shortest_path_same_node() {
        let csr = make_csr();
        let path = csr.shortest_path("a", "a", None, 5).unwrap();
        assert_eq!(path, vec!["a"]);
    }

    #[test]
    fn shortest_path_unreachable() {
        let csr = make_csr();
        let path = csr.shortest_path("d", "a", Some("KNOWS"), 5);
        assert!(path.is_none());
    }

    #[test]
    fn shortest_path_depth_limit() {
        let csr = make_csr();
        // a->b->c->d is 3 hops, depth limit of 1 should fail
        // (bidirectional BFS can find within 2 iterations what takes 3 unidirectional)
        let path = csr.shortest_path("a", "d", Some("KNOWS"), 1);
        assert!(path.is_none());
    }

    #[test]
    fn subgraph_materialization() {
        let csr = make_csr();
        let edges = csr.subgraph(&["a"], None, 2);
        // Depth 2 from a: a->b, a->e, b->c
        assert_eq!(edges.len(), 3);
        assert!(edges.contains(&("a".into(), "KNOWS".into(), "b".into())));
        assert!(edges.contains(&("a".into(), "WORKS".into(), "e".into())));
        assert!(edges.contains(&("b".into(), "KNOWS".into(), "c".into())));
    }

    #[test]
    fn incremental_remove() {
        let mut csr = make_csr();
        assert_eq!(csr.neighbors("a", Some("KNOWS"), Direction::Out).len(), 1);
        csr.remove_edge("a", "KNOWS", "b");
        assert_eq!(csr.neighbors("a", Some("KNOWS"), Direction::Out).len(), 0);
    }

    #[test]
    fn duplicate_add_is_idempotent() {
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b");
        csr.add_edge("a", "L", "b");
        assert_eq!(csr.neighbors("a", None, Direction::Out).len(), 1);
    }

    #[test]
    fn rebuild_from_edge_store() {
        let dir = tempfile::tempdir().unwrap();
        let store = EdgeStore::open(&dir.path().join("graph.redb")).unwrap();
        store.put_edge("x", "REL", "y", b"").unwrap();
        store.put_edge("y", "REL", "z", b"").unwrap();

        let csr = CsrIndex::rebuild_from(&store).unwrap();
        assert_eq!(csr.node_count(), 3);
        let mut result = csr.traverse_bfs(&["x"], Some("REL"), Direction::Out, 3);
        result.sort();
        assert_eq!(result, vec!["x", "y", "z"]);
    }
}
