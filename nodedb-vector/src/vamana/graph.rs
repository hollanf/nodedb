//! Vamana graph structure — adjacency-only, no vector data stored here.
//!
//! Vectors are held in the codec layer (RAM, compressed) and on SSD
//! (full-precision). The graph holds only the degree-bounded adjacency list
//! and the external ID mapping.

/// A single node in the Vamana graph.
///
/// The graph index (`usize`) is the node's position in `VamanaGraph::nodes`.
/// `id` is the caller-supplied external identifier (e.g. row ID).
/// `neighbors` is capped at `VamanaGraph::r` entries.
pub struct VamanaNode {
    /// External identifier supplied by the caller.
    pub id: u64,
    /// Adjacency list — indices into `VamanaGraph::nodes`, length ≤ `r`.
    pub neighbors: Vec<u32>,
}

/// Vamana graph index.
///
/// Holds only adjacency data; vector values are not stored here.
/// Use `build::build_vamana` to populate from a vector slice.
pub struct VamanaGraph {
    /// Vector dimensionality (informational; not enforced here).
    pub dim: usize,
    /// Maximum out-degree per node (typical: 64).
    pub r: usize,
    /// α-pruning factor (typical: 1.2). Stored for reference; pruning logic
    /// lives in `prune::alpha_prune`.
    pub alpha: f32,
    /// Index of the entry-point node (typically the medoid).
    pub entry: usize,
    /// Dense node storage indexed by insertion order.
    nodes: Vec<VamanaNode>,
}

impl VamanaGraph {
    /// Create an empty graph with the given parameters.
    pub fn new(dim: usize, r: usize, alpha: f32) -> Self {
        Self {
            dim,
            r,
            alpha,
            entry: 0,
            nodes: Vec::new(),
        }
    }

    /// Number of nodes in the graph.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Returns `true` if the graph has no nodes.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Add a node with the given external ID; returns its internal index.
    pub fn add_node(&mut self, id: u64) -> usize {
        let idx = self.nodes.len();
        self.nodes.push(VamanaNode {
            id,
            neighbors: Vec::new(),
        });
        idx
    }

    /// Replace the adjacency list for the node at `idx`.
    ///
    /// Truncates `neighbors` to at most `self.r` entries.
    pub fn set_neighbors(&mut self, idx: usize, mut neighbors: Vec<u32>) {
        neighbors.truncate(self.r);
        self.nodes[idx].neighbors = neighbors;
    }

    /// Immutable view of the adjacency list for the node at `idx`.
    pub fn neighbors(&self, idx: usize) -> &[u32] {
        &self.nodes[idx].neighbors
    }

    /// External ID of the node at `idx`.
    pub fn external_id(&self, idx: usize) -> u64 {
        self.nodes[idx].id
    }

    /// Iterate over all nodes as `(internal_index, &VamanaNode)`.
    pub fn iter(&self) -> impl Iterator<Item = (usize, &VamanaNode)> {
        self.nodes.iter().enumerate()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_nodes_len_matches() {
        let mut g = VamanaGraph::new(8, 4, 1.2);
        assert_eq!(g.len(), 0);
        assert!(g.is_empty());

        let i0 = g.add_node(100);
        let i1 = g.add_node(200);
        let i2 = g.add_node(300);

        assert_eq!(g.len(), 3);
        assert!(!g.is_empty());
        assert_eq!(i0, 0);
        assert_eq!(i1, 1);
        assert_eq!(i2, 2);
    }

    #[test]
    fn set_and_get_neighbors() {
        let mut g = VamanaGraph::new(8, 4, 1.2);
        g.add_node(10);
        g.add_node(20);
        g.add_node(30);

        g.set_neighbors(0, vec![1, 2]);
        assert_eq!(g.neighbors(0), &[1, 2]);
        assert_eq!(g.neighbors(1), &[] as &[u32]);
    }

    #[test]
    fn set_neighbors_respects_degree_bound() {
        let r = 3;
        let mut g = VamanaGraph::new(4, r, 1.2);
        for id in 0..10 {
            g.add_node(id);
        }
        // Provide more neighbors than R; they should be truncated.
        g.set_neighbors(0, vec![1, 2, 3, 4, 5]);
        assert_eq!(g.neighbors(0).len(), r);
    }

    #[test]
    fn external_id_roundtrip() {
        let mut g = VamanaGraph::new(4, 8, 1.2);
        g.add_node(9999);
        assert_eq!(g.external_id(0), 9999);
    }
}
