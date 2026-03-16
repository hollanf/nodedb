use super::super::distance::{DistanceMetric, distance};

/// HNSW index parameters.
#[derive(Debug, Clone)]
pub struct HnswParams {
    /// Max bidirectional connections per node at layers > 0.
    pub m: usize,
    /// Max connections at layer 0 (typically 2*M for denser base layer).
    pub m0: usize,
    /// Dynamic candidate list size during construction. Higher = better
    /// recall at the cost of slower inserts.
    pub ef_construction: usize,
    /// Distance metric for similarity computation.
    pub metric: DistanceMetric,
}

impl Default for HnswParams {
    fn default() -> Self {
        Self {
            m: 16,
            m0: 32,
            ef_construction: 200,
            metric: DistanceMetric::Cosine,
        }
    }
}

/// Result of a k-NN search.
#[derive(Debug, Clone)]
pub struct SearchResult {
    /// Internal node identifier (insertion order).
    pub id: u32,
    /// Distance from the query vector under the configured metric.
    pub distance: f32,
}

/// A node in the HNSW graph.
///
/// Stores the full-precision vector (FP32) for structural integrity during
/// construction, and per-layer neighbor lists.
pub(super) struct Node {
    /// Full-precision vector data.
    pub vector: Vec<f32>,
    /// Neighbors at each layer this node participates in.
    /// `neighbors[layer]` is the list of neighbor node IDs at that layer.
    pub neighbors: Vec<Vec<u32>>,
}

/// Hierarchical Navigable Small World graph index.
///
/// Production implementation per Malkov & Yashunin (2018):
/// - Multi-layer graph with exponential layer assignment
/// - FP32 construction for structural integrity
/// - Heuristic neighbor selection (Algorithm 4) for diverse connectivity
/// - Beam search with configurable ef parameter
/// - Roaring bitmap pre-filtering for HNSW traversal
///
/// This type is intentionally `!Send` — owned by a single Data Plane core.
pub struct HnswIndex {
    pub(super) params: HnswParams,
    pub(super) dim: usize,
    pub(super) nodes: Vec<Node>,
    pub(super) entry_point: Option<u32>,
    pub(super) max_layer: usize,
    pub(super) rng: Xorshift64,
}

/// Lightweight xorshift64 PRNG for layer assignment. No external dependency.
pub(super) struct Xorshift64(u64);

impl Xorshift64 {
    pub fn new(seed: u64) -> Self {
        Self(seed.max(1))
    }

    pub fn next_f64(&mut self) -> f64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 as f64) / (u64::MAX as f64)
    }
}

/// Ordered candidate used in priority queues during search and construction.
///
/// Implements `Ord` by distance (ascending), then by id for stability.
/// Rust's `BinaryHeap` is a max-heap, so use `Reverse<Candidate>` for min-heap.
#[derive(Clone, Copy, PartialEq)]
pub(super) struct Candidate {
    pub dist: f32,
    pub id: u32,
}

impl Eq for Candidate {}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.dist
            .partial_cmp(&other.dist)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(self.id.cmp(&other.id))
    }
}

impl HnswIndex {
    /// Create a new empty HNSW index for vectors of the given dimensionality.
    pub fn new(dim: usize, params: HnswParams) -> Self {
        Self {
            dim,
            nodes: Vec::new(),
            entry_point: None,
            max_layer: 0,
            rng: Xorshift64::new(42),
            params,
        }
    }

    /// Create with a specific RNG seed (for deterministic testing).
    pub fn with_seed(dim: usize, params: HnswParams, seed: u64) -> Self {
        Self {
            dim,
            nodes: Vec::new(),
            entry_point: None,
            max_layer: 0,
            rng: Xorshift64::new(seed),
            params,
        }
    }

    /// Return the number of vectors stored in the index.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Check whether the index contains no vectors.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Return the vector dimensionality this index was created for.
    pub fn dim(&self) -> usize {
        self.dim
    }

    /// Retrieve a stored vector by node ID.
    pub fn get_vector(&self, id: u32) -> Option<&[f32]> {
        self.nodes.get(id as usize).map(|n| n.vector.as_slice())
    }

    /// Assign a random layer for a new node using the exponential distribution.
    /// layer = floor(-ln(uniform()) * m_L) where m_L = 1 / ln(M).
    pub(super) fn random_layer(&mut self) -> usize {
        let ml = 1.0 / (self.params.m as f64).ln();
        let r = self.rng.next_f64().max(f64::MIN_POSITIVE);
        (-r.ln() * ml).floor() as usize
    }

    /// Compute distance between a query vector and a stored node.
    pub(super) fn dist_to_node(&self, query: &[f32], node_id: u32) -> f32 {
        distance(
            query,
            &self.nodes[node_id as usize].vector,
            self.params.metric,
        )
    }

    /// Max neighbors allowed at a given layer.
    pub(super) fn max_neighbors(&self, layer: usize) -> usize {
        if layer == 0 {
            self.params.m0
        } else {
            self.params.m
        }
    }
}
