//! Dense integer CSR adjacency index with interned node IDs and labels.
//!
//! Memory layout:
//! - Old-style `Vec<Vec<(String, u32)>>` ≈ 60 bytes/edge (heap String per edge)
//! - Dense CSR: contiguous `Vec<u32>` offsets + targets + `Vec<u16>` labels ≈ 10 bytes/edge
//!
//! Writes accumulate in a mutable buffer. Reads check both dense CSR and buffer.
//! `compact()` merges the buffer into dense arrays (double-buffered swap).

use std::collections::{HashMap, HashSet, hash_map::Entry};

use serde::{Deserialize, Serialize};

/// Edge traversal direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Out,
    In,
    Both,
}

/// Dense integer CSR adjacency index.
pub struct CsrIndex {
    // ── Node interning ──
    pub(crate) node_to_id: HashMap<String, u32>,
    pub(crate) id_to_node: Vec<String>,

    // ── Label interning ──
    pub(crate) label_to_id: HashMap<String, u16>,
    pub(crate) id_to_label: Vec<String>,

    // ── Dense CSR (read-only between compactions) ──
    pub(crate) out_offsets: Vec<u32>,
    pub(crate) out_targets: Vec<u32>,
    pub(crate) out_labels: Vec<u16>,

    pub(crate) in_offsets: Vec<u32>,
    pub(crate) in_targets: Vec<u32>,
    pub(crate) in_labels: Vec<u16>,

    // ── Mutable write buffer ──
    pub(crate) buffer_out: Vec<Vec<(u16, u32)>>,
    pub(crate) buffer_in: Vec<Vec<(u16, u32)>>,

    /// Edges deleted since last compaction: `(src, label, dst)`.
    pub(crate) deleted_edges: HashSet<(u32, u16, u32)>,
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
            label_to_id: HashMap::new(),
            id_to_label: Vec::new(),
            out_offsets: vec![0],
            out_targets: Vec::new(),
            out_labels: Vec::new(),
            in_offsets: vec![0],
            in_targets: Vec::new(),
            in_labels: Vec::new(),
            buffer_out: Vec::new(),
            buffer_in: Vec::new(),
            deleted_edges: HashSet::new(),
        }
    }

    /// Get or create a dense ID for a node.
    pub(crate) fn ensure_node(&mut self, node: &str) -> u32 {
        match self.node_to_id.entry(node.to_string()) {
            Entry::Occupied(e) => *e.get(),
            Entry::Vacant(e) => {
                let id = self.id_to_node.len() as u32;
                e.insert(id);
                self.id_to_node.push(node.to_string());
                self.out_offsets
                    .push(*self.out_offsets.last().unwrap_or(&0));
                self.in_offsets.push(*self.in_offsets.last().unwrap_or(&0));
                self.buffer_out.push(Vec::new());
                self.buffer_in.push(Vec::new());
                id
            }
        }
    }

    /// Get or create a dense ID for a label.
    fn ensure_label(&mut self, label: &str) -> u16 {
        match self.label_to_id.entry(label.to_string()) {
            Entry::Occupied(e) => *e.get(),
            Entry::Vacant(e) => {
                let id = self.id_to_label.len() as u16;
                e.insert(id);
                self.id_to_label.push(label.to_string());
                id
            }
        }
    }

    /// Add an edge (goes into mutable buffer). Deduplicates.
    pub fn add_edge(&mut self, src: &str, label: &str, dst: &str) {
        let src_id = self.ensure_node(src);
        let dst_id = self.ensure_node(dst);
        let label_id = self.ensure_label(label);

        // Deduplicate in buffer.
        if self.buffer_out[src_id as usize]
            .iter()
            .any(|&(l, d)| l == label_id && d == dst_id)
        {
            return;
        }
        // Deduplicate in dense CSR.
        if self.dense_has_edge(src_id, label_id, dst_id) {
            return;
        }

        self.buffer_out[src_id as usize].push((label_id, dst_id));
        self.buffer_in[dst_id as usize].push((label_id, src_id));
        self.deleted_edges.remove(&(src_id, label_id, dst_id));
    }

    /// Remove an edge.
    pub fn remove_edge(&mut self, src: &str, label: &str, dst: &str) {
        let (Some(&src_id), Some(&dst_id)) = (self.node_to_id.get(src), self.node_to_id.get(dst))
        else {
            return;
        };
        let Some(&label_id) = self.label_to_id.get(label) else {
            return;
        };

        self.buffer_out[src_id as usize].retain(|&(l, d)| !(l == label_id && d == dst_id));
        self.buffer_in[dst_id as usize].retain(|&(l, s)| !(l == label_id && s == src_id));

        if self.dense_has_edge(src_id, label_id, dst_id) {
            self.deleted_edges.insert((src_id, label_id, dst_id));
        }
    }

    /// Get immediate neighbors.
    pub fn neighbors(
        &self,
        node: &str,
        label_filter: Option<&str>,
        direction: Direction,
    ) -> Vec<(String, String)> {
        let Some(&node_id) = self.node_to_id.get(node) else {
            return Vec::new();
        };
        let label_id = label_filter.and_then(|l| self.label_to_id.get(l).copied());

        let mut result = Vec::new();

        if matches!(direction, Direction::Out | Direction::Both) {
            for (lid, dst) in self.iter_out_edges(node_id) {
                if label_id.is_none_or(|f| f == lid) {
                    result.push((
                        self.id_to_label[lid as usize].clone(),
                        self.id_to_node[dst as usize].clone(),
                    ));
                }
            }
        }
        if matches!(direction, Direction::In | Direction::Both) {
            for (lid, src) in self.iter_in_edges(node_id) {
                if label_id.is_none_or(|f| f == lid) {
                    result.push((
                        self.id_to_label[lid as usize].clone(),
                        self.id_to_node[src as usize].clone(),
                    ));
                }
            }
        }

        result
    }

    pub fn node_count(&self) -> usize {
        self.id_to_node.len()
    }

    pub fn contains_node(&self, node: &str) -> bool {
        self.node_to_id.contains_key(node)
    }

    /// Total edge count (dense + buffer - deleted).
    pub fn edge_count(&self) -> usize {
        let dense_out = self.out_targets.len();
        let buffer_out: usize = self.buffer_out.iter().map(|b| b.len()).sum();
        let deleted = self.deleted_edges.len();
        dense_out + buffer_out - deleted
    }

    /// Estimated memory usage in bytes.
    pub fn estimated_memory_bytes(&self) -> usize {
        let offsets = (self.out_offsets.len() + self.in_offsets.len()) * 4;
        let targets = (self.out_targets.len() + self.in_targets.len()) * 4;
        let labels = (self.out_labels.len() + self.in_labels.len()) * 2;
        let buffer: usize = self
            .buffer_out
            .iter()
            .chain(self.buffer_in.iter())
            .map(|b| b.len() * 6)
            .sum();
        let interning = self.id_to_node.iter().map(|s| s.len() + 24).sum::<usize>()
            + self.id_to_label.iter().map(|s| s.len() + 24).sum::<usize>();
        offsets + targets + labels + buffer + interning
    }

    /// Merge the mutable buffer into dense CSR arrays.
    pub fn compact(&mut self) {
        let n = self.id_to_node.len();
        let mut new_out_edges: Vec<Vec<(u16, u32)>> = vec![Vec::new(); n];
        let mut new_in_edges: Vec<Vec<(u16, u32)>> = vec![Vec::new(); n];

        // Collect surviving dense edges.
        for node in 0..n {
            let node_id = node as u32;
            for (lid, dst) in self.dense_out_edges(node_id) {
                if !self.deleted_edges.contains(&(node_id, lid, dst)) {
                    new_out_edges[node].push((lid, dst));
                }
            }
            for (lid, src) in self.dense_in_edges(node_id) {
                if !self.deleted_edges.contains(&(src, lid, node_id)) {
                    new_in_edges[node].push((lid, src));
                }
            }
        }

        // Merge buffer edges.
        for node in 0..n {
            for &(lid, dst) in &self.buffer_out[node] {
                if !new_out_edges[node]
                    .iter()
                    .any(|&(l, d)| l == lid && d == dst)
                {
                    new_out_edges[node].push((lid, dst));
                }
            }
            for &(lid, src) in &self.buffer_in[node] {
                if !new_in_edges[node]
                    .iter()
                    .any(|&(l, s)| l == lid && s == src)
                {
                    new_in_edges[node].push((lid, src));
                }
            }
        }

        // Build new dense arrays.
        let (out_offsets, out_targets, out_labels) = Self::build_dense(&new_out_edges);
        let (in_offsets, in_targets, in_labels) = Self::build_dense(&new_in_edges);

        self.out_offsets = out_offsets;
        self.out_targets = out_targets;
        self.out_labels = out_labels;
        self.in_offsets = in_offsets;
        self.in_targets = in_targets;
        self.in_labels = in_labels;

        for buf in &mut self.buffer_out {
            buf.clear();
        }
        for buf in &mut self.buffer_in {
            buf.clear();
        }
        self.deleted_edges.clear();
    }

    /// Serialize the index to MessagePack bytes for storage.
    pub fn checkpoint_to_bytes(&self) -> Vec<u8> {
        #[derive(Serialize, Deserialize)]
        struct CsrSnapshot {
            nodes: Vec<String>,
            labels: Vec<String>,
            out_offsets: Vec<u32>,
            out_targets: Vec<u32>,
            out_labels: Vec<u16>,
            in_offsets: Vec<u32>,
            in_targets: Vec<u32>,
            in_labels: Vec<u16>,
            buffer_out: Vec<Vec<(u16, u32)>>,
            buffer_in: Vec<Vec<(u16, u32)>>,
            deleted: Vec<(u32, u16, u32)>,
        }

        let snapshot = CsrSnapshot {
            nodes: self.id_to_node.clone(),
            labels: self.id_to_label.clone(),
            out_offsets: self.out_offsets.clone(),
            out_targets: self.out_targets.clone(),
            out_labels: self.out_labels.clone(),
            in_offsets: self.in_offsets.clone(),
            in_targets: self.in_targets.clone(),
            in_labels: self.in_labels.clone(),
            buffer_out: self.buffer_out.clone(),
            buffer_in: self.buffer_in.clone(),
            deleted: self.deleted_edges.iter().copied().collect(),
        };
        match rmp_serde::to_vec_named(&snapshot) {
            Ok(bytes) => bytes,
            Err(e) => {
                tracing::error!(error = %e, "CSR checkpoint serialization failed");
                Vec::new()
            }
        }
    }

    /// Restore an index from a checkpoint snapshot.
    pub fn from_checkpoint(bytes: &[u8]) -> Option<Self> {
        #[derive(Serialize, Deserialize)]
        struct CsrSnapshot {
            nodes: Vec<String>,
            labels: Vec<String>,
            out_offsets: Vec<u32>,
            out_targets: Vec<u32>,
            out_labels: Vec<u16>,
            in_offsets: Vec<u32>,
            in_targets: Vec<u32>,
            in_labels: Vec<u16>,
            buffer_out: Vec<Vec<(u16, u32)>>,
            buffer_in: Vec<Vec<(u16, u32)>>,
            deleted: Vec<(u32, u16, u32)>,
        }

        let snap: CsrSnapshot = rmp_serde::from_slice(bytes).ok()?;

        let node_to_id: HashMap<String, u32> = snap
            .nodes
            .iter()
            .enumerate()
            .map(|(i, n)| (n.clone(), i as u32))
            .collect();
        let label_to_id: HashMap<String, u16> = snap
            .labels
            .iter()
            .enumerate()
            .map(|(i, l)| (l.clone(), i as u16))
            .collect();

        Some(Self {
            node_to_id,
            id_to_node: snap.nodes,
            label_to_id,
            id_to_label: snap.labels,
            out_offsets: snap.out_offsets,
            out_targets: snap.out_targets,
            out_labels: snap.out_labels,
            in_offsets: snap.in_offsets,
            in_targets: snap.in_targets,
            in_labels: snap.in_labels,
            buffer_out: snap.buffer_out,
            buffer_in: snap.buffer_in,
            deleted_edges: snap.deleted.into_iter().collect(),
        })
    }

    // ── Internal helpers ──

    fn build_dense(edges: &[Vec<(u16, u32)>]) -> (Vec<u32>, Vec<u32>, Vec<u16>) {
        let n = edges.len();
        let total: usize = edges.iter().map(|e| e.len()).sum();
        let mut offsets = Vec::with_capacity(n + 1);
        let mut targets = Vec::with_capacity(total);
        let mut labels = Vec::with_capacity(total);

        let mut offset = 0u32;
        for node_edges in edges {
            offsets.push(offset);
            for &(lid, target) in node_edges {
                targets.push(target);
                labels.push(lid);
            }
            offset += node_edges.len() as u32;
        }
        offsets.push(offset);

        (offsets, targets, labels)
    }

    fn dense_has_edge(&self, src: u32, label: u16, dst: u32) -> bool {
        for (lid, target) in self.dense_out_edges(src) {
            if lid == label && target == dst {
                return true;
            }
        }
        false
    }

    pub(crate) fn dense_out_edges(&self, node: u32) -> impl Iterator<Item = (u16, u32)> + '_ {
        let idx = node as usize;
        if idx + 1 >= self.out_offsets.len() {
            return Vec::new().into_iter();
        }
        let start = self.out_offsets[idx] as usize;
        let end = self.out_offsets[idx + 1] as usize;
        (start..end)
            .map(move |i| (self.out_labels[i], self.out_targets[i]))
            .collect::<Vec<_>>()
            .into_iter()
    }

    pub(crate) fn dense_in_edges(&self, node: u32) -> impl Iterator<Item = (u16, u32)> + '_ {
        let idx = node as usize;
        if idx + 1 >= self.in_offsets.len() {
            return Vec::new().into_iter();
        }
        let start = self.in_offsets[idx] as usize;
        let end = self.in_offsets[idx + 1] as usize;
        (start..end)
            .map(move |i| (self.in_labels[i], self.in_targets[i]))
            .collect::<Vec<_>>()
            .into_iter()
    }

    pub(crate) fn iter_out_edges(&self, node: u32) -> impl Iterator<Item = (u16, u32)> + '_ {
        let idx = node as usize;
        let dense = self
            .dense_out_edges(node)
            .filter(move |&(lid, dst)| !self.deleted_edges.contains(&(node, lid, dst)));
        let buffer = if idx < self.buffer_out.len() {
            self.buffer_out[idx].to_vec()
        } else {
            Vec::new()
        };
        dense.chain(buffer)
    }

    pub(crate) fn iter_in_edges(&self, node: u32) -> impl Iterator<Item = (u16, u32)> + '_ {
        let idx = node as usize;
        let dense = self
            .dense_in_edges(node)
            .filter(move |&(lid, src)| !self.deleted_edges.contains(&(src, lid, node)));
        let buffer = if idx < self.buffer_in.len() {
            self.buffer_in[idx].to_vec()
        } else {
            Vec::new()
        };
        dense.chain(buffer)
    }

    /// Resolve a node string to its internal ID.
    pub(crate) fn node_id(&self, node: &str) -> Option<u32> {
        self.node_to_id.get(node).copied()
    }

    /// Resolve an internal ID to its node string.
    pub(crate) fn node_name(&self, id: u32) -> &str {
        &self.id_to_node[id as usize]
    }

    /// Resolve a label ID to its label string.
    pub(crate) fn label_name(&self, id: u16) -> &str {
        &self.id_to_label[id as usize]
    }

    /// Resolve a label string to its internal ID.
    pub(crate) fn label_id(&self, label: &str) -> Option<u16> {
        self.label_to_id.get(label).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_csr() -> CsrIndex {
        let mut csr = CsrIndex::new();
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
    fn compact_merges_buffer_into_dense() {
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b");
        csr.add_edge("b", "L", "c");
        assert_eq!(csr.neighbors("a", None, Direction::Out).len(), 1);

        csr.compact();
        assert!(csr.buffer_out.iter().all(|b| b.is_empty()));
        assert_eq!(csr.neighbors("a", None, Direction::Out).len(), 1);
        assert_eq!(csr.neighbors("b", None, Direction::Out).len(), 1);
    }

    #[test]
    fn compact_handles_deletes() {
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b");
        csr.add_edge("a", "L", "c");
        csr.compact();

        csr.remove_edge("a", "L", "b");
        assert_eq!(csr.neighbors("a", None, Direction::Out).len(), 1);

        csr.compact();
        assert_eq!(csr.neighbors("a", None, Direction::Out).len(), 1);
        assert_eq!(csr.neighbors("a", None, Direction::Out)[0].1, "c");
    }

    #[test]
    fn label_interning() {
        let mut csr = CsrIndex::new();
        for i in 0..100 {
            csr.add_edge(&format!("n{i}"), "FOLLOWS", &format!("n{}", i + 1));
        }
        assert_eq!(csr.id_to_label.len(), 1);
    }

    #[test]
    fn edge_count() {
        let csr = make_csr();
        assert_eq!(csr.edge_count(), 4);
    }

    #[test]
    fn checkpoint_roundtrip() {
        let mut csr = make_csr();
        csr.compact();

        let bytes = csr.checkpoint_to_bytes();
        assert!(!bytes.is_empty());

        let restored = CsrIndex::from_checkpoint(&bytes).unwrap();
        assert_eq!(restored.node_count(), csr.node_count());
        assert_eq!(restored.edge_count(), csr.edge_count());

        let n = restored.neighbors("a", Some("KNOWS"), Direction::Out);
        assert_eq!(n.len(), 1);
        assert_eq!(n[0].1, "b");
    }

    #[test]
    fn memory_estimation() {
        let csr = make_csr();
        let mem = csr.estimated_memory_bytes();
        assert!(mem > 0);
    }
}
