//! Edge insert / remove paths and node-edge cleanup.

use super::types::CsrIndex;

impl CsrIndex {
    /// Incrementally add an unweighted edge (goes into mutable buffer).
    /// Uses weight 1.0 if the graph already has weighted edges.
    ///
    /// Returns `Err(GraphError::LabelOverflow)` if the label id space is
    /// exhausted. Production callers should always surface this to the
    /// client; silently ignoring it reproduces the silent-wrap bug the
    /// `u32` widening was meant to fix.
    pub fn add_edge(&mut self, src: &str, label: &str, dst: &str) -> Result<(), crate::GraphError> {
        self.add_edge_internal(src, label, dst, 1.0, false)
    }

    /// Incrementally add a weighted edge (goes into mutable buffer).
    ///
    /// If this is the first weighted edge (weight != 1.0), initializes
    /// the weight tracking infrastructure (backfills existing buffer
    /// entries with 1.0).
    pub fn add_edge_weighted(
        &mut self,
        src: &str,
        label: &str,
        dst: &str,
        weight: f64,
    ) -> Result<(), crate::GraphError> {
        self.add_edge_internal(src, label, dst, weight, weight != 1.0)
    }

    fn add_edge_internal(
        &mut self,
        src: &str,
        label: &str,
        dst: &str,
        weight: f64,
        force_weights: bool,
    ) -> Result<(), crate::GraphError> {
        let src_id = self.ensure_node(src)?;
        let dst_id = self.ensure_node(dst)?;
        let label_id = self.ensure_label(label)?;

        // Check for duplicates in buffer.
        let out = &self.buffer_out[src_id as usize];
        if out.iter().any(|&(l, d)| l == label_id && d == dst_id) {
            return Ok(());
        }
        // Check for duplicates in dense CSR.
        if self.dense_has_edge(src_id, label_id, dst_id) {
            return Ok(());
        }

        // Initialize weight tracking on first non-default weight.
        if force_weights && !self.has_weights {
            self.enable_weights();
        }

        self.buffer_out[src_id as usize].push((label_id, dst_id));
        self.buffer_in[dst_id as usize].push((label_id, src_id));

        if self.has_weights {
            self.buffer_out_weights[src_id as usize].push(weight);
            self.buffer_in_weights[dst_id as usize].push(weight);
        }

        // If it was previously deleted, un-delete.
        self.deleted_edges.remove(&(src_id, label_id, dst_id));
        Ok(())
    }

    /// Incrementally remove an edge.
    pub fn remove_edge(&mut self, src: &str, label: &str, dst: &str) {
        let (Some(&src_id), Some(&dst_id)) = (self.node_to_id.get(src), self.node_to_id.get(dst))
        else {
            return;
        };
        let Some(&label_id) = self.label_to_id.get(label) else {
            return;
        };

        // Remove from buffer if present (keep weight buffers in sync).
        let out_buf = &self.buffer_out[src_id as usize];
        if let Some(pos) = out_buf
            .iter()
            .position(|&(l, d)| l == label_id && d == dst_id)
        {
            self.buffer_out[src_id as usize].swap_remove(pos);
            if self.has_weights {
                self.buffer_out_weights[src_id as usize].swap_remove(pos);
            }
        }
        let in_buf = &self.buffer_in[dst_id as usize];
        if let Some(pos) = in_buf
            .iter()
            .position(|&(l, s)| l == label_id && s == src_id)
        {
            self.buffer_in[dst_id as usize].swap_remove(pos);
            if self.has_weights {
                self.buffer_in_weights[dst_id as usize].swap_remove(pos);
            }
        }

        // Mark as deleted in dense CSR.
        if self.dense_has_edge(src_id, label_id, dst_id) {
            self.deleted_edges.insert((src_id, label_id, dst_id));
        }
    }

    /// Remove ALL edges touching a node. Returns the number of edges removed.
    pub fn remove_node_edges(&mut self, node: &str) -> usize {
        let Some(&node_id) = self.node_to_id.get(node) else {
            return 0;
        };
        let mut removed = 0;

        // Collect outgoing edges then remove reverse references.
        let out_edges: Vec<(u32, u32)> = self.dense_iter_out(node_id).collect();
        for (label_id, dst_id) in &out_edges {
            let in_buf = &self.buffer_in[*dst_id as usize];
            if let Some(pos) = in_buf
                .iter()
                .position(|&(l, s)| l == *label_id && s == node_id)
            {
                self.buffer_in[*dst_id as usize].swap_remove(pos);
                if self.has_weights {
                    self.buffer_in_weights[*dst_id as usize].swap_remove(pos);
                }
            }
            self.deleted_edges.insert((node_id, *label_id, *dst_id));
            removed += 1;
        }
        self.buffer_out[node_id as usize].clear();
        if self.has_weights {
            self.buffer_out_weights[node_id as usize].clear();
        }

        // Collect incoming edges then remove reverse references.
        let in_edges: Vec<(u32, u32)> = self.dense_iter_in(node_id).collect();
        for (label_id, src_id) in &in_edges {
            let out_buf = &self.buffer_out[*src_id as usize];
            if let Some(pos) = out_buf
                .iter()
                .position(|&(l, d)| l == *label_id && d == node_id)
            {
                self.buffer_out[*src_id as usize].swap_remove(pos);
                if self.has_weights {
                    self.buffer_out_weights[*src_id as usize].swap_remove(pos);
                }
            }
            self.deleted_edges.insert((*src_id, *label_id, node_id));
            removed += 1;
        }
        self.buffer_in[node_id as usize].clear();
        if self.has_weights {
            self.buffer_in_weights[node_id as usize].clear();
        }

        removed
    }

    /// Remove all edges touching any node whose ID starts with `prefix`.
    ///
    /// Used for tenant purge: `prefix = "{tenant_id}:"` removes all
    /// edges belonging to that tenant.
    pub fn remove_nodes_with_prefix(&mut self, prefix: &str) {
        let matching_nodes: Vec<String> = self
            .node_to_id
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect();
        for node in &matching_nodes {
            self.remove_node_edges(node);
        }
    }
}
