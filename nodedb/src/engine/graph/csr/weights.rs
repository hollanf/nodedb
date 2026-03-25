//! Edge weight management for the CSR index.
//!
//! Optional `f64` weight per edge stored in parallel arrays. `None` when the
//! graph is entirely unweighted (zero memory overhead). Populated from the
//! `"weight"` edge property at insertion time. Unweighted edges default to 1.0.

use super::index::CsrIndex;

impl CsrIndex {
    /// Enable weight tracking. Backfills existing buffer entries with 1.0.
    pub(super) fn enable_weights(&mut self) {
        self.has_weights = true;

        // Backfill existing dense arrays with 1.0.
        if !self.out_targets.is_empty() {
            self.out_weights = Some(vec![1.0; self.out_targets.len()]);
        }
        if !self.in_targets.is_empty() {
            self.in_weights = Some(vec![1.0; self.in_targets.len()]);
        }

        // Backfill existing buffer entries with 1.0.
        for (buf, wbuf) in self
            .buffer_out
            .iter()
            .zip(self.buffer_out_weights.iter_mut())
        {
            if wbuf.len() < buf.len() {
                wbuf.resize(buf.len(), 1.0);
            }
        }
        for (buf, wbuf) in self.buffer_in.iter().zip(self.buffer_in_weights.iter_mut()) {
            if wbuf.len() < buf.len() {
                wbuf.resize(buf.len(), 1.0);
            }
        }
    }

    /// Whether this CSR has any weighted edges.
    pub fn has_weights(&self) -> bool {
        self.has_weights
    }

    /// Get the weight of the i-th outbound edge from a node (dense CSR only).
    ///
    /// `edge_idx` is the absolute index into `out_targets`/`out_weights`.
    /// Returns 1.0 for unweighted graphs.
    pub fn out_edge_weight(&self, edge_idx: usize) -> f64 {
        self.out_weights
            .as_ref()
            .and_then(|ws| ws.get(edge_idx).copied())
            .unwrap_or(1.0)
    }

    /// Get the weight of the i-th inbound edge to a node (dense CSR only).
    pub fn in_edge_weight(&self, edge_idx: usize) -> f64 {
        self.in_weights
            .as_ref()
            .and_then(|ws| ws.get(edge_idx).copied())
            .unwrap_or(1.0)
    }

    /// Get the weight of a specific outbound edge from `src` to `dst` via `label`.
    ///
    /// Checks both dense and buffer. Returns 1.0 if the edge exists but has
    /// no weight, or `None` if the edge doesn't exist.
    pub fn edge_weight(&self, src: &str, label: &str, dst: &str) -> Option<f64> {
        let src_id = *self.node_to_id.get(src)?;
        let dst_id = *self.node_to_id.get(dst)?;
        let label_id = *self.label_to_id.get(label)?;

        // Check dense CSR.
        let idx = src_id as usize;
        if idx + 1 < self.out_offsets.len() {
            let start = self.out_offsets[idx] as usize;
            let end = self.out_offsets[idx + 1] as usize;
            for i in start..end {
                if self.out_labels[i] == label_id
                    && self.out_targets[i] == dst_id
                    && !self.deleted_edges.contains(&(src_id, label_id, dst_id))
                {
                    return Some(self.out_edge_weight(i));
                }
            }
        }

        // Check buffer.
        if idx < self.buffer_out.len() {
            for (buf_idx, &(l, d)) in self.buffer_out[idx].iter().enumerate() {
                if l == label_id && d == dst_id {
                    if self.has_weights {
                        return Some(
                            self.buffer_out_weights[idx]
                                .get(buf_idx)
                                .copied()
                                .unwrap_or(1.0),
                        );
                    }
                    return Some(1.0);
                }
            }
        }

        None
    }

    /// Iterate outbound edges of a node with weights: `(label_id, dst_id, weight)`.
    ///
    /// Yields from both dense CSR and buffer, excluding deleted edges.
    /// Weights are 1.0 for unweighted graphs.
    pub fn iter_out_edges_weighted(&self, node: u32) -> impl Iterator<Item = (u16, u32, f64)> + '_ {
        let idx = node as usize;

        // Dense edges with weights.
        let dense_start = if idx + 1 < self.out_offsets.len() {
            self.out_offsets[idx] as usize
        } else {
            0
        };
        let dense_end = if idx + 1 < self.out_offsets.len() {
            self.out_offsets[idx + 1] as usize
        } else {
            0
        };

        let dense = (dense_start..dense_end)
            .map(move |i| {
                let w = self.out_edge_weight(i);
                (self.out_labels[i], self.out_targets[i], w)
            })
            .filter(move |&(lid, dst, _)| !self.deleted_edges.contains(&(node, lid, dst)))
            .collect::<Vec<_>>();

        // Buffer edges with weights.
        let buffer = if idx < self.buffer_out.len() {
            self.buffer_out[idx]
                .iter()
                .enumerate()
                .map(|(buf_idx, &(lid, dst))| {
                    let w = if self.has_weights {
                        self.buffer_out_weights[idx]
                            .get(buf_idx)
                            .copied()
                            .unwrap_or(1.0)
                    } else {
                        1.0
                    };
                    (lid, dst, w)
                })
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };

        dense.into_iter().chain(buffer)
    }
}

/// Extract the `"weight"` property from MessagePack-encoded edge properties.
///
/// Returns 1.0 if properties are empty, malformed, or don't contain a
/// `"weight"` key. Handles F64, F32, and integer weight values; other
/// numeric types default to 1.0.
pub fn extract_weight_from_properties(properties: &[u8]) -> f64 {
    if properties.is_empty() {
        return 1.0;
    }
    let Ok(val) = rmpv::decode::read_value(&mut &properties[..]) else {
        return 1.0;
    };
    match val {
        rmpv::Value::Map(entries) => {
            for (k, v) in entries {
                if let rmpv::Value::String(ref s) = k
                    && s.as_str() == Some("weight")
                {
                    return match v {
                        rmpv::Value::F64(f) => f,
                        rmpv::Value::F32(f) => f as f64,
                        rmpv::Value::Integer(i) => i.as_f64().unwrap_or(1.0),
                        _ => 1.0,
                    };
                }
            }
            1.0
        }
        _ => 1.0,
    }
}
