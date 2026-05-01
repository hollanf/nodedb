//! Per-operation cap enforcement for direct Data Plane operations.
//!
//! All caps come from `SharedState::limits`, which is populated at startup
//! from configuration and announced to clients in `HelloAckFrame`.  Absent
//! caps (`None`) mean the server imposes no bound; only `Some(max)` caps are
//! enforced here.

use nodedb_types::protocol::{Limits, TextFields};

use crate::control::state::SharedState;
use crate::error::Error;

/// Check that the fields in `fields` do not exceed any cap stored on `state`.
///
/// Called at the very top of `handle_direct_op`, before planning or dispatch.
/// Returns `Ok(())` when all caps pass; returns `Err(Error::LimitExceeded { … })`
/// on the first violation.
pub(crate) fn check_op_limits(state: &SharedState, fields: &TextFields) -> Result<(), Error> {
    check_limits(&state.limits, fields)
}

/// Inner logic operating on a `&Limits` directly (testable without SharedState).
fn check_limits(limits: &Limits, fields: &TextFields) -> Result<(), Error> {
    // max_vector_dim — query_vector and vector fields carry f32 embeddings.
    if let Some(max) = limits.max_vector_dim {
        if let Some(qv) = &fields.query_vector {
            let dim = qv.len() as u64;
            if dim > max as u64 {
                return Err(Error::LimitExceeded {
                    limit_name: "max_vector_dim",
                    value: dim,
                    max: max as u64,
                });
            }
        }
        if let Some(v) = &fields.vector {
            let dim = v.len() as u64;
            if dim > max as u64 {
                return Err(Error::LimitExceeded {
                    limit_name: "max_vector_dim",
                    value: dim,
                    max: max as u64,
                });
            }
        }
        // Batch vectors.
        if let Some(vecs) = &fields.vectors {
            for bv in vecs {
                let dim = bv.embedding.len() as u64;
                if dim > max as u64 {
                    return Err(Error::LimitExceeded {
                        limit_name: "max_vector_dim",
                        value: dim,
                        max: max as u64,
                    });
                }
            }
        }
    }

    // max_top_k — top_k, top_k_count, vector_top_k, final_top_k fields.
    if let Some(max) = limits.max_top_k {
        for (name, val) in [
            ("max_top_k(top_k)", fields.top_k.map(|v| v as u64)),
            (
                "max_top_k(top_k_count)",
                fields.top_k_count.map(|v| v as u64),
            ),
            (
                "max_top_k(vector_top_k)",
                fields.vector_top_k.map(|v| v as u64),
            ),
            (
                "max_top_k(final_top_k)",
                fields.final_top_k.map(|v| v as u64),
            ),
        ] {
            if let Some(v) = val
                && v > max as u64
            {
                return Err(Error::LimitExceeded {
                    limit_name: name,
                    value: v,
                    max: max as u64,
                });
            }
        }
    }

    // max_scan_limit — limit field.
    if let Some(max) = limits.max_scan_limit
        && let Some(v) = fields.limit
        && v > max as u64
    {
        return Err(Error::LimitExceeded {
            limit_name: "max_scan_limit",
            value: v,
            max: max as u64,
        });
    }

    // max_batch_size — vectors and documents batch arrays.
    if let Some(max) = limits.max_batch_size {
        if let Some(vecs) = &fields.vectors {
            let n = vecs.len() as u64;
            if n > max as u64 {
                return Err(Error::LimitExceeded {
                    limit_name: "max_batch_size(vectors)",
                    value: n,
                    max: max as u64,
                });
            }
        }
        if let Some(docs) = &fields.documents {
            let n = docs.len() as u64;
            if n > max as u64 {
                return Err(Error::LimitExceeded {
                    limit_name: "max_batch_size(documents)",
                    value: n,
                    max: max as u64,
                });
            }
        }
    }

    // max_crdt_delta_bytes — delta field byte length.
    if let Some(max) = limits.max_crdt_delta_bytes
        && let Some(delta) = &fields.delta
    {
        let n = delta.len() as u64;
        if n > max as u64 {
            return Err(Error::LimitExceeded {
                limit_name: "max_crdt_delta_bytes",
                value: n,
                max: max as u64,
            });
        }
    }

    // max_query_text_bytes — query_text field byte length.
    if let Some(max) = limits.max_query_text_bytes
        && let Some(qt) = &fields.query_text
    {
        let n = qt.len() as u64;
        if n > max as u64 {
            return Err(Error::LimitExceeded {
                limit_name: "max_query_text_bytes",
                value: n,
                max: max as u64,
            });
        }
    }

    // max_graph_depth — depth and expansion_depth fields.
    if let Some(max) = limits.max_graph_depth {
        for (name, val) in [
            ("max_graph_depth(depth)", fields.depth.map(|v| v as u64)),
            (
                "max_graph_depth(expansion_depth)",
                fields.expansion_depth.map(|v| v as u64),
            ),
        ] {
            if let Some(v) = val
                && v > max as u64
            {
                return Err(Error::LimitExceeded {
                    limit_name: name,
                    value: v,
                    max: max as u64,
                });
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::protocol::BatchVector;

    fn empty_fields() -> TextFields {
        TextFields::default()
    }

    // ── Passing cases ─────────────────────────────────────────────────────

    #[test]
    fn no_caps_always_pass() {
        let limits = Limits::default();
        let mut fields = empty_fields();
        fields.query_vector = Some(vec![0.1; 2048]);
        fields.top_k = Some(u32::MAX);
        fields.limit = Some(u32::MAX as u64);
        assert!(check_limits(&limits, &fields).is_ok());
    }

    #[test]
    fn under_all_caps_pass() {
        let limits = Limits {
            max_vector_dim: Some(1536),
            max_top_k: Some(1000),
            max_scan_limit: Some(5000),
            max_batch_size: Some(256),
            max_crdt_delta_bytes: Some(1_048_576),
            max_query_text_bytes: Some(4096),
            max_graph_depth: Some(8),
        };
        let mut fields = empty_fields();
        fields.query_vector = Some(vec![0.0; 1536]);
        fields.top_k = Some(1000);
        fields.limit = Some(5000);
        fields.depth = Some(8);
        assert!(check_limits(&limits, &fields).is_ok());
    }

    // ── Rejection cases ───────────────────────────────────────────────────

    #[test]
    fn vector_dim_exceeded() {
        let limits = Limits {
            max_vector_dim: Some(768),
            ..Limits::default()
        };
        let mut fields = empty_fields();
        fields.query_vector = Some(vec![0.0; 769]);
        let err = check_limits(&limits, &fields).unwrap_err();
        assert!(
            matches!(err, Error::LimitExceeded { limit_name, value, max }
            if limit_name == "max_vector_dim" && value == 769 && max == 768)
        );
    }

    #[test]
    fn top_k_exceeded() {
        let limits = Limits {
            max_top_k: Some(100),
            ..Limits::default()
        };
        let mut fields = empty_fields();
        fields.top_k = Some(101);
        let err = check_limits(&limits, &fields).unwrap_err();
        assert!(
            matches!(err, Error::LimitExceeded { limit_name, value, max }
            if value == 101 && max == 100 && limit_name.contains("top_k"))
        );
    }

    #[test]
    fn scan_limit_exceeded() {
        let limits = Limits {
            max_scan_limit: Some(1000),
            ..Limits::default()
        };
        let mut fields = empty_fields();
        fields.limit = Some(1001);
        let err = check_limits(&limits, &fields).unwrap_err();
        assert!(matches!(
            err,
            Error::LimitExceeded {
                limit_name: "max_scan_limit",
                value: 1001,
                max: 1000
            }
        ));
    }

    #[test]
    fn crdt_delta_bytes_exceeded() {
        let limits = Limits {
            max_crdt_delta_bytes: Some(128),
            ..Limits::default()
        };
        let mut fields = empty_fields();
        fields.delta = Some(vec![0u8; 129]);
        let err = check_limits(&limits, &fields).unwrap_err();
        assert!(matches!(
            err,
            Error::LimitExceeded {
                limit_name: "max_crdt_delta_bytes",
                value: 129,
                max: 128
            }
        ));
    }

    #[test]
    fn query_text_bytes_exceeded() {
        let limits = Limits {
            max_query_text_bytes: Some(10),
            ..Limits::default()
        };
        let mut fields = empty_fields();
        fields.query_text = Some("hello world!".into()); // 12 bytes
        let err = check_limits(&limits, &fields).unwrap_err();
        assert!(matches!(
            err,
            Error::LimitExceeded {
                limit_name: "max_query_text_bytes",
                value: 12,
                max: 10
            }
        ));
    }

    #[test]
    fn graph_depth_exceeded() {
        let limits = Limits {
            max_graph_depth: Some(4),
            ..Limits::default()
        };
        let mut fields = empty_fields();
        fields.depth = Some(5);
        let err = check_limits(&limits, &fields).unwrap_err();
        assert!(matches!(
            err,
            Error::LimitExceeded {
                value: 5,
                max: 4,
                ..
            }
        ));
    }

    #[test]
    fn batch_size_exceeded() {
        let limits = Limits {
            max_batch_size: Some(2),
            ..Limits::default()
        };
        let mut fields = empty_fields();
        fields.vectors = Some(vec![
            BatchVector {
                id: "a".into(),
                embedding: vec![0.0],
                metadata: None,
            },
            BatchVector {
                id: "b".into(),
                embedding: vec![0.0],
                metadata: None,
            },
            BatchVector {
                id: "c".into(),
                embedding: vec![0.0],
                metadata: None,
            },
        ]);
        let err = check_limits(&limits, &fields).unwrap_err();
        assert!(matches!(
            err,
            Error::LimitExceeded {
                value: 3,
                max: 2,
                ..
            }
        ));
    }
}
