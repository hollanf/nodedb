//! Unit tests for the `CsrIndex` split modules.

use super::types::{CsrIndex, Direction};

fn make_csr() -> CsrIndex {
    let mut csr = CsrIndex::new();
    csr.add_edge("a", "KNOWS", "b").unwrap();
    csr.add_edge("b", "KNOWS", "c").unwrap();
    csr.add_edge("c", "KNOWS", "d").unwrap();
    csr.add_edge("a", "WORKS", "e").unwrap();
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
    csr.add_edge("a", "L", "b").unwrap();
    csr.add_edge("a", "L", "b").unwrap();
    assert_eq!(csr.neighbors("a", None, Direction::Out).len(), 1);
}

#[test]
fn compact_merges_buffer_into_dense() {
    let mut csr = CsrIndex::new();
    csr.add_edge("a", "L", "b").unwrap();
    csr.add_edge("b", "L", "c").unwrap();
    assert_eq!(csr.neighbors("a", None, Direction::Out).len(), 1);

    csr.compact();
    assert!(csr.buffer_out.iter().all(|b| b.is_empty()));
    assert_eq!(csr.neighbors("a", None, Direction::Out).len(), 1);
    assert_eq!(csr.neighbors("b", None, Direction::Out).len(), 1);
}

#[test]
fn compact_handles_deletes() {
    let mut csr = CsrIndex::new();
    csr.add_edge("a", "L", "b").unwrap();
    csr.add_edge("a", "L", "c").unwrap();
    csr.compact();

    csr.remove_edge("a", "L", "b");
    assert_eq!(csr.neighbors("a", None, Direction::Out).len(), 1);

    csr.compact();
    assert_eq!(csr.neighbors("a", None, Direction::Out).len(), 1);
    assert_eq!(csr.neighbors("a", None, Direction::Out)[0].1, "c");
}

#[test]
fn label_interning_reduces_memory() {
    let mut csr = CsrIndex::new();
    for i in 0..100 {
        csr.add_edge(&format!("n{i}"), "FOLLOWS", &format!("n{}", i + 1))
            .unwrap();
    }
    assert_eq!(csr.id_to_label.len(), 1);
    assert_eq!(csr.id_to_label[0], "FOLLOWS");
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

    let restored = CsrIndex::from_checkpoint(&bytes).expect("roundtrip");
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

#[test]
fn out_degree_and_in_degree() {
    let mut csr = CsrIndex::new();
    csr.add_edge("a", "L", "b").unwrap();
    csr.add_edge("a", "L", "c").unwrap();
    csr.add_edge("d", "L", "b").unwrap();

    let a_id = *csr.node_to_id.get("a").unwrap();
    let b_id = *csr.node_to_id.get("b").unwrap();

    assert_eq!(csr.out_degree_raw(a_id), 2);
    assert_eq!(csr.in_degree_raw(b_id), 2);
}

#[test]
fn remove_node_edges_all() {
    let mut csr = CsrIndex::new();
    csr.add_edge("a", "L", "b").unwrap();
    csr.add_edge("a", "L", "c").unwrap();
    csr.add_edge("d", "L", "a").unwrap();

    let removed = csr.remove_node_edges("a");
    assert_eq!(removed, 3);
    assert_eq!(csr.neighbors("a", None, Direction::Out).len(), 0);
    assert_eq!(csr.neighbors("a", None, Direction::In).len(), 0);
}

#[test]
fn surrogate_reverse_lookup_resolves_node_name() {
    use nodedb_types::Surrogate;
    let mut csr = CsrIndex::new();
    csr.add_edge("alice", "KNOWS", "bob").unwrap();
    csr.add_edge("alice", "KNOWS", "carol").unwrap();
    csr.set_node_surrogate("alice", Surrogate(101));
    csr.set_node_surrogate("bob", Surrogate(102));

    assert_eq!(csr.node_id_for_surrogate(Surrogate(101)), Some("alice"));
    assert_eq!(csr.node_id_for_surrogate(Surrogate(102)), Some("bob"));
    // ZERO sentinel never resolves.
    assert_eq!(csr.node_id_for_surrogate(Surrogate(0)), None);
    // Unbound surrogate (carol was never assigned) does not resolve.
    assert_eq!(csr.node_id_for_surrogate(Surrogate(999)), None);
}

#[test]
fn add_node_idempotent() {
    let mut csr = CsrIndex::new();
    let id1 = csr.add_node("x");
    let id2 = csr.add_node("x");
    assert_eq!(id1, id2);
    assert_eq!(csr.node_count(), 1);
}

#[test]
fn node_labels_bitset() {
    let mut csr = CsrIndex::new();
    csr.add_edge("alice", "KNOWS", "bob").unwrap();
    csr.add_edge("acme", "EMPLOYS", "alice").unwrap();

    // Set labels.
    assert!(csr.add_node_label("alice", "Person"));
    assert!(csr.add_node_label("bob", "Person"));
    assert!(csr.add_node_label("acme", "Company"));

    let alice_id = csr.node_id_raw("alice").unwrap();
    let bob_id = csr.node_id_raw("bob").unwrap();
    let acme_id = csr.node_id_raw("acme").unwrap();

    assert!(csr.node_has_label(alice_id, "Person"));
    assert!(!csr.node_has_label(alice_id, "Company"));
    assert!(csr.node_has_label(acme_id, "Company"));
    assert!(!csr.node_has_label(acme_id, "Person"));

    // Multiple labels on same node.
    assert!(csr.add_node_label("alice", "Employee"));
    assert!(csr.node_has_label(alice_id, "Person"));
    assert!(csr.node_has_label(alice_id, "Employee"));
    assert_eq!(csr.node_labels(alice_id), vec!["Person", "Employee"]);

    // Remove label.
    csr.remove_node_label("alice", "Employee");
    assert!(!csr.node_has_label(alice_id, "Employee"));
    assert!(csr.node_has_label(alice_id, "Person"));

    // Non-existent label check returns false.
    assert!(!csr.node_has_label(bob_id, "NonExistent"));
}

/// Spec: edge-label interning MUST assign a distinct id to each distinct
/// label, or fail loudly with an overflow error — never silently alias
/// two unrelated labels to the same id.
///
/// The current `ensure_label` casts `id_to_label.len() as u16`, so the
/// 65 537th label aliases id 1, cross-wiring its edges with whatever
/// label first took id 1. Any correct fix must satisfy both invariants
/// below for every (label, id) pair returned from the interner.
///
/// Regression guard: distinct label → distinct id AND round-trip through
/// `label_name(id) == label`. Aliasing would break the round-trip.
#[test]
fn edge_label_interning_does_not_alias_past_u16_max() {
    let mut csr = CsrIndex::new();

    // Push past the u16 boundary. 65_537 distinct labels forces the bug:
    // label 65_536 receives id = (65_536 as u16) = 0, aliasing id 0.
    const N: usize = 65_537;
    let mut ids: Vec<u32> = Vec::with_capacity(N);
    for i in 0..N {
        let label = format!("l_{i}");
        csr.add_edge("src", &label, "dst").unwrap();
        let id = csr
            .label_id(&label)
            .expect("label_id must resolve just-inserted label");
        ids.push(id);
    }

    // Distinct labels → distinct ids.
    let unique: std::collections::HashSet<u32> = ids.iter().copied().collect();
    assert_eq!(
        unique.len(),
        N,
        "every distinct label must map to a distinct id; got {} unique ids for {} labels",
        unique.len(),
        N
    );

    // Round-trip: label_name(id) returns the label we inserted.
    for (i, &id) in ids.iter().enumerate() {
        let name = csr.label_name(id);
        assert_eq!(
            name,
            format!("l_{i}"),
            "label_name({id}) must round-trip to inserted label l_{i}; got {name:?}"
        );
    }
}

/// Spec: edge-label interning is stable across `compact()`. A label id
/// assigned before compaction must still resolve to the same string
/// after the buffer→dense merge, and `label_id()` must still resolve
/// the original label to the same id. Any fix that widens label ids
/// (u16 → u32) MUST preserve this across the compaction path.
#[test]
fn edge_label_ids_survive_compaction() {
    let mut csr = CsrIndex::new();
    // Spread a moderate number of labels across many edges so
    // compaction actually touches the label table.
    const N: usize = 512;
    for i in 0..N {
        csr.add_edge(&format!("src_{i}"), &format!("L_{i}"), &format!("dst_{i}"))
            .unwrap();
    }

    let before: Vec<u32> = (0..N)
        .map(|i| csr.label_id(&format!("L_{i}")).expect("label present"))
        .collect();

    csr.compact();

    for (i, &id) in before.iter().enumerate() {
        let after_id = csr
            .label_id(&format!("L_{i}"))
            .expect("label must remain resolvable after compact");
        assert_eq!(
            after_id, id,
            "label id for L_{i} must be stable across compact(); before={id} after={after_id}"
        );
        assert_eq!(
            csr.label_name(id),
            format!("L_{i}"),
            "label_name({id}) must still round-trip after compact"
        );
    }
}
