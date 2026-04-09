//! Integration tests for transaction batch execution.

use nodedb::bridge::envelope::{PhysicalPlan, Status};
use nodedb::bridge::physical_plan::{DocumentOp, MetaOp, VectorOp};

use crate::helpers::*;

#[test]
fn transaction_batch_commits_atomically() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    let resp = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Meta(MetaOp::TransactionBatch {
            plans: vec![
                PhysicalPlan::Document(DocumentOp::PointPut {
                    collection: "docs".into(),
                    document_id: "d1".into(),
                    value: b"{\"name\":\"alice\"}".to_vec(),
                }),
                PhysicalPlan::Document(DocumentOp::PointPut {
                    collection: "docs".into(),
                    document_id: "d2".into(),
                    value: b"{\"name\":\"bob\"}".to_vec(),
                }),
            ],
        }),
    );
    assert_eq!(resp.status, Status::Ok);

    // Verify both documents exist via PointGet.
    let r1 = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointGet {
            collection: "docs".into(),
            document_id: "d1".into(),
            rls_filters: Vec::new(),
        }),
    );
    assert_eq!(r1.status, Status::Ok);

    let r2 = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointGet {
            collection: "docs".into(),
            document_id: "d2".into(),
            rls_filters: Vec::new(),
        }),
    );
    assert_eq!(r2.status, Status::Ok);
}

#[test]
fn transaction_batch_response_uses_outer_request_id() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    tx.try_push(nodedb::bridge::dispatch::BridgeRequest {
        inner: make_request_with_id(
            42,
            PhysicalPlan::Meta(MetaOp::TransactionBatch {
                plans: vec![PhysicalPlan::Document(DocumentOp::PointPut {
                    collection: "docs".into(),
                    document_id: "d1".into(),
                    value: b"{\"name\":\"alice\"}".to_vec(),
                })],
            }),
        ),
    })
    .unwrap();
    core.tick();

    let resp = rx.try_pop().unwrap().inner;
    assert_eq!(resp.status, Status::Ok);
    assert_eq!(resp.request_id, nodedb::types::RequestId::new(42));
}

#[test]
fn transaction_batch_rollback_on_failure() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Pre-insert d1 via SPSC.
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection: "docs".into(),
            document_id: "d1".into(),
            value: b"original".to_vec(),
        }),
    );

    // Create a vector index via SetVectorParams so we have a known dimension.
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Vector(VectorOp::SetParams {
            collection: "emb".into(),
            m: 16,
            ef_construction: 200,
            metric: "cosine".into(),
            index_type: String::new(),
            pq_m: 0,
            ivf_cells: 0,
            ivf_nprobe: 0,
        }),
    );
    // Insert one vector to create the index with dim=3.
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Vector(VectorOp::Insert {
            collection: "emb".into(),
            vector: vec![1.0, 2.0, 3.0],
            dim: 3,
            field_name: String::new(),
            doc_id: None,
        }),
    );

    // TransactionBatch: overwrite d1, then fail with wrong dimension.
    let resp = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Meta(MetaOp::TransactionBatch {
            plans: vec![
                PhysicalPlan::Document(DocumentOp::PointPut {
                    collection: "docs".into(),
                    document_id: "d1".into(),
                    value: b"{\"name\":\"modified\"}".to_vec(),
                }),
                // Dimension mismatch: index is dim=3 but vector has 2 elements.
                PhysicalPlan::Vector(VectorOp::Insert {
                    collection: "emb".into(),
                    vector: vec![1.0, 2.0],
                    dim: 3,
                    field_name: String::new(),
                    doc_id: None,
                }),
            ],
        }),
    );
    assert_eq!(resp.status, Status::Error);

    // d1 should be rolled back to original value.
    let r = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointGet {
            collection: "docs".into(),
            document_id: "d1".into(),
            rls_filters: Vec::new(),
        }),
    );
    assert_eq!(r.status, Status::Ok);
    assert_eq!(&*r.payload, b"original");
}
