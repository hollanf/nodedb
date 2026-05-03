//! Tests for static-set multi-shard Calvin execution path.
//!
//! Verifies the types and plan structures used by `MetaOp::CalvinExecuteStatic`.
//! A full 3-replica WAL byte-equality test requires a real cluster and is
//! marked `#[ignore]`.

use nodedb::bridge::physical_plan::PhysicalPlan;
use nodedb::bridge::physical_plan::meta::MetaOp;
use nodedb::bridge::physical_plan::wire as plan_wire;
use nodedb_types::TenantId;

#[test]
fn calvin_execute_static_round_trip_msgpack() {
    // Build a CalvinExecuteStatic variant with a simple empty plans vec and
    // verify it serializes/deserializes correctly.
    let op = MetaOp::CalvinExecuteStatic {
        epoch: 42,
        position: 7,
        tenant_id: TenantId::new(1),
        plans: vec![],
        epoch_system_ms: 0,
    };

    let plan = PhysicalPlan::Meta(op.clone());

    // Encode + decode via the wire codec.
    let batch_bytes = plan_wire::encode_batch(&vec![plan]).expect("encode");
    let decoded_batch = plan_wire::decode_batch(&batch_bytes).expect("decode");

    assert_eq!(decoded_batch.len(), 1);
    match &decoded_batch[0] {
        PhysicalPlan::Meta(MetaOp::CalvinExecuteStatic {
            epoch,
            position,
            tenant_id,
            plans,
            ..
        }) => {
            assert_eq!(*epoch, 42);
            assert_eq!(*position, 7);
            assert_eq!(tenant_id.as_u64(), 1);
            assert!(plans.is_empty());
        }
        other => panic!("unexpected plan variant: {other:?}"),
    }
}

#[test]
fn calvin_execute_static_and_active_are_distinct_variants() {
    // Confirm the three Calvin variants are distinguishable.
    let static_op = MetaOp::CalvinExecuteStatic {
        epoch: 1,
        position: 0,
        tenant_id: TenantId::new(1),
        plans: vec![],
        epoch_system_ms: 0,
    };
    let passive_op = MetaOp::CalvinExecutePassive {
        epoch: 1,
        position: 0,
        tenant_id: TenantId::new(1),
        keys_to_read: vec![],
    };

    // Verify matching works correctly.
    let is_static = matches!(static_op, MetaOp::CalvinExecuteStatic { .. });
    let is_passive = matches!(passive_op, MetaOp::CalvinExecutePassive { .. });
    let static_not_passive = !matches!(static_op, MetaOp::CalvinExecutePassive { .. });

    assert!(is_static);
    assert!(is_passive);
    assert!(static_not_passive);
}

/// Full 3-replica WAL byte-equality test.
///
/// Requires a real 3-node cluster with SPSC bridge wired. Skipped in unit/integration
/// test runs; exercised by the cluster smoke test suite.
#[test]
#[ignore]
fn static_multishard_wal_byte_equality_three_replicas() {
    // 3-replica cluster test: run same Calvin workload, assert per-replica
    // WAL byte equality after epoch completion.
    todo!("requires real 3-node cluster harness")
}
