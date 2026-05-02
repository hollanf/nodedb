//! Calvin cross-shard receive path: `MetaOp::TransactionBatch` applied via
//! `ReplicatedWrite::CrossShardForward`.
//!
//! Tests:
//!   1. Successful forward — both shards have the row after apply.
//!   2. Failing forward — shard B rolls back; error surfaces correctly.
//!   3. Serialization roundtrips — `ForwardEntry`, `AbortNotice`,
//!      `ReplicatedWrite::CrossShardForward`, `decode_forwarded_plans`.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use nodedb::bridge::dispatch::{BridgeRequest, BridgeResponse};
use nodedb::bridge::envelope::{ErrorCode, PhysicalPlan, Priority, Request, Status};
use nodedb::bridge::physical_plan::{KvOp, MetaOp, VectorOp};
use nodedb::control::wal_replication::{ReplicatedEntry, ReplicatedWrite, decode_forwarded_plans};
use nodedb::data::executor::core_loop::CoreLoop;
use nodedb::types::*;
use nodedb_bridge::buffer::{Consumer, Producer, RingBuffer};
use nodedb_cluster::cross_shard_txn::{
    AbortNotice, CrossShardTransaction, ForwardEntry, TransactionCoordinator,
};
use nodedb_types::OrdinalClock;

// ── Helpers ─────────────────────────────────────────────────────────────────

fn make_core() -> (
    CoreLoop,
    Producer<BridgeRequest>,
    Consumer<BridgeResponse>,
    tempfile::TempDir,
) {
    let dir = tempfile::tempdir().unwrap();
    let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
    let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
    let core = CoreLoop::open(
        0,
        req_rx,
        resp_tx,
        dir.path(),
        Arc::new(OrdinalClock::new()),
    )
    .unwrap();
    (core, req_tx, resp_rx, dir)
}

fn make_request(plan: PhysicalPlan) -> Request {
    Request {
        request_id: RequestId::new(1),
        tenant_id: TenantId::new(1),
        vshard_id: VShardId::new(0),
        plan,
        deadline: Instant::now() + Duration::from_secs(5),
        priority: Priority::Normal,
        trace_id: nodedb_types::TraceId::ZERO,
        consistency: ReadConsistency::Strong,
        idempotency_key: None,
        event_source: nodedb::event::EventSource::RaftFollower,
        user_roles: Vec::new(),
    }
}

fn send_raw(
    core: &mut CoreLoop,
    tx: &mut Producer<BridgeRequest>,
    rx: &mut Consumer<BridgeResponse>,
    plan: PhysicalPlan,
) -> nodedb::bridge::envelope::Response {
    tx.try_push(BridgeRequest {
        inner: make_request(plan),
    })
    .unwrap();
    core.tick();
    rx.try_pop().unwrap().inner
}

fn send_ok(
    core: &mut CoreLoop,
    tx: &mut Producer<BridgeRequest>,
    rx: &mut Consumer<BridgeResponse>,
    plan: PhysicalPlan,
) -> Vec<u8> {
    let resp = send_raw(core, tx, rx, plan);
    assert_eq!(
        resp.status,
        Status::Ok,
        "expected Ok, got {:?}",
        resp.error_code
    );
    resp.payload.to_vec()
}

fn kv_put(coll: &str, key: &[u8], value: &[u8]) -> PhysicalPlan {
    PhysicalPlan::Kv(KvOp::Put {
        collection: coll.to_string(),
        key: key.to_vec(),
        value: value.to_vec(),
        ttl_ms: 0,
        surrogate: nodedb_types::Surrogate::ZERO,
    })
}

fn kv_get(coll: &str, key: &[u8]) -> PhysicalPlan {
    PhysicalPlan::Kv(KvOp::Get {
        collection: coll.to_string(),
        key: key.to_vec(),
        rls_filters: Vec::new(),
    })
}

/// Encode `plans` as MessagePack — the same encoding the Calvin coordinator
/// uses when building `CrossShardForward.plans_bytes`.
fn encode_plans(plans: Vec<PhysicalPlan>) -> Vec<u8> {
    zerompk::to_msgpack_vec(&plans).expect("plans encode must not fail")
}

// ── Test 1: successful cross-shard forward ───────────────────────────────────

/// A `CrossShardForward` entry decoded into a `TransactionBatch` must apply
/// atomically. After a successful apply the written key must be readable.
#[test]
fn cross_shard_forward_success() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    let plans = vec![kv_put("orders", b"k1", b"v1")];
    let plans_bytes = encode_plans(plans);

    let decoded_plans =
        decode_forwarded_plans(&plans_bytes).expect("decode_forwarded_plans must succeed");

    let batch_plan = PhysicalPlan::Meta(MetaOp::TransactionBatch {
        plans: decoded_plans,
    });

    let resp = send_raw(&mut core, &mut tx, &mut rx, batch_plan);
    assert_eq!(
        resp.status,
        Status::Ok,
        "cross-shard forward apply must succeed; got {:?}",
        resp.error_code
    );

    // Verify the row is present after apply.
    let payload = send_ok(&mut core, &mut tx, &mut rx, kv_get("orders", b"k1"));
    assert!(
        !payload.is_empty(),
        "row must exist after successful forward apply"
    );
}

// ── Test 2: failing forward → error without RollbackFailed ──────────────────

/// When a sub-plan in the forwarded batch fails, the batch must roll back
/// cleanly. The response must be `Status::Error` and must NOT be
/// `RollbackFailed` (the rollback itself must succeed).
///
/// We trigger the failure via a `VectorOp::Insert` with a dimension mismatch:
/// the vector index is configured for dim=3 but we insert a dim=2 vector.
/// This is the same pattern used by the single-shard cross-engine tests.
#[test]
fn cross_shard_forward_apply_failure_rolls_back_cleanly() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Configure and seed the vector collection (dim=3).
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Vector(VectorOp::SetParams {
            collection: "vec_coll".to_string(),
            m: 16,
            ef_construction: 200,
            metric: "cosine".into(),
            index_type: String::new(),
            pq_m: 0,
            ivf_cells: 0,
            ivf_nprobe: 0,
        }),
    );
    // Seed one valid vector so the index knows dim=3.
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Vector(VectorOp::Insert {
            collection: "vec_coll".to_string(),
            vector: vec![1.0, 2.0, 3.0],
            dim: 3,
            field_name: String::new(),
            surrogate: nodedb_types::Surrogate::ZERO,
        }),
    );

    let plans = vec![
        // First sub-plan succeeds.
        kv_put("rollback_coll", b"should_be_gone", b"present"),
        // Second sub-plan fails: dim mismatch (index expects 3, we provide 2).
        PhysicalPlan::Vector(VectorOp::Insert {
            collection: "vec_coll".to_string(),
            vector: vec![1.0, 2.0],
            dim: 3,
            field_name: String::new(),
            surrogate: nodedb_types::Surrogate::new(99),
        }),
    ];
    let plans_bytes = encode_plans(plans);
    let decoded_plans = decode_forwarded_plans(&plans_bytes).expect("decode");

    let resp = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Meta(MetaOp::TransactionBatch {
            plans: decoded_plans,
        }),
    );

    assert_eq!(
        resp.status,
        Status::Error,
        "a failing sub-plan must cause the batch to fail; got {:?}",
        resp.error_code
    );
    // Rollback must succeed — the engine must not be in an unknown state.
    assert!(
        !matches!(resp.error_code, Some(ErrorCode::RollbackFailed { .. })),
        "rollback must succeed on failure path; got {:?}",
        resp.error_code
    );

    // The first sub-plan's write ("should_be_gone") must have been rolled back.
    let get_resp = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        kv_get("rollback_coll", b"should_be_gone"),
    );
    // KV returns empty payload (not found) after rollback.
    assert!(
        get_resp.payload.is_empty() || get_resp.status == Status::Error,
        "rolled-back write must not persist; got {:?}",
        get_resp
    );
}

// ── Test 3: serialization roundtrips ─────────────────────────────────────────

/// `ForwardEntry` with the new `tenant_id` field must survive a MessagePack
/// encode/decode cycle without field loss.
#[test]
fn forward_entry_tenant_id_roundtrip() {
    let entry = ForwardEntry {
        txn_id: 77,
        tenant_id: 42,
        writes: b"payload".to_vec(),
        source_vshard: 3,
        coordinator_log_index: 500,
    };
    let bytes = zerompk::to_msgpack_vec(&entry).expect("encode");
    let decoded: ForwardEntry = zerompk::from_msgpack(&bytes).expect("decode");
    assert_eq!(decoded.txn_id, 77);
    assert_eq!(decoded.tenant_id, 42);
    assert_eq!(decoded.source_vshard, 3);
    assert_eq!(decoded.coordinator_log_index, 500);
}

/// `AbortNotice` must survive a MessagePack encode/decode cycle.
#[test]
fn abort_notice_roundtrip() {
    let notice = AbortNotice {
        txn_id: 123,
        reason: "apply error: collection not found".into(),
    };
    let bytes = zerompk::to_msgpack_vec(&notice).expect("encode");
    let decoded: AbortNotice = zerompk::from_msgpack(&bytes).expect("decode");
    assert_eq!(decoded.txn_id, 123);
    assert_eq!(decoded.reason, "apply error: collection not found");
}

/// `generate_forwards` must embed `tenant_id` from the parent transaction into
/// every `ForwardEntry`.
#[test]
fn generate_forwards_propagates_tenant_id() {
    let txn = CrossShardTransaction {
        txn_id: 55,
        tenant_id: 9,
        shard_writes: vec![(10, b"w1".to_vec()), (20, b"w2".to_vec())],
        coordinator_node: 1,
        coordinator_log_index: 200,
    };
    let forwards = TransactionCoordinator::generate_forwards(&txn);
    assert_eq!(forwards.len(), 2);
    for (_, entry) in &forwards {
        assert_eq!(
            entry.tenant_id, 9,
            "tenant_id must propagate to ForwardEntry"
        );
        assert_eq!(entry.txn_id, 55);
    }
}

/// `decode_forwarded_plans` must return a descriptive typed error on garbage
/// input rather than panicking or silently returning empty.
#[test]
fn decode_forwarded_plans_rejects_garbage() {
    let result = decode_forwarded_plans(b"not valid msgpack \xff\x00");
    assert!(result.is_err(), "garbage input must produce an error");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("failed to decode forwarded cross-shard plans"),
        "error must be descriptive; got: {err_msg}"
    );
}

/// `ReplicatedWrite::CrossShardForward` must round-trip through MessagePack
/// encoding (the path the Raft log uses).
#[test]
fn replicated_write_cross_shard_forward_roundtrip() {
    let plans = vec![kv_put("rt_coll", b"key", b"val")];
    let plans_bytes = encode_plans(plans);

    let entry = ReplicatedEntry::new(
        1,
        0,
        ReplicatedWrite::CrossShardForward {
            txn_id: 88,
            tenant_id: 5,
            plans_bytes: plans_bytes.clone(),
            source_vshard: 2,
            coordinator_log_index: 300,
        },
    );

    let bytes = entry.to_bytes();
    let decoded = ReplicatedEntry::from_bytes(&bytes).expect("must decode");

    match decoded.write {
        ReplicatedWrite::CrossShardForward {
            txn_id,
            tenant_id,
            plans_bytes: decoded_pb,
            source_vshard,
            coordinator_log_index,
        } => {
            assert_eq!(txn_id, 88);
            assert_eq!(tenant_id, 5);
            assert_eq!(source_vshard, 2);
            assert_eq!(coordinator_log_index, 300);
            assert_eq!(decoded_pb, plans_bytes);
        }
        other => panic!("expected CrossShardForward, got {:?}", other),
    }
}

/// `decode_forwarded_plans` must faithfully reconstruct a `Vec<PhysicalPlan>`
/// encoded by `encode_plans`.
#[test]
fn decode_forwarded_plans_roundtrip() {
    let original = vec![kv_put("c1", b"k1", b"v1"), kv_put("c2", b"k2", b"v2")];
    let bytes = encode_plans(original);
    let decoded = decode_forwarded_plans(&bytes).expect("decode must succeed");
    assert_eq!(decoded.len(), 2, "decoded plan count must match original");
}
