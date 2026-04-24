//! Bitemporal CRDT sync: version preservation + live-scoped UNIQUE +
//! receiver-stamped system time.
//!
//! The CRDT engine must:
//! - Scope UNIQUE checks to currently-live rows (`_ts_valid_until == MAX`)
//!   so a superseded version does not falsely collide with a new live row.
//! - Stamp `_ts_system` with the receiving node's clock on `validate_and_apply`,
//!   ignoring whatever the sender supplied — keeping system-time receiver-
//!   authoritative for convergence.

use loro::LoroValue;

use nodedb::engine::crdt::tenant_state::TenantCrdtEngine;
use nodedb::types::TenantId;
use nodedb_crdt::CrdtAuthContext;
use nodedb_crdt::constraint::ConstraintSet;
use nodedb_crdt::policy::CollectionPolicy;
use nodedb_crdt::validator::ProposedChange;
use nodedb_crdt::validator::bitemporal::VALID_UNTIL_OPEN;

const USERS: &str = "users";

fn tenant() -> TenantCrdtEngine {
    let mut cs = ConstraintSet::new();
    cs.add_unique("users_email_unique", USERS, "email");
    let mut eng = TenantCrdtEngine::new(TenantId::new(1), 1, cs).unwrap();
    eng.mark_bitemporal(USERS);
    eng.set_collection_policy_typed(USERS, CollectionPolicy::strict());
    eng
}

fn change(
    row_id: &str,
    email: &str,
    valid_from: i64,
    valid_until: i64,
    sender_system_ts: i64,
) -> ProposedChange {
    ProposedChange {
        collection: USERS.into(),
        row_id: row_id.into(),
        fields: vec![
            ("name".into(), LoroValue::String("Alice".into())),
            ("email".into(), LoroValue::String(email.into())),
            ("_ts_valid_from".into(), LoroValue::I64(valid_from)),
            ("_ts_valid_until".into(), LoroValue::I64(valid_until)),
            ("_ts_system".into(), LoroValue::I64(sender_system_ts)),
        ],
    }
}

/// A superseded version with the same email as a new live row should not
/// trigger a UNIQUE collision: the old version is no longer live.
#[test]
fn bitemporal_unique_scoped_to_current() {
    let mut eng = tenant();

    // Version 1: Alice at alice@old.com, terminated at t=1000.
    eng.validate_and_apply(
        1,
        CrdtAuthContext::default(),
        &change("u1-v1", "alice@old.com", 0, 1_000, 100),
        b"delta1".to_vec(),
    )
    .expect("v1 should accept");

    // A second user insert at the SAME email (old one) — but the old row is
    // no longer live, so this must succeed, not collide.
    eng.validate_and_apply(
        1,
        CrdtAuthContext::default(),
        &change("u2-v1", "alice@old.com", 2_000, VALID_UNTIL_OPEN, 200),
        b"delta2".to_vec(),
    )
    .expect("reusing superseded email should not collide");
}

/// Two currently-live rows with the same UNIQUE value MUST collide even in
/// bitemporal mode — bitemporal relaxes only the old-vs-new axis.
#[test]
fn bitemporal_unique_still_collides_for_live_rows() {
    let mut eng = tenant();

    eng.validate_and_apply(
        1,
        CrdtAuthContext::default(),
        &change("u1", "bob@example.com", 0, VALID_UNTIL_OPEN, 100),
        b"delta1".to_vec(),
    )
    .expect("first live insert accepted");

    let err = eng.validate_and_apply(
        1,
        CrdtAuthContext::default(),
        &change("u2", "bob@example.com", 0, VALID_UNTIL_OPEN, 200),
        b"delta2".to_vec(),
    );
    assert!(
        err.is_err(),
        "two live rows with same UNIQUE value must collide"
    );
}

/// The sender's `_ts_system` must be overwritten with receiver's clock on
/// apply — receiver-authoritative system time.
#[test]
fn bitemporal_crdt_system_ts_receiver_stamped() {
    let mut eng = tenant();

    let sender_ts = 100_000i64; // some arbitrary sender-supplied stamp
    let before_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    eng.validate_and_apply(
        1,
        CrdtAuthContext::default(),
        &change("u1", "c@example.com", 0, VALID_UNTIL_OPEN, sender_ts),
        b"delta".to_vec(),
    )
    .unwrap();

    let row = eng.read_row(USERS, "u1").expect("row present");
    let map = match row {
        LoroValue::Map(m) => m,
        other => panic!("expected map, got {other:?}"),
    };
    let stamped = match map.get("_ts_system") {
        Some(LoroValue::I64(n)) => *n,
        other => panic!("expected i64 _ts_system, got {other:?}"),
    };
    assert_ne!(
        stamped, sender_ts,
        "receiver must overwrite sender-supplied _ts_system"
    );
    assert!(
        stamped >= before_ms,
        "stamped ts ({stamped}) must be >= receiver clock at apply ({before_ms})"
    );
}
