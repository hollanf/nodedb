//! Compile-time safety tests for Send/!Send invariants.
//!
//! These tests verify that the type system enforces the plane separation:
//! - ControlHandle is Send (can move between Tokio threads)
//! - DataHandle is Send (can be transferred to TPC core during setup)
//! - PinnedDataHandle is !Send (cannot leave the TPC core)

use synapsedb_bridge::{BridgeChannel, ControlHandle, DataHandle};

// Static assertions using trait bounds.
const _: () = {
    fn _assert_send<T: Send>() {}

    fn _control_is_send() {
        _assert_send::<ControlHandle<u64, u64>>();
    }

    fn _data_is_send() {
        _assert_send::<DataHandle<u64, u64>>();
    }
};

/// Verify PinnedDataHandle is !Send via doctest:
///
/// ```compile_fail,E0277
/// fn requires_send<T: Send>() {}
/// requires_send::<synapsedb_bridge::PinnedDataHandle<u64, u64>>();
/// ```
#[test]
fn pin_workflow_roundtrip() {
    let bridge: BridgeChannel<u64, u64> = BridgeChannel::new(16, 16).unwrap();
    let mut control = bridge.control;

    // DataHandle is Send — can be transferred to another thread.
    let data = bridge.data;

    // Pin it to simulate arrival on TPC core.
    let mut pinned = data.pin();

    // Full roundtrip.
    control.try_send_request(42).unwrap();
    let req = pinned.try_recv_request().unwrap();
    assert_eq!(req, 42);

    pinned.try_send_response(84).unwrap();
    let rsp = control.try_recv_response().unwrap();
    assert_eq!(rsp, 84);
}

#[test]
fn pin_throttle_queries_work() {
    let bridge: BridgeChannel<u64, u64> = BridgeChannel::new(16, 16).unwrap();
    let mut control = bridge.control;
    let pinned = bridge.data.pin();

    assert!(!pinned.should_throttle());

    // Fill past 85%.
    for i in 0..14 {
        control.try_send_request(i).unwrap();
    }

    assert!(pinned.should_throttle());
}
