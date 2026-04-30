//! RPC frame header layout and framing helpers.
//!
//! # Wire layouts
//!
//! v1/v2 (legacy, 10 bytes — accepted on inbound for rolling-upgrade
//! N-1 compatibility):
//!
//! ```text
//! ┌─────────┬──────────┬────────────┬──────────┬─────────────────────┐
//! │ version │ rpc_type │ payload_len│ crc32c   │ rkyv payload bytes  │
//! │  1 byte │  1 byte  │  4 bytes   │ 4 bytes  │  payload_len bytes  │
//! └─────────┴──────────┴────────────┴──────────┴─────────────────────┘
//! ```
//!
//! v3 (current, 18 bytes — emitted by every encoder; carries the
//! cluster epoch fence token; see `cluster_epoch.rs`):
//!
//! ```text
//! ┌─────────┬──────────┬────────────┬──────────┬───────────────┬─────────────────────┐
//! │ version │ rpc_type │ payload_len│ crc32c   │ cluster_epoch │ rkyv payload bytes  │
//! │  1 byte │  1 byte  │  4 bytes   │ 4 bytes  │   8 bytes LE  │  payload_len bytes  │
//! └─────────┴──────────┴────────────┴──────────┴───────────────┴─────────────────────┘
//! ```
//!
//! Receivers branch on the version byte: v1/v2 frames decode with
//! `peer_epoch = 0` (no observation); v3 frames extract the epoch and
//! call [`crate::cluster_epoch::observe_peer_cluster_epoch`].

use crate::cluster_epoch::{current_local_cluster_epoch, observe_peer_cluster_epoch};
use crate::error::{ClusterError, Result};
use crate::wire::WIRE_VERSION;

/// Header size in bytes for v1/v2: version(1) + rpc_type(1) + payload_len(4) + crc32c(4).
pub const HEADER_SIZE_V2: usize = 10;

/// Header size in bytes for v3: v2 layout plus an 8-byte cluster_epoch.
pub const HEADER_SIZE_V3: usize = HEADER_SIZE_V2 + 8;

/// Header size emitted by the current encoder (v3). Kept as `HEADER_SIZE`
/// so callers that pre-allocate scratch buffers don't have to track the
/// version themselves.
pub const HEADER_SIZE: usize = HEADER_SIZE_V3;

/// Wire-version byte stamped on every v3 frame.
const WIRE_VERSION_V3: u8 = 3;

/// Minimum RPC frame wire version this node will accept on inbound frames.
///
/// Senders always stamp `WIRE_VERSION_V3`. Receivers accept any version
/// in `[MIN_SUPPORTED_RPC_VERSION..=WIRE_VERSION_V3]`, honoring the
/// documented N-1 rolling-upgrade promise on `version.rs`. v1/v2 frames
/// decode with `peer_epoch = 0` (no fence-token observation).
pub const MIN_SUPPORTED_RPC_VERSION: u8 = 1;

/// Maximum RPC message payload size (64 MiB). Distinct from WAL's MAX_RPC_PAYLOAD_SIZE.
///
/// Prevents degenerate allocations from corrupt frames.
pub const MAX_RPC_PAYLOAD_SIZE: u32 = 64 * 1024 * 1024;

/// Write a framed v3 header + payload into `out`.
///
/// `rpc_type` is the discriminant byte; `payload` is the already-serialized
/// body. The current cluster epoch (read from
/// [`current_local_cluster_epoch`]) is stamped into the header.
pub fn write_frame(rpc_type: u8, payload: &[u8], out: &mut Vec<u8>) -> Result<()> {
    let payload_len: u32 = payload.len().try_into().map_err(|_| ClusterError::Codec {
        detail: format!("payload too large: {} bytes", payload.len()),
    })?;
    let crc = crc32c::crc32c(payload);
    let epoch = current_local_cluster_epoch();
    // `WIRE_VERSION` (the negotiated cluster wire version) and the v3
    // RPC-frame version are independent: WIRE_VERSION is 2 today and is
    // referenced by `wire.rs` for the VShardEnvelope binary layout.
    debug_assert!(
        WIRE_VERSION as u8 <= WIRE_VERSION_V3,
        "WIRE_VERSION should not race ahead of the RPC frame format"
    );
    out.push(WIRE_VERSION_V3);
    out.push(rpc_type);
    out.extend_from_slice(&payload_len.to_le_bytes());
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(&epoch.to_le_bytes());
    out.extend_from_slice(payload);
    Ok(())
}

/// Validate the CRC32C of an inbound frame and return the payload slice.
///
/// `data` must start at byte 0 (version byte). Returns `(rpc_type, payload)`.
///
/// Side effect: for v3 frames, observes the peer's cluster epoch via
/// [`observe_peer_cluster_epoch`] (monotonic max). v1/v2 frames are
/// passed through unchanged (epoch defaults to 0).
pub fn parse_frame(data: &[u8]) -> Result<(u8, &[u8])> {
    if data.is_empty() {
        return Err(ClusterError::Codec {
            detail: format!(
                "frame too short: 0 bytes, need {HEADER_SIZE_V2}"
            ),
        });
    }

    let version = data[0];
    if !(MIN_SUPPORTED_RPC_VERSION..=WIRE_VERSION_V3).contains(&version) {
        return Err(ClusterError::UnsupportedWireVersion {
            got: version,
            supported_min: MIN_SUPPORTED_RPC_VERSION,
            supported_max: WIRE_VERSION_V3,
        });
    }

    let header_size = if version >= WIRE_VERSION_V3 {
        HEADER_SIZE_V3
    } else {
        HEADER_SIZE_V2
    };
    if data.len() < header_size {
        return Err(ClusterError::Codec {
            detail: format!(
                "frame too short: {} bytes, need {header_size} for v{version}",
                data.len()
            ),
        });
    }

    let rpc_type = data[1];
    let payload_len = u32::from_le_bytes([data[2], data[3], data[4], data[5]]);
    let expected_crc = u32::from_le_bytes([data[6], data[7], data[8], data[9]]);
    let peer_epoch = if version >= WIRE_VERSION_V3 {
        u64::from_le_bytes([
            data[10], data[11], data[12], data[13], data[14], data[15], data[16], data[17],
        ])
    } else {
        0
    };

    if payload_len > MAX_RPC_PAYLOAD_SIZE {
        return Err(ClusterError::Codec {
            detail: format!("payload length {payload_len} exceeds maximum {MAX_RPC_PAYLOAD_SIZE}"),
        });
    }

    let expected_total = header_size + payload_len as usize;
    if data.len() < expected_total {
        return Err(ClusterError::Codec {
            detail: format!(
                "frame truncated: got {} bytes, expected {expected_total}",
                data.len()
            ),
        });
    }

    let payload = &data[header_size..expected_total];
    let actual_crc = crc32c::crc32c(payload);
    if actual_crc != expected_crc {
        return Err(ClusterError::Codec {
            detail: format!(
                "CRC32C mismatch: expected {expected_crc:#010x}, got {actual_crc:#010x}"
            ),
        });
    }

    if peer_epoch > 0 {
        observe_peer_cluster_epoch(peer_epoch);
    }

    Ok((rpc_type, payload))
}

/// Return the total frame size for a buffer that starts with a valid header.
///
/// The buffer must be at least [`HEADER_SIZE_V3`] bytes; for v1/v2 frames
/// only the leading [`HEADER_SIZE_V2`] bytes are read and the rest is
/// ignored.
pub fn frame_size(header: &[u8; HEADER_SIZE_V3]) -> Result<usize> {
    let version = header[0];
    let header_size = if version >= WIRE_VERSION_V3 {
        HEADER_SIZE_V3
    } else {
        HEADER_SIZE_V2
    };
    let payload_len = u32::from_le_bytes([header[2], header[3], header[4], header[5]]);
    if payload_len > MAX_RPC_PAYLOAD_SIZE {
        return Err(ClusterError::Codec {
            detail: format!("payload length {payload_len} exceeds maximum {MAX_RPC_PAYLOAD_SIZE}"),
        });
    }
    Ok(header_size + payload_len as usize)
}

// rkyv_deserialize and rkyv_serialize are macros in each sub-module because
// rkyv's generic bounds for Serialize and Deserialize are cumbersome to
// express generically across all types. Each sub-module calls rkyv directly.

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a v1/v2-shape frame (10-byte header, no epoch field).
    fn make_frame(version: u8, payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(HEADER_SIZE_V2 + payload.len());
        let payload_len = payload.len() as u32;
        let crc = crc32c::crc32c(payload);
        out.push(version);
        out.push(0xAB); // arbitrary rpc_type
        out.extend_from_slice(&payload_len.to_le_bytes());
        out.extend_from_slice(&crc.to_le_bytes());
        out.extend_from_slice(payload);
        out
    }

    /// Build a v3-shape frame (18-byte header, with epoch).
    fn make_frame_v3(payload: &[u8], epoch: u64) -> Vec<u8> {
        let mut out = Vec::with_capacity(HEADER_SIZE_V3 + payload.len());
        let payload_len = payload.len() as u32;
        let crc = crc32c::crc32c(payload);
        out.push(WIRE_VERSION_V3);
        out.push(0xAB);
        out.extend_from_slice(&payload_len.to_le_bytes());
        out.extend_from_slice(&crc.to_le_bytes());
        out.extend_from_slice(&epoch.to_le_bytes());
        out.extend_from_slice(payload);
        out
    }

    #[test]
    fn parse_frame_accepts_n_minus_one() {
        // N-1: WIRE_VERSION currently 2, so N-1 = 1, which is the minimum.
        // If WIRE_VERSION grows, MIN_SUPPORTED_RPC_VERSION grows with it
        // (or this test stays valid because 1 is still in range).
        let payload = b"hello";
        let frame = make_frame(MIN_SUPPORTED_RPC_VERSION, payload);
        let (rpc_type, body) = parse_frame(&frame).expect("N-1 must be accepted");
        assert_eq!(rpc_type, 0xAB);
        assert_eq!(body, payload);
    }

    #[test]
    fn parse_frame_accepts_current_version() {
        let payload = b"world";
        let frame = make_frame_v3(payload, 0);
        let (_t, body) = parse_frame(&frame).expect("current version must be accepted");
        assert_eq!(body, payload);
    }

    #[test]
    fn parse_frame_rejects_n_plus_one() {
        let payload = b"future";
        let frame = make_frame(WIRE_VERSION_V3.saturating_add(1), payload);
        let err = parse_frame(&frame).expect_err("N+1 must be rejected");
        match err {
            ClusterError::UnsupportedWireVersion {
                got,
                supported_min,
                supported_max,
            } => {
                assert_eq!(got, WIRE_VERSION_V3.saturating_add(1));
                assert_eq!(supported_min, MIN_SUPPORTED_RPC_VERSION);
                assert_eq!(supported_max, WIRE_VERSION_V3);
            }
            other => panic!("expected UnsupportedWireVersion, got {other:?}"),
        }
        // Suppress unused warning when WIRE_VERSION ever drifts behind.
        let _ = WIRE_VERSION;
    }

    #[test]
    fn parse_frame_rejects_version_zero() {
        let payload = b"zero";
        let frame = make_frame(0, payload);
        let err = parse_frame(&frame).expect_err("version 0 must be rejected");
        assert!(matches!(
            err,
            ClusterError::UnsupportedWireVersion { got: 0, .. }
        ));
    }

    #[test]
    fn v3_frame_round_trips_with_epoch() {
        use crate::cluster_epoch::set_local_cluster_epoch;
        // Stamp a known epoch via write_frame, then re-parse and check
        // the parser observed it (advanced the local mark).
        set_local_cluster_epoch(0);
        set_local_cluster_epoch(7);
        let payload = b"epoch-bound";
        let mut buf = Vec::new();
        write_frame(0xAB, payload, &mut buf).unwrap();
        // Reset before parsing so observe_peer_cluster_epoch has work to do.
        set_local_cluster_epoch(0);
        let (rpc_type, body) = parse_frame(&buf).unwrap();
        assert_eq!(rpc_type, 0xAB);
        assert_eq!(body, payload);
        assert_eq!(crate::cluster_epoch::current_local_cluster_epoch(), 7);
        set_local_cluster_epoch(0);
    }

    #[test]
    fn frame_size_handles_v2_and_v3() {
        // v2: 10-byte header + 5-byte payload.
        let v2 = make_frame(MIN_SUPPORTED_RPC_VERSION, b"abcde");
        let mut hdr = [0u8; HEADER_SIZE_V3];
        hdr[..HEADER_SIZE_V2].copy_from_slice(&v2[..HEADER_SIZE_V2]);
        assert_eq!(frame_size(&hdr).unwrap(), HEADER_SIZE_V2 + 5);

        // v3: 18-byte header + 5-byte payload.
        let v3 = make_frame_v3(b"abcde", 1);
        hdr.copy_from_slice(&v3[..HEADER_SIZE_V3]);
        assert_eq!(frame_size(&hdr).unwrap(), HEADER_SIZE_V3 + 5);
    }
}
