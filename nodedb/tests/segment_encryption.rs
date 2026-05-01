//! End-to-end test: snapshot core files are encrypted at rest when a KEK is
//! configured, and the same KEK is required to restore.
//!
//! Asserts:
//! - Raw bytes on disk contain the SEGP preamble magic.
//! - Raw bytes do NOT contain the plaintext snapshot payload.
//! - Drop + reopen with the same key successfully recovers the CoreSnapshot.

use nodedb::data::snapshot::CoreSnapshot;
use nodedb::storage::snapshot_writer::{create_base_snapshot, load_core_snapshot};
use nodedb_wal::crypto::WalEncryptionKey;

const KEK: [u8; 32] = [0x42u8; 32];

fn make_core_snapshot(watermark: u64) -> Vec<u8> {
    let snap = CoreSnapshot {
        watermark,
        ..CoreSnapshot::empty()
    };
    snap.to_bytes().expect("serialize CoreSnapshot")
}

#[test]
fn segment_encrypted_at_rest_restart_roundtrip() {
    let dir = tempfile::tempdir().expect("tempdir");

    // Produce a core snapshot with a recognisable payload.
    let watermark: u64 = 42_000;
    let snap_bytes = make_core_snapshot(watermark);

    // The plaintext must appear in the raw bytes when unencrypted.  We use
    // this below to confirm it is NOT visible after encryption.
    let plaintext_marker: Vec<u8> = snap_bytes.clone();

    // --- write with KEK -------------------------------------------------------
    let key_v1 = WalEncryptionKey::from_bytes(&KEK).expect("build key");
    let (_, snap_dir) = create_base_snapshot(
        dir.path(),
        vec![(0, snap_bytes.clone())],
        "test-node",
        Some(&key_v1),
    )
    .expect("create snapshot");

    // --- assert ciphertext-on-disk --------------------------------------------
    let snap_file = snap_dir.join("core-0.snap");
    let raw = std::fs::read(&snap_file).expect("read snap file");

    // SEGP magic must be present at offset 0.
    assert_eq!(
        &raw[..4],
        b"SEGP",
        "encrypted snap file must start with SEGP preamble magic"
    );

    // SYNS footer magic must be present at the end (last 4 bytes of FOOTER_SIZE=58).
    let syns_offset = raw.len() - 58;
    assert_eq!(
        &raw[syns_offset..syns_offset + 4],
        b"SYNS",
        "encrypted snap file must end with SYNS segment footer magic"
    );

    // Plaintext payload must NOT be present anywhere in the file bytes.
    // We search for the first 16 bytes of the plaintext as a witness.
    let witness = &plaintext_marker[..16.min(plaintext_marker.len())];
    let found = raw.windows(witness.len()).any(|w| w == witness);
    assert!(
        !found,
        "plaintext snapshot bytes must not appear in the encrypted segment file"
    );

    // --- simulate restart: new key instance, same bytes ----------------------
    // WalEncryptionKey::from_bytes generates a fresh random in-memory epoch.
    // Decryption must use the epoch recorded in the on-disk SEGP preamble.
    let key_v2 = WalEncryptionKey::from_bytes(&KEK).expect("build key v2");
    let restored = load_core_snapshot(&snap_dir, 0, Some(&key_v2))
        .expect("load encrypted snapshot after restart");

    assert_eq!(
        restored.watermark, watermark,
        "restored watermark must match original"
    );
}
