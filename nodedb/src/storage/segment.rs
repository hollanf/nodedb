use std::io::{Read, Write};
use std::path::Path;

use nodedb_wal::preamble::{PREAMBLE_SIZE, SEG_PREAMBLE_MAGIC, SegmentPreamble};

use crate::types::Lsn;

/// Magic bytes identifying a NodeDB segment file.
const SEGMENT_MAGIC: [u8; 4] = *b"SYNS";

/// Current segment format version.
///
/// v2 adds the 16-byte `SEGP` preamble at offset 0 of every encrypted segment.
/// The preamble persists the AES-256-GCM epoch used for that segment, making
/// decryption correct across process restarts. Pre-launch; no v1 migration.
const FORMAT_VERSION: u16 = 2;

/// Footer size in bytes: magic(4) + version(2) + created_by(32) + checksum(4) + min_lsn(8) + max_lsn(8) = 58.
const FOOTER_SIZE: usize = 58;

/// Segment file footer.
///
/// All persistent files embed this footer for crash-safe validation.
/// Footer is written at the end of the file; readers seek to `file_len - FOOTER_SIZE`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentFooter {
    /// Format version for forward compatibility.
    pub format_version: u16,
    /// Identifier of the process/node that created this segment.
    pub created_by: [u8; 32],
    /// CRC32C checksum of the segment data (excluding footer).
    pub checksum: u32,
    /// Minimum LSN of records in this segment.
    pub min_lsn: Lsn,
    /// Maximum LSN of records in this segment.
    pub max_lsn: Lsn,
}

impl SegmentFooter {
    /// Create a new footer for a segment.
    pub fn new(created_by: &str, checksum: u32, min_lsn: Lsn, max_lsn: Lsn) -> Self {
        let mut cb = [0u8; 32];
        let bytes = created_by.as_bytes();
        let len = bytes.len().min(32);
        cb[..len].copy_from_slice(&bytes[..len]);

        Self {
            format_version: FORMAT_VERSION,
            created_by: cb,
            checksum,
            min_lsn,
            max_lsn,
        }
    }

    /// Serialize the footer to bytes.
    pub fn to_bytes(&self) -> [u8; FOOTER_SIZE] {
        let mut buf = [0u8; FOOTER_SIZE];
        buf[0..4].copy_from_slice(&SEGMENT_MAGIC);
        buf[4..6].copy_from_slice(&self.format_version.to_le_bytes());
        buf[6..38].copy_from_slice(&self.created_by);
        buf[38..42].copy_from_slice(&self.checksum.to_le_bytes());
        buf[42..50].copy_from_slice(&self.min_lsn.as_u64().to_le_bytes());
        buf[50..58].copy_from_slice(&self.max_lsn.as_u64().to_le_bytes());
        buf
    }

    /// Deserialize a footer from bytes.
    pub fn from_bytes(buf: &[u8; FOOTER_SIZE]) -> crate::Result<Self> {
        if buf[0..4] != SEGMENT_MAGIC {
            return Err(crate::Error::SegmentCorrupted {
                detail: "invalid segment magic".into(),
            });
        }

        let format_version = u16::from_le_bytes([buf[4], buf[5]]);
        let mut created_by = [0u8; 32];
        created_by.copy_from_slice(&buf[6..38]);
        let checksum = u32::from_le_bytes([buf[38], buf[39], buf[40], buf[41]]);
        let min_lsn = Lsn::new(u64::from_le_bytes([
            buf[42], buf[43], buf[44], buf[45], buf[46], buf[47], buf[48], buf[49],
        ]));
        let max_lsn = Lsn::new(u64::from_le_bytes([
            buf[50], buf[51], buf[52], buf[53], buf[54], buf[55], buf[56], buf[57],
        ]));

        Ok(Self {
            format_version,
            created_by,
            checksum,
            min_lsn,
            max_lsn,
        })
    }

    /// Write the footer to a file.
    pub fn write_to(&self, path: &Path) -> crate::Result<()> {
        let mut file = std::fs::OpenOptions::new().append(true).open(path)?;
        file.write_all(&self.to_bytes())?;
        file.flush()?;
        Ok(())
    }

    /// Read the footer from the end of a file.
    pub fn read_from(path: &Path) -> crate::Result<Self> {
        let mut file = std::fs::File::open(path)?;
        let file_len = file.metadata()?.len() as usize;
        if file_len < FOOTER_SIZE {
            return Err(crate::Error::SegmentCorrupted {
                detail: "file too small for segment footer".into(),
            });
        }

        use std::io::Seek;
        file.seek(std::io::SeekFrom::End(-(FOOTER_SIZE as i64)))?;
        let mut buf = [0u8; FOOTER_SIZE];
        file.read_exact(&mut buf)?;

        Self::from_bytes(&buf)
    }

    /// Footer size in bytes.
    pub const fn size() -> usize {
        FOOTER_SIZE
    }
}

/// Write a segment file with optional encryption.
///
/// Layout (encrypted):
///   `[preamble(16B plaintext)] [ciphertext(data_len + 16B auth_tag)] [footer(58B plaintext)]`
///
/// Layout (unencrypted):
///   `[data] [footer(58B plaintext)]`
///
/// The preamble contains a freshly-generated random epoch. The nonce used for
/// AES-256-GCM is `(preamble.epoch, min_lsn)`. The preamble bytes and a fixed
/// AAD tag are included as Additional Authenticated Data, preventing preamble
/// swap attacks.
///
/// Nonce uniqueness: `(preamble.epoch, min_lsn)` is globally unique because
/// each call generates a fresh random epoch (4 bytes, 2^32 collision space).
/// Two segments that share `min_lsn` (e.g. LSN=0 at bootstrap) will have
/// different epochs and therefore non-colliding nonces.
pub fn write_encrypted_segment(
    path: &Path,
    data: &[u8],
    footer: &SegmentFooter,
    key: Option<&nodedb_wal::crypto::WalEncryptionKey>,
) -> crate::Result<()> {
    let mut file = std::fs::File::create(path)?;

    if let Some(key) = key {
        // Create a fresh-epoch key for this segment. Each segment write gets
        // its own epoch so even two segments with the same min_lsn (e.g.
        // both starting at LSN 0 at bootstrap) have non-colliding nonces.
        // `with_fresh_epoch` re-uses the same AES key bytes but generates a
        // new random epoch — the nonce for encryption will use this epoch,
        // and we record the same epoch in the preamble.
        let fresh_key = key.with_fresh_epoch().map_err(crate::Error::Wal)?;
        let epoch = *fresh_key.epoch();
        let preamble = SegmentPreamble::new_seg(epoch);
        let preamble_bytes = preamble.to_bytes();

        // AAD = preamble_bytes (binds ciphertext to this segment's preamble).
        let ciphertext = fresh_key
            .encrypt_aad(footer.min_lsn.as_u64(), &preamble_bytes, data)
            .map_err(|e| crate::Error::Storage {
                engine: "segment".into(),
                detail: format!("segment encryption failed: {e}"),
            })?;

        file.write_all(&preamble_bytes)?;
        file.write_all(&ciphertext)?;
    } else {
        file.write_all(data)?;
    }

    file.write_all(&footer.to_bytes())?;
    file.flush()?;
    Ok(())
}

/// Read and decrypt a segment file's data portion.
///
/// For encrypted segments, reads the 16-byte `SEGP` preamble at the start of
/// the file to recover the epoch, then uses it for nonce reconstruction.
/// Returns the plaintext data (footer is stripped and validated separately).
pub fn read_encrypted_segment(
    path: &Path,
    key: Option<&nodedb_wal::crypto::WalEncryptionKey>,
) -> crate::Result<Vec<u8>> {
    let raw = std::fs::read(path)?;

    if let Some(key) = key {
        // Encrypted layout: [preamble(16)] [ciphertext] [footer(58)]
        let min_len = PREAMBLE_SIZE + nodedb_wal::crypto::AUTH_TAG_SIZE + FOOTER_SIZE;
        if raw.len() < min_len {
            return Err(crate::Error::SegmentCorrupted {
                detail: "encrypted segment file too small".into(),
            });
        }

        // Read and validate the preamble.
        let preamble_bytes: [u8; PREAMBLE_SIZE] = raw[..PREAMBLE_SIZE]
            .try_into()
            .expect("slice is PREAMBLE_SIZE bytes");
        let preamble =
            SegmentPreamble::from_bytes(&preamble_bytes, &SEG_PREAMBLE_MAGIC).map_err(|e| {
                crate::Error::SegmentCorrupted {
                    detail: format!("invalid segment preamble: {e}"),
                }
            })?;

        // Footer is at the end.
        let footer_bytes: [u8; FOOTER_SIZE] = raw[raw.len() - FOOTER_SIZE..]
            .try_into()
            .expect("slice is FOOTER_SIZE bytes");
        let footer = SegmentFooter::from_bytes(&footer_bytes)?;

        // Ciphertext is between preamble and footer.
        let ciphertext = &raw[PREAMBLE_SIZE..raw.len() - FOOTER_SIZE];

        // Decrypt: epoch from preamble, nonce input is min_lsn.
        key.decrypt_aad(
            preamble.epoch(),
            footer.min_lsn.as_u64(),
            &preamble_bytes,
            ciphertext,
        )
        .map_err(|e| crate::Error::Storage {
            engine: "segment".into(),
            detail: format!("segment decryption failed: {e}"),
        })
    } else {
        // Unencrypted layout: [data] [footer(58)]
        if raw.len() < FOOTER_SIZE {
            return Err(crate::Error::SegmentCorrupted {
                detail: "file too small".into(),
            });
        }
        Ok(raw[..raw.len() - FOOTER_SIZE].to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_key() -> nodedb_wal::crypto::WalEncryptionKey {
        nodedb_wal::crypto::WalEncryptionKey::from_bytes(&[0x42u8; 32]).unwrap()
    }

    #[test]
    fn roundtrip_bytes() {
        let footer = SegmentFooter::new("node-1", 0xDEADBEEF, Lsn::new(10), Lsn::new(99));
        let bytes = footer.to_bytes();
        let parsed = SegmentFooter::from_bytes(&bytes).unwrap();
        assert_eq!(footer, parsed);
    }

    #[test]
    fn invalid_magic_rejected() {
        let mut bytes = [0u8; FOOTER_SIZE];
        bytes[0..4].copy_from_slice(b"NOPE");
        assert!(SegmentFooter::from_bytes(&bytes).is_err());
    }

    #[test]
    fn write_and_read_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.seg");

        // Write some data + footer (unencrypted path).
        std::fs::write(&path, b"segment data here").unwrap();
        let footer = SegmentFooter::new("test", 42, Lsn::new(1), Lsn::new(50));
        footer.write_to(&path).unwrap();

        // Read back.
        let read_footer = SegmentFooter::read_from(&path).unwrap();
        assert_eq!(read_footer.checksum, 42);
        assert_eq!(read_footer.min_lsn, Lsn::new(1));
        assert_eq!(read_footer.max_lsn, Lsn::new(50));
    }

    #[test]
    fn write_and_read_file_encrypted_restart_roundtrip() {
        // G-01 equivalent for segment-store: write encrypted, simulate restart
        // by creating a new key instance (same bytes, fresh in-memory epoch),
        // and verify decryption still succeeds using the on-disk preamble epoch.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("enc.seg");

        let data = b"secret segment payload";
        let footer = SegmentFooter::new("node-1", 0xABCD, Lsn::new(5), Lsn::new(100));

        // Write with key_v1 (epoch chosen randomly at construction).
        let key_v1 = test_key();
        write_encrypted_segment(&path, data, &footer, Some(&key_v1)).unwrap();

        // Simulate restart: new key instance with same bytes but different
        // in-memory epoch. Decryption MUST use the epoch from the on-disk preamble.
        let key_v2 = test_key(); // fresh random epoch
        let plaintext = read_encrypted_segment(&path, Some(&key_v2)).unwrap();
        assert_eq!(plaintext, data);
    }

    #[test]
    fn preamble_coexists_with_footer() {
        // Verify the preamble-at-start and footer-at-end layout is consistent:
        // the file has exactly preamble(16) + ciphertext(data+tag) + footer(58).
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("layout.seg");

        let data = b"layout test";
        let footer = SegmentFooter::new("n", 0, Lsn::new(1), Lsn::new(1));
        let key = test_key();

        write_encrypted_segment(&path, data, &footer, Some(&key)).unwrap();

        let raw = std::fs::read(&path).unwrap();
        // Starts with SEGP magic.
        assert_eq!(&raw[..4], b"SEGP");
        // Ends with SYNS footer magic.
        assert_eq!(
            &raw[raw.len() - FOOTER_SIZE..raw.len() - FOOTER_SIZE + 4],
            b"SYNS"
        );
        // Total size = preamble + ciphertext(data.len + 16B tag) + footer.
        assert_eq!(
            raw.len(),
            PREAMBLE_SIZE + data.len() + nodedb_wal::crypto::AUTH_TAG_SIZE + FOOTER_SIZE
        );
    }

    #[test]
    fn two_segments_same_min_lsn_produce_different_ciphertext() {
        // Nonce uniqueness: two segments with the same min_lsn=0 must produce
        // different ciphertext because each segment gets a fresh random epoch.
        let dir = tempfile::tempdir().unwrap();
        let path1 = dir.path().join("seg1.seg");
        let path2 = dir.path().join("seg2.seg");

        let data = b"same data, same lsn";
        let footer = SegmentFooter::new("n", 0, Lsn::ZERO, Lsn::ZERO);
        let key = test_key();

        write_encrypted_segment(&path1, data, &footer, Some(&key)).unwrap();
        write_encrypted_segment(&path2, data, &footer, Some(&key)).unwrap();

        let raw1 = std::fs::read(&path1).unwrap();
        let raw2 = std::fs::read(&path2).unwrap();

        // Epochs (bytes 8..12 of the preamble) must differ.
        assert_ne!(
            &raw1[8..12],
            &raw2[8..12],
            "epoch collision: two segments with the same min_lsn must use different epochs"
        );
        // Ciphertext (after preamble, before footer) must differ.
        let ct1 = &raw1[PREAMBLE_SIZE..raw1.len() - FOOTER_SIZE];
        let ct2 = &raw2[PREAMBLE_SIZE..raw2.len() - FOOTER_SIZE];
        assert_ne!(ct1, ct2);
    }

    #[test]
    fn preamble_tamper_rejected() {
        // G-02 binding test: swapping the preamble (different epoch) must
        // cause decryption to fail due to AAD mismatch.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tamper.seg");

        let data = b"authentic payload";
        let footer = SegmentFooter::new("n", 0, Lsn::new(10), Lsn::new(20));
        let key = test_key();
        write_encrypted_segment(&path, data, &footer, Some(&key)).unwrap();

        // Flip a byte in the preamble epoch field (bytes 8-11).
        let mut raw = std::fs::read(&path).unwrap();
        raw[9] ^= 0xFF;
        std::fs::write(&path, &raw).unwrap();

        // Decryption must fail — preamble bytes are part of AAD.
        assert!(
            read_encrypted_segment(&path, Some(&key)).is_err(),
            "preamble tamper must cause decryption failure"
        );
    }

    #[test]
    fn unencrypted_segment_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("plain.seg");

        let data = b"plain data";
        let footer = SegmentFooter::new("n", 99, Lsn::new(1), Lsn::new(5));

        write_encrypted_segment(&path, data, &footer, None).unwrap();
        let read_back = read_encrypted_segment(&path, None).unwrap();
        assert_eq!(read_back, data);
    }

    #[test]
    fn created_by_truncates_long_names() {
        let footer = SegmentFooter::new(
            "this-is-a-very-long-node-name-that-exceeds-32-bytes",
            0,
            Lsn::ZERO,
            Lsn::ZERO,
        );
        // Should not panic, truncates to 32 bytes.
        let bytes = footer.to_bytes();
        let parsed = SegmentFooter::from_bytes(&bytes).unwrap();
        assert_eq!(parsed.created_by[..4], *b"this");
    }

    #[test]
    fn lsn_ordering_preserved() {
        let footer = SegmentFooter::new("n", 0, Lsn::new(100), Lsn::new(200));
        assert!(footer.min_lsn < footer.max_lsn);
    }
}
