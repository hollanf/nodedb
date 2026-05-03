//! Calvin scheduler WAL recovery.
//!
//! Provides [`read_last_applied_epoch`] which scans the WAL for
//! `RecordType::CalvinApplied` records and returns the highest epoch that was
//! successfully committed on a given vshard.  The scheduler uses this on
//! startup to determine the rebuild range (`last_applied + 1 ..= committed`).

use nodedb_wal::record::RecordType;
use nodedb_wal::{CalvinAppliedPayload, WalRecord};
use tracing::warn;

use crate::wal::manager::WalManager;

/// Scan the WAL and return the highest `epoch` from `CalvinApplied` records
/// for the given vshard.
///
/// Returns `0` for a greenfield node (no `CalvinApplied` records exist).
///
/// Records that fail to decode are logged and skipped — a corrupt record does
/// not abort the scan.
pub fn read_last_applied_epoch(wal: &WalManager, vshard_id: u32) -> crate::Result<u64> {
    let records = wal.replay()?;
    let mut last_epoch: u64 = 0;

    for record in &records {
        if !is_calvin_applied_record(record) {
            continue;
        }
        match CalvinAppliedPayload::from_bytes(&record.payload) {
            Ok(p) if p.vshard_id == vshard_id => {
                if p.epoch > last_epoch {
                    last_epoch = p.epoch;
                }
            }
            Ok(_) => {
                // Different vshard — skip.
            }
            Err(e) => {
                warn!(
                    lsn = record.header.lsn,
                    error = %e,
                    "calvin recovery: failed to decode CalvinApplied payload; skipping"
                );
            }
        }
    }

    Ok(last_epoch)
}

fn is_calvin_applied_record(record: &WalRecord) -> bool {
    // Strip the encryption flag (bit 31) before comparing record type.
    let raw_type = record.header.record_type & !nodedb_wal::record::ENCRYPTED_FLAG;
    matches!(
        RecordType::from_raw(raw_type),
        Some(RecordType::CalvinApplied)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    use crate::wal::manager::WalManager;

    fn open_wal(dir: &TempDir) -> WalManager {
        WalManager::open(dir.path(), false).expect("open wal")
    }

    #[test]
    fn read_last_applied_epoch_zero_for_greenfield() {
        let dir = TempDir::new().unwrap();
        let wal = open_wal(&dir);
        let epoch = read_last_applied_epoch(&wal, 1).unwrap();
        assert_eq!(epoch, 0, "greenfield WAL should return epoch 0");
    }

    #[test]
    fn read_last_applied_epoch_returns_max_calvin_applied_for_vshard() {
        let dir = TempDir::new().unwrap();
        let wal = open_wal(&dir);

        use crate::types::VShardId;
        // Write three CalvinApplied records for vshard 1 at epochs 2, 5, 3.
        wal.append_calvin_applied(VShardId::new(1), 2, 0).unwrap();
        wal.append_calvin_applied(VShardId::new(1), 5, 0).unwrap();
        wal.append_calvin_applied(VShardId::new(1), 3, 0).unwrap();
        // Write one record for a different vshard (should be ignored).
        wal.append_calvin_applied(VShardId::new(2), 99, 0).unwrap();
        // Flush the WAL write buffer to disk so replay() can read the records.
        wal.sync().unwrap();

        let epoch = read_last_applied_epoch(&wal, 1).unwrap();
        assert_eq!(epoch, 5, "should return the highest epoch for vshard 1");

        let epoch_other = read_last_applied_epoch(&wal, 2).unwrap();
        assert_eq!(epoch_other, 99, "vshard 2 should see its own epoch");
    }
}
