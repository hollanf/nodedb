//! One-time migration from legacy single-file WAL to segmented layout.

use std::fs;
use std::path::Path;

use crate::error::{Result, WalError};

use super::meta::segment_path;

/// Migrate a legacy single-file WAL to the segmented format.
///
/// If a file named `legacy_path` (e.g., `data/wal`) exists and is not a directory,
/// it is renamed to `{wal_dir}/wal-00000000000000000001.seg`. The WAL directory
/// is created if needed.
///
/// This is a one-time migration that runs transparently on startup. After
/// migration, the old path no longer exists as a file.
///
/// Returns `true` if migration occurred, `false` if no legacy file found.
pub fn migrate_legacy_wal(legacy_path: &Path, wal_dir: &Path) -> Result<bool> {
    if !legacy_path.is_file() {
        return Ok(false);
    }

    let metadata = fs::metadata(legacy_path).map_err(WalError::Io)?;
    if metadata.len() == 0 {
        let _ = fs::remove_file(legacy_path);
        return Ok(false);
    }

    let info = crate::recovery::recover(legacy_path)?;
    let first_lsn = if info.record_count == 0 {
        1
    } else {
        let mut reader = crate::reader::WalReader::open(legacy_path)?;
        match reader.next_record()? {
            Some(record) => record.header.lsn,
            None => 1,
        }
    };

    fs::create_dir_all(wal_dir).map_err(WalError::Io)?;

    let new_path = segment_path(wal_dir, first_lsn);
    fs::rename(legacy_path, &new_path).map_err(WalError::Io)?;

    let legacy_dwb = legacy_path.with_extension("dwb");
    if legacy_dwb.exists() {
        let new_dwb = new_path.with_extension("dwb");
        fs::rename(&legacy_dwb, &new_dwb).map_err(WalError::Io)?;
    }

    tracing::info!(
        legacy = %legacy_path.display(),
        segment = %new_path.display(),
        first_lsn,
        "migrated legacy WAL to segmented format"
    );

    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::super::discovery::discover_segments;
    use super::*;

    #[test]
    fn migrate_legacy_wal_moves_file() {
        let dir = tempfile::tempdir().unwrap();
        let legacy_path = dir.path().join("test.wal");
        let wal_dir = dir.path().join("wal_segments");

        {
            let mut writer =
                crate::writer::WalWriter::open_without_direct_io(&legacy_path).unwrap();
            writer
                .append(crate::record::RecordType::Put as u32, 1, 0, b"hello")
                .unwrap();
            writer
                .append(crate::record::RecordType::Put as u32, 1, 0, b"world")
                .unwrap();
            writer.sync().unwrap();
        }

        let migrated = migrate_legacy_wal(&legacy_path, &wal_dir).unwrap();
        assert!(migrated);
        assert!(!legacy_path.exists());

        let segments = discover_segments(&wal_dir).unwrap();
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].first_lsn, 1);

        let reader = crate::reader::WalReader::open(&segments[0].path).unwrap();
        let records: Vec<_> = reader.records().collect::<crate::Result<_>>().unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].payload, b"hello");
    }

    #[test]
    fn migrate_nonexistent_legacy_is_noop() {
        let dir = tempfile::tempdir().unwrap();
        let legacy_path = dir.path().join("nonexistent.wal");
        let wal_dir = dir.path().join("wal_segments");

        let migrated = migrate_legacy_wal(&legacy_path, &wal_dir).unwrap();
        assert!(!migrated);
    }

    #[test]
    fn migrate_empty_legacy_is_noop() {
        let dir = tempfile::tempdir().unwrap();
        let legacy_path = dir.path().join("empty.wal");
        let wal_dir = dir.path().join("wal_segments");

        fs::write(&legacy_path, b"").unwrap();

        let migrated = migrate_legacy_wal(&legacy_path, &wal_dir).unwrap();
        assert!(!migrated);
        assert!(!legacy_path.exists());
    }
}
