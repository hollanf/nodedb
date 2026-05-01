//! Catalog format migration runner.
//!
//! Stamps fresh catalogs at `CATALOG_FORMAT_VERSION = 1` and rejects older
//! and newer formats with a hard error. No live migration arms — first launch
//! baseline.

use tracing::info;

use crate::error::{ClusterError, Result};

use super::core::{read_format_version, write_format_version};
use super::schema::CATALOG_FORMAT_VERSION;

/// Stamp a fresh catalog and validate the on-disk format version
/// against the current binary.
///
/// Called from `ClusterCatalog::open` exactly once per process per
/// catalog path. Idempotent: running it repeatedly on an
/// already-stamped catalog is a no-op.
pub(super) fn migrate_if_needed(db: &redb::Database) -> Result<()> {
    let current = read_format_version(db)?;
    match current {
        None => {
            // Fresh catalog — no stamp yet. This is the common
            // case on every new data directory. Stamp the current
            // version so the next open takes the fast path.
            info!(
                format_version = CATALOG_FORMAT_VERSION,
                "stamping catalog with current format version"
            );
            write_format_version(db, CATALOG_FORMAT_VERSION)?;
            Ok(())
        }
        Some(v) if v == CATALOG_FORMAT_VERSION => {
            // Current format — fast path, no work to do.
            Ok(())
        }
        Some(v) if v > CATALOG_FORMAT_VERSION => {
            // Future format. The caller in `ClusterCatalog::open`
            // already refuses this case with a clear message; we
            // mirror that here as a defensive guard so a direct
            // caller of `migrate_if_needed` gets the same
            // behaviour.
            Err(ClusterError::Transport {
                detail: format!(
                    "catalog format version {v} is newer than supported ({CATALOG_FORMAT_VERSION}); refusing to open to avoid data corruption"
                ),
            })
        }
        Some(v) => {
            // Older format that we have no migration arm for.
            Err(ClusterError::Codec {
                detail: format!(
                    "catalog format version {v} is older than supported ({CATALOG_FORMAT_VERSION}) and no migration arm is defined"
                ),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_db() -> (tempfile::TempDir, redb::Database) {
        use super::super::schema::{
            GHOST_TABLE, METADATA_TABLE, MIGRATION_STATE_TABLE, ROUTING_TABLE, TOPOLOGY_TABLE,
        };
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cluster.redb");
        let db = redb::Database::create(&path).unwrap();
        let txn = db.begin_write().unwrap();
        {
            let _ = txn.open_table(TOPOLOGY_TABLE).unwrap();
            let _ = txn.open_table(ROUTING_TABLE).unwrap();
            let _ = txn.open_table(METADATA_TABLE).unwrap();
            let _ = txn.open_table(GHOST_TABLE).unwrap();
            let _ = txn.open_table(MIGRATION_STATE_TABLE).unwrap();
        }
        txn.commit().unwrap();
        (dir, db)
    }

    #[test]
    fn fresh_catalog_gets_stamped() {
        let (_dir, db) = temp_db();
        assert!(read_format_version(&db).unwrap().is_none());
        migrate_if_needed(&db).unwrap();
        assert_eq!(
            read_format_version(&db).unwrap(),
            Some(CATALOG_FORMAT_VERSION)
        );
    }

    #[test]
    fn reopening_stamped_catalog_is_a_noop() {
        let (_dir, db) = temp_db();
        migrate_if_needed(&db).unwrap();
        migrate_if_needed(&db).unwrap();
        migrate_if_needed(&db).unwrap();
        assert_eq!(
            read_format_version(&db).unwrap(),
            Some(CATALOG_FORMAT_VERSION)
        );
    }

    #[test]
    fn future_version_is_refused() {
        let (_dir, db) = temp_db();
        write_format_version(&db, CATALOG_FORMAT_VERSION + 1).unwrap();
        let err = migrate_if_needed(&db).unwrap_err().to_string();
        assert!(
            err.contains("newer than supported"),
            "expected future-version rejection, got: {err}"
        );
    }

    #[test]
    fn older_version_is_refused() {
        let (_dir, db) = temp_db();
        write_format_version(&db, 0).unwrap();
        let err = migrate_if_needed(&db).unwrap_err().to_string();
        assert!(
            err.contains("older than supported"),
            "expected older-without-arm rejection, got: {err}"
        );
    }
}
