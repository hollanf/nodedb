//! Catalog format migration runner.
//!
//! This module exists as infrastructure, not because there are
//! currently any legacy catalogs to migrate. The format version
//! stamped into every new catalog by `ClusterCatalog::open` is the
//! pivot that future schema changes land on: when the next
//! breaking change to a zerompk-persisted type ships, add a
//! `v{N}_to_v{N+1}` arm to [`migrate_if_needed`], bump
//! `CATALOG_FORMAT_VERSION`, and the upgrade path is wired
//! automatically.
//!
//! # Why zerompk needs versioning at all
//!
//! zerompk's `FromMessagePack` derive enforces exact map length
//! and returns `KeyNotFound` for any missing field. That makes
//! adding a field to a zerompk-persisted struct a breaking wire
//! change, and the only safe way to do it is through a versioned
//! migration. The format version key is the pivot that future
//! upgrade arms hang off.
//!
//! # Future migrations
//!
//! When the next breaking schema change lands, the pattern is:
//!
//! 1. Define a private `FooVN` shim struct mirroring the old
//!    shape with its own `zerompk::FromMessagePack` derive.
//! 2. Add a `v{N}_to_v{N+1}` arm in `migrate_if_needed` that
//!    reads via the shim and writes via the current serializer.
//! 3. Bump `CATALOG_FORMAT_VERSION` in `schema.rs`.
//! 4. Add an integration test in
//!    `tests/cluster_catalog_migration.rs` (create the file when
//!    needed) that builds a vN fixture and verifies the upgrade.
//!
//! Failures are returned as `ClusterError::Codec` so startup
//! aborts loudly — there is no silent-clobber code path.

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
            // Currently every older-than-current case is
            // unsupported by construction: history starts at the
            // current stamped version. When a future breaking
            // change lands, add a `v{N}_to_v{N+1}` arm above this
            // catch-all.
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
    fn older_version_without_migration_is_refused() {
        let (_dir, db) = temp_db();
        // Simulate a theoretical older format — we have no
        // migration arms yet, so this must fail loudly rather
        // than silently stamp the current version.
        write_format_version(&db, CATALOG_FORMAT_VERSION - 1).unwrap();
        let err = migrate_if_needed(&db).unwrap_err().to_string();
        assert!(
            err.contains("older than supported"),
            "expected older-without-arm rejection, got: {err}"
        );
    }
}
