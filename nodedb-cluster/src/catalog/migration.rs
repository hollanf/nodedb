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

use super::cluster_settings::{ClusterSettings, KEY_CLUSTER_SETTINGS};
use super::core::{read_format_version, write_format_version};
use super::schema::{CATALOG_FORMAT_VERSION, METADATA_TABLE, catalog_err};

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
        Some(1) => {
            // v1 → v2: write default ClusterSettings when the key is absent.
            info!("migrating catalog v1 → v2: writing default cluster_settings");
            migrate_v1_to_v2(db)?;
            write_format_version(db, CATALOG_FORMAT_VERSION)?;
            Ok(())
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

/// v1 → v2: insert default `ClusterSettings` if absent.
///
/// A v1 catalog was written before `cluster_settings` existed. We write
/// the default value so the key is present and future opens can always
/// deserialize it without a second migration hop.
fn migrate_v1_to_v2(db: &redb::Database) -> Result<()> {
    // Only write the key if it's absent — the migration must be idempotent.
    {
        let txn = db.begin_read().map_err(catalog_err)?;
        let table = txn.open_table(METADATA_TABLE).map_err(catalog_err)?;
        if table
            .get(KEY_CLUSTER_SETTINGS)
            .map_err(catalog_err)?
            .is_some()
        {
            return Ok(());
        }
    }

    let defaults = ClusterSettings::default();
    let bytes = zerompk::to_msgpack_vec(&defaults).map_err(|e| ClusterError::Codec {
        detail: format!("serialize default ClusterSettings during v1→v2 migration: {e}"),
    })?;

    let txn = db.begin_write().map_err(catalog_err)?;
    {
        let mut table = txn.open_table(METADATA_TABLE).map_err(catalog_err)?;
        table
            .insert(KEY_CLUSTER_SETTINGS, bytes.as_slice())
            .map_err(catalog_err)?;
    }
    txn.commit().map_err(catalog_err)?;
    Ok(())
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
        // Version 0 predates v1 and has no migration arm — must fail loudly.
        write_format_version(&db, 0).unwrap();
        let err = migrate_if_needed(&db).unwrap_err().to_string();
        assert!(
            err.contains("older than supported"),
            "expected older-without-arm rejection, got: {err}"
        );
    }

    #[test]
    fn v1_catalog_migrates_to_v2_with_default_cluster_settings() {
        use super::super::cluster_settings::{ClusterSettings, PlacementHashId};

        let (_dir, db) = temp_db();
        // Stamp v1 explicitly (simulates a catalog written by the previous binary).
        write_format_version(&db, 1).unwrap();

        // Migration must succeed.
        migrate_if_needed(&db).unwrap();

        // Format version must be bumped to the current value.
        assert_eq!(
            read_format_version(&db).unwrap(),
            Some(CATALOG_FORMAT_VERSION)
        );

        // The cluster_settings key must now be present with default values.
        let txn = db.begin_read().unwrap();
        let table = txn
            .open_table(super::super::schema::METADATA_TABLE)
            .unwrap();
        let guard = table
            .get(super::super::cluster_settings::KEY_CLUSTER_SETTINGS)
            .unwrap()
            .expect("cluster_settings must be written by migration");
        let settings: ClusterSettings = zerompk::from_msgpack(guard.value()).unwrap();
        assert_eq!(settings.placement_hash_id, PlacementHashId::Fnv1a);
        assert_eq!(settings.vshard_count, crate::routing::VSHARD_COUNT);
        assert_eq!(settings.min_wire_version, 1);
    }

    #[test]
    fn v1_to_v2_migration_is_idempotent() {
        let (_dir, db) = temp_db();
        write_format_version(&db, 1).unwrap();
        migrate_if_needed(&db).unwrap();
        // Running again must be a no-op (v2 fast path).
        migrate_if_needed(&db).unwrap();
        assert_eq!(
            read_format_version(&db).unwrap(),
            Some(CATALOG_FORMAT_VERSION)
        );
    }
}
