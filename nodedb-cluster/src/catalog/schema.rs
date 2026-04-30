//! redb table definitions, key constants, and shared error helpers
//! for the cluster catalog.
//!
//! These are `pub(super)` so every sibling file in `catalog/` can
//! reach them without re-declaring.

use redb::TableDefinition;

use crate::error::ClusterError;

/// Single-row table for the serialised `ClusterTopology`.
pub(super) const TOPOLOGY_TABLE: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_cluster.topology");

/// Single-row table for the serialised `RoutingTable`.
pub(super) const ROUTING_TABLE: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_cluster.routing");

/// Cluster metadata (cluster_id, catalog format version, CA cert, …).
pub(super) const METADATA_TABLE: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_cluster.metadata");

/// Ghost stub table — persists ghost edge refcounts across restarts.
pub(super) const GHOST_TABLE: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_cluster.ghosts");

/// Migration state table — keyed by `migration_id` (UUID as hyphenated string),
/// value is zerompk-encoded `PersistedMigrationCheckpoint` (the latest checkpoint
/// for that migration).  Rows are upserted on every `MigrationCheckpoint` commit
/// and deleted when a `MigrationAbort` commits with all compensations applied.
pub(super) const MIGRATION_STATE_TABLE: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_cluster.migration_state");

pub(super) const KEY_TOPOLOGY: &str = "topology";
pub(super) const KEY_ROUTING: &str = "routing";
pub(super) const KEY_CLUSTER_ID: &str = "cluster_id";
pub(super) const KEY_CA_CERT: &str = "ca_cert";
/// Metadata key holding the catalog format version (u32 LE).
///
/// Stamped by `ClusterCatalog::open` on every fresh catalog and
/// read on every subsequent open. Its presence is the pivot that
/// future schema migrations land on — when the next breaking
/// change to a zerompk-persisted type ships, a
/// `v{N}_to_v{N+1}` arm in `catalog::migration::migrate_if_needed`
/// reads the key, runs the upgrade, and writes the new value.
pub(super) const KEY_FORMAT_VERSION: &str = "format_version";

/// Current catalog format version.
///
/// Bump this (and add a matching migration arm in
/// `catalog::migration`) on every breaking change to a
/// zerompk-derived catalog type.
///
/// Version history:
/// - 1: initial format (cluster_id, ca_cert, format_version).
/// - 2: added `cluster_settings` (PlacementHashId, vshard_count,
///   replication_factor, min_wire_version). Migration arm
///   writes defaults when upgrading from v1.
/// - 3: `Hlc::logical` widened from `u32` to `u64` (T4-11). All
///   zerompk-persisted types embedding `Hlc` (e.g. `DescriptorDrainStart`)
///   now encode `logical` as a 64-bit integer. Old nodes cannot decode
///   entries written by new nodes when `logical > u32::MAX`. Migration
///   arm writes the new format version; no on-disk data transformation
///   is required because MessagePack integers decode by value and
///   existing stored `logical` values fit in u32.
pub(super) const CATALOG_FORMAT_VERSION: u32 = 3;

/// Wrap any `std::fmt::Display` error as a transport-layer
/// `ClusterError`. Kept here so every catalog file uses the same
/// error shape.
pub(super) fn catalog_err(e: impl std::fmt::Display) -> ClusterError {
    ClusterError::Transport {
        detail: format!("cluster catalog: {e}"),
    }
}
