//! Replicated metadata Raft group for cluster-wide state.
//!
//! A dedicated Raft group (ID = 0, separate from the 1024 data vShards)
//! that replicates cluster metadata across all nodes:
//!
//! - RoutingTable (vShard → node mapping)
//! - DDL schemas (collection definitions, index declarations)
//! - Security catalog (users, roles, permissions)
//!
//! All nodes participate in this group. The leader serves authoritative
//! reads; followers cache locally and subscribe to the leader's change
//! stream for updates.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use tracing::{debug, info};

/// Well-known Raft group ID for the metadata group.
/// Distinct from data vShard groups (which start at group 1).
pub const METADATA_GROUP_ID: u64 = 0;

/// Types of metadata entries replicated via the metadata Raft group.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub enum MetadataEntry {
    /// Routing table update (vShard assignment change).
    RoutingUpdate {
        vshard_id: u16,
        new_node_id: u64,
        new_group_id: u64,
    },
    /// Collection DDL (create/drop/alter).
    CollectionDdl {
        tenant_id: u32,
        collection: String,
        action: DdlAction,
    },
    /// Security policy change (user/role/permission).
    SecurityChange {
        tenant_id: u32,
        change: SecurityChangeType,
    },
    /// Node membership change (join/leave/decommission).
    MembershipChange {
        node_id: u64,
        action: MembershipAction,
    },
}

#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub enum DdlAction {
    Create { fields: Vec<(String, String)> },
    Drop,
    AlterAddField { name: String, field_type: String },
}

#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub enum SecurityChangeType {
    CreateUser { username: String },
    DropUser { username: String },
    GrantPermission { role: String, resource: String },
    RevokePermission { role: String, resource: String },
}

#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub enum MembershipAction {
    Join { addr: String },
    Leave,
    Decommission,
    PromoteToVoter,
}

/// Local cache of the metadata state, updated from the Raft log.
///
/// Each node maintains this cache. The leader writes to it; followers
/// apply committed entries from the Raft log to stay consistent.
#[derive(Debug, Clone, Default)]
pub struct MetadataCache {
    /// Applied log index — entries up to this index are reflected in cache.
    pub applied_index: u64,
    /// Cached routing table version.
    pub routing_version: u64,
    /// Collection schemas: `(tenant_id, collection_name)` → field definitions.
    pub collections: HashMap<(u32, String), Vec<(String, String)>>,
    /// Node membership: `node_id` → address.
    pub members: HashMap<u64, String>,
}

impl MetadataCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply a metadata entry from the Raft log.
    ///
    /// Called by the Raft commit applier when a metadata entry is committed.
    /// Updates the local cache to reflect the change.
    pub fn apply(&mut self, index: u64, entry: &MetadataEntry) {
        self.applied_index = index;
        match entry {
            MetadataEntry::RoutingUpdate {
                vshard_id,
                new_node_id,
                ..
            } => {
                debug!(
                    vshard_id,
                    new_node_id, index, "metadata: routing update applied"
                );
                self.routing_version += 1;
            }
            MetadataEntry::CollectionDdl {
                tenant_id,
                collection,
                action,
            } => match action {
                DdlAction::Create { fields } => {
                    self.collections
                        .insert((*tenant_id, collection.clone()), fields.clone());
                    info!(tenant_id, collection, index, "metadata: collection created");
                }
                DdlAction::Drop => {
                    self.collections.remove(&(*tenant_id, collection.clone()));
                    info!(tenant_id, collection, index, "metadata: collection dropped");
                }
                DdlAction::AlterAddField { name, field_type } => {
                    if let Some(fields) =
                        self.collections.get_mut(&(*tenant_id, collection.clone()))
                    {
                        fields.push((name.clone(), field_type.clone()));
                    }
                }
            },
            MetadataEntry::SecurityChange { tenant_id, change } => {
                debug!(tenant_id, ?change, index, "metadata: security change");
            }
            MetadataEntry::MembershipChange { node_id, action } => match action {
                MembershipAction::Join { addr } => {
                    self.members.insert(*node_id, addr.clone());
                    info!(node_id, addr, "metadata: node joined");
                }
                MembershipAction::Leave | MembershipAction::Decommission => {
                    self.members.remove(node_id);
                    info!(node_id, "metadata: node left");
                }
                MembershipAction::PromoteToVoter => {
                    debug!(node_id, "metadata: node promoted to voter");
                }
            },
        }
    }

    /// Serialize a metadata entry for Raft proposal.
    pub fn serialize_entry(entry: &MetadataEntry) -> crate::Result<Vec<u8>> {
        zerompk::to_msgpack_vec(entry).map_err(|e| crate::ClusterError::Codec {
            detail: format!("metadata serialize: {e}"),
        })
    }

    /// Deserialize a metadata entry from Raft log data.
    pub fn deserialize_entry(data: &[u8]) -> crate::Result<MetadataEntry> {
        zerompk::from_msgpack(data).map_err(|e| crate::ClusterError::Codec {
            detail: format!("metadata deserialize: {e}"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_routing_update() {
        let mut cache = MetadataCache::new();
        let entry = MetadataEntry::RoutingUpdate {
            vshard_id: 42,
            new_node_id: 3,
            new_group_id: 10,
        };
        cache.apply(1, &entry);
        assert_eq!(cache.applied_index, 1);
        assert_eq!(cache.routing_version, 1);
    }

    #[test]
    fn apply_collection_ddl() {
        let mut cache = MetadataCache::new();
        cache.apply(
            1,
            &MetadataEntry::CollectionDdl {
                tenant_id: 1,
                collection: "users".into(),
                action: DdlAction::Create {
                    fields: vec![("name".into(), "VARCHAR".into())],
                },
            },
        );
        assert!(cache.collections.contains_key(&(1, "users".into())));

        cache.apply(
            2,
            &MetadataEntry::CollectionDdl {
                tenant_id: 1,
                collection: "users".into(),
                action: DdlAction::Drop,
            },
        );
        assert!(!cache.collections.contains_key(&(1, "users".into())));
    }

    #[test]
    fn apply_membership() {
        let mut cache = MetadataCache::new();
        cache.apply(
            1,
            &MetadataEntry::MembershipChange {
                node_id: 5,
                action: MembershipAction::Join {
                    addr: "10.0.0.5:9000".into(),
                },
            },
        );
        assert_eq!(cache.members.get(&5), Some(&"10.0.0.5:9000".to_string()));

        cache.apply(
            2,
            &MetadataEntry::MembershipChange {
                node_id: 5,
                action: MembershipAction::Decommission,
            },
        );
        assert!(!cache.members.contains_key(&5));
    }

    #[test]
    fn serialize_roundtrip() {
        let entry = MetadataEntry::RoutingUpdate {
            vshard_id: 100,
            new_node_id: 2,
            new_group_id: 50,
        };
        let bytes = MetadataCache::serialize_entry(&entry).unwrap();
        let decoded = MetadataCache::deserialize_entry(&bytes).unwrap();
        match decoded {
            MetadataEntry::RoutingUpdate {
                vshard_id,
                new_node_id,
                ..
            } => {
                assert_eq!(vshard_id, 100);
                assert_eq!(new_node_id, 2);
            }
            _ => panic!("wrong variant"),
        }
    }
}
