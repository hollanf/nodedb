//! [`MetadataApplier`] trait: the contract raft_loop uses to dispatch
//! committed entries on the metadata group (group 0).

use std::sync::{Arc, RwLock};

use tracing::warn;

use crate::metadata_group::cache::MetadataCache;
use crate::metadata_group::codec::decode_entry;

/// Applies committed metadata entries to local state.
///
/// Implemented in the `nodedb-cluster` crate as [`CacheApplier`] (writes to
/// an in-memory [`MetadataCache`]) and wrapped by the production applier in
/// the `nodedb` crate to also persist to redb and broadcast change events.
pub trait MetadataApplier: Send + Sync + 'static {
    /// Apply a batch of committed raft entries. Entries with empty `data`
    /// (raft no-ops) are skipped. Returns the highest log index applied.
    fn apply(&self, entries: &[(u64, Vec<u8>)]) -> u64;
}

/// Default applier that writes committed entries to an in-memory
/// [`MetadataCache`]. The cache is shared with the rest of the process
/// via `Arc<RwLock<_>>`.
#[derive(Clone)]
pub struct CacheApplier {
    cache: Arc<RwLock<MetadataCache>>,
}

impl CacheApplier {
    pub fn new(cache: Arc<RwLock<MetadataCache>>) -> Self {
        Self { cache }
    }

    pub fn cache(&self) -> Arc<RwLock<MetadataCache>> {
        self.cache.clone()
    }
}

impl MetadataApplier for CacheApplier {
    fn apply(&self, entries: &[(u64, Vec<u8>)]) -> u64 {
        let mut last = 0u64;
        let mut guard = self
            .cache
            .write()
            .unwrap_or_else(|poison| poison.into_inner());
        for (index, data) in entries {
            last = *index;
            if data.is_empty() {
                continue;
            }
            match decode_entry(data) {
                Ok(entry) => guard.apply(*index, &entry),
                Err(e) => warn!(index = *index, error = %e, "metadata decode failed"),
            }
        }
        last
    }
}

/// No-op applier used by tests and subsystems that don't care about the
/// metadata stream. Still drains entries and returns the correct last index
/// so raft can advance its applied watermark.
pub struct NoopMetadataApplier;

impl MetadataApplier for NoopMetadataApplier {
    fn apply(&self, entries: &[(u64, Vec<u8>)]) -> u64 {
        entries.last().map(|(idx, _)| *idx).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata_group::actions::{CollectionAction, CollectionAlter};
    use crate::metadata_group::codec::encode_entry;
    use crate::metadata_group::descriptors::collection::{CollectionDescriptor, ColumnDef};
    use crate::metadata_group::descriptors::common::{
        DescriptorHeader, DescriptorId, DescriptorKind,
    };
    use crate::metadata_group::entry::MetadataEntry;
    use nodedb_types::Hlc;
    use std::sync::RwLock as StdRwLock;

    fn coll_id() -> DescriptorId {
        DescriptorId::new(1, DescriptorKind::Collection, "users")
    }

    fn make_create(version: u64, hlc: Hlc) -> MetadataEntry {
        MetadataEntry::CollectionDdl {
            tenant_id: 1,
            action: CollectionAction::Create(Box::new(CollectionDescriptor {
                header: DescriptorHeader::new_public(coll_id(), version, hlc),
                collection_type: "document_schemaless".into(),
                columns: vec![ColumnDef {
                    name: "name".into(),
                    data_type: "TEXT".into(),
                    nullable: true,
                    default: None,
                }],
                with_options: vec![],
                primary_key: None,
            })),
            host_payload: vec![],
        }
    }

    #[test]
    fn cache_applier_create_alter_drop_roundtrip() {
        let cache = Arc::new(StdRwLock::new(MetadataCache::new()));
        let applier = CacheApplier::new(cache.clone());

        let create = encode_entry(&make_create(1, Hlc::new(10, 0))).unwrap();
        let add_col = encode_entry(&MetadataEntry::CollectionDdl {
            tenant_id: 1,
            action: CollectionAction::Alter {
                id: coll_id(),
                change: CollectionAlter::AddColumn(ColumnDef {
                    name: "email".into(),
                    data_type: "TEXT".into(),
                    nullable: true,
                    default: None,
                }),
            },
            host_payload: vec![],
        })
        .unwrap();
        let drop = encode_entry(&MetadataEntry::CollectionDdl {
            tenant_id: 1,
            action: CollectionAction::Drop { id: coll_id() },
            host_payload: vec![],
        })
        .unwrap();

        let last = applier.apply(&[(1, create), (2, add_col), (3, drop)]);
        assert_eq!(last, 3);

        let guard = cache.read().unwrap();
        assert_eq!(guard.applied_index, 3);
        assert_eq!(guard.collection_count(), 0);
    }

    #[test]
    fn cache_applier_create_visible_before_drop() {
        let cache = Arc::new(StdRwLock::new(MetadataCache::new()));
        let applier = CacheApplier::new(cache.clone());

        let bytes = encode_entry(&make_create(1, Hlc::new(42, 0))).unwrap();
        applier.apply(&[(1, bytes)]);

        let guard = cache.read().unwrap();
        assert_eq!(guard.collection_count(), 1);
        let desc = guard.collection(&coll_id()).expect("present");
        assert_eq!(desc.header.version, 1);
        assert_eq!(desc.header.modification_time, Hlc::new(42, 0));
        assert_eq!(desc.columns.len(), 1);
    }

    #[test]
    fn cache_applier_idempotent_on_replay() {
        let cache = Arc::new(StdRwLock::new(MetadataCache::new()));
        let applier = CacheApplier::new(cache.clone());

        let bytes = encode_entry(&make_create(1, Hlc::new(7, 0))).unwrap();
        applier.apply(&[(5, bytes.clone())]);
        // Re-applying an earlier index is a no-op.
        applier.apply(&[(3, bytes)]);

        let guard = cache.read().unwrap();
        assert_eq!(guard.applied_index, 5);
        assert_eq!(guard.collection_count(), 1);
    }

    #[test]
    fn noop_applier_advances_watermark() {
        let noop = NoopMetadataApplier;
        assert_eq!(noop.apply(&[(7, b"x".to_vec()), (9, b"y".to_vec())]), 9);
        assert_eq!(noop.apply(&[]), 0);
    }
}
