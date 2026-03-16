use std::collections::HashMap;

use crate::types::VShardId;

/// Maps virtual shards to Data Plane core IDs.
///
/// In single-node mode, vShards are distributed round-robin across cores.
/// In cluster mode, the routing table is maintained by Raft consensus and
/// updated atomically during vShard migrations.
pub struct VShardRouter {
    /// vShard -> core ID mapping. Core ID is the index into the Data Plane
    /// core array (0..data_plane_cores-1).
    routes: HashMap<VShardId, usize>,
    num_cores: usize,
}

impl VShardRouter {
    /// Create a round-robin router for single-node mode.
    pub fn round_robin(num_cores: usize) -> Self {
        let mut routes = HashMap::with_capacity(VShardId::COUNT as usize);
        for i in 0..VShardId::COUNT {
            routes.insert(VShardId::new(i), i as usize % num_cores);
        }
        Self { routes, num_cores }
    }

    /// Resolve which core owns a vShard.
    pub fn resolve(&self, vshard: VShardId) -> Option<usize> {
        self.routes.get(&vshard).copied()
    }

    /// Number of Data Plane cores.
    pub fn num_cores(&self) -> usize {
        self.num_cores
    }

    /// Number of vShards assigned to a given core.
    pub fn shards_on_core(&self, core_id: usize) -> usize {
        self.routes.values().filter(|&&c| c == core_id).count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_robin_distributes_evenly() {
        let router = VShardRouter::round_robin(4);
        for core in 0..4 {
            let count = router.shards_on_core(core);
            // 1024 / 4 = 256 per core.
            assert_eq!(count, 256, "core {core} has {count} shards, expected 256");
        }
    }

    #[test]
    fn resolve_returns_correct_core() {
        let router = VShardRouter::round_robin(4);
        assert_eq!(router.resolve(VShardId::new(0)), Some(0));
        assert_eq!(router.resolve(VShardId::new(1)), Some(1));
        assert_eq!(router.resolve(VShardId::new(4)), Some(0));
    }

    #[test]
    fn all_vshards_routed() {
        let router = VShardRouter::round_robin(8);
        for i in 0..VShardId::COUNT {
            assert!(router.resolve(VShardId::new(i)).is_some());
        }
    }
}
