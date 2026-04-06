/// Storage tier classification.
///
/// Data flows downward: L0 → L1 → L2 as it ages and cools.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageTier {
    /// L0 (Hot): Lock-free radix trees, memtables, active CRDT states.
    /// Pure RAM — no I/O.
    L0Ram,

    /// L1 (Warm): HNSW graphs, metadata indexes, segment files.
    /// NVMe via mmap + madvise(MADV_WILLNEED).
    /// Zero-copy deserialization, SIMD reads directly from mmap.
    L1Nvme,

    /// L2 (Cold): Historical logs, compressed vector layers.
    /// Remote object storage (S3/GCS), Parquet-encoded.
    /// DataFusion predicate pushdown for minimal egress.
    L2Remote,
}

impl StorageTier {
    /// Whether this tier is memory-resident (no I/O needed).
    pub fn is_memory_resident(self) -> bool {
        matches!(self, StorageTier::L0Ram)
    }

    /// Whether this tier uses mmap (potential page faults on TPC cores).
    pub fn uses_mmap(self) -> bool {
        matches!(self, StorageTier::L1Nvme)
    }

    /// Whether this tier requires network I/O.
    pub fn is_remote(self) -> bool {
        matches!(self, StorageTier::L2Remote)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tier_properties() {
        assert!(StorageTier::L0Ram.is_memory_resident());
        assert!(!StorageTier::L0Ram.uses_mmap());
        assert!(!StorageTier::L0Ram.is_remote());

        assert!(!StorageTier::L1Nvme.is_memory_resident());
        assert!(StorageTier::L1Nvme.uses_mmap());
        assert!(!StorageTier::L1Nvme.is_remote());

        assert!(!StorageTier::L2Remote.is_memory_resident());
        assert!(!StorageTier::L2Remote.uses_mmap());
        assert!(StorageTier::L2Remote.is_remote());
    }
}
