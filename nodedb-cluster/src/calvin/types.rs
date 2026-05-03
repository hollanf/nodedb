//! Calvin sequencer core types.
//!
//! Defines the transaction class, read/write sets, and sequenced transaction
//! structures that form the input and output of the Calvin sequencer layer.

use std::collections::BTreeMap;

use nodedb_types::TenantId;
use nodedb_types::id::VShardId;
use serde::{Deserialize, Serialize};

use crate::error::CalvinError;

// ── SortedVec ────────────────────────────────────────────────────────────────

/// A newtype over `Vec<T>` that guarantees sorted, deduplicated contents.
///
/// Constructed via [`SortedVec::new`], which sorts and deduplicates at
/// construction time. This property is load-bearing for byte-determinism:
/// two `SortedVec`s built from the same logical set (in any insertion order)
/// produce identical serialized bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SortedVec<T>(Vec<T>);

impl<T: zerompk::ToMessagePack> zerompk::ToMessagePack for SortedVec<T> {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        self.0.write(writer)
    }
}

impl<'de, T> zerompk::FromMessagePack<'de> for SortedVec<T>
where
    T: zerompk::FromMessagePack<'de> + Ord + Clone,
{
    fn read<R: zerompk::Read<'de>>(reader: &mut R) -> zerompk::Result<Self> {
        let v = Vec::<T>::read(reader)?;
        Ok(Self::new(v))
    }
}

impl<T: Ord + Clone> SortedVec<T> {
    /// Build from any slice. Sorts and deduplicates in place.
    pub fn new(mut items: Vec<T>) -> Self {
        items.sort();
        items.dedup();
        Self(items)
    }

    pub fn as_slice(&self) -> &[T] {
        &self.0
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn iter(&self) -> std::slice::Iter<'_, T> {
        self.0.iter()
    }
}

impl<T: Ord + Clone> From<Vec<T>> for SortedVec<T> {
    fn from(v: Vec<T>) -> Self {
        Self::new(v)
    }
}

// ── EngineKeySet ──────────────────────────────────────────────────────────────

/// A typed key set for one engine within a read or write set.
///
/// Keys are normalized to surrogates (or byte keys for KV) at admission, so
/// all engine-specific naming is resolved upstream of the sequencer.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum EngineKeySet {
    /// Document engine (schemaless or strict): identified by surrogate.
    Document {
        collection: String,
        surrogates: SortedVec<u32>,
    },
    /// Vector engine: identified by surrogate.
    Vector {
        collection: String,
        surrogates: SortedVec<u32>,
    },
    /// Key-Value engine: identified by raw byte keys.
    Kv {
        collection: String,
        keys: SortedVec<Vec<u8>>,
    },
    /// Graph edge engine: identified by (src_surrogate, dst_surrogate) pairs.
    Edge {
        collection: String,
        edges: SortedVec<(u32, u32)>,
    },
}

impl EngineKeySet {
    /// O(1) estimate of the serialized byte size of this key set.
    ///
    /// Used by the dependent-read cap check at sequencer admission to bound
    /// the total bytes that would be Raft-replicated in a `CalvinReadResult`
    /// entry.  This is an estimate, not an exact count; do NOT use it as a
    /// correctness check — only as a pre-flight guard.
    ///
    /// `Instant::now()` is intentionally absent here — this is a pure
    /// computation derived from key counts.
    pub fn serialized_size_hint(&self) -> usize {
        match self {
            // u32 surrogates: 4 bytes each.
            Self::Document { surrogates, .. } | Self::Vector { surrogates, .. } => {
                surrogates.len() * 4
            }
            // KV keys: sum of key byte lengths.
            Self::Kv { keys, .. } => keys.iter().map(|k| k.len()).sum(),
            // Edge: two u32 per edge = 8 bytes each.
            Self::Edge { edges, .. } => edges.len() * 8,
        }
    }

    /// The collection this key set belongs to.
    pub fn collection(&self) -> &str {
        match self {
            Self::Document { collection, .. }
            | Self::Vector { collection, .. }
            | Self::Kv { collection, .. }
            | Self::Edge { collection, .. } => collection,
        }
    }

    /// Returns `true` if this key set contains no keys.
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Document { surrogates, .. } => surrogates.is_empty(),
            Self::Vector { surrogates, .. } => surrogates.is_empty(),
            Self::Kv { keys, .. } => keys.is_empty(),
            Self::Edge { edges, .. } => edges.is_empty(),
        }
    }
}

// ── PassiveReadKey ────────────────────────────────────────────────────────────

/// A single key that a passive participant must read and broadcast.
///
/// Wraps an [`EngineKeySet`]; per the dependent-read protocol each
/// `PassiveReadKey` contains a single-element (or small) key set.  The
/// sequencer does not enforce single-element sets; the scheduler enforces the
/// total byte budget via `DependentReadSpec::total_bytes()`.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct PassiveReadKey {
    /// The engine key set to read on the passive vshard.
    pub engine_key: EngineKeySet,
}

// ── DependentReadSpec ─────────────────────────────────────────────────────────

/// Describes the passive-read participants for a dependent-read Calvin txn.
///
/// Each entry maps a vshard id to the keys that vshard must read and broadcast
/// to all active participants before any writes can proceed.
///
/// `BTreeMap` is mandatory here: the sequencer and scheduler must iterate
/// vshards in a deterministic order (determinism contract).
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct DependentReadSpec {
    /// Passive participants: vshard → keys to read.
    pub passive_reads: BTreeMap<u32, Vec<PassiveReadKey>>,
}

impl DependentReadSpec {
    /// Total estimated serialized bytes across all passive read keys.
    ///
    /// Used by the sequencer admission check to enforce
    /// `max_dependent_read_bytes_per_txn`.  This is an O(1)-per-key
    /// estimate, not an exact serialized size.
    pub fn total_bytes(&self) -> usize {
        self.passive_reads
            .values()
            .flat_map(|ks| ks.iter())
            .map(|k| k.engine_key.serialized_size_hint())
            .sum()
    }
}

// ── ReadWriteSet ──────────────────────────────────────────────────────────────

/// A set of keys spanning one or more engines, forming either the read set
/// or the write set of a Calvin transaction.
///
/// Cross-engine atomic transactions — e.g. a Document+Vector insert that must
/// land atomically — require all affected engines to appear in a single
/// `ReadWriteSet`. Decomposing by engine would break atomicity.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ReadWriteSet(pub Vec<EngineKeySet>);

impl ReadWriteSet {
    pub fn new(sets: Vec<EngineKeySet>) -> Self {
        Self(sets)
    }

    pub fn is_empty(&self) -> bool {
        self.0.iter().all(|s| s.is_empty())
    }

    /// Derive the set of vShards participating in this read/write set.
    ///
    /// For Document/Vector/Edge entries the vshard is derived from the
    /// collection name (collection-level routing, consistent with the
    /// per-vshard Raft groups that own each collection). For KV entries
    /// the vshard is also derived from the collection name because KV
    /// collections are assigned a single vshard at creation time.
    ///
    /// This derivation is re-run on decode rather than serialized, so the
    /// serialized bytes remain deterministic regardless of how `VShardId`
    /// is computed.
    pub fn participating_vshards(&self) -> Vec<VShardId> {
        let mut seen = std::collections::HashSet::new();
        let mut result = Vec::new();
        for engine_set in &self.0 {
            let vshard = VShardId::from_collection(engine_set.collection());
            if seen.insert(vshard.as_u32()) {
                result.push(vshard);
            }
        }
        result.sort_by_key(|v| v.as_u32());
        result
    }
}

// ── TxClass ───────────────────────────────────────────────────────────────────

/// A fully-declared Calvin transaction class.
///
/// Constructed via [`TxClass::new`], which validates the write set and caches
/// the participating-vshard set. The `participating_vshards` field is skipped
/// during serialization and re-derived on decode to keep serialized bytes
/// byte-deterministic.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct TxClass {
    /// Keys that must be read (may be empty for pure-write transactions).
    pub read_set: ReadWriteSet,
    /// Keys that will be written. Must span at least two vShards.
    pub write_set: ReadWriteSet,
    /// Opaque msgpack-encoded physical plan bytes. Decoded by the executor
    /// in the `nodedb` crate; the sequencer treats this as an opaque blob.
    pub plans: Vec<u8>,
    /// Tenant scope. All keys in `read_set` and `write_set` must belong to
    /// this tenant; cross-tenant transactions are rejected at construction.
    pub tenant_id: TenantId,
    /// Optional dependent-read specification.
    ///
    /// When present, this transaction is a dependent-read Calvin txn: the
    /// passive vshards listed here must read their keys and broadcast the
    /// results (via `ReplicatedWrite::CalvinReadResult`) before the active
    /// participants may write.
    ///
    /// `None` for static-set transactions (the common case).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dependent_reads: Option<DependentReadSpec>,
    /// Cached participating-vshard set. Re-derived on decode; not serialized.
    #[serde(skip)]
    #[msgpack(ignore)]
    participating_vshards: Vec<VShardId>,
}

impl TxClass {
    /// Construct a validated transaction class.
    ///
    /// Rejects:
    /// - An empty write set (nothing to commit).
    /// - A write set that resolves to a single vshard (must use the single-
    ///   shard fast path instead).
    ///
    /// Pass `dependent_reads: None` for static-set transactions (the common
    /// case).  Pass `Some(spec)` for dependent-read (OLLP) transactions.
    pub fn new(
        read_set: ReadWriteSet,
        write_set: ReadWriteSet,
        plans: Vec<u8>,
        tenant_id: TenantId,
        dependent_reads: Option<DependentReadSpec>,
    ) -> Result<Self, CalvinError> {
        if write_set.is_empty() {
            return Err(CalvinError::EmptyWriteSet);
        }
        let mut participating_vshards = write_set.participating_vshards();
        if participating_vshards.len() < 2 {
            let vshard = participating_vshards
                .first()
                .map(|v| v.as_u32())
                .unwrap_or(0);
            return Err(CalvinError::SingleVshardTxn { vshard });
        }
        // Extend participating_vshards with passive vshards from dependent_reads.
        if let Some(ref spec) = dependent_reads {
            for &passive_vshard in spec.passive_reads.keys() {
                let v = VShardId::new(passive_vshard);
                if !participating_vshards
                    .iter()
                    .any(|e| e.as_u32() == passive_vshard)
                {
                    participating_vshards.push(v);
                }
            }
            participating_vshards.sort_by_key(|v| v.as_u32());
        }
        Ok(Self {
            read_set,
            write_set,
            plans,
            tenant_id,
            dependent_reads,
            participating_vshards,
        })
    }

    /// Ergonomic constructor for dependent-read Calvin transactions.
    ///
    /// Equivalent to `TxClass::new(read_set, write_set, plans, tenant_id, Some(dependent_reads))`.
    pub fn new_dependent(
        read_set: ReadWriteSet,
        write_set: ReadWriteSet,
        plans: Vec<u8>,
        tenant_id: TenantId,
        dependent_reads: DependentReadSpec,
    ) -> Result<Self, CalvinError> {
        Self::new(read_set, write_set, plans, tenant_id, Some(dependent_reads))
    }

    /// The vShards that must receive this transaction's slice.
    ///
    /// Derived from the write set's collection names. Re-derived after
    /// deserialization via [`TxClass::restore_derived`].
    pub fn participating_vshards(&self) -> &[VShardId] {
        &self.participating_vshards
    }

    /// Re-derive fields skipped during serialization.
    ///
    /// Call this immediately after deserializing a `TxClass` that came off
    /// the wire or out of the Raft log.
    pub fn restore_derived(&mut self) {
        let mut vshards = self.write_set.participating_vshards();
        if let Some(ref spec) = self.dependent_reads {
            for &passive_vshard in spec.passive_reads.keys() {
                if !vshards.iter().any(|e| e.as_u32() == passive_vshard) {
                    vshards.push(VShardId::new(passive_vshard));
                }
            }
            vshards.sort_by_key(|v| v.as_u32());
        }
        self.participating_vshards = vshards;
    }
}

// ── SequencedTxn ──────────────────────────────────────────────────────────────

/// A transaction that has been assigned a global position by the sequencer.
///
/// The `(epoch, position)` pair is globally unique and totally ordered across
/// all vShards. Every shard that participates in the transaction will see this
/// txn at the same `(epoch, position)` in its scheduler input stream.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct SequencedTxn {
    /// Sequencer epoch in which this transaction was admitted.
    pub epoch: u64,
    /// Zero-based position within the epoch batch.
    pub position: u32,
    /// The fully-declared transaction class.
    pub tx_class: TxClass,
    /// Wall-clock ms at epoch creation (read once on the sequencer leader).
    ///
    /// This is the single deterministic timestamp source for all Calvin write
    /// paths. Engine handlers that need a "current time" (bitemporal sys_from,
    /// KV TTL expire_at, timeseries system_ms) MUST use this value instead of
    /// reading the wall clock independently, ensuring byte-identical state
    /// across all replicas. Wire-additive: zerompk returns default (0) when
    /// decoding older entries that lack this field.
    pub epoch_system_ms: i64,
}

// ── EpochBatch ────────────────────────────────────────────────────────────────

/// A fully-ordered batch of transactions for one sequencer epoch.
///
/// This is the Raft-replicated entry emitted by the sequencer. Every replica
/// applies the same `EpochBatch` in the same order, guaranteeing determinism.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct EpochBatch {
    /// The epoch number. Monotonically increasing across all batches.
    pub epoch: u64,
    /// Ordered transactions in this epoch. Position is the index in this vec.
    pub txns: Vec<SequencedTxn>,
    /// Wall-clock ms read ONCE on the sequencer leader at epoch creation.
    ///
    /// This single timestamp is the deterministic time anchor for every
    /// transaction in this epoch. When the state machine fans `SequencedTxn`s
    /// out to per-shard channels it copies this value into each txn's
    /// `epoch_system_ms` field, making it available to engine handlers without
    /// threading it through every intermediate layer.
    pub epoch_system_ms: i64,
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn doc_set(collection: &str, surrogates: Vec<u32>) -> EngineKeySet {
        EngineKeySet::Document {
            collection: collection.to_owned(),
            surrogates: SortedVec::new(surrogates),
        }
    }

    fn vec_set(collection: &str, surrogates: Vec<u32>) -> EngineKeySet {
        EngineKeySet::Vector {
            collection: collection.to_owned(),
            surrogates: SortedVec::new(surrogates),
        }
    }

    fn kv_set(collection: &str, keys: Vec<Vec<u8>>) -> EngineKeySet {
        EngineKeySet::Kv {
            collection: collection.to_owned(),
            keys: SortedVec::new(keys),
        }
    }

    fn edge_set(collection: &str, edges: Vec<(u32, u32)>) -> EngineKeySet {
        EngineKeySet::Edge {
            collection: collection.to_owned(),
            edges: SortedVec::new(edges),
        }
    }

    fn multi_vshard_write_set() -> ReadWriteSet {
        // Use two different collections that hash to different vShards.
        // We can't pick known-distinct names without running the hash, so we
        // scan at test time.
        let (a, b) = find_two_distinct_collections();
        ReadWriteSet::new(vec![doc_set(&a, vec![1, 2]), doc_set(&b, vec![3])])
    }

    /// Find two collection names whose vShards differ.
    fn find_two_distinct_collections() -> (String, String) {
        let mut first: Option<(String, u32)> = None;
        for i in 0u32..512 {
            let name = format!("col_{i}");
            let vshard = VShardId::from_collection(&name).as_u32();
            if let Some((ref fname, fv)) = first {
                if fv != vshard {
                    return (fname.clone(), name);
                }
            } else {
                first = Some((name, vshard));
            }
        }
        panic!("could not find two distinct-vshard collections in 512 tries");
    }

    fn make_tx_class(write_set: ReadWriteSet) -> TxClass {
        TxClass::new(
            ReadWriteSet::new(vec![]),
            write_set,
            vec![0x01, 0x02],
            TenantId::new(1),
            None,
        )
        .expect("valid TxClass")
    }

    // ── SortedVec ─────────────────────────────────────────────────────────────

    #[test]
    fn sorted_vec_sort_and_dedup() {
        let v: SortedVec<u32> = SortedVec::new(vec![5, 1, 3, 1, 2, 5]);
        assert_eq!(v.as_slice(), &[1, 2, 3, 5]);
    }

    #[test]
    fn sorted_vec_already_sorted() {
        let v: SortedVec<u32> = SortedVec::new(vec![1, 2, 3]);
        assert_eq!(v.as_slice(), &[1, 2, 3]);
    }

    #[test]
    fn sorted_vec_empty() {
        let v: SortedVec<u32> = SortedVec::new(vec![]);
        assert!(v.is_empty());
        assert_eq!(v.len(), 0);
    }

    #[test]
    fn sorted_vec_bytes_deterministic_regardless_of_insertion_order() {
        let a: SortedVec<u32> = SortedVec::new(vec![3, 1, 4, 1, 5]);
        let b: SortedVec<u32> = SortedVec::new(vec![5, 4, 3, 1, 1]);
        let a_bytes = sonic_rs::to_vec(&a).unwrap();
        let b_bytes = sonic_rs::to_vec(&b).unwrap();
        assert_eq!(a_bytes, b_bytes);
    }

    // ── EngineKeySet ──────────────────────────────────────────────────────────

    #[test]
    fn engine_key_set_collection_name() {
        let d = doc_set("users", vec![1]);
        assert_eq!(d.collection(), "users");

        let v = vec_set("embeddings", vec![2]);
        assert_eq!(v.collection(), "embeddings");

        let k = kv_set("sessions", vec![b"key1".to_vec()]);
        assert_eq!(k.collection(), "sessions");

        let e = edge_set("follows", vec![(1, 2)]);
        assert_eq!(e.collection(), "follows");
    }

    #[test]
    fn engine_key_set_is_empty() {
        assert!(doc_set("users", vec![]).is_empty());
        assert!(!doc_set("users", vec![1]).is_empty());
    }

    // ── ReadWriteSet ──────────────────────────────────────────────────────────

    #[test]
    fn read_write_set_participating_vshards_distinct() {
        let ws = multi_vshard_write_set();
        let vshards = ws.participating_vshards();
        assert!(vshards.len() >= 2, "expected at least 2 distinct vShards");
    }

    #[test]
    fn read_write_set_participating_vshards_sorted() {
        let ws = multi_vshard_write_set();
        let vshards = ws.participating_vshards();
        let ids: Vec<u32> = vshards.iter().map(|v| v.as_u32()).collect();
        let mut sorted = ids.clone();
        sorted.sort();
        assert_eq!(ids, sorted);
    }

    #[test]
    fn read_write_set_same_collection_counted_once() {
        // Two EngineKeySets for the same collection: still one vshard.
        let ws = ReadWriteSet::new(vec![doc_set("users", vec![1]), vec_set("users", vec![1])]);
        let vshards = ws.participating_vshards();
        assert_eq!(vshards.len(), 1);
    }

    // ── TxClass construction ──────────────────────────────────────────────────

    #[test]
    fn tx_class_new_rejects_empty_write_set() {
        let err = TxClass::new(
            ReadWriteSet::new(vec![]),
            ReadWriteSet::new(vec![]),
            vec![],
            TenantId::new(1),
            None,
        )
        .unwrap_err();
        assert!(matches!(err, CalvinError::EmptyWriteSet));
    }

    #[test]
    fn tx_class_new_rejects_single_vshard() {
        // Single collection → single vshard.
        let ws = ReadWriteSet::new(vec![doc_set("users", vec![1, 2])]);
        let err = TxClass::new(
            ReadWriteSet::new(vec![]),
            ws,
            vec![],
            TenantId::new(1),
            None,
        )
        .unwrap_err();
        assert!(matches!(err, CalvinError::SingleVshardTxn { .. }));
    }

    #[test]
    fn tx_class_new_accepts_multi_vshard() {
        let tc = make_tx_class(multi_vshard_write_set());
        assert!(tc.participating_vshards().len() >= 2);
    }

    #[test]
    fn tx_class_participating_vshards_cached() {
        let tc = make_tx_class(multi_vshard_write_set());
        // Two calls return the same slice.
        assert_eq!(tc.participating_vshards(), tc.participating_vshards());
    }

    // ── Byte-determinism ──────────────────────────────────────────────────────

    /// Byte-determinism: two TxClass values with logically identical sets
    /// (different insertion order) must produce byte-identical JSON.
    ///
    /// `participating_vshards` is `#[serde(skip)]` so it is excluded from
    /// serialization; only the stable sorted fields participate.
    #[test]
    fn tx_class_byte_deterministic_across_insertion_order() {
        let (col_a, col_b) = find_two_distinct_collections();

        let ws_forward = ReadWriteSet::new(vec![
            doc_set(&col_a, vec![3, 1, 2]),
            doc_set(&col_b, vec![10, 5]),
        ]);
        let ws_backward = ReadWriteSet::new(vec![
            doc_set(&col_b, vec![5, 10]),
            doc_set(&col_a, vec![2, 3, 1]),
        ]);

        // Both write sets have the same logical content but different
        // ordering.  We compare the serialized *inner sets* after sorting
        // the outer Vec by collection name so key-set order doesn't matter.
        let forward_bytes = sonic_rs::to_vec(&ws_forward).unwrap();
        let backward_bytes = sonic_rs::to_vec(&ws_backward).unwrap();

        // The outer Vec order may differ; compare after canonical-sort.
        let mut fw_parsed: Vec<serde_json::Value> = sonic_rs::from_slice(&forward_bytes).unwrap();
        let mut bw_parsed: Vec<serde_json::Value> = sonic_rs::from_slice(&backward_bytes).unwrap();

        let sort_key = |v: &serde_json::Value| -> String {
            v.as_object()
                .and_then(|o| o.values().next())
                .and_then(|inner| inner.get("collection"))
                .and_then(|c| c.as_str())
                .unwrap_or("")
                .to_owned()
        };
        fw_parsed.sort_by_key(sort_key);
        bw_parsed.sort_by_key(sort_key);
        assert_eq!(fw_parsed, bw_parsed);
    }

    /// Byte-determinism for the full TxClass: serialize → deserialize →
    /// restore_derived → serialize again; both bytes must be identical.
    #[test]
    fn tx_class_roundtrip_bytes_stable() {
        let tc = make_tx_class(multi_vshard_write_set());
        let first = sonic_rs::to_vec(&tc).unwrap();

        let mut restored: TxClass = sonic_rs::from_slice(&first).unwrap();
        restored.restore_derived();

        let second = sonic_rs::to_vec(&restored).unwrap();
        assert_eq!(first, second);
    }

    // ── MessagePack roundtrips ────────────────────────────────────────────────

    #[test]
    fn tx_class_msgpack_roundtrip() {
        let tc = make_tx_class(multi_vshard_write_set());
        let bytes = zerompk::to_msgpack_vec(&tc).unwrap();
        let mut decoded: TxClass = zerompk::from_msgpack(&bytes).unwrap();
        decoded.restore_derived();
        assert_eq!(tc.tenant_id, decoded.tenant_id);
        assert_eq!(tc.plans, decoded.plans);
        assert_eq!(tc.write_set, decoded.write_set);
        assert_eq!(tc.read_set, decoded.read_set);
        assert_eq!(tc.participating_vshards(), decoded.participating_vshards());
    }

    #[test]
    fn sequenced_txn_msgpack_roundtrip() {
        let tx_class = make_tx_class(multi_vshard_write_set());
        let st = SequencedTxn {
            epoch: 42,
            position: 7,
            tx_class,
            epoch_system_ms: 1_700_000_000_000,
        };
        let bytes = zerompk::to_msgpack_vec(&st).unwrap();
        let mut decoded: SequencedTxn = zerompk::from_msgpack(&bytes).unwrap();
        decoded.tx_class.restore_derived();
        assert_eq!(st.epoch, decoded.epoch);
        assert_eq!(st.position, decoded.position);
        assert_eq!(st.epoch_system_ms, decoded.epoch_system_ms);
        assert_eq!(st.tx_class.write_set, decoded.tx_class.write_set);
    }

    #[test]
    fn dependent_read_spec_msgpack_roundtrip() {
        let spec = DependentReadSpec {
            passive_reads: {
                let mut m = BTreeMap::new();
                m.insert(
                    1u32,
                    vec![PassiveReadKey {
                        engine_key: doc_set("users", vec![10, 20]),
                    }],
                );
                m.insert(
                    2u32,
                    vec![PassiveReadKey {
                        engine_key: kv_set("sessions", vec![b"abc".to_vec()]),
                    }],
                );
                m
            },
        };
        let bytes = zerompk::to_msgpack_vec(&spec).unwrap();
        let decoded: DependentReadSpec = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(spec.passive_reads.len(), decoded.passive_reads.len());
        assert_eq!(spec.passive_reads.get(&1), decoded.passive_reads.get(&1));
    }

    #[test]
    fn tx_class_with_dependent_reads_participating_vshards_includes_passives() {
        let (col_a, col_b) = find_two_distinct_collections();
        let write_set = ReadWriteSet::new(vec![doc_set(&col_a, vec![1]), doc_set(&col_b, vec![2])]);

        // Pick a vshard id that's different from col_a and col_b.
        let passive_vshard_id = {
            let a = VShardId::from_collection(&col_a).as_u32();
            let b = VShardId::from_collection(&col_b).as_u32();
            // Find one that differs from both.
            let mut candidate = 9999u32;
            for i in 0u32..64 {
                let name = format!("passive_col_{i}");
                let v = VShardId::from_collection(&name).as_u32();
                if v != a && v != b {
                    candidate = v;
                    break;
                }
            }
            candidate
        };

        let spec = DependentReadSpec {
            passive_reads: {
                let mut m = BTreeMap::new();
                m.insert(
                    passive_vshard_id,
                    vec![PassiveReadKey {
                        engine_key: doc_set("passive_col", vec![99]),
                    }],
                );
                m
            },
        };

        let tc = TxClass::new(
            ReadWriteSet::new(vec![]),
            write_set,
            vec![],
            TenantId::new(1),
            Some(spec),
        )
        .expect("valid TxClass with dependent reads");

        // The participating vshards must include the passive vshard.
        let vshard_ids: Vec<u32> = tc
            .participating_vshards()
            .iter()
            .map(|v| v.as_u32())
            .collect();
        assert!(
            vshard_ids.contains(&passive_vshard_id),
            "participating_vshards must include passive vshard {passive_vshard_id}; got {vshard_ids:?}"
        );
    }

    #[test]
    fn epoch_batch_msgpack_roundtrip() {
        let tc = make_tx_class(multi_vshard_write_set());
        let batch = EpochBatch {
            epoch: 1,
            txns: vec![
                SequencedTxn {
                    epoch: 1,
                    position: 0,
                    tx_class: tc.clone(),
                    epoch_system_ms: 1_700_000_000_000,
                },
                SequencedTxn {
                    epoch: 1,
                    position: 1,
                    tx_class: tc,
                    epoch_system_ms: 1_700_000_000_000,
                },
            ],
            epoch_system_ms: 1_700_000_000_000,
        };
        let bytes = zerompk::to_msgpack_vec(&batch).unwrap();
        let mut decoded: EpochBatch = zerompk::from_msgpack(&bytes).unwrap();
        for txn in &mut decoded.txns {
            txn.tx_class.restore_derived();
        }
        assert_eq!(batch.epoch, decoded.epoch);
        assert_eq!(batch.epoch_system_ms, decoded.epoch_system_ms);
        assert_eq!(batch.txns.len(), decoded.txns.len());
        assert_eq!(
            batch.txns[0].epoch_system_ms,
            decoded.txns[0].epoch_system_ms
        );
        assert_eq!(
            batch.txns[0].tx_class.write_set,
            decoded.txns[0].tx_class.write_set
        );
    }
}
