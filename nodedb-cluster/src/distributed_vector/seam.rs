//! Trait seam for distributed vector algorithms.
//!
//! `VectorShardSeam` defines the cross-shard exchange surface that advanced
//! distributed vector search algorithms require. The current production
//! implementation uses flat scatter-gather (`VectorScatterGather`), which
//! corresponds to `select_shards` returning all shards and the other methods
//! being no-ops. The trait reserves the shape so future algorithms can be
//! added by implementing a concrete type without a protocol break.
//!
//! # Algorithm mapping
//!
//! ## Compass (coarse-code multi-stage routing)
//!
//! Compass performs a global coarse-code lookup before the fine search to
//! select only the most relevant shards. The coordinator calls
//! `select_shards` with the query to obtain a pruned shard subset (instead
//! of fanning out to every shard), then issues a standard k-NN scatter
//! against that subset. The wire messages `VectorCoarseRouteRequest` and
//! `VectorCoarseRouteResponse` carry the per-shard coarse descriptor used
//! by `select_shards` to make the pruning decision.
//!
//! Methods used: `select_shards`.
//! Default behaviour: all shards selected (equivalent to current scatter-gather).
//!
//! ## SPIRE (Shared Per-shard Inverted Residual Encoding)
//!
//! SPIRE requires each shard to share its IVF centroid table with peers at
//! index-build time so queries can leverage cross-shard centroid knowledge.
//! This exchange is initiated shard-to-shard (not coordinator-mediated) and
//! happens once per build, not per query. `build_time_exchange` is the hook
//! for this; `direct_message` is the transport primitive it calls.
//!
//! Methods used: `build_time_exchange`, `direct_message`.
//! Default behaviour: no-op build exchange; `direct_message` routes through
//! the coordinator if a `ShardRpcDispatch` is available, otherwise returns
//! `Err(VectorSeamError::DirectMessagingUnsupported)`.
//!
//! ## CoTra-RDMA (Cooperative Traversal over RDMA)
//!
//! CoTra-RDMA performs one-sided RDMA READs directly into a remote shard's
//! HNSW graph without involving the remote CPU. The reading shard first calls
//! `exposed_region` on the remote shard descriptor to obtain the registered
//! memory region (address + rkey); it then issues one-sided reads at the
//! hardware level. When RDMA is unavailable, `exposed_region` returns `None`
//! and the algorithm falls back to standard scatter-gather.
//!
//! Methods used: `exposed_region`.
//! Default behaviour: `None` — RDMA direct reads not supported by this impl.

use crate::distributed_array::rpc::ShardRpcDispatch;
use crate::error::{ClusterError, Result};
use crate::wire::VShardEnvelope;

/// Opaque payload exchanged between shards for algorithm-specific messages.
///
/// The contents are algorithm-defined and serialized with zerompk. The seam
/// trait does not interpret the payload; algorithms define their own types
/// and encode/decode them.
#[derive(Debug, Clone)]
pub struct ShardMessage {
    /// Discriminant identifying which algorithm/phase produced this message.
    pub kind: ShardMessageKind,
    /// zerompk-encoded algorithm-specific payload.
    pub payload: Vec<u8>,
}

/// Discriminant for `ShardMessage` payloads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardMessageKind {
    /// Compass phase-1 coarse descriptor request/response.
    CompassCoarseDescriptor,
    /// SPIRE build-time IVF centroid table exchange.
    SpireCentroidTable,
}

/// Reply to a `direct_message` call.
#[derive(Debug, Clone)]
pub struct ShardMessageReply {
    /// zerompk-encoded reply payload. Empty for fire-and-forget messages.
    pub payload: Vec<u8>,
}

/// Errors specific to `VectorShardSeam` operations.
#[derive(Debug, thiserror::Error)]
pub enum VectorSeamError {
    #[error("direct shard-to-shard messaging is not supported by this implementation")]
    DirectMessagingUnsupported,
    #[error("build-time exchange failed for shard {peer_shard}: {detail}")]
    BuildExchangeFailed { peer_shard: u16, detail: String },
    #[error("cluster transport error during seam call: {0}")]
    Transport(#[from] ClusterError),
}

/// A registered memory region exposed for one-sided (RDMA) reads.
///
/// When `exposed_region` returns `Some(MemoryRegion)`, the receiving side
/// may read `len` bytes starting at `remote_addr` using RDMA with the
/// provided `rkey`. No CPU involvement on the target shard is required.
///
/// When RDMA hardware or software support is absent, `exposed_region`
/// returns `None` and callers must fall back to standard RPC-based access.
#[derive(Debug, Clone, Copy)]
pub struct MemoryRegion {
    /// Remote virtual address of the pinned region.
    pub remote_addr: u64,
    /// RDMA remote key authorizing reads of this region.
    pub rkey: u32,
    /// Length of the region in bytes.
    pub len: usize,
}

/// A reference to a peer shard used as a target for seam calls.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ShardRef {
    /// The peer node ID.
    pub node_id: u64,
    /// The vShard ID on that node.
    pub vshard_id: u16,
}

/// Subset of shards selected for a query phase.
///
/// `All` means the caller should contact every shard in the collection —
/// identical to current scatter-gather behaviour. `Subset` carries a pruned
/// list; the coordinator fans out only to those shards.
#[derive(Debug, Clone)]
pub enum ShardSubset {
    /// Contact all shards in the collection (default scatter-gather).
    All,
    /// Contact only this subset of shard IDs.
    Subset(Vec<u16>),
}

impl ShardSubset {
    /// Resolve the subset against a full shard list.
    ///
    /// Returns the shards to actually contact: either the full list (for
    /// `All`) or the stored subset filtered to those present in `all_shards`.
    pub fn resolve<'a>(&'a self, all_shards: &'a [u16]) -> &'a [u16] {
        match self {
            ShardSubset::All => all_shards,
            ShardSubset::Subset(ids) => ids.as_slice(),
        }
    }
}

/// Cross-shard exchange seam for distributed vector search algorithms.
///
/// The default implementations preserve the current flat scatter-gather
/// behaviour. Concrete types implementing advanced algorithms (Compass,
/// SPIRE, CoTra-RDMA) override only the methods they need. Existing call
/// sites that construct `VectorScatterGather` directly are unaffected.
///
/// # Plane safety
///
/// This trait is `Send + Sync + 'static` so it can be held by the Control
/// Plane coordinator. Methods that touch the Data Plane must do so via the
/// existing SPSC bridge — they must not perform storage I/O directly.
pub trait VectorShardSeam: Send + Sync + 'static {
    /// Select the subset of shards to contact for a query.
    ///
    /// Default: `ShardSubset::All` — equivalent to current scatter-gather.
    ///
    /// Compass overrides this to perform a coarse-code lookup against
    /// previously obtained shard descriptors and return a pruned
    /// `ShardSubset::Subset`. The coordinator calls this before building
    /// scatter envelopes.
    fn select_shards(&self, _query_vector: &[f32], _all_shards: &[u16]) -> ShardSubset {
        ShardSubset::All
    }

    /// Build-time exchange with a peer shard.
    ///
    /// Default: no-op — returns `Ok(())` immediately.
    ///
    /// SPIRE overrides this to send the local shard's IVF centroid table to
    /// `peer` via `direct_message` so the peer can build cross-shard centroid
    /// knowledge. Called once per peer at index-build time, not per query.
    ///
    /// Implementors must not block the Tokio thread pool for heavy work;
    /// offload encoding to `spawn_blocking` if the payload is large.
    fn build_time_exchange(&self, _peer: ShardRef, _dispatch: &dyn ShardRpcDispatch) -> Result<()> {
        Ok(())
    }

    /// Send a message directly to a peer shard and await its reply.
    ///
    /// Default: routes through `dispatch` (coordinator-mediated QUIC) if
    /// provided, making it equivalent to a normal RPC call. Algorithms that
    /// need genuinely shard-to-shard messaging (without coordinator
    /// involvement) implement this method with a direct connection pool.
    ///
    /// When direct messaging is structurally impossible for the given
    /// implementation, return
    /// `Err(VectorSeamError::DirectMessagingUnsupported)`.
    ///
    /// # Wire mapping
    ///
    /// The `msg` payload is wrapped in a `VShardEnvelope` using the
    /// appropriate `VShardMessageType` for `msg.kind` before dispatch.
    fn direct_message(
        &self,
        peer: ShardRef,
        msg: ShardMessage,
        dispatch: &dyn ShardRpcDispatch,
        source_node: u64,
    ) -> impl std::future::Future<Output = std::result::Result<ShardMessageReply, VectorSeamError>> + Send
    {
        let envelope = build_message_envelope(source_node, peer, &msg);
        async move {
            let reply = dispatch
                .call(envelope, 5_000)
                .await
                .map_err(VectorSeamError::Transport)?;
            Ok(ShardMessageReply {
                payload: reply.payload,
            })
        }
    }

    /// Return the memory region this shard exposes for one-sided reads, if any.
    ///
    /// Default: `None` — RDMA direct reads are not supported.
    ///
    /// CoTra-RDMA overrides this to return the registered pinned region for
    /// the HNSW graph. The calling shard uses the returned `MemoryRegion` to
    /// issue one-sided RDMA READs without involving this shard's CPU. When
    /// `None` is returned, callers fall back to coordinator-mediated RPC.
    fn exposed_region(&self) -> Option<MemoryRegion> {
        None
    }
}

/// Encode a `ShardMessage` into a `VShardEnvelope` for dispatch.
fn build_message_envelope(source_node: u64, peer: ShardRef, msg: &ShardMessage) -> VShardEnvelope {
    use crate::wire::{VShardEnvelope, VShardMessageType, WIRE_VERSION};
    let msg_type = match msg.kind {
        ShardMessageKind::CompassCoarseDescriptor => VShardMessageType::VectorCoarseRouteRequest,
        ShardMessageKind::SpireCentroidTable => VShardMessageType::VectorBuildExchangeRequest,
    };
    VShardEnvelope {
        version: WIRE_VERSION,
        msg_type,
        source_node,
        target_node: peer.node_id,
        vshard_id: peer.vshard_id,
        payload: msg.payload.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A no-op seam using all default implementations.
    struct DefaultSeam;
    impl VectorShardSeam for DefaultSeam {}

    #[test]
    fn default_select_shards_returns_all() {
        let seam = DefaultSeam;
        let all = [0u16, 1, 2, 3];
        let result = seam.select_shards(&[0.1, 0.2], &all);
        assert!(matches!(result, ShardSubset::All));
        assert_eq!(result.resolve(&all), &all);
    }

    #[test]
    fn default_exposed_region_is_none() {
        let seam = DefaultSeam;
        assert!(seam.exposed_region().is_none());
    }

    #[test]
    fn default_build_time_exchange_is_noop() {
        struct MockDispatch;
        #[async_trait::async_trait]
        impl crate::distributed_array::rpc::ShardRpcDispatch for MockDispatch {
            async fn call(
                &self,
                _req: VShardEnvelope,
                _timeout_ms: u64,
            ) -> crate::error::Result<VShardEnvelope> {
                Err(crate::error::ClusterError::Transport {
                    detail: "mock".into(),
                })
            }
        }
        let seam = DefaultSeam;
        let peer = ShardRef {
            node_id: 2,
            vshard_id: 5,
        };
        let dispatch = MockDispatch;
        // Default build_time_exchange is a no-op — never calls dispatch.
        assert!(seam.build_time_exchange(peer, &dispatch).is_ok());
    }

    #[test]
    fn shard_subset_resolve_subset() {
        let all = [0u16, 1, 2, 3, 4];
        let subset = ShardSubset::Subset(vec![1, 3]);
        assert_eq!(subset.resolve(&all), &[1u16, 3]);
    }

    #[test]
    fn shard_subset_resolve_all() {
        let all = [0u16, 1, 2];
        let subset = ShardSubset::All;
        assert_eq!(subset.resolve(&all), &[0u16, 1, 2]);
    }

    #[test]
    fn memory_region_fields() {
        let region = MemoryRegion {
            remote_addr: 0xDEAD_BEEF_0000_0000,
            rkey: 42,
            len: 1024 * 1024,
        };
        assert_eq!(region.rkey, 42);
        assert_eq!(region.len, 1024 * 1024);
    }

    #[test]
    fn build_message_envelope_compass() {
        use crate::wire::VShardMessageType;
        let peer = ShardRef {
            node_id: 7,
            vshard_id: 3,
        };
        let msg = ShardMessage {
            kind: ShardMessageKind::CompassCoarseDescriptor,
            payload: vec![1, 2, 3],
        };
        let env = build_message_envelope(1, peer, &msg);
        assert_eq!(env.msg_type, VShardMessageType::VectorCoarseRouteRequest);
        assert_eq!(env.target_node, 7);
        assert_eq!(env.vshard_id, 3);
        assert_eq!(env.payload, vec![1, 2, 3]);
    }

    #[test]
    fn build_message_envelope_spire() {
        use crate::wire::VShardMessageType;
        let peer = ShardRef {
            node_id: 9,
            vshard_id: 1,
        };
        let msg = ShardMessage {
            kind: ShardMessageKind::SpireCentroidTable,
            payload: vec![0xFF, 0xAA],
        };
        let env = build_message_envelope(2, peer, &msg);
        assert_eq!(env.msg_type, VShardMessageType::VectorBuildExchangeRequest);
        assert_eq!(env.source_node, 2);
    }
}
