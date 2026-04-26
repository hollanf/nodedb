//! RPC dispatch trait for array shard fan-out.
//!
//! `ShardRpcDispatch` abstracts the transport used to send one request
//! envelope to a shard and receive one response envelope. The cluster
//! crate defines the trait; the main `nodedb` binary implements it,
//! wiring the QUIC connection pool or an in-process loopback for local
//! shards.
//!
//! Fan-out functions in `scatter.rs` accept `Arc<dyn ShardRpcDispatch>`
//! so they can be tested with mock implementations without a real cluster.

use crate::error::Result;
use crate::wire::VShardEnvelope;

/// Send one request envelope to a shard and await one response envelope.
///
/// Implementors are responsible for:
/// - Routing the envelope to the correct peer (local loopback or QUIC).
/// - Applying per-call timeouts (the caller sets `timeout_ms` in
///   `FanOutParams`; the implementor enforces it).
/// - Returning `Err` on transport failure so the coordinator can
///   propagate the error rather than silently dropping the shard.
#[async_trait::async_trait]
pub trait ShardRpcDispatch: Send + Sync + 'static {
    /// Send `req` to the shard identified by `req.vshard_id` and return
    /// the shard's response envelope.
    async fn call(&self, req: VShardEnvelope, timeout_ms: u64) -> Result<VShardEnvelope>;
}
