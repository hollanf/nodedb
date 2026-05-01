//! Per-peer QUIC connection pool: registration, dialling, eviction, warm-up.

use std::net::SocketAddr;

use tracing::debug;

use crate::error::{ClusterError, Result};
use crate::transport::config::SNI_HOSTNAME;
use crate::wire_version::handshake_io::{local_version_range, perform_version_handshake_client};

use super::transport::NexarTransport;

impl NexarTransport {
    /// Register a peer's address for outbound connections.
    pub fn register_peer(&self, node_id: u64, addr: SocketAddr) {
        let mut addrs = self.peer_addrs.write().unwrap_or_else(|p| p.into_inner());
        addrs.insert(node_id, addr);
        debug!(node_id, %addr, "peer registered");
    }

    /// Pre-warm the QUIC connection cache for a peer by performing the full
    /// dial + handshake and inserting the connection into the peer cache. On
    /// success, the next `send_rpc(target, ...)` skips the dial entirely and
    /// reuses the cached `quinn::Connection`.
    ///
    /// Caller MUST have called [`register_peer`] first — this function
    /// resolves the peer address from the `peer_addrs` map. Used by the
    /// startup `warm_peers` phase so the first replicated request after boot
    /// doesn't pay a cold-connect penalty.
    ///
    /// [`register_peer`]: Self::register_peer
    pub async fn warm_peer(&self, node_id: u64) -> Result<()> {
        self.get_or_connect(node_id).await.map(|_| ())
    }

    /// Get the stable ID of the cached connection to a peer.
    ///
    /// Returns `None` if no connection is cached or the connection is closed.
    /// Used during migrations to detect if the peer connection changed
    /// (indicating possible node replacement).
    pub fn peer_connection_stable_id(&self, target: u64) -> Option<usize> {
        let peers = self.peers.read().unwrap_or_else(|p| p.into_inner());
        peers.get(&target).and_then(|conn| {
            if conn.close_reason().is_none() {
                Some(conn.stable_id())
            } else {
                None
            }
        })
    }

    /// Get an existing connection to a peer, or establish a new one.
    pub(super) async fn get_or_connect(&self, target: u64) -> Result<quinn::Connection> {
        // Check cache — fast path.
        {
            let peers = self.peers.read().unwrap_or_else(|p| p.into_inner());
            if let Some(conn) = peers.get(&target)
                && conn.close_reason().is_none()
            {
                return Ok(conn.clone());
            }
        }

        // Resolve address.
        let addr = {
            let addrs = self.peer_addrs.read().unwrap_or_else(|p| p.into_inner());
            addrs
                .get(&target)
                .copied()
                .ok_or(ClusterError::NodeUnreachable { node_id: target })?
        };

        // Connect — bounded by rpc_timeout so a hung QUIC handshake
        // (peer not yet serving) doesn't block for the full 30s idle timeout.
        let connecting = self
            .listener
            .endpoint()
            .connect_with(self.client_config.clone(), addr, SNI_HOSTNAME)
            .map_err(|e| ClusterError::Transport {
                detail: format!("connect to node {target} at {addr}: {e}"),
            })?;
        let conn = tokio::time::timeout(self.rpc_timeout, connecting)
            .await
            .map_err(|_| ClusterError::Transport {
                detail: format!(
                    "handshake timeout ({}ms) with node {target} at {addr}",
                    self.rpc_timeout.as_millis()
                ),
            })?
            .map_err(|e| ClusterError::Transport {
                detail: format!("handshake with node {target} at {addr}: {e}"),
            })?;

        debug!(target, %addr, "connected to peer");

        // Open a dedicated bidi stream for the wire-version handshake.
        // This must complete before any RPC stream is opened on this connection.
        let agreed = {
            let (mut hs_send, mut hs_recv) =
                conn.open_bi().await.map_err(|e| ClusterError::Transport {
                    detail: format!("open handshake stream to node {target} at {addr}: {e}"),
                })?;
            let version = tokio::time::timeout(
                self.rpc_timeout,
                perform_version_handshake_client(&mut hs_send, &mut hs_recv),
            )
            .await
            .map_err(|_| ClusterError::Transport {
                detail: format!(
                    "handshake timeout ({}ms) with node {target} at {addr}",
                    self.rpc_timeout.as_millis()
                ),
            })??;
            // Finish the handshake send stream — the server reads it until FIN.
            let _ = hs_send.finish();
            version
        };

        let local = local_version_range();
        debug!(
            target,
            %addr,
            agreed_version = %agreed,
            local_min = %local.min,
            local_max = %local.max,
            "wire version negotiated"
        );

        // Cache the agreed version keyed on the QUIC connection's stable id.
        self.store_agreed_version(conn.stable_id(), agreed);

        // Cache (harmless race: last writer wins, both connections valid).
        let mut peers = self.peers.write().unwrap_or_else(|p| p.into_inner());
        peers.insert(target, conn.clone());
        Ok(conn)
    }

    /// Remove a cached connection (forces reconnect on next use).
    pub(super) fn evict_peer(&self, target: u64) {
        let stable_id = {
            let peers = self.peers.read().unwrap_or_else(|p| p.into_inner());
            peers.get(&target).map(|c| c.stable_id())
        };
        let mut peers = self.peers.write().unwrap_or_else(|p| p.into_inner());
        peers.remove(&target);
        drop(peers);
        if let Some(id) = stable_id {
            self.evict_agreed_version(id);
        }
    }
}
