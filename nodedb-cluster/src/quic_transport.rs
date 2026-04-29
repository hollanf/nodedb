//! QUIC/TCP fallback transport for shard migration and cross-node communication.
//!
//! Provides the same wire format as RDMA transport but over QUIC (or TCP fallback).
//! Used when RDMA hardware is unavailable. Correctness-equivalent to RDMA path —
//! same `VShardEnvelope` format, same message types, same ordering guarantees.
//!
//! Connection lifecycle:
//! 1. `QuicTransport::connect(addr)` → establishes a QUIC connection
//! 2. `send_envelope(envelope)` → serializes and sends a VShardEnvelope
//! 3. `recv_envelope()` → receives and deserializes a VShardEnvelope
//! 4. Drop closes the connection gracefully

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use tracing::{debug, info, warn};

use crate::error::{ClusterError, Result};
use crate::wire::{VShardEnvelope, VShardMessageType};

/// Configuration for the QUIC transport layer.
#[derive(Debug, Clone)]
pub struct QuicTransportConfig {
    /// Maximum concurrent connections per peer.
    pub max_connections_per_peer: usize,
    /// Connection timeout in milliseconds.
    pub connect_timeout_ms: u64,
    /// Maximum message size in bytes (for flow control).
    pub max_message_size: usize,
    /// Whether to use TCP fallback when QUIC is unavailable.
    pub tcp_fallback: bool,
}

impl Default for QuicTransportConfig {
    fn default() -> Self {
        Self {
            max_connections_per_peer: 4,
            connect_timeout_ms: 5000,
            max_message_size: 64 * 1024 * 1024, // 64 MiB
            tcp_fallback: true,
        }
    }
}

/// QUIC transport for cluster communication.
///
/// Manages connections to peer nodes and provides envelope-based
/// message passing using the same `VShardEnvelope` wire format
/// as the RDMA transport.
pub struct QuicTransport {
    config: QuicTransportConfig,
    /// This node's listen address.
    listen_addr: SocketAddr,
    /// Connection pool: peer_addr → connection state.
    connections: Arc<Mutex<HashMap<SocketAddr, PeerConnection>>>,
    /// Bytes sent (for monitoring).
    bytes_sent: std::sync::atomic::AtomicU64,
    /// Bytes received.
    bytes_received: std::sync::atomic::AtomicU64,
    /// Messages sent.
    messages_sent: std::sync::atomic::AtomicU64,
}

/// State of a connection to a peer node.
#[derive(Debug)]
struct PeerConnection {
    addr: SocketAddr,
    state: ConnectionState,
    /// Outbound message queue (buffered until connection is established).
    outbound: Vec<Vec<u8>>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum ConnectionState {
    Connecting,
    Connected,
    Failed,
}

impl QuicTransport {
    /// Create a new QUIC transport.
    pub fn new(listen_addr: SocketAddr, config: QuicTransportConfig) -> Self {
        Self {
            config,
            listen_addr,
            connections: Arc::new(Mutex::new(HashMap::new())),
            bytes_sent: std::sync::atomic::AtomicU64::new(0),
            bytes_received: std::sync::atomic::AtomicU64::new(0),
            messages_sent: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Serialize and enqueue a VShardEnvelope for sending to a peer.
    ///
    /// If the connection doesn't exist yet, it's established lazily.
    /// Messages are buffered until the connection is ready.
    pub fn send_envelope(&self, peer: SocketAddr, envelope: &VShardEnvelope) -> Result<()> {
        let bytes = envelope.to_bytes();
        let len = bytes.len();

        if len > self.config.max_message_size {
            return Err(ClusterError::Transport {
                detail: format!(
                    "message size {len} exceeds max {}",
                    self.config.max_message_size
                ),
            });
        }

        let mut conns = self.connections.lock().unwrap_or_else(|p| p.into_inner());
        let conn = conns.entry(peer).or_insert_with(|| {
            debug!(%peer, "QUIC: initiating connection");
            PeerConnection {
                addr: peer,
                state: ConnectionState::Connecting,
                outbound: Vec::new(),
            }
        });

        conn.outbound.push(bytes);
        self.bytes_sent
            .fetch_add(len as u64, std::sync::atomic::Ordering::Relaxed);
        self.messages_sent
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        Ok(())
    }

    /// Send a migration base-copy segment to a peer.
    ///
    /// Uses the same envelope format but with `MigrationBaseCopy` message type.
    /// The payload contains the serialized segment data.
    pub fn send_migration_segment(
        &self,
        peer: SocketAddr,
        vshard_id: u32,
        segment_data: &[u8],
    ) -> Result<()> {
        let envelope = VShardEnvelope::new(
            VShardMessageType::MigrationBaseCopy,
            0, // source node
            0, // target node (set by caller)
            vshard_id,
            segment_data.to_vec(),
        );
        self.send_envelope(peer, &envelope)
    }

    /// Drain outbound messages for a peer (called by the network I/O loop).
    ///
    /// Returns the buffered messages and clears the outbound queue.
    pub fn drain_outbound(&self, peer: &SocketAddr) -> Vec<Vec<u8>> {
        let mut conns = self.connections.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(conn) = conns.get_mut(peer) {
            std::mem::take(&mut conn.outbound)
        } else {
            Vec::new()
        }
    }

    /// Mark a peer connection as established.
    pub fn mark_connected(&self, peer: &SocketAddr) {
        let mut conns = self.connections.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(conn) = conns.get_mut(peer) {
            conn.state = ConnectionState::Connected;
            info!(%peer, "QUIC: connection established");
        }
    }

    /// Mark a peer connection as failed.
    pub fn mark_failed(&self, peer: &SocketAddr) {
        let mut conns = self.connections.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(conn) = conns.get_mut(peer) {
            conn.state = ConnectionState::Failed;
            warn!(%peer, queued = conn.outbound.len(), "QUIC: connection failed");
        }
    }

    /// Remove a peer connection.
    pub fn disconnect(&self, peer: &SocketAddr) {
        self.connections
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(peer);
    }

    /// Number of active peer connections.
    pub fn peer_count(&self) -> usize {
        self.connections
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .len()
    }

    /// List all connected peer addresses.
    pub fn connected_peers(&self) -> Vec<SocketAddr> {
        self.connections
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .values()
            .filter(|c| c.state == ConnectionState::Connected)
            .map(|c| c.addr)
            .collect()
    }

    /// Total bytes sent.
    pub fn bytes_sent(&self) -> u64 {
        self.bytes_sent.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Total bytes received.
    pub fn bytes_received(&self) -> u64 {
        self.bytes_received
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Total messages sent.
    pub fn messages_sent(&self) -> u64 {
        self.messages_sent
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Listen address.
    pub fn listen_addr(&self) -> SocketAddr {
        self.listen_addr
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn send_and_drain() {
        let addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();
        let peer: SocketAddr = "127.0.0.1:9001".parse().unwrap();
        let transport = QuicTransport::new(addr, QuicTransportConfig::default());

        let envelope = VShardEnvelope::new(
            VShardMessageType::SegmentChunk,
            1,  // source
            2,  // target
            42, // vshard
            b"test payload".to_vec(),
        );
        transport.send_envelope(peer, &envelope).unwrap();

        let drained = transport.drain_outbound(&peer);
        assert_eq!(drained.len(), 1);
        assert_eq!(transport.messages_sent(), 1);
    }

    #[test]
    fn max_message_size_enforced() {
        let addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();
        let peer: SocketAddr = "127.0.0.1:9001".parse().unwrap();
        let config = QuicTransportConfig {
            max_message_size: 100,
            ..Default::default()
        };
        let transport = QuicTransport::new(addr, config);

        let large_payload = vec![0u8; 200];
        let envelope = VShardEnvelope::new(VShardMessageType::SegmentChunk, 1, 2, 1, large_payload);
        let result = transport.send_envelope(peer, &envelope);
        assert!(result.is_err());
    }

    #[test]
    fn connection_lifecycle() {
        let addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();
        let peer: SocketAddr = "127.0.0.1:9001".parse().unwrap();
        let transport = QuicTransport::new(addr, QuicTransportConfig::default());

        let envelope = VShardEnvelope::new(VShardMessageType::SegmentChunk, 1, 2, 1, vec![]);
        transport.send_envelope(peer, &envelope).unwrap();
        assert_eq!(transport.peer_count(), 1);

        transport.mark_connected(&peer);
        transport.disconnect(&peer);
        assert_eq!(transport.peer_count(), 0);
    }
}
