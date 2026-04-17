//! Inbound Raft RPC handling.
//!
//! Accepts connections from the QUIC endpoint, dispatches incoming bidi
//! streams to a [`RaftRpcHandler`], and writes back the response frame.
//!
//! # Authenticated wire envelope
//!
//! Every on-wire message is an [`auth_envelope`]-wrapped
//! [`rpc_codec`] frame. The envelope carries `from_node_id`, a per-peer
//! monotonic `seq`, and an HMAC-SHA256 MAC. [`handle_stream`]:
//!
//! 1. reads one envelope from the QUIC stream,
//! 2. verifies the MAC against the cluster MAC key held by
//!    [`AuthContext`],
//! 3. rejects replays via the per-peer sliding window,
//! 4. decodes the inner frame and dispatches to the handler,
//! 5. wraps the handler's response in its own authenticated envelope
//!    with `from_node_id = local_node_id` and a fresh outbound seq for
//!    the caller's id.
//!
//! # Cooperative shutdown
//!
//! Every long-lived `.await` is wrapped in a `tokio::select!` over a
//! `watch::Receiver<bool>` shutdown signal that is cloned into every
//! spawned child task, so graceful shutdown promptly releases handler
//! Arcs held by grandchild per-stream tasks.
//!
//! [`auth_envelope`]: crate::rpc_codec::auth_envelope
//! [`rpc_codec`]: crate::rpc_codec

use std::sync::Arc;

use tokio::sync::watch;
use tracing::debug;

use crate::error::{ClusterError, Result};
use crate::rpc_codec::{self, MAX_RPC_PAYLOAD_SIZE, RaftRpc, auth_envelope};
use crate::transport::auth_context::AuthContext;

/// Trait for handling incoming Raft RPCs.
///
/// Implementors receive a request [`RaftRpc`] and return the corresponding
/// response variant. The transport calls this for each incoming bidi stream.
pub trait RaftRpcHandler: Send + Sync + 'static {
    fn handle_rpc(&self, rpc: RaftRpc)
    -> impl std::future::Future<Output = Result<RaftRpc>> + Send;
}

/// Handle all bidi streams on a single connection.
///
/// Exits cleanly (Ok) on shutdown, on normal connection close,
/// or on unrecoverable transport error.
pub(crate) async fn handle_connection<H: RaftRpcHandler>(
    conn: quinn::Connection,
    handler: Arc<H>,
    auth: Arc<AuthContext>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    loop {
        let accepted = tokio::select! {
            biased;
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    return Ok(());
                }
                continue;
            }
            result = conn.accept_bi() => result,
        };

        let (send, recv) = match accepted {
            Ok(streams) => streams,
            Err(quinn::ConnectionError::ApplicationClosed(_)) => return Ok(()),
            Err(quinn::ConnectionError::LocallyClosed) => return Ok(()),
            Err(e) => {
                return Err(ClusterError::Transport {
                    detail: format!("accept_bi: {e}"),
                });
            }
        };

        let h = handler.clone();
        let stream_shutdown = shutdown.clone();
        let stream_auth = auth.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_stream(h, stream_auth, send, recv, stream_shutdown).await {
                debug!(error = %e, "raft RPC stream error");
            }
        });
    }
}

/// Handle a single bidi stream: read request → dispatch → write response.
///
/// Every long-lived await is racing a shutdown signal — see the
/// module docstring for the rationale.
async fn handle_stream<H: RaftRpcHandler>(
    handler: Arc<H>,
    auth: Arc<AuthContext>,
    mut send: quinn::SendStream,
    mut recv: quinn::RecvStream,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let work = async {
        // 1. Read one envelope.
        let envelope = read_envelope(&mut recv).await?;
        let (fields, inner_frame) = auth_envelope::parse_envelope(&envelope, &auth.mac_key)?;

        // 2. Replay window — under the advertised from_node_id (MAC-verified).
        auth.peer_seq_in.accept(fields.from_node_id, fields.seq)?;

        // 3. Decode inner RPC and hand to handler.
        let request = rpc_codec::decode(inner_frame)?;
        let response = handler.handle_rpc(request).await?;

        // 4. Wrap the response in its own envelope. `from = local_node_id`,
        //    `seq = next outbound seq scoped to the caller`.
        let response_inner = rpc_codec::encode(&response)?;
        let response_seq = auth.peer_seq_out.next(fields.from_node_id);
        let mut response_envelope =
            Vec::with_capacity(auth_envelope::ENVELOPE_OVERHEAD + response_inner.len());
        auth_envelope::write_envelope(
            auth.local_node_id,
            response_seq,
            &response_inner,
            &auth.mac_key,
            &mut response_envelope,
        )?;

        send.write_all(&response_envelope)
            .await
            .map_err(|e| ClusterError::Transport {
                detail: format!("write response: {e}"),
            })?;
        send.finish().map_err(|e| ClusterError::Transport {
            detail: format!("finish response: {e}"),
        })?;
        Ok::<(), ClusterError>(())
    };

    tokio::select! {
        biased;
        _ = shutdown.changed() => Ok(()),
        result = work => result,
    }
}

/// Read a complete authenticated envelope from a QUIC receive stream.
///
/// Reads the fixed envelope pre-header (version + from_node_id + seq +
/// inner_len), then the inner frame, then the MAC tag. Returns the full
/// envelope bytes for caller-side parsing.
pub(crate) async fn read_envelope(recv: &mut quinn::RecvStream) -> Result<Vec<u8>> {
    // Envelope header is version(1) + from_node_id(8) + seq(8) + inner_len(4).
    const ENV_HDR_LEN: usize = 21;

    let mut hdr = [0u8; ENV_HDR_LEN];
    recv.read_exact(&mut hdr)
        .await
        .map_err(|e| ClusterError::Transport {
            detail: format!("read envelope header: {e}"),
        })?;

    let inner_len = u32::from_le_bytes([hdr[17], hdr[18], hdr[19], hdr[20]]);
    if inner_len > MAX_RPC_PAYLOAD_SIZE {
        return Err(ClusterError::Codec {
            detail: format!(
                "envelope inner length {inner_len} exceeds maximum {MAX_RPC_PAYLOAD_SIZE}"
            ),
        });
    }

    let total = ENV_HDR_LEN + inner_len as usize + rpc_codec::MAC_LEN;
    let mut buf = vec![0u8; total];
    buf[..ENV_HDR_LEN].copy_from_slice(&hdr);
    if total > ENV_HDR_LEN {
        recv.read_exact(&mut buf[ENV_HDR_LEN..])
            .await
            .map_err(|e| ClusterError::Transport {
                detail: format!("read envelope payload+mac: {e}"),
            })?;
    }

    Ok(buf)
}
