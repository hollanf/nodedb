//! Server-side native protocol handshake.
//!
//! Called immediately after TCP (or TLS) accept, before any regular
//! request/response frames are exchanged.
//!
//! # Protocol
//!
//! 1. Server reads exactly 16 bytes (`HelloFrame`) with a 5-second timeout.
//! 2. Server validates magic, checks version overlap, picks `proto_ver`.
//! 3. On success: sends `HelloAckFrame`, returns negotiated `proto_ver`.
//!    On failure: sends `HelloErrorFrame`, closes the write side, returns `Err`.

use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tracing::debug;

use nodedb_types::protocol::handshake::{
    HelloAckFrame, HelloErrorCode, HelloErrorFrame, HelloFrame, Limits, PROTO_VERSION_MAX,
    PROTO_VERSION_MIN,
};

/// Timeout for reading the `HelloFrame` from a newly-connected client.
const HELLO_READ_TIMEOUT: Duration = Duration::from_secs(5);

/// Negotiate the native protocol version with a newly-connected client.
///
/// Reads a `HelloFrame`, validates it, picks a `proto_ver` in the overlap of
/// `[client_proto_min, client_proto_max]` and `[PROTO_VERSION_MIN, PROTO_VERSION_MAX]`,
/// then sends a `HelloAckFrame` with `limits` embedded.
///
/// On any error, a `HelloErrorFrame` is written, the write side is shut down,
/// and `Err` is returned so the caller can close the connection.
pub async fn perform_server_handshake<S>(stream: &mut S, limits: &Limits) -> crate::Result<u16>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // ── 1. Read HelloFrame with timeout ─────────────────────────────────
    let mut buf = [0u8; HelloFrame::WIRE_SIZE];
    let read_result = tokio::time::timeout(HELLO_READ_TIMEOUT, stream.read_exact(&mut buf)).await;

    let hello = match read_result {
        Err(_timeout) => {
            send_error(stream, HelloErrorCode::Malformed, "hello read timeout").await;
            return Err(crate::Error::BadRequest {
                detail: "hello frame read timed out".into(),
            });
        }
        Ok(Err(io)) => {
            // Don't bother sending an error frame on a connection-reset/EOF —
            // the peer is already gone.
            return Err(crate::Error::Io(io));
        }
        Ok(Ok(_)) => match HelloFrame::decode(&buf) {
            Ok(f) => f,
            Err(code) => {
                let msg = format!("bad hello frame: {code}");
                send_error(stream, code, &msg).await;
                return Err(crate::Error::BadRequest { detail: msg });
            }
        },
    };

    debug!(
        proto_min = hello.proto_min,
        proto_max = hello.proto_max,
        caps = hello.caps,
        "hello received"
    );

    // ── 2. Negotiate version ─────────────────────────────────────────────
    let proto_ver = match negotiate_version(hello.proto_min, hello.proto_max) {
        Some(v) => v,
        None => {
            let msg = format!(
                "client speaks [{}, {}], server speaks [{}, {}]",
                hello.proto_min, hello.proto_max, PROTO_VERSION_MIN, PROTO_VERSION_MAX
            );
            send_error(stream, HelloErrorCode::VersionMismatch, &msg).await;
            return Err(crate::Error::VersionCompat { detail: msg });
        }
    };

    // ── 3. Send HelloAck ─────────────────────────────────────────────────
    let ack = HelloAckFrame {
        proto_ver,
        caps: hello.caps, // echo back what client offered (server supports all for v1)
        server_info: format!("NodeDB/{}", env!("CARGO_PKG_VERSION")),
        limits: limits.clone(),
    };
    let ack_bytes = ack.encode();
    stream
        .write_all(&ack_bytes)
        .await
        .map_err(crate::Error::Io)?;
    stream.flush().await.map_err(crate::Error::Io)?;

    debug!(proto_ver, "handshake complete");
    Ok(proto_ver)
}

/// Pick the highest version in the overlap of client's and server's ranges.
///
/// Returns `None` if there is no overlap.
fn negotiate_version(client_min: u16, client_max: u16) -> Option<u16> {
    let lo = client_min.max(PROTO_VERSION_MIN);
    let hi = client_max.min(PROTO_VERSION_MAX);
    if lo <= hi { Some(hi) } else { None }
}

/// Write a `HelloErrorFrame` and shut down the write side of the stream.
///
/// Errors here are swallowed — we're already on the error path.
async fn send_error<S: AsyncWrite + Unpin>(stream: &mut S, code: HelloErrorCode, msg: &str) {
    let frame = HelloErrorFrame {
        code,
        message: msg.to_string(),
    }
    .encode();
    let _ = stream.write_all(&frame).await;
    let _ = stream.flush().await;
    let _ = stream.shutdown().await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::protocol::handshake::{CAP_MSGPACK, CAP_STREAMING, HELLO_MAGIC};
    use tokio::io::duplex;

    /// Run a server handshake concurrently with a simulated client that sends
    /// `client_bytes` then shuts down its write side.
    ///
    /// Returns `(server_result, bytes_sent_to_client)`.
    async fn server_shake(client_bytes: Vec<u8>) -> (crate::Result<u16>, Vec<u8>) {
        server_shake_with_limits(client_bytes, Limits::default()).await
    }

    async fn server_shake_with_limits(
        client_bytes: Vec<u8>,
        limits: Limits,
    ) -> (crate::Result<u16>, Vec<u8>) {
        // client_end ↔ server_end: writes on one side are reads on the other.
        let (mut client_end, mut server_end) = duplex(4096);

        // Simulate the client: write its bytes, then half-close.
        let client_task = tokio::spawn(async move {
            tokio::io::AsyncWriteExt::write_all(&mut client_end, &client_bytes)
                .await
                .unwrap();
            tokio::io::AsyncWriteExt::shutdown(&mut client_end)
                .await
                .unwrap();
            // Read whatever the server replied (becomes the response buffer).
            let mut response = Vec::new();
            tokio::io::AsyncReadExt::read_to_end(&mut client_end, &mut response)
                .await
                .unwrap();
            response
        });

        let result = perform_server_handshake(&mut server_end, &limits).await;
        // Close server's write side so the client task can finish read_to_end.
        drop(server_end);

        let response = client_task.await.unwrap();
        (result, response)
    }

    #[tokio::test]
    async fn happy_path() {
        let hello = HelloFrame::current().encode();
        let (result, response) = server_shake(hello.to_vec()).await;
        assert!(result.is_ok(), "expected Ok, got {result:?}");
        assert_eq!(result.unwrap(), 1);

        // Parse the ack.
        let ack = HelloAckFrame::decode(&response).expect("should be valid ack");
        assert_eq!(ack.proto_ver, 1);
        assert!(ack.server_info.contains("NodeDB"));
    }

    #[tokio::test]
    async fn bad_magic_rejected() {
        let mut buf = HelloFrame::current().encode();
        buf[0] = 0xFF; // corrupt magic
        let (result, response) = server_shake(buf.to_vec()).await;
        assert!(result.is_err());

        let err = HelloErrorFrame::decode(&response).expect("should be a HelloErrorFrame");
        assert_eq!(err.code, HelloErrorCode::BadMagic);
    }

    #[tokio::test]
    async fn version_mismatch_rejected() {
        // Build a hello where client speaks versions 99..100 (no overlap with server's 1..1).
        let frame = HelloFrame {
            proto_min: 99,
            proto_max: 100,
            caps: CAP_MSGPACK | CAP_STREAMING,
        }
        .encode();
        let (result, response) = server_shake(frame.to_vec()).await;
        assert!(result.is_err());

        let err = HelloErrorFrame::decode(&response).expect("should be a HelloErrorFrame");
        assert_eq!(err.code, HelloErrorCode::VersionMismatch);
    }

    #[tokio::test]
    async fn short_read_returns_error() {
        // Only 4 bytes — way too short for a HelloFrame.
        let (result, _) = server_shake(vec![b'N', b'D', b'B', 0x01]).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn malformed_frame_rejected() {
        // 16 bytes but wrong magic.
        let mut bad = [0u8; 16];
        bad[..4].copy_from_slice(b"XXXX");
        let (result, response) = server_shake(bad.to_vec()).await;
        assert!(result.is_err());

        let err = HelloErrorFrame::decode(&response).expect("should be a HelloErrorFrame");
        assert_eq!(err.code, HelloErrorCode::BadMagic);
    }

    #[test]
    fn negotiate_version_overlap() {
        assert_eq!(negotiate_version(1, 3), Some(1)); // only server version 1 available
        assert_eq!(negotiate_version(1, 1), Some(1));
    }

    #[test]
    fn negotiate_version_no_overlap() {
        assert_eq!(negotiate_version(2, 5), None);
        assert_eq!(negotiate_version(0, 0), None);
    }

    #[test]
    fn hello_magic_correct() {
        assert_eq!(HELLO_MAGIC, b"NDB\x01");
    }

    #[tokio::test]
    async fn limits_propagated_in_ack() {
        let limits = Limits {
            max_vector_dim: Some(768),
            max_top_k: Some(500),
            max_scan_limit: None,
            max_batch_size: Some(100),
            max_crdt_delta_bytes: None,
            max_query_text_bytes: Some(1024),
            max_graph_depth: Some(6),
        };
        let hello = HelloFrame::current().encode();
        let (result, response) = server_shake_with_limits(hello.to_vec(), limits.clone()).await;
        assert!(result.is_ok());
        let ack = HelloAckFrame::decode(&response).expect("valid ack");
        assert_eq!(ack.limits.max_vector_dim, Some(768));
        assert_eq!(ack.limits.max_top_k, Some(500));
        assert_eq!(ack.limits.max_scan_limit, None);
        assert_eq!(ack.limits.max_batch_size, Some(100));
        assert_eq!(ack.limits.max_crdt_delta_bytes, None);
        assert_eq!(ack.limits.max_query_text_bytes, Some(1024));
        assert_eq!(ack.limits.max_graph_depth, Some(6));
    }
}
