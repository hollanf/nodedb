//! Single TCP connection to a NodeDB server over the native binary protocol.
//!
//! Handles MessagePack framing, request/response correlation via sequence
//! numbers, authentication, and optional TLS encryption.

use std::sync::atomic::{AtomicU64, Ordering};

use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::protocol::{
    AuthMethod, CAP_COLUMNAR, CAP_CRDT, CAP_FTS, CAP_GRAPHRAG, CAP_SPATIAL, CAP_STREAMING,
    CAP_TIMESERIES, FRAME_HEADER_LEN, HelloAckFrame, HelloFrame, Limits, MAX_FRAME_SIZE,
    NativeRequest, NativeResponse, OpCode, PROTO_VERSION, RequestFields, ResponseStatus,
    TextFields,
};
use nodedb_types::result::QueryResult;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;

/// Connection stream — either plain TCP or TLS-wrapped.
enum ConnStream {
    Plain(TcpStream),
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
}

impl AsyncRead for ConnStream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            ConnStream::Plain(s) => std::pin::Pin::new(s).poll_read(cx, buf),
            ConnStream::Tls(s) => std::pin::Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for ConnStream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match self.get_mut() {
            ConnStream::Plain(s) => std::pin::Pin::new(s).poll_write(cx, buf),
            ConnStream::Tls(s) => std::pin::Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            ConnStream::Plain(s) => std::pin::Pin::new(s).poll_flush(cx),
            ConnStream::Tls(s) => std::pin::Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            ConnStream::Plain(s) => std::pin::Pin::new(s).poll_shutdown(cx),
            ConnStream::Tls(s) => std::pin::Pin::new(s).poll_shutdown(cx),
        }
    }
}

/// TLS configuration for client connections.
#[derive(Debug, Clone, Default)]
pub struct TlsConfig {
    /// Enable TLS.
    pub enabled: bool,
    /// Path to CA certificate file (PEM). If None, uses system roots.
    pub ca_cert_path: Option<std::path::PathBuf>,
    /// Server name for SNI. If None, derived from connect address.
    pub server_name: Option<String>,
    /// Accept invalid certificates (DANGEROUS — for testing only).
    pub danger_accept_invalid_certs: bool,
}

/// A single connection to a NodeDB server using the native binary protocol.
pub struct NativeConnection {
    stream: ConnStream,
    seq: AtomicU64,
    authenticated: bool,
    /// Protocol version negotiated during the handshake (0 = handshake not performed).
    pub proto_version: u16,
    /// Capability bits advertised by the server in `HelloAckFrame`.
    pub capabilities: u64,
    /// Human-readable server version string from `HelloAckFrame`.
    pub server_version: String,
    /// Per-operation limits from `HelloAckFrame`.
    pub limits: Limits,
}

impl NativeConnection {
    /// Connect to a NodeDB server at the given address (plain TCP).
    ///
    /// Performs the native protocol handshake immediately after connecting.
    pub async fn connect(addr: &str) -> NodeDbResult<Self> {
        let stream = TcpStream::connect(addr)
            .await
            .map_err(|e| NodeDbError::sync_connection_failed(format!("connect to {addr}: {e}")))?;
        let mut conn = Self {
            stream: ConnStream::Plain(stream),
            seq: AtomicU64::new(1),
            authenticated: false,
            proto_version: 0,
            capabilities: 0,
            server_version: String::new(),
            limits: Limits::default(),
        };
        conn.perform_client_handshake().await?;
        Ok(conn)
    }

    /// Connect to a NodeDB server with TLS.
    pub async fn connect_tls(addr: &str, tls: &TlsConfig) -> NodeDbResult<Self> {
        let tcp = TcpStream::connect(addr)
            .await
            .map_err(|e| NodeDbError::sync_connection_failed(format!("connect to {addr}: {e}")))?;

        let config = build_tls_client_config(tls)?;
        let connector = tokio_rustls::TlsConnector::from(std::sync::Arc::new(config));

        // Derive server name from address (host part before ':').
        let server_name = tls
            .server_name
            .as_deref()
            .unwrap_or_else(|| addr.split(':').next().unwrap_or("localhost"));

        let sni = tokio_rustls::rustls::pki_types::ServerName::try_from(server_name.to_string())
            .map_err(|e| {
                NodeDbError::sync_connection_failed(format!(
                    "invalid server name '{server_name}': {e}"
                ))
            })?;

        let tls_stream = connector.connect(sni, tcp).await.map_err(|e| {
            NodeDbError::sync_connection_failed(format!("TLS handshake failed: {e}"))
        })?;

        let mut conn = Self {
            stream: ConnStream::Tls(Box::new(tls_stream)),
            seq: AtomicU64::new(1),
            authenticated: false,
            proto_version: 0,
            capabilities: 0,
            server_version: String::new(),
            limits: Limits::default(),
        };
        conn.perform_client_handshake().await?;
        Ok(conn)
    }

    /// Perform the native protocol handshake.
    ///
    /// Sends a `HelloFrame` and reads the server's `HelloAckFrame`. Stores the
    /// negotiated `proto_version`, `capabilities`, `server_version`, and `limits`
    /// on the connection so callers can inspect them via the `NodeDb` trait.
    ///
    /// Returns a protocol error if the server does not respond with a valid
    /// `HelloAckFrame` (wrong magic or a frame too short to contain the magic).
    pub async fn perform_client_handshake(&mut self) -> NodeDbResult<()> {
        // Client capability mask — advertise everything this SDK understands.
        let client_caps = CAP_STREAMING
            | CAP_GRAPHRAG
            | CAP_FTS
            | CAP_CRDT
            | CAP_SPATIAL
            | CAP_TIMESERIES
            | CAP_COLUMNAR;

        let hello = HelloFrame {
            proto_min: 1,
            proto_max: PROTO_VERSION,
            capabilities: client_caps,
        };

        // HelloFrame is sent raw (no length prefix) — exactly 16 bytes.
        // The server reads `HelloFrame::WIRE_SIZE` bytes directly.
        let payload = hello.encode();
        self.stream.write_all(&payload).await.map_err(io_err)?;
        self.stream.flush().await.map_err(io_err)?;

        // The server replies with either a HelloAckFrame or a HelloErrorFrame,
        // both of which start with a 4-byte magic. Peek those 4 bytes then read
        // the rest dynamically.
        //
        // HelloAckFrame: variable-length; read until end-of-relevant-data by
        // buffering a generous upper bound (1 KiB covers all valid acks).
        let mut magic_buf = [0u8; 4];
        self.stream
            .read_exact(&mut magic_buf)
            .await
            .map_err(io_err)?;

        let magic = u32::from_be_bytes(magic_buf);

        if magic == nodedb_types::protocol::HELLO_ERROR_MAGIC_U32 {
            // Server rejected the handshake — read the rest of the error frame.
            let mut header = [0u8; 2]; // code (1) + msg_len (1)
            self.stream.read_exact(&mut header).await.map_err(io_err)?;
            let msg_len = header[1] as usize;
            let mut msg_bytes = vec![0u8; msg_len];
            self.stream
                .read_exact(&mut msg_bytes)
                .await
                .map_err(io_err)?;

            let code = match header[0] {
                0 => nodedb_types::protocol::HelloErrorCode::BadMagic,
                1 => nodedb_types::protocol::HelloErrorCode::VersionMismatch,
                _ => nodedb_types::protocol::HelloErrorCode::Malformed,
            };
            let message = String::from_utf8_lossy(&msg_bytes).into_owned();
            return Err(NodeDbError::handshake_failed(code, message));
        }

        if magic != nodedb_types::protocol::HELLO_ACK_MAGIC {
            return Err(NodeDbError::internal(format!(
                "HelloAck magic mismatch: expected {:#010x}, got {:#010x}",
                nodedb_types::protocol::HELLO_ACK_MAGIC,
                magic,
            )));
        }

        // Read the remainder of the HelloAckFrame.
        // Fixed header after magic: proto_version(2) + capabilities(8) + sv_len(1) = 11 bytes.
        let mut fixed_rest = [0u8; 11];
        self.stream
            .read_exact(&mut fixed_rest)
            .await
            .map_err(io_err)?;
        let sv_len = fixed_rest[10] as usize;
        // Variable: server_version bytes + optional limits block (1 flag + 7*5 = 36 bytes max).
        let var_len = sv_len + 1 + 7 * 5;
        let mut var_buf = vec![0u8; var_len];
        self.stream.read_exact(&mut var_buf).await.map_err(io_err)?;

        let mut ack_buf = Vec::with_capacity(4 + 11 + var_len);
        ack_buf.extend_from_slice(&magic_buf);
        ack_buf.extend_from_slice(&fixed_rest);
        ack_buf.extend_from_slice(&var_buf);

        let ack = HelloAckFrame::decode(&ack_buf)
            .ok_or_else(|| NodeDbError::internal("failed to decode HelloAckFrame from server"))?;

        self.proto_version = ack.proto_version;
        self.capabilities = ack.capabilities;
        self.server_version = ack.server_version;
        self.limits = ack.limits;

        Ok(())
    }

    /// Authenticate with the server.
    pub async fn authenticate(&mut self, method: AuthMethod) -> NodeDbResult<()> {
        let resp = self
            .send(
                OpCode::Auth,
                TextFields {
                    auth: Some(method),
                    ..Default::default()
                },
            )
            .await?;

        if resp.status == ResponseStatus::Error {
            let msg = resp
                .error
                .map(|e| e.message)
                .unwrap_or_else(|| "auth failed".into());
            return Err(NodeDbError::authorization_denied(msg));
        }

        self.authenticated = true;
        Ok(())
    }

    /// Send a ping and await the pong.
    pub async fn ping(&mut self) -> NodeDbResult<()> {
        let resp = self.send(OpCode::Ping, TextFields::default()).await?;
        if resp.status == ResponseStatus::Error {
            return Err(NodeDbError::internal("ping failed"));
        }
        Ok(())
    }

    /// Whether this connection has been authenticated.
    pub fn is_authenticated(&self) -> bool {
        self.authenticated
    }

    /// Execute a SQL query and return the result.
    pub async fn execute_sql(&mut self, sql: &str) -> NodeDbResult<QueryResult> {
        let resp = self
            .send(
                OpCode::Sql,
                TextFields {
                    sql: Some(sql.to_string()),
                    ..Default::default()
                },
            )
            .await?;
        response_to_query_result(resp)
    }

    /// Execute a DDL command.
    pub async fn execute_ddl(&mut self, sql: &str) -> NodeDbResult<QueryResult> {
        let resp = self
            .send(
                OpCode::Ddl,
                TextFields {
                    sql: Some(sql.to_string()),
                    ..Default::default()
                },
            )
            .await?;
        response_to_query_result(resp)
    }

    /// Begin a transaction.
    pub async fn begin(&mut self) -> NodeDbResult<()> {
        let resp = self.send(OpCode::Begin, TextFields::default()).await?;
        check_error(resp)
    }

    /// Commit the current transaction.
    pub async fn commit(&mut self) -> NodeDbResult<()> {
        let resp = self.send(OpCode::Commit, TextFields::default()).await?;
        check_error(resp)
    }

    /// Rollback the current transaction.
    pub async fn rollback(&mut self) -> NodeDbResult<()> {
        let resp = self.send(OpCode::Rollback, TextFields::default()).await?;
        check_error(resp)
    }

    /// Set a session parameter.
    pub async fn set_parameter(&mut self, key: &str, value: &str) -> NodeDbResult<()> {
        let resp = self
            .send(
                OpCode::Set,
                TextFields {
                    key: Some(key.to_string()),
                    value: Some(value.to_string()),
                    ..Default::default()
                },
            )
            .await?;
        check_error(resp)
    }

    /// Show a session parameter.
    pub async fn show_parameter(&mut self, key: &str) -> NodeDbResult<String> {
        let resp = self
            .send(
                OpCode::Show,
                TextFields {
                    key: Some(key.to_string()),
                    ..Default::default()
                },
            )
            .await?;
        if resp.status == ResponseStatus::Error {
            let msg = resp
                .error
                .map(|e| e.message)
                .unwrap_or_else(|| "show failed".into());
            return Err(NodeDbError::internal(msg));
        }
        let value = resp
            .rows
            .and_then(|rows| rows.into_iter().next())
            .and_then(|row| row.into_iter().next())
            .and_then(|v| v.as_str().map(String::from))
            .unwrap_or_default();
        Ok(value)
    }

    // ─── Low-level transport ────────────────────────────────────

    fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::Relaxed)
    }

    /// Send a request and read the response.
    pub(crate) async fn send(
        &mut self,
        op: OpCode,
        fields: TextFields,
    ) -> NodeDbResult<NativeResponse> {
        let req = NativeRequest {
            op,
            seq: self.next_seq(),
            fields: RequestFields::Text(fields),
        };

        // Encode request as MessagePack.
        let payload = zerompk::to_msgpack_vec(&req)
            .map_err(|e| NodeDbError::serialization("msgpack", format!("request encode: {e}")))?;

        // Write length-prefixed frame.
        let len = payload.len() as u32;
        self.stream
            .write_all(&len.to_be_bytes())
            .await
            .map_err(io_err)?;
        self.stream.write_all(&payload).await.map_err(io_err)?;
        self.stream.flush().await.map_err(io_err)?;

        // Read response frame(s) — handle chunked partial responses.
        let mut combined_rows: Vec<Vec<nodedb_types::Value>> = Vec::new();
        let mut final_resp: Option<NativeResponse> = None;

        loop {
            let mut len_buf = [0u8; FRAME_HEADER_LEN];
            self.stream.read_exact(&mut len_buf).await.map_err(io_err)?;
            let resp_len = u32::from_be_bytes(len_buf);
            if resp_len > MAX_FRAME_SIZE {
                return Err(NodeDbError::internal(format!(
                    "response frame too large: {resp_len}"
                )));
            }

            let mut resp_buf = vec![0u8; resp_len as usize];
            self.stream
                .read_exact(&mut resp_buf)
                .await
                .map_err(io_err)?;

            let resp: NativeResponse = zerompk::from_msgpack(&resp_buf).map_err(|e| {
                NodeDbError::serialization("msgpack", format!("response decode: {e}"))
            })?;

            if resp.status == ResponseStatus::Partial {
                // Partial chunk — accumulate rows and continue.
                if let Some(rows) = resp.rows {
                    combined_rows.extend(rows);
                }
                // Capture columns from first chunk.
                if final_resp.is_none() {
                    final_resp = Some(NativeResponse { rows: None, ..resp });
                }
            } else {
                // Final frame.
                if combined_rows.is_empty() {
                    // Non-chunked: single response.
                    final_resp = Some(resp);
                } else {
                    // Chunked: merge accumulated rows with final chunk.
                    if let Some(ref rows) = resp.rows {
                        combined_rows.extend(rows.iter().cloned());
                    }
                    let mut merged = final_resp.unwrap_or(resp);
                    merged.rows = Some(combined_rows);
                    merged.status = ResponseStatus::Ok;
                    final_resp = Some(merged);
                }
                break;
            }
        }

        final_resp.ok_or_else(|| NodeDbError::internal("no final response received"))
    }
}

/// Build a rustls ClientConfig for TLS connections.
fn build_tls_client_config(tls: &TlsConfig) -> NodeDbResult<tokio_rustls::rustls::ClientConfig> {
    use tokio_rustls::rustls;

    let builder = rustls::ClientConfig::builder();

    if tls.danger_accept_invalid_certs {
        // DANGEROUS: accept any certificate. For testing/dev only.
        let config = builder
            .dangerous()
            .with_custom_certificate_verifier(std::sync::Arc::new(NoCertVerifier))
            .with_no_client_auth();
        return Ok(config);
    }

    if let Some(ref ca_path) = tls.ca_cert_path {
        // Custom CA certificate.
        let mut root_store = rustls::RootCertStore::empty();
        let cert_file = std::fs::File::open(ca_path).map_err(|e| {
            NodeDbError::sync_connection_failed(format!("open CA cert {}: {e}", ca_path.display()))
        })?;
        let mut reader = std::io::BufReader::new(cert_file);
        for cert in rustls_pemfile::certs(&mut reader) {
            match cert {
                Ok(c) => {
                    root_store.add(c).map_err(|e| {
                        NodeDbError::sync_connection_failed(format!("add CA cert: {e}"))
                    })?;
                }
                Err(e) => {
                    return Err(NodeDbError::sync_connection_failed(format!(
                        "parse CA cert: {e}"
                    )));
                }
            }
        }
        let config = builder
            .with_root_certificates(root_store)
            .with_no_client_auth();
        Ok(config)
    } else {
        // Use platform/webpki root certificates.
        let root_store = rustls::RootCertStore::empty();
        let config = builder
            .with_root_certificates(root_store)
            .with_no_client_auth();
        Ok(config)
    }
}

/// Certificate verifier that accepts everything (DANGEROUS).
#[derive(Debug)]
struct NoCertVerifier;

impl tokio_rustls::rustls::client::danger::ServerCertVerifier for NoCertVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &tokio_rustls::rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[tokio_rustls::rustls::pki_types::CertificateDer<'_>],
        _server_name: &tokio_rustls::rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: tokio_rustls::rustls::pki_types::UnixTime,
    ) -> Result<tokio_rustls::rustls::client::danger::ServerCertVerified, tokio_rustls::rustls::Error>
    {
        Ok(tokio_rustls::rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &tokio_rustls::rustls::pki_types::CertificateDer<'_>,
        _dss: &tokio_rustls::rustls::DigitallySignedStruct,
    ) -> Result<
        tokio_rustls::rustls::client::danger::HandshakeSignatureValid,
        tokio_rustls::rustls::Error,
    > {
        Ok(tokio_rustls::rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &tokio_rustls::rustls::pki_types::CertificateDer<'_>,
        _dss: &tokio_rustls::rustls::DigitallySignedStruct,
    ) -> Result<
        tokio_rustls::rustls::client::danger::HandshakeSignatureValid,
        tokio_rustls::rustls::Error,
    > {
        Ok(tokio_rustls::rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<tokio_rustls::rustls::SignatureScheme> {
        tokio_rustls::rustls::crypto::aws_lc_rs::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

fn io_err(e: std::io::Error) -> NodeDbError {
    NodeDbError::sync_connection_failed(format!("I/O: {e}"))
}

fn check_error(resp: NativeResponse) -> NodeDbResult<()> {
    if resp.status == ResponseStatus::Error {
        let msg = resp
            .error
            .map(|e| e.message)
            .unwrap_or_else(|| "unknown error".into());
        return Err(NodeDbError::internal(msg));
    }
    Ok(())
}

fn response_to_query_result(resp: NativeResponse) -> NodeDbResult<QueryResult> {
    if resp.status == ResponseStatus::Error {
        let msg = resp
            .error
            .map(|e| e.message)
            .unwrap_or_else(|| "query failed".into());
        return Err(NodeDbError::internal(msg));
    }
    Ok(QueryResult {
        columns: resp.columns.unwrap_or_default(),
        rows: resp.rows.unwrap_or_default(),
        rows_affected: resp.rows_affected.unwrap_or(0),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::protocol::{
        CAP_MSGPACK, CAP_STREAMING, HELLO_ACK_MAGIC, HELLO_MAGIC, HelloAckFrame,
    };

    #[test]
    fn response_to_query_result_ok() {
        let resp = NativeResponse::from_query_result(
            1,
            QueryResult {
                columns: vec!["x".into()],
                rows: vec![vec![nodedb_types::Value::Integer(42)]],
                rows_affected: 0,
            },
            0,
        );
        let qr = response_to_query_result(resp).unwrap();
        assert_eq!(qr.columns, vec!["x"]);
        assert_eq!(qr.rows[0][0].as_i64(), Some(42));
    }

    #[test]
    fn response_to_query_result_error() {
        let resp = NativeResponse::error(1, "42P01", "not found");
        let err = response_to_query_result(resp).unwrap_err();
        assert!(format!("{err}").contains("not found"));
    }

    #[test]
    fn check_error_ok() {
        let resp = NativeResponse::ok(1);
        assert!(check_error(resp).is_ok());
    }

    #[test]
    fn check_error_fail() {
        let resp = NativeResponse::error(1, "XX000", "boom");
        assert!(check_error(resp).is_err());
    }

    // ── Handshake unit tests ─────────────────────────────────────────────────
    //
    // These tests drive the client-side handshake logic by simulating a server
    // on a `tokio::io::duplex` stream pair.

    #[tokio::test]
    async fn client_handshake_succeeds_when_versions_match() {
        use nodedb_types::protocol::HelloAckFrame;
        use tokio::io::{AsyncWriteExt, duplex};

        let (mut server_half, mut client_half) = duplex(4096);

        let server_task = tokio::spawn(async move {
            // Drain the 16-byte HelloFrame the client sends.
            let mut hello_buf = [0u8; HelloFrame::WIRE_SIZE];
            tokio::io::AsyncReadExt::read_exact(&mut server_half, &mut hello_buf)
                .await
                .unwrap();
            // Verify magic.
            let magic =
                u32::from_be_bytes([hello_buf[0], hello_buf[1], hello_buf[2], hello_buf[3]]);
            assert_eq!(magic, HELLO_MAGIC, "client sent correct HelloFrame magic");

            // Send back a valid HelloAckFrame.
            let ack = HelloAckFrame {
                proto_version: 1,
                capabilities: CAP_STREAMING | CAP_MSGPACK,
                server_version: "NodeDB/test".into(),
                limits: Limits::default(),
            };
            server_half.write_all(&ack.encode()).await.unwrap();
            server_half.flush().await.unwrap();
        });

        // Run the client-side handshake inline on the duplex stream.
        let result = handshake_on_duplex(&mut client_half).await;
        server_task.await.unwrap();

        assert!(result.is_ok(), "expected Ok, got {result:?}");
        let (proto_version, server_version) = result.unwrap();
        assert_eq!(proto_version, 1);
        assert!(server_version.contains("NodeDB"));
    }

    #[tokio::test]
    async fn client_handshake_returns_typed_error_on_version_mismatch() {
        use nodedb_types::protocol::{HelloErrorCode, HelloErrorFrame};
        use tokio::io::{AsyncWriteExt, duplex};

        let (mut server_half, mut client_half) = duplex(4096);

        let server_task = tokio::spawn(async move {
            let mut hello_buf = [0u8; HelloFrame::WIRE_SIZE];
            tokio::io::AsyncReadExt::read_exact(&mut server_half, &mut hello_buf)
                .await
                .unwrap();
            let err_frame = HelloErrorFrame {
                code: HelloErrorCode::VersionMismatch,
                message: "client range 99..100 does not overlap server range 1..1".into(),
            };
            server_half.write_all(&err_frame.encode()).await.unwrap();
            server_half.flush().await.unwrap();
        });

        let result = handshake_on_duplex(&mut client_half).await;
        server_task.await.unwrap();

        assert!(result.is_err(), "expected Err");
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("handshake"),
            "error should mention handshake: {err_msg}"
        );
    }

    /// Drive the client hello/ack exchange on a `DuplexStream` and return
    /// `(proto_version, server_version)` on success.
    async fn handshake_on_duplex(
        stream: &mut tokio::io::DuplexStream,
    ) -> NodeDbResult<(u16, String)> {
        use tokio::io::AsyncWriteExt;

        // Send HelloFrame.
        let hello = HelloFrame {
            proto_min: 1,
            proto_max: PROTO_VERSION,
            capabilities: CAP_STREAMING | CAP_MSGPACK,
        };
        stream.write_all(&hello.encode()).await.map_err(io_err)?;
        stream.flush().await.map_err(io_err)?;

        // Read 4-byte magic prefix.
        let mut magic_buf = [0u8; 4];
        tokio::io::AsyncReadExt::read_exact(stream, &mut magic_buf)
            .await
            .map_err(io_err)?;
        let magic = u32::from_be_bytes(magic_buf);

        if magic == nodedb_types::protocol::HELLO_ERROR_MAGIC_U32 {
            let mut header = [0u8; 2];
            tokio::io::AsyncReadExt::read_exact(stream, &mut header)
                .await
                .map_err(io_err)?;
            let msg_len = header[1] as usize;
            let mut msg_bytes = vec![0u8; msg_len];
            tokio::io::AsyncReadExt::read_exact(stream, &mut msg_bytes)
                .await
                .map_err(io_err)?;
            let code = match header[0] {
                0 => nodedb_types::protocol::HelloErrorCode::BadMagic,
                1 => nodedb_types::protocol::HelloErrorCode::VersionMismatch,
                _ => nodedb_types::protocol::HelloErrorCode::Malformed,
            };
            return Err(NodeDbError::handshake_failed(
                code,
                String::from_utf8_lossy(&msg_bytes),
            ));
        }

        if magic != HELLO_ACK_MAGIC {
            return Err(NodeDbError::internal(format!(
                "unexpected HelloAck magic: {magic:#010x}"
            )));
        }

        // Read fixed remainder: proto_version(2) + capabilities(8) + sv_len(1).
        let mut fixed_rest = [0u8; 11];
        tokio::io::AsyncReadExt::read_exact(stream, &mut fixed_rest)
            .await
            .map_err(io_err)?;
        let sv_len = fixed_rest[10] as usize;
        let var_len = sv_len + 1 + 7 * 5;
        let mut var_buf = vec![0u8; var_len];
        tokio::io::AsyncReadExt::read_exact(stream, &mut var_buf)
            .await
            .map_err(io_err)?;

        let mut ack_buf = Vec::with_capacity(4 + 11 + var_len);
        ack_buf.extend_from_slice(&magic_buf);
        ack_buf.extend_from_slice(&fixed_rest);
        ack_buf.extend_from_slice(&var_buf);

        let ack = HelloAckFrame::decode(&ack_buf)
            .ok_or_else(|| NodeDbError::internal("failed to decode HelloAckFrame"))?;

        Ok((ack.proto_version, ack.server_version))
    }
}
