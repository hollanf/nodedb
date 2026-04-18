//! Joiner-side fetch: connect to a seed's bootstrap listener, present
//! a token, receive the creds bundle.

use std::net::SocketAddr;
use std::time::Duration;

use quinn::Endpoint;

use super::protocol::{
    BootstrapCredsRequest, BootstrapCredsResponse, MAX_FRAME_BYTES, decode_response, encode_request,
};

/// Errors from [`fetch_creds`].
#[derive(Debug, thiserror::Error)]
pub enum FetchError {
    #[error("bootstrap client config: {0}")]
    ClientConfig(String),
    #[error("bootstrap client bind: {0}")]
    Bind(String),
    #[error("bootstrap connect {addr}: {detail}")]
    Connect { addr: SocketAddr, detail: String },
    #[error("bootstrap stream open: {0}")]
    Stream(String),
    #[error("bootstrap io: {0}")]
    Io(String),
    #[error("bootstrap encode/decode: {0}")]
    Codec(String),
    #[error("bootstrap rejected: {0}")]
    Rejected(String),
    #[error("bootstrap frame too large: {0}")]
    FrameTooLarge(usize),
}

/// Connect to the bootstrap listener at `seed` and fetch a cred
/// bundle for `node_id` using `token_hex`. Returns the decoded
/// response; callers still inspect `ok` and extract the DER fields.
pub async fn fetch_creds(
    seed: SocketAddr,
    token_hex: &str,
    node_id: u64,
    timeout: Duration,
) -> Result<BootstrapCredsResponse, FetchError> {
    // Mirror `spawn_listener` — make sure rustls has a default
    // CryptoProvider registered before we build the client config.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let client_config = nexar::transport::tls::make_bootstrap_client_config()
        .map_err(|e| FetchError::ClientConfig(e.to_string()))?;

    let bind_addr: SocketAddr = if seed.is_ipv6() {
        "[::]:0".parse().expect("valid ipv6 any")
    } else {
        "0.0.0.0:0".parse().expect("valid ipv4 any")
    };
    let mut endpoint = Endpoint::client(bind_addr).map_err(|e| FetchError::Bind(e.to_string()))?;
    endpoint.set_default_client_config(client_config);

    // The self-signed server cert's SNI is "localhost" in nexar's
    // `generate_self_signed_cert`; we skip verification via the
    // bootstrap client config so the SNI value is cosmetic.
    let fut = async {
        let connecting = endpoint
            .connect(seed, "localhost")
            .map_err(|e| FetchError::Connect {
                addr: seed,
                detail: e.to_string(),
            })?;
        let conn = connecting.await.map_err(|e| FetchError::Connect {
            addr: seed,
            detail: e.to_string(),
        })?;
        let (mut send, mut recv) = conn
            .open_bi()
            .await
            .map_err(|e| FetchError::Stream(e.to_string()))?;

        let req = BootstrapCredsRequest {
            token_hex: token_hex.to_string(),
            node_id,
        };
        let body = encode_request(&req).map_err(FetchError::Codec)?;
        write_frame(&mut send, &body).await?;
        send.finish().map_err(|e| FetchError::Io(e.to_string()))?;

        let resp_bytes = read_frame(&mut recv).await?;
        let resp: BootstrapCredsResponse =
            decode_response(&resp_bytes).map_err(FetchError::Codec)?;
        Ok::<_, FetchError>(resp)
    };

    let resp = tokio::time::timeout(timeout, fut)
        .await
        .map_err(|_| FetchError::Connect {
            addr: seed,
            detail: format!("timed out after {timeout:?}"),
        })??;

    endpoint.close(0u32.into(), b"");
    endpoint.wait_idle().await;

    if !resp.ok {
        return Err(FetchError::Rejected(resp.error.clone()));
    }
    Ok(resp)
}

async fn read_frame(recv: &mut quinn::RecvStream) -> Result<Vec<u8>, FetchError> {
    let mut hdr = [0u8; 4];
    recv.read_exact(&mut hdr)
        .await
        .map_err(|e| FetchError::Io(format!("read length prefix: {e}")))?;
    let len = u32::from_be_bytes(hdr) as usize;
    if len > MAX_FRAME_BYTES {
        return Err(FetchError::FrameTooLarge(len));
    }
    let mut buf = vec![0u8; len];
    recv.read_exact(&mut buf)
        .await
        .map_err(|e| FetchError::Io(format!("read frame body: {e}")))?;
    Ok(buf)
}

async fn write_frame(send: &mut quinn::SendStream, bytes: &[u8]) -> Result<(), FetchError> {
    let len = u32::try_from(bytes.len()).map_err(|_| FetchError::FrameTooLarge(bytes.len()))?;
    send.write_all(&len.to_be_bytes())
        .await
        .map_err(|e| FetchError::Io(format!("write length prefix: {e}")))?;
    send.write_all(bytes)
        .await
        .map_err(|e| FetchError::Io(format!("write frame body: {e}")))?;
    Ok(())
}
