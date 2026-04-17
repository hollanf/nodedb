//! QUIC and TLS configuration for Raft RPCs.

use std::sync::Arc;
use std::time::Duration;

use nodedb_types::config::tuning::ClusterTransportTuning;

use crate::error::{ClusterError, Result};

/// ALPN protocol identifier for NodeDB Raft RPCs.
pub const ALPN_NODEDB_RAFT: &[u8] = b"nodedb-raft/1";

/// SNI hostname used for QUIC connections between NodeDB nodes.
pub const SNI_HOSTNAME: &str = "nodedb";

/// Default RPC timeout.
///
/// Matches `ClusterTransportTuning::rpc_timeout_secs` default. Override by
/// constructing `NexarTransport::with_tuning()` with a custom `ClusterTransportTuning`.
pub const DEFAULT_RPC_TIMEOUT: Duration = Duration::from_secs(5);

/// Transport config tuned for Raft RPCs in a datacenter.
///
/// All values are read from `tuning`. Pass `&ClusterTransportTuning::default()`
/// to get the same behaviour as the previous hardcoded defaults.
pub fn raft_transport_config(tuning: &ClusterTransportTuning) -> quinn::TransportConfig {
    let mut config = quinn::TransportConfig::default();
    // Raft RPCs use bidi streams: one per request-response pair.
    config.max_concurrent_bidi_streams(quinn::VarInt::from_u32(tuning.quic_max_bi_streams));
    // Also allow uni streams for future migration/snapshot streaming.
    config.max_concurrent_uni_streams(quinn::VarInt::from_u32(tuning.quic_max_uni_streams));
    // Datacenter tuning: generous windows, low RTT estimate.
    config.receive_window(quinn::VarInt::from_u32(tuning.quic_receive_window));
    config.send_window(u64::from(tuning.quic_send_window));
    config.stream_receive_window(quinn::VarInt::from_u32(tuning.quic_stream_receive_window));
    config.initial_rtt(Duration::from_micros(100));
    config.keep_alive_interval(Some(Duration::from_secs(tuning.quic_keep_alive_secs)));
    config.max_idle_timeout(Some(
        Duration::from_secs(tuning.quic_idle_timeout_secs)
            .try_into()
            .expect("idle timeout fits IdleTimeout"),
    ));
    config
}

/// Build a QUIC server config with self-signed TLS (unauthenticated).
///
/// **Crate-private** — reachable only via
/// [`TransportCredentials::Insecure`](super::credentials::TransportCredentials::Insecure),
/// which logs a loud startup warning and bumps
/// [`insecure_transport_count`](super::credentials::insecure_transport_count).
/// Production clusters use the mTLS variant.
pub(crate) fn make_raft_server_config(
    tuning: &ClusterTransportTuning,
) -> Result<quinn::ServerConfig> {
    let (cert, key) = nexar::transport::tls::generate_self_signed_cert().map_err(|e| {
        ClusterError::Transport {
            detail: format!("generate cert: {e}"),
        }
    })?;

    let provider = rustls::crypto::ring::default_provider();
    let mut tls_config = rustls::ServerConfig::builder_with_provider(Arc::new(provider))
        .with_safe_default_protocol_versions()
        .map_err(|e| ClusterError::Transport {
            detail: format!("server TLS protocol versions: {e}"),
        })?
        .with_no_client_auth()
        .with_single_cert(vec![cert], key)
        .map_err(|e| ClusterError::Transport {
            detail: format!("server TLS config: {e}"),
        })?;

    tls_config.alpn_protocols = vec![ALPN_NODEDB_RAFT.to_vec()];

    let quic_crypto = quinn::crypto::rustls::QuicServerConfig::try_from(Arc::new(tls_config))
        .map_err(|e| ClusterError::Transport {
            detail: format!("QUIC server config: {e}"),
        })?;

    let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(quic_crypto));
    server_config.transport_config(Arc::new(raft_transport_config(tuning)));
    Ok(server_config)
}

/// Build a QUIC client config that skips server verification (unauthenticated).
///
/// **Crate-private** — reachable only via
/// [`TransportCredentials::Insecure`](super::credentials::TransportCredentials::Insecure),
/// which logs a loud startup warning and bumps
/// [`insecure_transport_count`](super::credentials::insecure_transport_count).
/// Production clusters use the mTLS variant.
pub(crate) fn make_raft_client_config(
    tuning: &ClusterTransportTuning,
) -> Result<quinn::ClientConfig> {
    let provider = rustls::crypto::ring::default_provider();
    let mut tls_config = rustls::ClientConfig::builder_with_provider(Arc::new(provider))
        .with_safe_default_protocol_versions()
        .map_err(|e| ClusterError::Transport {
            detail: format!("client TLS protocol versions: {e}"),
        })?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(SkipServerVerification))
        .with_no_client_auth();

    tls_config.alpn_protocols = vec![ALPN_NODEDB_RAFT.to_vec()];

    let quic_crypto = quinn::crypto::rustls::QuicClientConfig::try_from(Arc::new(tls_config))
        .map_err(|e| ClusterError::Transport {
            detail: format!("QUIC client config: {e}"),
        })?;

    let mut client_config = quinn::ClientConfig::new(Arc::new(quic_crypto));
    client_config.transport_config(Arc::new(raft_transport_config(tuning)));
    Ok(client_config)
}

/// TLS credentials for a node (used for mTLS in production).
pub struct TlsCredentials {
    pub cert: rustls::pki_types::CertificateDer<'static>,
    pub key: rustls::pki_types::PrivateKeyDer<'static>,
    pub ca_cert: rustls::pki_types::CertificateDer<'static>,
    /// Optional CRL (Certificate Revocation List) in DER format.
    /// When present, revoked peer certificates are rejected during handshake.
    pub crls: Vec<rustls::pki_types::CertificateRevocationListDer<'static>>,
    /// Cluster-wide 32-byte symmetric secret used as the HMAC-SHA256 key for
    /// the authenticated frame envelope (see
    /// [`crate::rpc_codec::auth_envelope`]). Generated at bootstrap, persisted
    /// under `data_dir/tls/cluster_secret.bin`, distributed to joining nodes
    /// via the join RPC (L.4). Treat as key material — never log, always
    /// 0600 at rest.
    pub cluster_secret: [u8; 32],
}

/// Build a QUIC server config with mutual TLS (production mode).
///
/// Requires connecting clients to present a certificate signed by the cluster CA.
pub fn make_raft_server_config_mtls(
    creds: &TlsCredentials,
    tuning: &ClusterTransportTuning,
) -> Result<quinn::ServerConfig> {
    let mut root_store = rustls::RootCertStore::empty();
    root_store
        .add(creds.ca_cert.clone())
        .map_err(|e| ClusterError::Transport {
            detail: format!("add CA to root store: {e}"),
        })?;

    let mut verifier_builder = rustls::server::WebPkiClientVerifier::builder(Arc::new(root_store));

    // Add CRLs for certificate revocation checking.
    for crl in &creds.crls {
        verifier_builder = verifier_builder.with_crls(vec![crl.clone()]);
    }

    let client_verifier = verifier_builder
        .build()
        .map_err(|e| ClusterError::Transport {
            detail: format!("build client verifier: {e}"),
        })?;

    let provider = rustls::crypto::ring::default_provider();
    let mut tls_config = rustls::ServerConfig::builder_with_provider(Arc::new(provider))
        .with_safe_default_protocol_versions()
        .map_err(|e| ClusterError::Transport {
            detail: format!("server TLS protocol versions: {e}"),
        })?
        .with_client_cert_verifier(client_verifier)
        .with_single_cert(vec![creds.cert.clone()], creds.key.clone_key())
        .map_err(|e| ClusterError::Transport {
            detail: format!("mTLS server config: {e}"),
        })?;

    tls_config.alpn_protocols = vec![ALPN_NODEDB_RAFT.to_vec()];

    let quic_crypto = quinn::crypto::rustls::QuicServerConfig::try_from(Arc::new(tls_config))
        .map_err(|e| ClusterError::Transport {
            detail: format!("QUIC mTLS server config: {e}"),
        })?;

    let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(quic_crypto));
    server_config.transport_config(Arc::new(raft_transport_config(tuning)));
    Ok(server_config)
}

/// Build a QUIC client config with mutual TLS (production mode).
///
/// Verifies server cert and presents client cert, both signed by cluster CA.
pub fn make_raft_client_config_mtls(
    creds: &TlsCredentials,
    tuning: &ClusterTransportTuning,
) -> Result<quinn::ClientConfig> {
    let mut root_store = rustls::RootCertStore::empty();
    root_store
        .add(creds.ca_cert.clone())
        .map_err(|e| ClusterError::Transport {
            detail: format!("add CA to root store: {e}"),
        })?;

    let provider = rustls::crypto::ring::default_provider();
    let mut tls_config = rustls::ClientConfig::builder_with_provider(Arc::new(provider))
        .with_safe_default_protocol_versions()
        .map_err(|e| ClusterError::Transport {
            detail: format!("client TLS protocol versions: {e}"),
        })?
        .with_root_certificates(root_store)
        .with_client_auth_cert(vec![creds.cert.clone()], creds.key.clone_key())
        .map_err(|e| ClusterError::Transport {
            detail: format!("mTLS client config: {e}"),
        })?;

    tls_config.alpn_protocols = vec![ALPN_NODEDB_RAFT.to_vec()];

    let quic_crypto = quinn::crypto::rustls::QuicClientConfig::try_from(Arc::new(tls_config))
        .map_err(|e| ClusterError::Transport {
            detail: format!("QUIC mTLS client config: {e}"),
        })?;

    let mut client_config = quinn::ClientConfig::new(Arc::new(quic_crypto));
    client_config.transport_config(Arc::new(raft_transport_config(tuning)));
    Ok(client_config)
}

/// Generate a cluster CA and issue a node certificate.
///
/// Called during bootstrap. The CA cert is stored in the catalog and
/// distributed to joining nodes via the JoinResponse.
pub fn generate_node_credentials(
    node_san: &str,
) -> Result<(nexar::transport::tls::ClusterCa, TlsCredentials)> {
    let ca = nexar::transport::tls::ClusterCa::generate().map_err(|e| ClusterError::Transport {
        detail: format!("generate cluster CA: {e}"),
    })?;
    let ca_cert = ca.cert_der();
    let (cert, key) = ca
        .issue_cert(node_san)
        .map_err(|e| ClusterError::Transport {
            detail: format!("issue node cert: {e}"),
        })?;
    use rand::RngCore;
    let mut cluster_secret = [0u8; 32];
    rand::rng().fill_bytes(&mut cluster_secret);
    Ok((
        ca,
        TlsCredentials {
            cert,
            key,
            ca_cert,
            crls: Vec::new(),
            cluster_secret,
        },
    ))
}

/// Load CRLs from a PEM file.
///
/// Returns a list of CRL DER blobs parsed from the PEM-encoded file.
/// The file may contain multiple CRLs.
pub fn load_crls_from_pem(
    path: &std::path::Path,
) -> Result<Vec<rustls::pki_types::CertificateRevocationListDer<'static>>> {
    let pem_data = std::fs::read(path).map_err(|e| ClusterError::Transport {
        detail: format!("read CRL file {}: {e}", path.display()),
    })?;

    let mut reader = std::io::BufReader::new(&pem_data[..]);
    let crls: std::result::Result<Vec<_>, _> = rustls_pemfile::crls(&mut reader).collect();
    let crls = crls.map_err(|e| ClusterError::Transport {
        detail: format!("parse CRL from {}: {e}", path.display()),
    })?;

    Ok(crls)
}

/// Certificate verifier that accepts any server certificate (dev/bootstrap only).
#[derive(Debug)]
struct SkipServerVerification;

impl rustls::client::danger::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::CryptoProvider::get_default()
            .map(|p| p.signature_verification_algorithms.supported_schemes())
            .unwrap_or_else(|| {
                rustls::crypto::ring::default_provider()
                    .signature_verification_algorithms
                    .supported_schemes()
            })
    }
}
