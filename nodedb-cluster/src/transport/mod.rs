pub mod auth_context;
pub mod client;
pub mod config;
pub mod credentials;
pub mod server;

pub use auth_context::AuthContext;

pub use client::NexarTransport;
pub use config::{
    TlsCredentials, generate_node_credentials, load_crls_from_pem, make_raft_client_config_mtls,
    make_raft_server_config_mtls,
};

/// Re-exported PKI types used in the public shape of [`TlsCredentials`].
///
/// Consumers constructing `TlsCredentials` from PEM files or network bytes
/// do not need their own `rustls` dependency.
pub mod pki_types {
    pub use rustls::pki_types::{
        CertificateDer, CertificateRevocationListDer, PrivateKeyDer, PrivatePkcs8KeyDer, UnixTime,
    };
}
pub use credentials::{TransportCredentials, insecure_transport_count};
pub use server::RaftRpcHandler;
