//! Token-authenticated bootstrap RPC for cred delivery (L.4).
//!
//! Joining nodes that have **no** cluster TLS material yet connect
//! to this listener, present an HMAC-SHA256 join token (issued by
//! an already-joined node via `nodedb join-token --create`), and
//! receive `(ca_cert, node_cert, node_key, cluster_secret)` signed
//! by the cluster CA and bound to their `node_id`.
//!
//! ## Channel
//!
//! The listener uses a **self-signed QUIC + no-client-auth** config
//! via `nexar::transport::tls::{generate_self_signed_cert,
//! make_server_config}`. Joiners skip server verification (they have
//! nothing to verify against yet). Authentication is entirely on the
//! token; the channel is for confidentiality only.
//!
//! Once creds are delivered the joiner writes them to
//! `data_dir/tls/*`, reconnects via normal mTLS-authenticated Raft
//! transport, and never talks to the bootstrap listener again.
//!
//! ## Threat model (explicit)
//!
//! A network-level attacker who can MITM the self-signed channel can
//! observe the token and forward it to the legitimate server,
//! receiving valid creds. The expected deployment is a controlled
//! private network where the operator can bound who can reach the
//! listener (firewall / service-mesh policy). For stronger binding,
//! include a ca-cert-hash pre-share in the token command output so
//! operators can paste it into the joiner config as a pinned trust
//! anchor — see README.

pub mod client;
pub mod protocol;
pub mod server;

pub use client::{FetchError, fetch_creds};
pub use protocol::{BootstrapCredsRequest, BootstrapCredsResponse};
pub use server::{BootstrapHandler, spawn_listener};
