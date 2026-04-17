//! Credential selection for the Raft QUIC transport.
//!
//! [`TransportCredentials`] is the mandatory argument threaded into every
//! [`NexarTransport`] constructor. There is no credential-less path — the
//! caller must explicitly choose between mutual TLS (production) and the
//! unauthenticated escape hatch (`Insecure`). The escape hatch exists for
//! isolated-network dev clusters and legacy bootstrap paths, and is loudly
//! announced via [`tracing::warn!`] plus the [`insecure_transport_count`]
//! counter every time a transport is constructed with it.
//!
//! [`NexarTransport`]: super::client::NexarTransport
//! [`insecure_transport_count`]: self::insecure_transport_count

use std::sync::atomic::{AtomicU64, Ordering};

use tracing::warn;

use super::config::TlsCredentials;

/// Caller's choice of channel-level authentication for the Raft QUIC
/// transport.
///
/// There is no default. Every production code path resolves this before
/// constructing a transport, making the insecure path impossible to reach
/// by omission.
pub enum TransportCredentials {
    /// Mutual TLS — both sides present certificates signed by the cluster
    /// CA. Handshake rejects peers whose cert chains to any other trust
    /// anchor, whose cert is on the provided CRL, or who present no cert
    /// at all.
    Mtls(TlsCredentials),

    /// Explicit opt-out. Server accepts any client cert (including none);
    /// client skips server verification. Any network peer reaching the
    /// QUIC port can complete the handshake and send Raft RPCs.
    ///
    /// Selecting this variant triggers a one-line startup WARN and
    /// increments [`insecure_transport_count`]. Use only on isolated
    /// networks where every listening address is inside a controlled
    /// trust boundary.
    Insecure,
}

impl TransportCredentials {
    /// Whether this variant leaves the channel unauthenticated.
    pub fn is_insecure(&self) -> bool {
        matches!(self, TransportCredentials::Insecure)
    }
}

/// Manual `Debug` — redacts key material. `{:?}` on an `Mtls` value prints
/// only that credentials are present, never the cert or key bytes.
impl std::fmt::Debug for TransportCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransportCredentials::Mtls(_) => write!(f, "Mtls(<redacted>)"),
            TransportCredentials::Insecure => write!(f, "Insecure"),
        }
    }
}

static INSECURE_TRANSPORT_COUNT: AtomicU64 = AtomicU64::new(0);

/// Number of [`NexarTransport`] instances constructed in `Insecure` mode
/// since process start. Exposed for operational dashboards — any non-zero
/// value in a production deployment is a misconfiguration.
///
/// [`NexarTransport`]: super::client::NexarTransport
pub fn insecure_transport_count() -> u64 {
    INSECURE_TRANSPORT_COUNT.load(Ordering::Relaxed)
}

/// Emit the startup warning and bump the counter. Called exactly once per
/// transport construction when [`TransportCredentials::Insecure`] is the
/// selected variant.
pub(crate) fn announce_insecure_transport(node_id: u64) {
    INSECURE_TRANSPORT_COUNT.fetch_add(1, Ordering::Relaxed);
    warn!(
        node_id,
        "cluster transport running WITHOUT authentication — any peer reaching the QUIC port \
         can forge Raft RPCs. Only use on isolated networks. Set cluster.insecure_transport = \
         false and provide TLS credentials for production."
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insecure_flag() {
        assert!(TransportCredentials::Insecure.is_insecure());
    }

    #[test]
    fn announce_bumps_counter() {
        let before = insecure_transport_count();
        announce_insecure_transport(42);
        assert_eq!(insecure_transport_count(), before + 1);
    }
}
