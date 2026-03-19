use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::task::JoinSet;
use tracing::{info, warn};

use pgwire::tokio::process_socket;

use crate::config::auth::AuthMode;
use crate::control::state::SharedState;

use super::factory::NodeDbPgHandlerFactory;

/// PostgreSQL wire protocol listener.
///
/// Accepts TCP connections and handles them using the pgwire crate.
/// Optionally supports TLS (SSLRequest negotiation + upgrade).
/// Runs on the Control Plane (Tokio).
pub struct PgListener {
    tcp: TcpListener,
    addr: SocketAddr,
}

impl PgListener {
    pub async fn bind(addr: SocketAddr) -> crate::Result<Self> {
        let tcp = TcpListener::bind(addr).await?;
        let local_addr = tcp.local_addr()?;
        info!(%local_addr, "pgwire listener bound");
        Ok(Self {
            tcp,
            addr: local_addr,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// Run the accept loop for pgwire connections.
    ///
    /// `tls_acceptor`: if Some, pgwire will negotiate SSL on SSLRequest.
    /// If None, all connections are plaintext.
    ///
    /// On shutdown signal:
    /// 1. Stop accepting new connections.
    /// 2. Wait up to `drain_timeout` for in-flight connections to finish.
    /// 3. Abort remaining connections after timeout.
    pub async fn run(
        self,
        state: Arc<SharedState>,
        auth_mode: AuthMode,
        tls_acceptor: Option<pgwire::tokio::TlsAcceptor>,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> crate::Result<()> {
        let factory = Arc::new(NodeDbPgHandlerFactory::new(state, auth_mode));

        let tls_label = if tls_acceptor.is_some() {
            "tls"
        } else {
            "plain"
        };
        info!(addr = %self.addr, tls = tls_label, "accepting pgwire connections");

        let mut connections = JoinSet::new();

        loop {
            tokio::select! {
                result = self.tcp.accept() => {
                    match result {
                        Ok((stream, peer_addr)) => {
                            info!(%peer_addr, "new pgwire connection");
                            let factory = Arc::clone(&factory);
                            let tls = tls_acceptor.clone();
                            connections.spawn(async move {
                                if let Err(e) = process_socket(stream, tls, factory).await {
                                    warn!(%peer_addr, error = %e, "pgwire session error");
                                }
                                peer_addr
                            });
                        }
                        Err(e) => {
                            warn!(error = %e, "pgwire accept failed, retrying");
                        }
                    }
                }
                // Reap completed connections to avoid unbounded growth.
                Some(result) = connections.join_next(), if !connections.is_empty() => {
                    if let Ok(peer_addr) = result {
                        info!(%peer_addr, "pgwire connection closed");
                    }
                }
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        info!(
                            addr = %self.addr,
                            active = connections.len(),
                            "shutdown signal, draining pgwire connections"
                        );
                        break;
                    }
                }
            }
        }

        // Graceful drain: wait for in-flight connections with timeout.
        let drain_timeout = Duration::from_secs(30);
        if !connections.is_empty() {
            info!(
                active = connections.len(),
                timeout_secs = drain_timeout.as_secs(),
                "waiting for pgwire connections to drain"
            );

            let drain_result = tokio::time::timeout(drain_timeout, async {
                while let Some(result) = connections.join_next().await {
                    if let Ok(peer_addr) = result {
                        info!(%peer_addr, "drained pgwire connection");
                    }
                }
            })
            .await;

            if drain_result.is_err() {
                let remaining = connections.len();
                warn!(
                    remaining,
                    "drain timeout exceeded, aborting remaining pgwire connections"
                );
                connections.abort_all();
            }
        }

        info!(addr = %self.addr, "pgwire listener stopped");
        Ok(())
    }
}
