use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::TcpListener;
use tracing::{info, warn};

use pgwire::tokio::process_socket;

use crate::config::auth::AuthMode;
use crate::control::state::SharedState;

use super::handler::NodeDbPgHandlerFactory;

/// PostgreSQL wire protocol listener.
///
/// Accepts TCP connections and handles them using the pgwire crate.
/// Runs on the Control Plane (Tokio). Shares the same `SharedState`
/// as the native wire protocol listener.
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
    pub async fn run(
        self,
        state: Arc<SharedState>,
        auth_mode: AuthMode,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> crate::Result<()> {
        let factory = Arc::new(NodeDbPgHandlerFactory::new(state, auth_mode));

        info!(addr = %self.addr, "accepting pgwire connections");

        loop {
            tokio::select! {
                result = self.tcp.accept() => {
                    match result {
                        Ok((stream, peer_addr)) => {
                            info!(%peer_addr, "new pgwire connection");
                            let factory = Arc::clone(&factory);
                            tokio::spawn(async move {
                                if let Err(e) = process_socket(stream, None, factory).await {
                                    warn!(%peer_addr, error = %e, "pgwire session error");
                                }
                            });
                        }
                        Err(e) => {
                            warn!(error = %e, "pgwire accept failed, retrying");
                        }
                    }
                }
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        info!(addr = %self.addr, "shutdown signal, stopping pgwire listener");
                        break;
                    }
                }
            }
        }

        Ok(())
    }
}
