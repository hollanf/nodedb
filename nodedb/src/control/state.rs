use std::sync::{Arc, Mutex};

use tracing::warn;

use crate::bridge::dispatch::Dispatcher;
use crate::control::request_tracker::RequestTracker;
use crate::control::security::audit::AuditLog;
use crate::control::security::credential::CredentialStore;
use crate::wal::WalManager;

/// Shared state accessible by all Control Plane sessions.
///
/// This is the glue that connects TCP sessions to the Data Plane via the
/// Dispatcher/SPSC bridge, and to the WAL for the write path.
///
/// All fields are `Send + Sync` — safe for sharing across Tokio tasks.
pub struct SharedState {
    /// Routes requests to Data Plane cores via SPSC.
    pub dispatcher: Mutex<Dispatcher>,

    /// Tracks in-flight requests and routes responses back to sessions.
    pub tracker: RequestTracker,

    /// Write-ahead log for durability.
    pub wal: Arc<WalManager>,

    /// Credential store for user authentication.
    pub credentials: Arc<CredentialStore>,

    /// Audit log for security-relevant events.
    pub audit: Mutex<AuditLog>,
}

impl SharedState {
    pub fn new(dispatcher: Dispatcher, wal: Arc<WalManager>) -> Arc<Self> {
        Arc::new(Self {
            dispatcher: Mutex::new(dispatcher),
            tracker: RequestTracker::new(),
            wal,
            credentials: Arc::new(CredentialStore::new()),
            audit: Mutex::new(AuditLog::new(10_000)),
        })
    }

    /// Record an audit event.
    pub fn audit_record(
        &self,
        event: crate::control::security::audit::AuditEvent,
        tenant_id: Option<crate::types::TenantId>,
        source: &str,
        detail: &str,
    ) {
        match self.audit.lock() {
            Ok(mut log) => {
                log.record(event, tenant_id, source, detail);
            }
            Err(poisoned) => {
                warn!("audit log mutex poisoned, recovering");
                poisoned
                    .into_inner()
                    .record(event, tenant_id, source, detail);
            }
        }
    }

    /// Poll responses from all Data Plane cores and route them to waiting sessions.
    ///
    /// This should be called periodically from a background Tokio task.
    pub fn poll_and_route_responses(&self) {
        let responses = match self.dispatcher.lock() {
            Ok(mut d) => d.poll_responses(),
            Err(poisoned) => {
                warn!("dispatcher mutex poisoned, recovering");
                poisoned.into_inner().poll_responses()
            }
        };
        for resp in responses {
            if !self.tracker.complete(resp) {
                warn!("response for unknown or cancelled request");
            }
        }
    }
}
