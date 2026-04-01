//! Concurrent session store — keyed by socket address.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::RwLock;

use super::state::{PgSession, TransactionState};

/// Concurrent session store — keyed by socket address.
pub struct SessionStore {
    sessions: RwLock<HashMap<SocketAddr, PgSession>>,
}

impl Default for SessionStore {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionStore {
    pub fn new() -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
        }
    }

    /// Ensure a session exists for this address.
    pub fn ensure_session(&self, addr: SocketAddr) {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        sessions.entry(addr).or_insert_with(PgSession::new);
    }

    /// Remove a session (connection closed).
    pub fn remove(&self, addr: &SocketAddr) {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        sessions.remove(addr);
    }

    /// List all active sessions as (peer_address, transaction_state) pairs.
    pub fn all_sessions(&self) -> Vec<(String, String)> {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        sessions
            .iter()
            .map(|(addr, session)| {
                let tx = match session.tx_state {
                    TransactionState::Idle => "idle",
                    TransactionState::InBlock => "in_transaction",
                    TransactionState::Failed => "failed",
                };
                (addr.to_string(), tx.to_string())
            })
            .collect()
    }

    /// Number of active sessions.
    pub fn count(&self) -> usize {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        sessions.len()
    }

    /// Look up cached physical tasks for a SQL string in the session's plan cache.
    pub fn get_cached_plan(
        &self,
        addr: &SocketAddr,
        sql: &str,
        schema_version: u64,
    ) -> Option<Vec<crate::control::planner::physical::PhysicalTask>> {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        sessions
            .get_mut(addr)
            .and_then(|s| s.plan_cache.get(sql, schema_version))
    }

    /// Store compiled physical tasks in the session's plan cache.
    pub fn put_cached_plan(
        &self,
        addr: &SocketAddr,
        sql: &str,
        tasks: Vec<crate::control::planner::physical::PhysicalTask>,
        schema_version: u64,
    ) {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        if let Some(session) = sessions.get_mut(addr) {
            session.plan_cache.put(sql, tasks, schema_version);
        }
    }

    /// Access the session map with a read lock for use by other session submodules.
    pub(super) fn read_session<R>(
        &self,
        addr: &SocketAddr,
        f: impl FnOnce(&PgSession) -> R,
    ) -> Option<R> {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        sessions.get(addr).map(f)
    }

    /// Access the session map with a write lock for use by other session submodules.
    pub(super) fn write_session<R>(
        &self,
        addr: &SocketAddr,
        f: impl FnOnce(&mut PgSession) -> R,
    ) -> Option<R> {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        sessions.get_mut(addr).map(f)
    }
}
