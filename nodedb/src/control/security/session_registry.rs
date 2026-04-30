//! Active session registry for BLACKLIST WITH KILL SESSIONS.
//!
//! Tracks all active connections (native, pgwire, HTTP) and provides
//! a mechanism to signal session termination by user ID.

use std::collections::HashMap;
use std::sync::RwLock;

use tokio::sync::watch;

/// Parameters for registering a new session.
pub struct SessionParams {
    pub user_id: String,
    pub db_user: String,
    pub peer_addr: String,
    pub protocol: String,
    pub auth_method: String,
    pub tenant_id: u64,
}

/// A registered session with its kill signal.
struct RegisteredSession {
    user_id: String,
    db_user: String,
    peer_addr: String,
    protocol: String,
    auth_method: String,
    tenant_id: u64,
    connected_at: u64,
    last_active: std::sync::atomic::AtomicU64,
    /// Send `true` to this channel to signal the session to terminate.
    kill_tx: watch::Sender<bool>,
}

/// Thread-safe session registry.
pub struct SessionRegistry {
    /// session_id → registered session.
    sessions: RwLock<HashMap<String, RegisteredSession>>,
}

impl SessionRegistry {
    pub fn new() -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
        }
    }

    /// Register a new session. Returns a kill signal receiver.
    pub fn register(&self, session_id: &str, params: &SessionParams) -> watch::Receiver<bool> {
        let now = crate::control::security::time::now_secs();
        let (kill_tx, kill_rx) = watch::channel(false);
        let entry = RegisteredSession {
            user_id: params.user_id.clone(),
            db_user: params.db_user.clone(),
            peer_addr: params.peer_addr.clone(),
            protocol: params.protocol.clone(),
            auth_method: params.auth_method.clone(),
            tenant_id: params.tenant_id,
            connected_at: now,
            last_active: std::sync::atomic::AtomicU64::new(now),
            kill_tx,
        };

        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        sessions.insert(session_id.into(), entry);
        kill_rx
    }

    /// Unregister a session (on disconnect).
    pub fn unregister(&self, session_id: &str) {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        sessions.remove(session_id);
    }

    /// Kill all sessions for a specific user.
    /// Returns the number of sessions killed.
    pub fn kill_sessions_for_user(&self, user_id: &str) -> usize {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        let mut killed = 0;
        for session in sessions.values() {
            if session.user_id == user_id {
                let _ = session.kill_tx.send(true);
                killed += 1;
            }
        }
        killed
    }

    /// Kill all sessions matching a peer address (for IP blacklist).
    pub fn kill_sessions_for_ip(&self, peer_addr: &str) -> usize {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        let mut killed = 0;
        for session in sessions.values() {
            if session.peer_addr.starts_with(peer_addr) {
                let _ = session.kill_tx.send(true);
                killed += 1;
            }
        }
        killed
    }

    /// Count active sessions, optionally filtered by user.
    pub fn count(&self, user_filter: Option<&str>) -> usize {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        match user_filter {
            Some(uid) => sessions.values().filter(|s| s.user_id == uid).count(),
            None => sessions.len(),
        }
    }

    /// Update last_active timestamp for a session.
    pub fn touch(&self, session_id: &str) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        if let Some(s) = sessions.get(session_id) {
            s.last_active
                .store(now, std::sync::atomic::Ordering::Relaxed);
        }
    }

    /// List active sessions for a user.
    pub fn sessions_for_user(&self, user_id: &str) -> Vec<(String, String, String)> {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        sessions
            .iter()
            .filter(|(_, s)| s.user_id == user_id)
            .map(|(id, s)| (id.clone(), s.peer_addr.clone(), s.protocol.clone()))
            .collect()
    }

    /// List all active sessions with full details for SHOW SESSIONS.
    pub fn list_all(&self) -> Vec<SessionInfo> {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        sessions
            .iter()
            .map(|(id, s)| SessionInfo {
                session_id: id.clone(),
                user_id: s.user_id.clone(),
                db_user: s.db_user.clone(),
                auth_method: s.auth_method.clone(),
                connected_at: s.connected_at,
                last_active: s.last_active.load(std::sync::atomic::Ordering::Relaxed),
                client_ip: s.peer_addr.clone(),
                protocol: s.protocol.clone(),
                tenant_id: s.tenant_id,
            })
            .collect()
    }
}

/// Session info for SHOW SESSIONS output.
#[derive(Debug, Clone)]
pub struct SessionInfo {
    pub session_id: String,
    pub user_id: String,
    pub db_user: String,
    pub auth_method: String,
    pub connected_at: u64,
    pub last_active: u64,
    pub client_ip: String,
    pub protocol: String,
    pub tenant_id: u64,
}

impl Default for SessionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(user: &str, addr: &str, proto: &str, auth: &str) -> SessionParams {
        SessionParams {
            user_id: user.into(),
            db_user: user.into(),
            peer_addr: addr.into(),
            protocol: proto.into(),
            auth_method: auth.into(),
            tenant_id: 1,
        }
    }

    #[test]
    fn register_and_kill() {
        let reg = SessionRegistry::new();
        let mut rx = reg.register(
            "s1",
            &params("user_42", "10.0.0.1:5000", "native", "password"),
        );

        assert_eq!(reg.count(None), 1);
        assert_eq!(reg.count(Some("user_42")), 1);

        let killed = reg.kill_sessions_for_user("user_42");
        assert_eq!(killed, 1);

        assert!(rx.has_changed().unwrap_or(false));
        assert!(*rx.borrow_and_update());
    }

    #[test]
    fn unregister_removes() {
        let reg = SessionRegistry::new();
        let _rx = reg.register(
            "s1",
            &params("user_42", "10.0.0.1:5000", "native", "password"),
        );
        assert_eq!(reg.count(None), 1);

        reg.unregister("s1");
        assert_eq!(reg.count(None), 0);
    }

    #[test]
    fn kill_by_ip() {
        let reg = SessionRegistry::new();
        let _rx1 = reg.register("s1", &params("u1", "10.0.0.1:5000", "native", "password"));
        let _rx2 = reg.register("s2", &params("u2", "10.0.0.1:5001", "pgwire", "password"));
        let _rx3 = reg.register("s3", &params("u3", "192.168.1.1:5000", "http", "api_key"));

        let killed = reg.kill_sessions_for_ip("10.0.0.1");
        assert_eq!(killed, 2);
    }

    #[test]
    fn different_users_isolated() {
        let reg = SessionRegistry::new();
        let _rx1 = reg.register("s1", &params("u1", "10.0.0.1:5000", "native", "password"));
        let _rx2 = reg.register("s2", &params("u2", "10.0.0.2:5000", "native", "password"));

        let killed = reg.kill_sessions_for_user("u1");
        assert_eq!(killed, 1);
        assert_eq!(reg.count(Some("u2")), 1);
    }
}
