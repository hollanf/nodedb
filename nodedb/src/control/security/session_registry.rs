//! Active session registry for BLACKLIST WITH KILL SESSIONS.
//!
//! Tracks all active connections (native, pgwire, HTTP) and provides
//! a mechanism to signal session termination by user ID.

use std::collections::HashMap;
use std::sync::RwLock;

use tokio::sync::watch;

/// A registered session with its kill signal.
struct RegisteredSession {
    user_id: String,
    peer_addr: String,
    protocol: String,
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
    ///
    /// The session run loop should `select!` on this receiver — when it
    /// receives `true`, the session should close gracefully.
    pub fn register(
        &self,
        session_id: &str,
        user_id: &str,
        peer_addr: &str,
        protocol: &str,
    ) -> watch::Receiver<bool> {
        let (kill_tx, kill_rx) = watch::channel(false);
        let entry = RegisteredSession {
            user_id: user_id.into(),
            peer_addr: peer_addr.into(),
            protocol: protocol.into(),
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

    /// List active sessions for a user.
    pub fn sessions_for_user(&self, user_id: &str) -> Vec<(String, String, String)> {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        sessions
            .iter()
            .filter(|(_, s)| s.user_id == user_id)
            .map(|(id, s)| (id.clone(), s.peer_addr.clone(), s.protocol.clone()))
            .collect()
    }
}

impl Default for SessionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_and_kill() {
        let reg = SessionRegistry::new();
        let mut rx = reg.register("s1", "user_42", "10.0.0.1:5000", "native");

        assert_eq!(reg.count(None), 1);
        assert_eq!(reg.count(Some("user_42")), 1);

        // Kill sessions for user.
        let killed = reg.kill_sessions_for_user("user_42");
        assert_eq!(killed, 1);

        // Receiver should have the kill signal.
        assert!(rx.has_changed().unwrap_or(false));
        assert!(*rx.borrow_and_update());
    }

    #[test]
    fn unregister_removes() {
        let reg = SessionRegistry::new();
        let _rx = reg.register("s1", "user_42", "10.0.0.1:5000", "native");
        assert_eq!(reg.count(None), 1);

        reg.unregister("s1");
        assert_eq!(reg.count(None), 0);
    }

    #[test]
    fn kill_by_ip() {
        let reg = SessionRegistry::new();
        let _rx1 = reg.register("s1", "u1", "10.0.0.1:5000", "native");
        let _rx2 = reg.register("s2", "u2", "10.0.0.1:5001", "pgwire");
        let _rx3 = reg.register("s3", "u3", "192.168.1.1:5000", "http");

        let killed = reg.kill_sessions_for_ip("10.0.0.1");
        assert_eq!(killed, 2);
    }

    #[test]
    fn different_users_isolated() {
        let reg = SessionRegistry::new();
        let _rx1 = reg.register("s1", "u1", "10.0.0.1:5000", "native");
        let _rx2 = reg.register("s2", "u2", "10.0.0.2:5000", "native");

        let killed = reg.kill_sessions_for_user("u1");
        assert_eq!(killed, 1);
        assert_eq!(reg.count(Some("u2")), 1); // u2 unaffected.
    }
}
