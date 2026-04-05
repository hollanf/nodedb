//! Immutable audit log for security-relevant events.

use std::collections::VecDeque;
use std::time::SystemTime;

use crate::types::TenantId;

/// Auth context for enriched audit entries.
#[derive(Debug, Clone, Default)]
pub struct AuditAuth {
    /// Authenticated user ID.
    pub user_id: String,
    /// Authenticated username (for display).
    pub user_name: String,
    /// Session ID for correlation.
    pub session_id: String,
}

/// Audit level: controls which events are recorded.
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    serde::Serialize,
    serde::Deserialize,
)]
pub enum AuditLevel {
    /// Auth success/failure, privilege changes only.
    Minimal = 0,
    /// + admin actions, config changes, DDL (default).
    #[default]
    Standard = 1,
    /// + every query execution, RLS denials.
    Full = 2,
    /// + row-level changes, CRDT deltas, full request/response.
    Forensic = 3,
}

/// Security-relevant audit event.
#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct AuditEntry {
    /// Monotonic sequence number within this node.
    pub seq: u64,
    /// UTC timestamp (microseconds since epoch).
    pub timestamp_us: u64,
    /// Event category.
    pub event: AuditEvent,
    /// Tenant context (if applicable).
    pub tenant_id: Option<TenantId>,
    /// Authenticated user ID (from AuthContext). Empty for unauthenticated.
    #[serde(default)]
    pub auth_user_id: String,
    /// Authenticated username (for display/audit trail).
    #[serde(default)]
    pub auth_user_name: String,
    /// Session ID (for audit correlation across events).
    #[serde(default)]
    pub session_id: String,
    /// Source IP or node identifier.
    pub source: String,
    /// Human-readable detail.
    pub detail: String,
    /// SHA-256 hash of the previous entry (hex). Empty for first entry.
    pub prev_hash: String,
}

/// Categories of audit events.
#[repr(u8)]
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(c_enum)]
pub enum AuditEvent {
    /// Authentication succeeded.
    AuthSuccess = 0,
    /// Authentication failed.
    AuthFailure = 1,
    /// Authorization denied.
    AuthzDenied = 2,
    /// Privilege/role change.
    PrivilegeChange = 3,
    /// Tenant created.
    TenantCreated = 4,
    /// Tenant deleted.
    TenantDeleted = 5,
    /// Snapshot initiated.
    SnapshotBegin = 6,
    /// Snapshot completed.
    SnapshotEnd = 7,
    /// Snapshot restore initiated.
    RestoreBegin = 8,
    /// Snapshot restore completed.
    RestoreEnd = 9,
    /// TLS certificate rotated.
    CertRotation = 10,
    /// TLS certificate rotation failed.
    CertRotationFailed = 11,
    /// Encryption key rotated.
    KeyRotation = 12,
    /// Configuration change.
    ConfigChange = 13,
    /// Node joined cluster.
    NodeJoined = 14,
    /// Node left cluster.
    NodeLeft = 15,
    /// Admin action (catch-all for ops).
    AdminAction = 16,
    /// Session connected.
    SessionConnect = 17,
    /// Session disconnected.
    SessionDisconnect = 18,
    /// Query executed (full/forensic level only).
    QueryExec = 19,
    /// RLS denial (full level).
    RlsDenied = 20,
    /// Row-level change (forensic level only).
    RowChange = 21,
}

impl AuditEvent {
    /// Whether this event belongs to the auth event stream.
    pub fn is_auth_event(&self) -> bool {
        matches!(
            self,
            Self::AuthSuccess
                | Self::AuthFailure
                | Self::AuthzDenied
                | Self::SessionConnect
                | Self::SessionDisconnect
        )
    }

    /// Minimum audit level required to record this event.
    pub fn min_level(&self) -> AuditLevel {
        match self {
            Self::AuthSuccess | Self::AuthFailure | Self::AuthzDenied => AuditLevel::Minimal,
            Self::PrivilegeChange
            | Self::AdminAction
            | Self::ConfigChange
            | Self::SessionConnect
            | Self::SessionDisconnect
            | Self::TenantCreated
            | Self::TenantDeleted
            | Self::SnapshotBegin
            | Self::SnapshotEnd
            | Self::RestoreBegin
            | Self::RestoreEnd
            | Self::CertRotation
            | Self::CertRotationFailed
            | Self::KeyRotation
            | Self::NodeJoined
            | Self::NodeLeft => AuditLevel::Standard,
            Self::QueryExec | Self::RlsDenied => AuditLevel::Full,
            Self::RowChange => AuditLevel::Forensic,
        }
    }
}

/// Append-only audit log.
///
/// Entries are immutable once appended. The log supports bounded in-memory
/// retention with overflow to the WAL or a dedicated audit segment file.
///
/// Lives on the Control Plane (Send + Sync).
pub struct AuditLog {
    entries: VecDeque<AuditEntry>,
    /// Separate auth event stream for login/logout/MFA/deactivation.
    auth_events: VecDeque<AuditEntry>,
    /// Maximum entries retained in memory.
    max_entries: usize,
    /// Next sequence number.
    next_seq: u64,
    /// Total entries ever recorded (including evicted).
    total_entries: u64,
    /// Hash of the last recorded entry (for chain continuity).
    last_hash: String,
    /// Current audit level (controls which events are recorded).
    level: AuditLevel,
}

impl AuditLog {
    pub fn new(max_entries: usize) -> Self {
        Self {
            entries: VecDeque::with_capacity(max_entries.min(10_000)),
            auth_events: VecDeque::with_capacity(1_000),
            max_entries,
            next_seq: 1,
            total_entries: 0,
            last_hash: String::new(),
            level: AuditLevel::default(),
        }
    }

    /// Set the audit level.
    pub fn set_level(&mut self, level: AuditLevel) {
        self.level = level;
    }

    /// Get the current audit level.
    pub fn level(&self) -> AuditLevel {
        self.level
    }

    /// Set the next sequence number (used on startup to resume from catalog).
    pub fn set_next_seq(&mut self, seq: u64) {
        self.next_seq = seq;
    }

    /// Set the last hash (used on startup to continue the chain from catalog).
    pub fn set_last_hash(&mut self, hash: String) {
        self.last_hash = hash;
    }

    /// Record an audit event with hash chain (legacy — no auth context).
    pub fn record(
        &mut self,
        event: AuditEvent,
        tenant_id: Option<TenantId>,
        source: &str,
        detail: &str,
    ) -> u64 {
        self.record_with_auth(event, tenant_id, source, detail, &AuditAuth::default())
    }

    /// Record an audit event with auth context enrichment.
    pub fn record_with_auth(
        &mut self,
        event: AuditEvent,
        tenant_id: Option<TenantId>,
        source: &str,
        detail: &str,
        auth: &AuditAuth,
    ) -> u64 {
        let seq = self.next_seq;
        self.next_seq += 1;
        let prev_hash = self.last_hash.clone();

        let entry = AuditEntry {
            seq,
            timestamp_us: now_us(),
            event: event.clone(),
            tenant_id,
            auth_user_id: auth.user_id.clone(),
            auth_user_name: auth.user_name.clone(),
            session_id: auth.session_id.clone(),
            source: source.to_string(),
            detail: detail.to_string(),
            prev_hash,
        };

        self.last_hash = hash_entry(&entry);

        // Route auth events to the separate auth_events stream.
        if event.is_auth_event() {
            if self.auth_events.len() >= 10_000 {
                self.auth_events.pop_front();
            }
            self.auth_events.push_back(entry.clone());
        }

        if self.entries.len() >= self.max_entries {
            self.entries.pop_front();
        }
        self.entries.push_back(entry);
        self.total_entries += 1;
        seq
    }

    /// Query entries by event type.
    pub fn query_by_event(&self, event: &AuditEvent) -> Vec<&AuditEntry> {
        self.entries.iter().filter(|e| &e.event == event).collect()
    }

    /// Query entries for a specific tenant.
    pub fn query_by_tenant(&self, tenant_id: TenantId) -> Vec<&AuditEntry> {
        self.entries
            .iter()
            .filter(|e| e.tenant_id == Some(tenant_id))
            .collect()
    }

    /// Query entries since a given sequence number.
    pub fn since(&self, seq: u64) -> Vec<&AuditEntry> {
        self.entries.iter().filter(|e| e.seq >= seq).collect()
    }

    /// Get all entries in memory.
    pub fn all(&self) -> &VecDeque<AuditEntry> {
        &self.entries
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn total_recorded(&self) -> u64 {
        self.total_entries
    }

    /// Query entries by auth user ID.
    pub fn query_by_user(&self, auth_user_id: &str) -> Vec<&AuditEntry> {
        self.entries
            .iter()
            .filter(|e| e.auth_user_id == auth_user_id)
            .collect()
    }

    /// Get the separate auth events stream (login/logout/MFA/deactivation).
    pub fn auth_events(&self) -> &VecDeque<AuditEntry> {
        &self.auth_events
    }

    /// Drain entries for persistence (WAL or segment file).
    pub fn drain_for_persistence(&mut self) -> Vec<AuditEntry> {
        self.entries.drain(..).collect()
    }

    /// Get the current last hash (for chain continuity on restart).
    pub fn last_hash(&self) -> &str {
        &self.last_hash
    }

    /// Verify the hash chain integrity of in-memory entries.
    /// Returns Ok(()) if chain is valid, Err with the broken sequence number.
    pub fn verify_chain(&self) -> Result<(), u64> {
        let mut expected_prev = String::new();
        for entry in &self.entries {
            if entry.prev_hash != expected_prev {
                return Err(entry.seq);
            }
            expected_prev = hash_entry(entry);
        }
        Ok(())
    }
}

/// Compute SHA-256 hash of an audit entry for chain linking.
///
/// Hash covers: prev_hash + seq + timestamp + event + source + detail.
/// This ensures any modification to any field breaks the chain.
fn hash_entry(entry: &AuditEntry) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(entry.prev_hash.as_bytes());
    hasher.update(entry.seq.to_le_bytes());
    hasher.update(entry.timestamp_us.to_le_bytes());
    hasher.update(format!("{:?}", entry.event).as_bytes());
    hasher.update(entry.auth_user_id.as_bytes());
    hasher.update(entry.auth_user_name.as_bytes());
    hasher.update(entry.session_id.as_bytes());
    hasher.update(entry.source.as_bytes());
    hasher.update(entry.detail.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_log() {
        let log = AuditLog::new(100);
        assert!(log.is_empty());
        assert_eq!(log.total_recorded(), 0);
    }

    #[test]
    fn record_and_query() {
        let mut log = AuditLog::new(100);

        log.record(
            AuditEvent::AuthSuccess,
            Some(TenantId::new(1)),
            "10.0.0.1",
            "user login",
        );
        log.record(
            AuditEvent::AuthFailure,
            Some(TenantId::new(2)),
            "10.0.0.2",
            "bad password",
        );
        log.record(
            AuditEvent::AuthSuccess,
            Some(TenantId::new(1)),
            "10.0.0.1",
            "api key auth",
        );

        assert_eq!(log.len(), 3);
        assert_eq!(log.total_recorded(), 3);

        let successes = log.query_by_event(&AuditEvent::AuthSuccess);
        assert_eq!(successes.len(), 2);

        let tenant1 = log.query_by_tenant(TenantId::new(1));
        assert_eq!(tenant1.len(), 2);

        let tenant2 = log.query_by_tenant(TenantId::new(2));
        assert_eq!(tenant2.len(), 1);
    }

    #[test]
    fn sequence_numbers_monotonic() {
        let mut log = AuditLog::new(100);
        let s1 = log.record(AuditEvent::AuthSuccess, None, "src", "");
        let s2 = log.record(AuditEvent::AuthFailure, None, "src", "");
        let s3 = log.record(AuditEvent::AdminAction, None, "src", "");
        assert_eq!(s1, 1);
        assert_eq!(s2, 2);
        assert_eq!(s3, 3);
    }

    #[test]
    fn since_query() {
        let mut log = AuditLog::new(100);
        log.record(AuditEvent::AuthSuccess, None, "src", "a");
        log.record(AuditEvent::AuthFailure, None, "src", "b");
        log.record(AuditEvent::AdminAction, None, "src", "c");

        let since2 = log.since(2);
        assert_eq!(since2.len(), 2);
        assert_eq!(since2[0].detail, "b");
        assert_eq!(since2[1].detail, "c");
    }

    #[test]
    fn bounded_eviction() {
        let mut log = AuditLog::new(3);
        for i in 0..5 {
            log.record(AuditEvent::AuthSuccess, None, "src", &format!("entry-{i}"));
        }

        assert_eq!(log.len(), 3);
        assert_eq!(log.total_recorded(), 5);
        // Oldest entries evicted.
        assert_eq!(log.all()[0].detail, "entry-2");
    }

    #[test]
    fn drain_for_persistence() {
        let mut log = AuditLog::new(100);
        log.record(
            AuditEvent::SnapshotBegin,
            None,
            "node-1",
            "snapshot started",
        );
        log.record(AuditEvent::SnapshotEnd, None, "node-1", "snapshot done");

        let drained = log.drain_for_persistence();
        assert_eq!(drained.len(), 2);
        assert!(log.is_empty());
    }

    #[test]
    fn all_event_types_representable() {
        let events = [
            AuditEvent::AuthSuccess,
            AuditEvent::AuthFailure,
            AuditEvent::AuthzDenied,
            AuditEvent::PrivilegeChange,
            AuditEvent::TenantCreated,
            AuditEvent::TenantDeleted,
            AuditEvent::SnapshotBegin,
            AuditEvent::SnapshotEnd,
            AuditEvent::RestoreBegin,
            AuditEvent::RestoreEnd,
            AuditEvent::CertRotation,
            AuditEvent::CertRotationFailed,
            AuditEvent::KeyRotation,
            AuditEvent::ConfigChange,
            AuditEvent::NodeJoined,
            AuditEvent::NodeLeft,
            AuditEvent::AdminAction,
        ];

        let mut log = AuditLog::new(100);
        for event in events {
            log.record(event, None, "test", "");
        }
        assert_eq!(log.len(), 17);
    }
}
