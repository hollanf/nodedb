//! Audit event taxonomy + per-variant routing rules (auth-stream flag,
//! minimum level).

use super::level::AuditLevel;

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
    /// DDL change committed to the metadata Raft group. Emitted on
    /// every replica from `MetadataCommitApplier` with full before /
    /// after descriptor versions + HLC + raw SQL text. (J.4)
    DdlChange = 22,
    /// Session handle resolve failed fingerprint check — caller's
    /// (tenant_id, ip) didn't match the fingerprint captured at
    /// `SessionHandleStore::create()`. Signals handle theft across
    /// origins even when the handle itself is otherwise valid.
    SessionHandleFingerprintMismatch = 23,
    /// Resolve-miss rate on a single connection crossed the configured
    /// threshold within the detection window. Signals enumeration
    /// attempts or misconfigured clients probing bogus handles.
    SessionHandleResolveMissSpike = 24,
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
            Self::DdlChange => AuditLevel::Standard,
            Self::SessionHandleFingerprintMismatch | Self::SessionHandleResolveMissSpike => {
                AuditLevel::Standard
            }
        }
    }
}
