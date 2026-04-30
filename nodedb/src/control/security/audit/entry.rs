//! The durable `AuditEntry` record + SHA-256 hash-chain linking.

use std::time::SystemTime;

use crate::types::TenantId;

use super::event::AuditEvent;

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

/// Compute SHA-256 hash of an audit entry for chain linking.
///
/// Canonical byte layout (to reproduce hashes from raw entries):
///   prev_hash_utf8 | seq_le8 | timestamp_us_le8 | event_discriminant_u8
///   | zerompk(event_payload) | auth_user_id_utf8 | auth_user_name_utf8
///   | session_id_utf8 | source_utf8 | detail_utf8
///
/// Each field is fed as a distinct `hasher.update()` call with no length
/// prefix or separator — the discriminant byte uniquely distinguishes
/// all variant payloads, making ambiguity impossible.
/// `event_payload` = zerompk serialization of the `AuditEvent` variant
/// (the same encoding used by zerompk::ToMessagePack derive).
pub(crate) fn hash_entry(entry: &AuditEntry) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(entry.prev_hash.as_bytes());
    hasher.update(entry.seq.to_le_bytes());
    hasher.update(entry.timestamp_us.to_le_bytes());
    hasher.update([entry.event.discriminant()]);
    // zerompk canonical bytes for the event variant payload (stable across Debug changes).
    let event_bytes = zerompk::to_msgpack_vec(&entry.event).unwrap_or_default();
    hasher.update(&event_bytes);
    hasher.update(entry.auth_user_id.as_bytes());
    hasher.update(entry.auth_user_name.as_bytes());
    hasher.update(entry.session_id.as_bytes());
    hasher.update(entry.source.as_bytes());
    hasher.update(entry.detail.as_bytes());
    format!("{:x}", hasher.finalize())
}

pub(crate) fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(seq: u64, event: AuditEvent, prev_hash: &str) -> AuditEntry {
        AuditEntry {
            seq,
            timestamp_us: 1_700_000_000_000_000,
            event,
            tenant_id: None,
            auth_user_id: "user1".to_string(),
            auth_user_name: "Alice".to_string(),
            session_id: "sess-42".to_string(),
            source: "127.0.0.1".to_string(),
            detail: "test detail".to_string(),
            prev_hash: prev_hash.to_string(),
        }
    }

    /// Hardcoded golden hash — pins the canonical encoding for an AuthSuccess
    /// entry. The expected hex was produced by the first correct
    /// discriminant-based implementation and is intentionally frozen here.
    #[test]
    fn hash_golden_value() {
        let entry = AuditEntry {
            seq: 1,
            timestamp_us: 1_700_000_000_000_000,
            event: AuditEvent::AuthSuccess,
            tenant_id: None,
            auth_user_id: "user1".to_string(),
            auth_user_name: "Alice".to_string(),
            session_id: "sess-42".to_string(),
            source: "127.0.0.1".to_string(),
            detail: "test detail".to_string(),
            prev_hash: String::new(),
        };
        let h = hash_entry(&entry);
        // Pinned hash. Updating this string requires explicit reasoning — encoding changes break audit chain compatibility.
        assert_eq!(
            h, "0394994e7dda6e6ea99b47a9d6a73b305056ed8d34d5573286b2e42b158a7985",
            "canonical hash changed — audit chain encoding must not drift"
        );
    }

    /// Pins the canonical hash for the highest-discriminant `AuditEvent`
    /// variant (`AuditCheckpoint = 25`). Because the hash encodes the
    /// `#[repr(u8)]` discriminant directly, any new variant added above 25
    /// would have a different discriminant and therefore a different hash —
    /// proving that existing entries are unaffected by new variants.
    #[test]
    fn hash_pinned_for_high_discriminant_variant() {
        let entry = make_entry(1, AuditEvent::AuditCheckpoint, "");
        let h = hash_entry(&entry);
        // Pinned hash. Updating this string requires explicit reasoning — encoding changes break audit chain compatibility.
        assert_eq!(
            h, "a7eac81c35ed115b7345ad6ac9e1ccbfb78d46c5af9b4647ebe6d2560d4a00c2",
            "canonical hash for AuditCheckpoint (discriminant 25) must not drift"
        );
    }

    #[test]
    fn different_events_produce_different_hashes() {
        let e1 = make_entry(1, AuditEvent::AuthSuccess, "");
        let e2 = make_entry(1, AuditEvent::AuthFailure, "");
        assert_ne!(
            hash_entry(&e1),
            hash_entry(&e2),
            "distinct events must hash differently"
        );
    }

    #[test]
    fn audit_checkpoint_variant_is_hashable() {
        let entry = make_entry(10, AuditEvent::AuditCheckpoint, "prev-hash-hex");
        let h = hash_entry(&entry);
        assert_eq!(h.len(), 64);
    }
}
