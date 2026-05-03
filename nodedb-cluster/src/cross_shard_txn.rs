//! Cross-shard auxiliary types for GSI forwarding and edge validation.
//!
//! This module retains the supporting types that are still active under
//! the Calvin architecture:
//!
//! - [`GsiForwardEntry`] / [`GsiAction`]: forwarded GSI index updates.
//! - [`EdgeValidationRequest`] / [`EdgeValidationResult`]: cross-shard
//!   edge existence checks before edge creation.
//!
//! The 2PC scaffolding (`ForwardEntry`, `AbortNotice`, `CrossShardTransaction`,
//! `TransactionCoordinator`, `generate_forwards`) has been removed. Calvin-style
//! deterministic scheduling supersedes 2PC: constraints are validated at
//! sequencer admission, and every shard executes the same globally-ordered
//! epoch batch deterministically. See
//! `nodedb/src/control/cluster/calvin/scheduler/` for the current cross-shard
//! write path.

use serde::{Deserialize, Serialize};

/// Cross-shard GSI update entry.
///
/// When a document write triggers a GSI update on a different shard,
/// this entry is forwarded to the target shard as a Raft-replicated
/// side-effect, ensuring atomic consistency with the primary write.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct GsiForwardEntry {
    /// GSI index name.
    pub index_name: String,
    /// The indexed field value.
    pub value: String,
    /// Document location in the primary shard.
    pub tenant_id: u64,
    pub collection: String,
    pub document_id: String,
    pub source_vshard: u32,
    /// Whether to add or remove the GSI entry.
    pub action: GsiAction,
}

#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
#[repr(u8)]
#[msgpack(c_enum)]
pub enum GsiAction {
    /// Add/update the GSI entry for this document.
    Upsert = 0,
    /// Remove the GSI entry for this document.
    Remove = 1,
}

/// Cross-shard edge creation validation request.
///
/// Before creating an edge where src and dst are on different shards,
/// the Control Plane sends a validation request to the destination
/// shard to confirm the dst node exists.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct EdgeValidationRequest {
    pub src_id: String,
    pub src_vshard: u32,
    pub dst_id: String,
    pub dst_vshard: u32,
    pub label: String,
}

#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
#[repr(u8)]
#[msgpack(c_enum)]
pub enum EdgeValidationResult {
    /// Destination exists — safe to create the edge.
    Exists = 0,
    /// Destination not found — reject the edge.
    NotFound = 1,
    /// Destination shard unreachable — retry later.
    Unavailable = 2,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn edge_validation_types() {
        let req = EdgeValidationRequest {
            src_id: "alice".into(),
            src_vshard: 10,
            dst_id: "bob".into(),
            dst_vshard: 20,
            label: "KNOWS".into(),
        };
        let bytes = zerompk::to_msgpack_vec(&req).unwrap();
        let decoded: EdgeValidationRequest = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(decoded.src_id, "alice");
        assert_eq!(decoded.dst_vshard, 20);
    }

    #[test]
    fn gsi_forward_roundtrip() {
        let entry = GsiForwardEntry {
            index_name: "email_idx".into(),
            value: "alice@example.com".into(),
            tenant_id: 1,
            collection: "users".into(),
            document_id: "u1".into(),
            source_vshard: 10,
            action: GsiAction::Upsert,
        };
        let bytes = zerompk::to_msgpack_vec(&entry).unwrap();
        let decoded: GsiForwardEntry = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(decoded.index_name, "email_idx");
    }
}
