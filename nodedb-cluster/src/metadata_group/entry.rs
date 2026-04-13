//! The canonical wire-type for every entry proposed to the metadata Raft group.

use serde::{Deserialize, Serialize};

use crate::metadata_group::actions::{
    ApiKeyAction, AuditRetentionAction, ChangeStreamAction, CollectionAction, FunctionAction,
    GrantAction, IndexAction, MaterializedViewAction, ProcedureAction, RlsAction, RoleAction,
    ScheduleAction, SequenceAction, TenantAction, TriggerAction, UserAction,
};
use crate::metadata_group::descriptors::DescriptorLease;
use crate::metadata_group::descriptors::common::DescriptorId;

/// An entry in the replicated metadata log.
///
/// Every mutation to cluster-wide state — DDL, topology, routing, descriptor
/// leases, cluster version bumps — is encoded as one of these variants,
/// proposed against the metadata Raft group, and applied on every node by a
/// [`crate::metadata_group::applier::MetadataApplier`].
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum MetadataEntry {
    // ── DDL ────────────────────────────────────────────────────────────
    CollectionDdl {
        tenant_id: u32,
        action: CollectionAction,
        /// Opaque host-crate payload used by the production
        /// applier to materialize the descriptor into the host's
        /// local store (e.g. nodedb's `SystemCatalog` redb). The
        /// `nodedb-cluster` crate treats this as an opaque blob;
        /// the in-cluster `CacheApplier` ignores it entirely.
        /// Empty on `Drop` (the host only needs the
        /// `DescriptorId`) and on test fixtures that don't round
        /// trip through the production applier.
        host_payload: Vec<u8>,
    },
    IndexDdl {
        tenant_id: u32,
        action: IndexAction,
    },
    TriggerDdl {
        tenant_id: u32,
        action: TriggerAction,
    },
    SequenceDdl {
        tenant_id: u32,
        action: SequenceAction,
    },
    UserDdl {
        tenant_id: u32,
        action: UserAction,
    },
    RoleDdl {
        tenant_id: u32,
        action: RoleAction,
    },
    GrantDdl {
        tenant_id: u32,
        action: GrantAction,
    },
    RlsDdl {
        tenant_id: u32,
        action: RlsAction,
    },
    ChangeStreamDdl {
        tenant_id: u32,
        action: ChangeStreamAction,
    },
    MaterializedViewDdl {
        tenant_id: u32,
        action: MaterializedViewAction,
    },
    ScheduleDdl {
        tenant_id: u32,
        action: ScheduleAction,
    },
    FunctionDdl {
        tenant_id: u32,
        action: FunctionAction,
    },
    ProcedureDdl {
        tenant_id: u32,
        action: ProcedureAction,
    },
    TenantDdl {
        action: TenantAction,
    },
    ApiKeyDdl {
        tenant_id: u32,
        action: ApiKeyAction,
    },
    AuditRetentionDdl {
        tenant_id: u32,
        action: AuditRetentionAction,
    },

    // ── Topology / routing ─────────────────────────────────────────────
    TopologyChange(TopologyChange),
    RoutingChange(RoutingChange),

    // ── Cluster version ────────────────────────────────────────────────
    ClusterVersionBump {
        from: u16,
        to: u16,
    },

    // ── Descriptor leases ──────────────────────────────────────────────
    DescriptorLeaseGrant(DescriptorLease),
    DescriptorLeaseRelease {
        node_id: u64,
        descriptor_ids: Vec<DescriptorId>,
    },
}

/// Topology mutations proposed through the metadata group.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum TopologyChange {
    Join { node_id: u64, addr: String },
    Leave { node_id: u64 },
    PromoteToVoter { node_id: u64 },
    StartDecommission { node_id: u64 },
    FinishDecommission { node_id: u64 },
}

/// Routing-table mutations proposed through the metadata group.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum RoutingChange {
    /// Move a vShard to a new raft group leaseholder.
    ReassignVShard {
        vshard_id: u16,
        new_group_id: u64,
        new_leaseholder_node_id: u64,
    },
    /// Record a leadership transfer within an existing group.
    LeadershipTransfer {
        group_id: u64,
        new_leader_node_id: u64,
    },
}
