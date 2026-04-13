//! Per-node in-memory view of the replicated metadata state.
//!
//! One `MetadataCache` per node. Updated exclusively by a
//! [`super::applier::MetadataApplier`] as entries are committed on the
//! metadata Raft group. Planners, pgwire handlers, and the rest of the
//! control plane read from this cache — never directly from redb.

use std::collections::HashMap;

use nodedb_types::Hlc;
use tracing::{debug, warn};

use crate::metadata_group::actions::{
    ApiKeyAction, AuditRetentionAction, ChangeStreamAction, CollectionAction, CollectionAlter,
    FunctionAction, GrantAction, IndexAction, MaterializedViewAction, ProcedureAction, RlsAction,
    RoleAction, ScheduleAction, SequenceAction, TenantAction, TriggerAction, UserAction,
};
use crate::metadata_group::descriptors::{
    ApiKeyDescriptor, AuditRetentionDescriptor, ChangeStreamDescriptor, CollectionDescriptor,
    DescriptorId, DescriptorLease, FunctionDescriptor, GrantDescriptor, IndexDescriptor,
    MaterializedViewDescriptor, ProcedureDescriptor, RlsDescriptor, RoleDescriptor,
    ScheduleDescriptor, SequenceDescriptor, TenantDescriptor, TriggerDescriptor, UserDescriptor,
};
use crate::metadata_group::entry::{MetadataEntry, RoutingChange, TopologyChange};

/// In-memory view of the committed metadata state.
#[derive(Debug, Default)]
pub struct MetadataCache {
    pub applied_index: u64,
    pub last_applied_hlc: Hlc,

    pub collections: HashMap<DescriptorId, CollectionDescriptor>,
    pub indexes: HashMap<DescriptorId, IndexDescriptor>,
    pub triggers: HashMap<DescriptorId, TriggerDescriptor>,
    pub sequences: HashMap<DescriptorId, SequenceDescriptor>,
    pub users: HashMap<DescriptorId, UserDescriptor>,
    pub roles: HashMap<DescriptorId, RoleDescriptor>,
    pub grants: HashMap<DescriptorId, GrantDescriptor>,
    pub rls_policies: HashMap<DescriptorId, RlsDescriptor>,
    pub change_streams: HashMap<DescriptorId, ChangeStreamDescriptor>,
    pub materialized_views: HashMap<DescriptorId, MaterializedViewDescriptor>,
    pub schedules: HashMap<DescriptorId, ScheduleDescriptor>,
    pub functions: HashMap<DescriptorId, FunctionDescriptor>,
    pub procedures: HashMap<DescriptorId, ProcedureDescriptor>,
    pub tenants: HashMap<DescriptorId, TenantDescriptor>,
    pub api_keys: HashMap<DescriptorId, ApiKeyDescriptor>,
    pub audit_retention: HashMap<DescriptorId, AuditRetentionDescriptor>,

    /// `(descriptor_id, node_id) -> lease`.
    pub leases: HashMap<(DescriptorId, u64), DescriptorLease>,

    pub topology_log: Vec<TopologyChange>,
    pub routing_log: Vec<RoutingChange>,

    pub cluster_version: u16,
}

impl MetadataCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply a committed entry. Idempotent by `applied_index`: entries at
    /// or below the current watermark are ignored.
    pub fn apply(&mut self, index: u64, entry: &MetadataEntry) {
        if index != 0 && index <= self.applied_index {
            debug!(
                index,
                watermark = self.applied_index,
                "metadata cache: skipping already-applied entry"
            );
            return;
        }
        self.applied_index = index;

        match entry {
            MetadataEntry::CollectionDdl {
                action,
                host_payload: _,
                ..
            } => {
                apply_collection(&mut self.collections, action, &mut self.last_applied_hlc);
            }
            MetadataEntry::IndexDdl { action, .. } => apply_index(&mut self.indexes, action),
            MetadataEntry::TriggerDdl { action, .. } => apply_trigger(&mut self.triggers, action),
            MetadataEntry::SequenceDdl { action, .. } => {
                apply_sequence(&mut self.sequences, action)
            }
            MetadataEntry::UserDdl { action, .. } => apply_user(&mut self.users, action),
            MetadataEntry::RoleDdl { action, .. } => apply_role(&mut self.roles, action),
            MetadataEntry::GrantDdl { action, .. } => apply_grant(&mut self.grants, action),
            MetadataEntry::RlsDdl { action, .. } => apply_rls(&mut self.rls_policies, action),
            MetadataEntry::ChangeStreamDdl { action, .. } => {
                apply_change_stream(&mut self.change_streams, action)
            }
            MetadataEntry::MaterializedViewDdl { action, .. } => {
                apply_mv(&mut self.materialized_views, action)
            }
            MetadataEntry::ScheduleDdl { action, .. } => {
                apply_schedule(&mut self.schedules, action)
            }
            MetadataEntry::FunctionDdl { action, .. } => {
                apply_function(&mut self.functions, action)
            }
            MetadataEntry::ProcedureDdl { action, .. } => {
                apply_procedure(&mut self.procedures, action)
            }
            MetadataEntry::TenantDdl { action } => apply_tenant(&mut self.tenants, action),
            MetadataEntry::ApiKeyDdl { action, .. } => apply_api_key(&mut self.api_keys, action),
            MetadataEntry::AuditRetentionDdl { action, .. } => {
                apply_audit(&mut self.audit_retention, action)
            }

            MetadataEntry::TopologyChange(change) => self.topology_log.push(change.clone()),
            MetadataEntry::RoutingChange(change) => self.routing_log.push(change.clone()),

            MetadataEntry::ClusterVersionBump { from, to } => {
                if *from != self.cluster_version && self.cluster_version != 0 {
                    warn!(
                        expected = self.cluster_version,
                        got = *from,
                        "cluster version bump mismatch"
                    );
                }
                self.cluster_version = *to;
            }

            MetadataEntry::DescriptorLeaseGrant(lease) => {
                if lease.expires_at > self.last_applied_hlc {
                    self.last_applied_hlc = lease.expires_at;
                }
                self.leases
                    .insert((lease.descriptor_id.clone(), lease.node_id), lease.clone());
            }
            MetadataEntry::DescriptorLeaseRelease {
                node_id,
                descriptor_ids,
            } => {
                for id in descriptor_ids {
                    self.leases.remove(&(id.clone(), *node_id));
                }
            }
        }
    }

    pub fn collection(&self, id: &DescriptorId) -> Option<&CollectionDescriptor> {
        self.collections.get(id)
    }

    pub fn collection_count(&self) -> usize {
        self.collections.len()
    }
}

// ── Per-type apply helpers ──────────────────────────────────────────────

fn apply_collection(
    map: &mut HashMap<DescriptorId, CollectionDescriptor>,
    action: &CollectionAction,
    last_hlc: &mut Hlc,
) {
    match action {
        CollectionAction::Create(desc) => {
            if desc.header.modification_time > *last_hlc {
                *last_hlc = desc.header.modification_time;
            }
            map.insert(desc.header.id.clone(), (**desc).clone());
        }
        CollectionAction::Drop { id } => {
            map.remove(id);
        }
        CollectionAction::Alter { id, change } => {
            let Some(existing) = map.get_mut(id) else {
                warn!(?id, "alter on unknown collection");
                return;
            };
            match change {
                CollectionAlter::AddColumn(col) => existing.columns.push(col.clone()),
                CollectionAlter::DropColumn { name } => {
                    existing.columns.retain(|c| c.name != *name)
                }
                CollectionAlter::RenameColumn { from, to } => {
                    for c in existing.columns.iter_mut() {
                        if c.name == *from {
                            c.name = to.clone();
                        }
                    }
                }
                CollectionAlter::SetColumnDefault { name, default } => {
                    for c in existing.columns.iter_mut() {
                        if c.name == *name {
                            c.default = default.clone();
                        }
                    }
                }
                CollectionAlter::SetColumnNullable { name, nullable } => {
                    for c in existing.columns.iter_mut() {
                        if c.name == *name {
                            c.nullable = *nullable;
                        }
                    }
                }
                CollectionAlter::SetWithOption { key, value } => {
                    if let Some(opt) = existing.with_options.iter_mut().find(|(k, _)| k == key) {
                        opt.1 = value.clone();
                    } else {
                        existing.with_options.push((key.clone(), value.clone()));
                    }
                }
                CollectionAlter::RemoveWithOption { key } => {
                    existing.with_options.retain(|(k, _)| k != key);
                }
            }
            existing.header.version = existing.header.version.saturating_add(1);
        }
    }
}

/// Generate a simple Create/Drop/Alter apply function for a descriptor type.
///
/// For descriptors without specialized alter semantics, Alter just bumps the
/// version — the authoritative handler-side migration (batch 2) rewrites the
/// entire descriptor via Create with an incremented version, so per-type
/// alter branches are either trivial or unused today. Specialised alter
/// handling for RLS expressions, sequence restarts, etc. lives alongside
/// each pgwire handler migration in batch 2.
macro_rules! impl_simple_apply {
    ($fn:ident, $action:ident, $desc:ty) => {
        fn $fn(map: &mut HashMap<DescriptorId, $desc>, action: &$action) {
            match action {
                $action::Create(desc) => {
                    map.insert(desc.header.id.clone(), (**desc).clone());
                }
                $action::Drop { id } => {
                    map.remove(id);
                }
                $action::Alter { id, change: _ } => {
                    if let Some(existing) = map.get_mut(id) {
                        existing.header.version = existing.header.version.saturating_add(1);
                    }
                }
            }
        }
    };
}

impl_simple_apply!(apply_index, IndexAction, IndexDescriptor);
impl_simple_apply!(apply_trigger, TriggerAction, TriggerDescriptor);
impl_simple_apply!(apply_sequence, SequenceAction, SequenceDescriptor);
impl_simple_apply!(apply_user, UserAction, UserDescriptor);
impl_simple_apply!(apply_role, RoleAction, RoleDescriptor);
impl_simple_apply!(apply_grant, GrantAction, GrantDescriptor);
impl_simple_apply!(apply_rls, RlsAction, RlsDescriptor);
impl_simple_apply!(
    apply_change_stream,
    ChangeStreamAction,
    ChangeStreamDescriptor
);
impl_simple_apply!(apply_mv, MaterializedViewAction, MaterializedViewDescriptor);
impl_simple_apply!(apply_schedule, ScheduleAction, ScheduleDescriptor);
impl_simple_apply!(apply_function, FunctionAction, FunctionDescriptor);
impl_simple_apply!(apply_procedure, ProcedureAction, ProcedureDescriptor);
impl_simple_apply!(apply_tenant, TenantAction, TenantDescriptor);
impl_simple_apply!(apply_api_key, ApiKeyAction, ApiKeyDescriptor);
impl_simple_apply!(apply_audit, AuditRetentionAction, AuditRetentionDescriptor);
