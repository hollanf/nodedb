//! Shared descriptor identity + header.

use nodedb_types::Hlc;
use serde::{Deserialize, Serialize};

use crate::metadata_group::state::DescriptorState;

/// Globally unique, tenant-scoped identifier for a schema descriptor.
///
/// `kind` disambiguates object types sharing the same `(tenant_id, name)`
/// (e.g. a collection and an index can both be named `orders`).
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct DescriptorId {
    pub tenant_id: u64,
    pub kind: DescriptorKind,
    pub name: String,
}

impl DescriptorId {
    pub fn new(tenant_id: u64, kind: DescriptorKind, name: impl Into<String>) -> Self {
        Self {
            tenant_id,
            kind,
            name: name.into(),
        }
    }
}

/// Discriminant for [`DescriptorId`] — one variant per schema object type.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum DescriptorKind {
    Collection,
    Index,
    Trigger,
    Sequence,
    User,
    Role,
    Grant,
    Rls,
    ChangeStream,
    MaterializedView,
    Schedule,
    Function,
    Procedure,
    Tenant,
    ApiKey,
    AuditRetention,
}

/// Common header embedded in every descriptor.
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
pub struct DescriptorHeader {
    pub id: DescriptorId,
    /// Monotonic version; incremented on every Alter.
    pub version: u64,
    /// HLC at which this version was committed.
    pub modification_time: Hlc,
    /// Lifecycle state.
    pub state: DescriptorState,
}

impl DescriptorHeader {
    pub fn new_public(id: DescriptorId, version: u64, modification_time: Hlc) -> Self {
        Self {
            id,
            version,
            modification_time,
            state: DescriptorState::Public,
        }
    }
}
