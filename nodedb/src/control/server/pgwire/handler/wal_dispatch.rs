//! WAL append logic: delegates to shared dispatch utilities.

use crate::types::{TenantId, VShardId};

use super::core::NodeDbPgHandler;

impl NodeDbPgHandler {
    /// Append a write operation to the WAL for single-node durability.
    ///
    /// Delegates to the shared `dispatch_utils::wal_append_if_write` to
    /// avoid duplication between pgwire and HTTP endpoints.
    pub(super) fn wal_append_if_write(
        &self,
        tenant_id: TenantId,
        vshard_id: VShardId,
        plan: &crate::bridge::envelope::PhysicalPlan,
    ) -> crate::Result<()> {
        crate::control::server::wal_dispatch::wal_append_if_write(
            &self.state.wal,
            tenant_id,
            vshard_id,
            plan,
        )
    }
}
