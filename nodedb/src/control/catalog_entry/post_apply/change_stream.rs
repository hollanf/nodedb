//! ChangeStream post-apply side effects — sync the in-memory
//! `stream_registry`, tear down the CDC router buffer on drop, and
//! cascade consumer-group state cleanup.

use std::sync::Arc;

use tracing::warn;

use crate::control::state::SharedState;
use crate::event::cdc::stream_def::ChangeStreamDef;

pub fn put(stored: ChangeStreamDef, shared: Arc<SharedState>) {
    super::owner::install_from_parent(
        "change_stream",
        stored.tenant_id,
        &stored.name,
        &stored.owner,
        &shared,
    );
    shared.stream_registry.register(stored);
}

pub fn delete(tenant_id: u32, name: String, shared: Arc<SharedState>) {
    // 1. Drop the stream def from the in-memory registry so no new
    //    events are routed to it.
    shared.stream_registry.unregister(tenant_id, &name);

    // 2. Drop the per-stream retention buffer from the CDC router.
    shared.cdc_router.remove_buffer(tenant_id, &name);

    // 3. Cascade consumer-group teardown. Every group scoped to this
    //    stream must have its in-memory registry entry dropped AND
    //    its persisted offset state wiped — otherwise a `CREATE
    //    CHANGE STREAM` with the same name after a drop would
    //    resume from a stale consumer-group offset and silently
    //    skip real events.
    let groups = shared.group_registry.list_for_stream(tenant_id, &name);
    for def in &groups {
        shared
            .group_registry
            .unregister(tenant_id, &name, &def.name);
        if let Err(e) = shared
            .offset_store
            .delete_group(tenant_id, &name, &def.name)
        {
            warn!(
                tenant = tenant_id,
                stream = %name,
                group = %def.name,
                error = %e,
                "failed to delete persisted consumer-group offsets on stream drop"
            );
        }
    }

    shared
        .permissions
        .install_replicated_remove_owner("change_stream", tenant_id, &name);
}
