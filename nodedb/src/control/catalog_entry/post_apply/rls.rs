//! Post-apply side effects for RLS policy `CatalogEntry` variants.
//!
//! After the synchronous `apply::rls` step has written the redb row,
//! this rehydrates the runtime `RlsPolicy` (deserializing the
//! compiled predicate / deny mode JSON) and installs it into the
//! in-memory `RlsPolicyStore` on every node so the read/write
//! evaluators see the new policy on their next request.

use std::sync::Arc;

use tracing::warn;

use crate::control::security::catalog::StoredRlsPolicy;
use crate::control::state::SharedState;

pub fn put(stored: StoredRlsPolicy, shared: Arc<SharedState>) {
    match stored.to_runtime() {
        Ok(runtime) => {
            shared.rls.install_replicated_policy(runtime);
            tracing::debug!(
                policy = %stored.name,
                collection = %stored.collection,
                tenant = stored.tenant_id,
                "post_apply: RLS policy replicated"
            );
        }
        Err(e) => {
            warn!(
                policy = %stored.name,
                collection = %stored.collection,
                tenant = stored.tenant_id,
                error = %e,
                "post_apply: RLS policy rehydration failed"
            );
        }
    }
}

pub fn delete(tenant_id: u64, collection: String, name: String, shared: Arc<SharedState>) {
    let removed = shared
        .rls
        .install_replicated_drop_policy(tenant_id, &collection, &name);
    tracing::debug!(
        policy = %name,
        collection = %collection,
        tenant = tenant_id,
        removed,
        "post_apply: RLS policy drop replicated"
    );
}
