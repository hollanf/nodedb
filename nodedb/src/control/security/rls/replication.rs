//! Applier-side helpers for replicated RLS policies.
//!
//! `install_replicated_policy` / `install_replicated_drop_policy`
//! mutate the in-memory `RlsPolicyStore` from the
//! `CatalogEntry::{PutRlsPolicy, DeleteRlsPolicy}` applier, bypassing
//! the normal `create_policy` path so the proposer and follower
//! paths apply identical state.

use super::store::RlsPolicyStore;
use super::types::{RlsPolicy, policy_key};

impl RlsPolicyStore {
    /// Install (create-or-replace) a replicated policy into the
    /// in-memory registry. Called from the `CatalogEntry::PutRlsPolicy`
    /// post-apply side effect on every node.
    pub fn install_replicated_policy(&self, policy: RlsPolicy) {
        let key = policy_key(policy.tenant_id, &policy.collection);
        let mut policies = self.lock_write();
        let list = policies.entry(key).or_default();
        if let Some(existing) = list.iter_mut().find(|p| p.name == policy.name) {
            *existing = policy;
        } else {
            list.push(policy);
        }
    }

    /// Remove a replicated policy from the in-memory registry.
    /// Returns `true` if a policy was removed.
    pub fn install_replicated_drop_policy(
        &self,
        tenant_id: u64,
        collection: &str,
        policy_name: &str,
    ) -> bool {
        let key = policy_key(tenant_id, collection);
        let mut policies = self.lock_write();
        if let Some(list) = policies.get_mut(&key) {
            let before = list.len();
            list.retain(|p| p.name != policy_name);
            list.len() < before
        } else {
            false
        }
    }

    /// Check whether a policy with the given name already exists
    /// on the given (tenant, collection). Used by the handler
    /// pre-check before proposing `PutRlsPolicy`.
    pub fn policy_exists(&self, tenant_id: u64, collection: &str, policy_name: &str) -> bool {
        let key = policy_key(tenant_id, collection);
        let policies = self.lock_read();
        policies
            .get(&key)
            .map(|list| list.iter().any(|p| p.name == policy_name))
            .unwrap_or(false)
    }
}
