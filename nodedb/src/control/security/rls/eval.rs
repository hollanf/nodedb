//! RLS predicate evaluation and `$auth.*` substitution.
//!
//! **Read path**: `combined_read_predicate_with_auth` merges all
//! enabled read policies into a single `ScanFilter` list injected
//! into `DocumentScan`.
//!
//! **Write path**: `check_write_with_auth` evaluates each write policy
//! against the document; failures return `Error::RejectedAuthz`.

use tracing::info;

use crate::control::security::auth_context::AuthContext;
use crate::control::security::predicate::{PolicyMode, RlsPredicate};
use crate::control::security::predicate_eval::{combine_policies, substitute_to_scan_filters};

use super::store::RlsPolicyStore;
use super::types::RlsPolicy;

impl RlsPolicyStore {
    /// Primary read-path RLS method: substitutes `$auth.*` variables
    /// and returns the combined `ScanFilter` bytes.
    ///
    /// Returns `None` when a required `$auth` field is missing
    /// (fail-closed semantics).
    pub fn combined_read_predicate_with_auth(
        &self,
        tenant_id: u64,
        collection: &str,
        auth: &AuthContext,
    ) -> Option<Vec<u8>> {
        if auth.is_superuser() {
            return Some(Vec::new());
        }

        let policies = self.read_policies(tenant_id, collection);
        if policies.is_empty() {
            return Some(Vec::new());
        }

        let compiled_policies: Vec<(RlsPredicate, PolicyMode)> = policies
            .iter()
            .filter_map(|p| p.compiled_predicate.as_ref().map(|c| (c.clone(), p.mode)))
            .collect();

        let all_filters = if !compiled_policies.is_empty() {
            combine_policies(&compiled_policies, auth)?
        } else {
            Vec::new()
        };

        if all_filters.is_empty() {
            Some(Vec::new())
        } else {
            Some(zerompk::to_msgpack_vec(&all_filters).unwrap_or_default())
        }
    }

    /// Write-path RLS check with `$auth.*` support. Evaluates compiled
    /// write policies; fail-closed on unresolved auth references.
    pub fn check_write_with_auth(
        &self,
        tenant_id: u64,
        collection: &str,
        document: &serde_json::Value,
        auth: &AuthContext,
    ) -> crate::Result<()> {
        if auth.is_superuser() {
            return Ok(());
        }

        let policies = self.write_policies(tenant_id, collection);
        if policies.is_empty() {
            return Ok(());
        }

        for policy in &policies {
            if let Some(ref compiled) = policy.compiled_predicate {
                check_compiled_write(policy, compiled, document, auth, tenant_id, collection)?;
            }
        }

        Ok(())
    }
}

fn check_compiled_write(
    policy: &RlsPolicy,
    compiled: &RlsPredicate,
    document: &serde_json::Value,
    auth: &AuthContext,
    tenant_id: u64,
    collection: &str,
) -> crate::Result<()> {
    let filters = match substitute_to_scan_filters(compiled, auth) {
        Some(f) => f,
        None => {
            info!(
                policy = %policy.name,
                username = %auth.username,
                %collection,
                "RLS write policy: unresolved $auth reference → denied"
            );
            return Err(crate::Error::RejectedAuthz {
                tenant_id: crate::types::TenantId::new(tenant_id),
                resource: format!(
                    "RLS policy '{}' on '{}': unresolved session variable",
                    policy.name, collection
                ),
            });
        }
    };

    let doc_mp = nodedb_types::json_to_msgpack_or_empty(document);
    if !filters.iter().all(|f| f.matches_binary(&doc_mp)) {
        info!(
            policy = %policy.name,
            username = %auth.username,
            %collection,
            "RLS write policy rejected (compiled)"
        );
        return Err(crate::Error::RejectedAuthz {
            tenant_id: crate::types::TenantId::new(tenant_id),
            resource: format!(
                "RLS policy '{}' on collection '{}'",
                policy.name, collection
            ),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::auth_context::AuthContext;
    use crate::control::security::predicate::{CompareOp, PredicateValue, RlsPredicate};
    use crate::control::security::rls::types::PolicyType;

    fn make_policy(name: &str, collection: &str, policy_type: PolicyType) -> RlsPolicy {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        RlsPolicy {
            name: name.into(),
            collection: collection.into(),
            tenant_id: 1,
            policy_type,
            compiled_predicate: None,
            mode: PolicyMode::default(),
            on_deny: Default::default(),
            enabled: true,
            created_by: "admin".into(),
            created_at: now,
        }
    }

    fn nonsuper_auth() -> AuthContext {
        use crate::control::security::identity::{AuthMethod, AuthenticatedIdentity, Role};
        use crate::types::TenantId;
        let identity = AuthenticatedIdentity {
            user_id: 42,
            username: "alice".into(),
            tenant_id: TenantId::new(1),
            auth_method: AuthMethod::ScramSha256,
            roles: vec![Role::ReadWrite],
            is_superuser: false,
        };
        AuthContext::from_identity(&identity, "s_test".into())
    }

    /// `$auth.id` reference: resolves to user_id string "42" which must
    /// equal doc["owner_id"] = "42" → policy passes.
    #[test]
    fn write_policy_enforcement_compiled() {
        let store = RlsPolicyStore::new();
        let predicate = RlsPredicate::Compare {
            field: "owner_id".into(),
            op: CompareOp::Eq,
            value: PredicateValue::AuthRef("id".into()),
        };
        let mut policy = make_policy("require_owner", "orders", PolicyType::Write);
        policy.compiled_predicate = Some(predicate);
        store.create_policy(policy).unwrap();

        let auth = nonsuper_auth();
        // alice's user_id is 42; owner_id matches → allowed.
        let doc_ok = serde_json::json!({"owner_id": "42", "amount": 100});
        assert!(
            store
                .check_write_with_auth(1, "orders", &doc_ok, &auth)
                .is_ok()
        );

        // Different owner → denied.
        let doc_bad = serde_json::json!({"owner_id": "99", "amount": 100});
        assert!(
            store
                .check_write_with_auth(1, "orders", &doc_bad, &auth)
                .is_err()
        );
    }

    #[test]
    fn no_policies_allows_everything() {
        let store = RlsPolicyStore::new();
        let auth = nonsuper_auth();
        let doc = serde_json::json!({"anything": "goes"});
        assert!(
            store
                .check_write_with_auth(1, "whatever", &doc, &auth)
                .is_ok()
        );
    }

    /// A policy with an unresolvable `$auth.*` reference must deny
    /// the write (fail-closed).
    #[test]
    fn check_write_denies_on_unresolvable_auth_ref() {
        let store = RlsPolicyStore::new();
        let predicate = RlsPredicate::Compare {
            field: "org_id".into(),
            op: CompareOp::Eq,
            value: PredicateValue::AuthRef("nonexistent_field".into()),
        };
        let mut policy = make_policy("require_org", "orders", PolicyType::Write);
        policy.compiled_predicate = Some(predicate);
        store.create_policy(policy).unwrap();

        let auth = nonsuper_auth();
        let doc = serde_json::json!({"org_id": "some_org"});
        let result = store.check_write_with_auth(1, "orders", &doc, &auth);
        assert!(
            result.is_err(),
            "RLS must fail-closed on unresolvable $auth ref; got {result:?}"
        );
    }

    /// `check_write_with_auth` must also fail-closed when an `$auth` reference
    /// cannot be resolved.
    #[test]
    fn check_write_with_auth_denies_on_unresolvable_ref() {
        let store = RlsPolicyStore::new();
        let predicate = RlsPredicate::Compare {
            field: "secret_scope".into(),
            op: CompareOp::Eq,
            value: PredicateValue::AuthRef("nonexistent_field".into()),
        };
        let mut policy = make_policy("require_scope", "orders", PolicyType::Write);
        policy.compiled_predicate = Some(predicate);
        store.create_policy(policy).unwrap();

        let doc = serde_json::json!({"secret_scope": "admin"});
        let auth = nonsuper_auth();
        let result = store.check_write_with_auth(1, "orders", &doc, &auth);
        assert!(
            result.is_err(),
            "write path must fail-closed on unresolvable auth ref; got {result:?}"
        );
    }

    /// Read-path: `combined_read_predicate_with_auth` with a policy whose
    /// compiled predicate requires a literal filter. A policy that has no
    /// `compiled_predicate` is vacuous (passes all rows). With a predicate
    /// present the combined result must be non-empty (filter applied).
    #[test]
    fn combined_read_with_auth_applies_compiled_predicate() {
        let store = RlsPolicyStore::new();
        let predicate = RlsPredicate::Compare {
            field: "user_id".into(),
            op: CompareOp::Eq,
            value: PredicateValue::AuthRef("id".into()),
        };
        let mut policy = make_policy("user_scope", "orders", PolicyType::Read);
        policy.compiled_predicate = Some(predicate);
        store.create_policy(policy).unwrap();

        let auth = nonsuper_auth();
        let result = store.combined_read_predicate_with_auth(1, "orders", &auth);
        // Must return Some with non-empty filter bytes (not unrestricted).
        assert!(
            result.is_some_and(|b| !b.is_empty()),
            "read path must produce filter bytes when compiled predicate is set"
        );
    }

    /// Read-path: a policy with an unresolvable `$auth.*` reference must
    /// deny the read (fail-closed: returns None).
    #[test]
    fn combined_read_with_auth_fails_closed_on_unresolvable_ref() {
        let store = RlsPolicyStore::new();
        let predicate = RlsPredicate::Compare {
            field: "org_id".into(),
            op: CompareOp::Eq,
            value: PredicateValue::AuthRef("nonexistent_field".into()),
        };
        let mut policy = make_policy("org_scope", "orders", PolicyType::Read);
        policy.compiled_predicate = Some(predicate);
        store.create_policy(policy).unwrap();

        let auth = nonsuper_auth();
        let result = store.combined_read_predicate_with_auth(1, "orders", &auth);
        assert!(
            result.is_none(),
            "read path must fail-closed on unresolvable auth ref; got {result:?}"
        );
    }
}
