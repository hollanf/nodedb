//! RLS predicate evaluation and `$auth.*` substitution.
//!
//! **Read path**: `combined_read_predicate_with_auth` merges all
//! enabled read policies into a single `ScanFilter` list injected
//! into `DocumentScan`.
//!
//! **Write path**: `check_write` / `check_write_with_auth` evaluate
//! each write policy against the document; failures return
//! `Error::RejectedAuthz`.

use tracing::{info, warn};

use crate::control::security::auth_context::AuthContext;
use crate::control::security::predicate::{PolicyMode, RlsPredicate};
use crate::control::security::predicate_eval::{combine_policies, substitute_to_scan_filters};

use super::store::RlsPolicyStore;
use super::types::RlsPolicy;

impl RlsPolicyStore {
    /// Check if a document passes all legacy write policies.
    pub fn check_write(
        &self,
        tenant_id: u32,
        collection: &str,
        document: &serde_json::Value,
        username: &str,
    ) -> crate::Result<()> {
        let policies = self.write_policies(tenant_id, collection);
        if policies.is_empty() {
            return Ok(());
        }

        for policy in &policies {
            if !policy.predicate.is_empty() {
                let filters: Vec<crate::bridge::scan_filter::ScanFilter> =
                    match zerompk::from_msgpack(&policy.predicate) {
                        Ok(f) => f,
                        Err(e) => {
                            warn!(
                                policy = %policy.name,
                                error = %e,
                                "RLS write policy predicate deserialization failed \
                                 — denying write (fail-closed)"
                            );
                            return Err(crate::Error::RejectedAuthz {
                                tenant_id: crate::types::TenantId::new(tenant_id),
                                resource: format!(
                                    "RLS policy '{}' on collection '{}': \
                                     predicate deserialization failed",
                                    policy.name, collection
                                ),
                            });
                        }
                    };

                let doc_mp = nodedb_types::json_to_msgpack_or_empty(document);
                let passes = filters.iter().all(|f| f.matches_binary(&doc_mp));
                if !passes {
                    info!(
                        policy = %policy.name,
                        %username,
                        %collection,
                        "RLS write policy rejected"
                    );
                    return Err(crate::Error::RejectedAuthz {
                        tenant_id: crate::types::TenantId::new(tenant_id),
                        resource: format!(
                            "RLS policy '{}' on collection '{}'",
                            policy.name, collection
                        ),
                    });
                }
            }
        }

        Ok(())
    }

    /// Primary read-path RLS method: substitutes `$auth.*` variables
    /// and returns the combined `ScanFilter` bytes.
    ///
    /// Returns `None` when a required `$auth` field is missing
    /// (fail-closed semantics).
    pub fn combined_read_predicate_with_auth(
        &self,
        tenant_id: u32,
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

        let mut compiled_policies: Vec<(RlsPredicate, PolicyMode)> = Vec::new();
        let mut legacy_filters: Vec<crate::bridge::scan_filter::ScanFilter> = Vec::new();

        for policy in &policies {
            if let Some(ref compiled) = policy.compiled_predicate {
                compiled_policies.push((compiled.clone(), policy.mode));
            } else if !policy.predicate.is_empty() {
                match zerompk::from_msgpack::<Vec<crate::bridge::scan_filter::ScanFilter>>(
                    &policy.predicate,
                ) {
                    Ok(filters) => legacy_filters.extend(filters),
                    Err(e) => {
                        warn!(
                            policy = %policy.name,
                            error = %e,
                            "RLS read policy predicate deserialization failed \
                             — denying read (fail-closed)"
                        );
                        return None;
                    }
                }
            }
        }

        let mut all_filters = if !compiled_policies.is_empty() {
            combine_policies(&compiled_policies, auth)?
        } else {
            Vec::new()
        };

        all_filters.extend(legacy_filters);

        if all_filters.is_empty() {
            Some(Vec::new())
        } else {
            Some(zerompk::to_msgpack_vec(&all_filters).unwrap_or_default())
        }
    }

    /// Write-path RLS check with `$auth.*` support. Evaluates both
    /// compiled and legacy write policies; fail-closed on unresolved
    /// auth references.
    pub fn check_write_with_auth(
        &self,
        tenant_id: u32,
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
            } else if !policy.predicate.is_empty() {
                check_legacy_write(policy, document, auth, tenant_id, collection)?;
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
    tenant_id: u32,
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

fn check_legacy_write(
    policy: &RlsPolicy,
    document: &serde_json::Value,
    auth: &AuthContext,
    tenant_id: u32,
    collection: &str,
) -> crate::Result<()> {
    let filters: Vec<crate::bridge::scan_filter::ScanFilter> =
        match zerompk::from_msgpack(&policy.predicate) {
            Ok(f) => f,
            Err(e) => {
                warn!(
                    policy = %policy.name,
                    error = %e,
                    "RLS write policy predicate deserialization failed \
                     — denying write (fail-closed)"
                );
                return Err(crate::Error::RejectedAuthz {
                    tenant_id: crate::types::TenantId::new(tenant_id),
                    resource: format!(
                        "RLS policy '{}' on collection '{}': \
                         predicate deserialization failed",
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
            "RLS write policy rejected (legacy)"
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
            predicate: Vec::new(),
            compiled_predicate: None,
            mode: PolicyMode::default(),
            on_deny: Default::default(),
            enabled: true,
            created_by: "admin".into(),
            created_at: now,
        }
    }

    #[test]
    fn write_policy_enforcement() {
        let store = RlsPolicyStore::new();

        let filter = crate::bridge::scan_filter::ScanFilter {
            field: "status".into(),
            op: "eq".into(),
            value: nodedb_types::Value::String("active".into()),
            clauses: Vec::new(),
            expr: None,
        };
        let predicate = zerompk::to_msgpack_vec(&vec![filter]).unwrap();
        let mut policy = make_policy("require_active", "orders", PolicyType::Write);
        policy.predicate = predicate;
        store.create_policy(policy).unwrap();

        let doc_ok = serde_json::json!({"status": "active", "amount": 100});
        assert!(store.check_write(1, "orders", &doc_ok, "alice").is_ok());

        let doc_bad = serde_json::json!({"status": "draft", "amount": 100});
        assert!(store.check_write(1, "orders", &doc_bad, "alice").is_err());
    }

    #[test]
    fn no_policies_allows_everything() {
        let store = RlsPolicyStore::new();
        let doc = serde_json::json!({"anything": "goes"});
        assert!(store.check_write(1, "whatever", &doc, "anyone").is_ok());
    }

    /// A policy whose stored predicate bytes cannot be deserialized must
    /// deny the write (fail-closed). Legacy-branch of `check_write`.
    #[test]
    fn check_write_denies_on_undeserializable_predicate() {
        let store = RlsPolicyStore::new();
        let mut policy = make_policy("corrupt", "orders", PolicyType::Write);
        // Non-empty, syntactically invalid msgpack bytes.
        policy.predicate = vec![0xFF, 0xFE, 0xFD, 0xFC];
        store.create_policy(policy).unwrap();

        let doc = serde_json::json!({"status": "active"});
        let result = store.check_write(1, "orders", &doc, "alice");
        assert!(
            result.is_err(),
            "RLS must fail-closed on predicate deser failure; got {result:?}"
        );
    }

    /// Legacy branch of `check_write_with_auth` (`check_legacy_write`) must
    /// also deny when the stored predicate cannot be deserialized.
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

    #[test]
    fn check_write_with_auth_denies_on_undeserializable_legacy_predicate() {
        let store = RlsPolicyStore::new();
        let mut policy = make_policy("corrupt_legacy", "orders", PolicyType::Write);
        policy.predicate = vec![0xFF, 0xFE, 0xFD, 0xFC];
        // No compiled_predicate → goes through legacy branch.
        store.create_policy(policy).unwrap();

        let doc = serde_json::json!({"status": "active"});
        let auth = nonsuper_auth();
        let result = store.check_write_with_auth(1, "orders", &doc, &auth);
        assert!(
            result.is_err(),
            "legacy write path must fail-closed on deser failure; got {result:?}"
        );
    }

    /// Read-path: `combined_read_predicate_with_auth` legacy branch must
    /// not silently drop a policy whose predicate bytes fail to
    /// deserialize. Dropping it would give an unrestricted read.
    #[test]
    fn combined_read_with_auth_fails_closed_on_undeserializable_predicate() {
        let store = RlsPolicyStore::new();
        let mut policy = make_policy("corrupt_read", "orders", PolicyType::Read);
        policy.predicate = vec![0xFF, 0xFE, 0xFD, 0xFC];
        store.create_policy(policy).unwrap();

        let auth = nonsuper_auth();
        let result = store.combined_read_predicate_with_auth(1, "orders", &auth);
        // Fail-closed: either None (denial) or a predicate that evaluates
        // to no rows. Silently returning `Some(empty)` removes the policy.
        assert!(
            result.is_none() || result.as_ref().is_some_and(|b| !b.is_empty()),
            "read path must fail-closed on deser failure; got unrestricted Some(empty)"
        );
    }
}
