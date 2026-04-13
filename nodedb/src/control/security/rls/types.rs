//! `RlsPolicy` / `PolicyType` data shapes and the internal
//! `policy_key` helper used by the store.

use serde::{Deserialize, Serialize};

use crate::control::security::deny::DenyMode;
use crate::control::security::predicate::{PolicyMode, RlsPredicate};

/// A single RLS policy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RlsPolicy {
    /// Policy name (unique per collection).
    pub name: String,
    /// Collection this policy applies to.
    pub collection: String,
    /// Tenant scope.
    pub tenant_id: u32,
    /// Policy type: read, write, or both.
    pub policy_type: PolicyType,
    /// Legacy predicate as serialized `ScanFilter` (static, no `$auth`).
    /// Retained for backward compatibility with existing policies.
    pub predicate: Vec<u8>,
    /// Compiled predicate AST with `$auth.*` support.
    ///
    /// If present, this takes precedence over `predicate`. Substituted at
    /// plan time via `AuthContext` to produce concrete `ScanFilter` values.
    #[serde(default)]
    pub compiled_predicate: Option<RlsPredicate>,
    /// Policy combination mode: permissive (OR) or restrictive (AND).
    #[serde(default)]
    pub mode: PolicyMode,
    /// What happens when this policy denies access.
    #[serde(default)]
    pub on_deny: DenyMode,
    /// Whether this policy is enabled.
    pub enabled: bool,
    /// Creator username (for audit).
    pub created_by: String,
    /// Creation timestamp (epoch seconds).
    pub created_at: u64,
}

/// Policy type: when the policy is evaluated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PolicyType {
    /// Applied on SELECT/read — filters rows the user can see.
    Read,
    /// Applied on INSERT/UPDATE/DELETE — blocks disallowed writes.
    Write,
    /// Applied on both read and write paths.
    All,
}

/// Build the lookup key for the policy map: `"{tenant_id}:{collection}"`.
pub(super) fn policy_key(tenant_id: u32, collection: &str) -> String {
    format!("{tenant_id}:{collection}")
}
