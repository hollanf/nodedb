//! Namespace-scoped authorization helper.
//!
//! Extends collection-level grants with namespace scoping:
//! `GRANT READ ON namespace.collection TO role`.
//! Namespaces are dot-separated prefixes.

use crate::control::security::identity::{AuthenticatedIdentity, Permission};
use crate::control::security::permission::PermissionStore;
use crate::control::security::role::RoleStore;

/// Check tenant + namespace authorization for a collection.
/// Order: direct collection grant → namespace prefix grants → wildcard.
pub fn check_namespace_authz(
    identity: &AuthenticatedIdentity,
    collection: &str,
    required_permission: Permission,
    permission_store: &PermissionStore,
    role_store: &RoleStore,
) -> bool {
    if identity.is_superuser {
        return true;
    }

    if permission_store.check(identity, required_permission, collection, role_store) {
        return true;
    }

    let parts: Vec<&str> = collection.split('.').collect();
    for i in (0..parts.len()).rev() {
        let namespace = parts[..i].join(".");
        if !namespace.is_empty()
            && permission_store.check(identity, required_permission, &namespace, role_store)
        {
            return true;
        }
    }

    permission_store.check(identity, required_permission, "*", role_store)
}
