//! Top-level routers for `GRANT` and `REVOKE` SQL statements.
//! Decides between role-membership and permission-grant paths
//! based on whether the second token is `ROLE`.

use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::sqlstate_error;
use super::permission::{grant_permission, revoke_permission};
use super::role::{grant_role, revoke_role};

/// `GRANT ROLE <role> TO <user>` or
/// `GRANT <perm> ON <collection|FUNCTION name> TO <grantee>`.
pub fn handle_grant(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if parts.len() < 5 {
        return Err(sqlstate_error(
            "42601",
            "syntax: GRANT ROLE <role> TO <user> | GRANT <perm> ON <collection> TO <grantee>",
        ));
    }

    if parts[1].eq_ignore_ascii_case("ROLE") {
        if parts.len() < 5 {
            return Err(sqlstate_error(
                "42601",
                "syntax: GRANT ROLE <role> TO <user>",
            ));
        }
        return grant_role(state, identity, parts[2], parts[4]);
    }

    // GRANT <perm> ON [FUNCTION] <name> TO <grantee>
    let perm_str = parts[1];
    let (target_type, target_name) = if parts
        .get(3)
        .map(|s| s.eq_ignore_ascii_case("FUNCTION"))
        .unwrap_or(false)
    {
        ("FUNCTION", parts.get(4).copied().unwrap_or(""))
    } else {
        ("COLLECTION", parts.get(3).copied().unwrap_or(""))
    };
    let grantee = parts
        .iter()
        .position(|p| p.eq_ignore_ascii_case("TO"))
        .and_then(|i| parts.get(i + 1))
        .copied()
        .unwrap_or("");
    grant_permission(state, identity, perm_str, target_type, target_name, grantee)
}

/// `REVOKE ROLE <role> FROM <user>` or
/// `REVOKE <perm> ON <collection|FUNCTION name> FROM <grantee>`.
pub fn handle_revoke(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if parts.len() < 5 {
        return Err(sqlstate_error(
            "42601",
            "syntax: REVOKE ROLE <role> FROM <user> | REVOKE <perm> ON <collection> FROM <grantee>",
        ));
    }

    if parts[1].eq_ignore_ascii_case("ROLE") {
        if parts.len() < 5 {
            return Err(sqlstate_error(
                "42601",
                "syntax: REVOKE ROLE <role> FROM <user>",
            ));
        }
        return revoke_role(state, identity, parts[2], parts[4]);
    }

    // REVOKE <perm> ON [FUNCTION] <name> FROM <grantee>
    let perm_str = parts[1];
    let (target_type, target_name) = if parts
        .get(3)
        .map(|s| s.eq_ignore_ascii_case("FUNCTION"))
        .unwrap_or(false)
    {
        ("FUNCTION", parts.get(4).copied().unwrap_or(""))
    } else {
        ("COLLECTION", parts.get(3).copied().unwrap_or(""))
    };
    let grantee = parts
        .iter()
        .position(|p| p.eq_ignore_ascii_case("FROM"))
        .and_then(|i| parts.get(i + 1))
        .copied()
        .unwrap_or("");
    revoke_permission(state, identity, perm_str, target_type, target_name, grantee)
}
