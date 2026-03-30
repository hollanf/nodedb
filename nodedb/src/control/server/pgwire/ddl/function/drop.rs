//! `DROP FUNCTION [IF EXISTS]` DDL handler.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::{require_admin, sqlstate_error};
use super::parse::validate_identifier;

/// Handle `DROP FUNCTION [IF EXISTS] <name>`
///
/// Requires superuser or tenant_admin — same privilege level as CREATE FUNCTION.
pub fn drop_function(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    require_admin(identity, "drop functions")?;

    let (name, if_exists) = parse_drop_function(parts)?;
    let tenant_id = identity.tenant_id.as_u32();

    let catalog = state
        .credentials
        .catalog()
        .as_ref()
        .ok_or_else(|| sqlstate_error("XX000", "system catalog not available"))?;

    let existed = catalog
        .delete_function(tenant_id, &name)
        .map_err(|e| sqlstate_error("XX000", &format!("catalog write: {e}")))?;

    if !existed && !if_exists {
        return Err(sqlstate_error(
            "42883",
            &format!("function '{name}' does not exist"),
        ));
    }

    if existed {
        state.audit_record(
            crate::control::security::audit::AuditEvent::AdminAction,
            Some(identity.tenant_id),
            &identity.username,
            &format!("DROP FUNCTION {name}"),
        );
    }

    Ok(vec![Response::Execution(Tag::new("DROP FUNCTION"))])
}

/// Parse `DROP FUNCTION [IF EXISTS] <name>`.
fn parse_drop_function(parts: &[&str]) -> PgWireResult<(String, bool)> {
    // parts[0] = "DROP", parts[1] = "FUNCTION", ...
    if parts.len() < 3 {
        return Err(sqlstate_error(
            "42601",
            "syntax: DROP FUNCTION [IF EXISTS] <name>",
        ));
    }

    let mut idx = 2;
    let if_exists = if parts.len() > 4
        && parts[2].eq_ignore_ascii_case("IF")
        && parts[3].eq_ignore_ascii_case("EXISTS")
    {
        idx = 4;
        true
    } else {
        false
    };

    if idx >= parts.len() {
        return Err(sqlstate_error("42601", "function name required"));
    }

    let name = parts[idx].to_lowercase().trim_end_matches(';').to_string();
    validate_identifier(&name)?;
    Ok((name, if_exists))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_drop_basic() {
        let parts: Vec<&str> = "DROP FUNCTION normalize_email".split_whitespace().collect();
        let (name, if_exists) = parse_drop_function(&parts).unwrap();
        assert_eq!(name, "normalize_email");
        assert!(!if_exists);
    }

    #[test]
    fn parse_drop_if_exists() {
        let parts: Vec<&str> = "DROP FUNCTION IF EXISTS myf".split_whitespace().collect();
        let (name, if_exists) = parse_drop_function(&parts).unwrap();
        assert_eq!(name, "myf");
        assert!(if_exists);
    }

    #[test]
    fn parse_drop_with_semicolon() {
        let parts: Vec<&str> = "DROP FUNCTION myf;".split_whitespace().collect();
        let (name, _) = parse_drop_function(&parts).unwrap();
        assert_eq!(name, "myf");
    }

    #[test]
    fn parse_drop_too_short() {
        let parts: Vec<&str> = "DROP FUNCTION".split_whitespace().collect();
        assert!(parse_drop_function(&parts).is_err());
    }
}
