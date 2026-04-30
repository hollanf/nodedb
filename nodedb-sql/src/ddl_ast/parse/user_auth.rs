//! Parse users/roles/permissions/grants + audit/tenants/constraints/typeguards.

use crate::ddl_ast::statement::NodedbStatement;
use crate::error::SqlError;

pub(super) fn try_parse(
    upper: &str,
    parts: &[&str],
    trimmed: &str,
) -> Option<Result<NodedbStatement, SqlError>> {
    try_parse_inner(upper, parts, trimmed).map(Ok)
}

fn try_parse_inner(upper: &str, parts: &[&str], trimmed: &str) -> Option<NodedbStatement> {
    if upper.starts_with("CREATE USER ") {
        return Some(parse_create_user(parts, trimmed));
    }
    if upper.starts_with("DROP USER ") {
        let username = parts.get(2)?.to_string();
        return Some(NodedbStatement::DropUser { username });
    }
    if upper.starts_with("ALTER USER ") {
        return Some(parse_alter_user(parts, trimmed));
    }
    if upper.starts_with("SHOW USERS") {
        return Some(NodedbStatement::ShowUsers);
    }
    if upper.starts_with("GRANT ROLE ") {
        // GRANT ROLE <role> TO <user>
        let role = parts.get(2).map(|s| s.to_string()).unwrap_or_default();
        let username = parts.get(4).map(|s| s.to_string()).unwrap_or_default();
        return Some(NodedbStatement::GrantRole { role, username });
    }
    if upper.starts_with("REVOKE ROLE ") {
        // REVOKE ROLE <role> FROM <user>
        let role = parts.get(2).map(|s| s.to_string()).unwrap_or_default();
        let username = parts.get(4).map(|s| s.to_string()).unwrap_or_default();
        return Some(NodedbStatement::RevokeRole { role, username });
    }
    if upper.starts_with("GRANT ") {
        // GRANT <perm> ON [FUNCTION] <name> TO <grantee>
        let permission = parts.get(1).map(|s| s.to_string()).unwrap_or_default();
        // ON is at index 2
        let (target_type, target_name) = if parts
            .get(3)
            .map(|s| s.eq_ignore_ascii_case("FUNCTION"))
            .unwrap_or(false)
        {
            (
                "FUNCTION".to_string(),
                parts.get(4).map(|s| s.to_lowercase()).unwrap_or_default(),
            )
        } else {
            (
                "COLLECTION".to_string(),
                parts.get(3).map(|s| s.to_string()).unwrap_or_default(),
            )
        };
        let grantee = parts
            .iter()
            .position(|p| p.eq_ignore_ascii_case("TO"))
            .and_then(|i| parts.get(i + 1))
            .map(|s| s.to_string())
            .unwrap_or_default();
        return Some(NodedbStatement::GrantPermission {
            permission,
            target_type,
            target_name,
            grantee,
        });
    }
    if upper.starts_with("REVOKE ")
        && !upper.starts_with("REVOKE API KEY")
        && !upper.starts_with("REVOKE SCOPE")
        && !upper.starts_with("REVOKE DELEGATION")
    {
        // REVOKE <perm> ON [FUNCTION] <name> FROM <grantee>
        let permission = parts.get(1).map(|s| s.to_string()).unwrap_or_default();
        let (target_type, target_name) = if parts
            .get(3)
            .map(|s| s.eq_ignore_ascii_case("FUNCTION"))
            .unwrap_or(false)
        {
            (
                "FUNCTION".to_string(),
                parts.get(4).map(|s| s.to_lowercase()).unwrap_or_default(),
            )
        } else {
            (
                "COLLECTION".to_string(),
                parts.get(3).map(|s| s.to_string()).unwrap_or_default(),
            )
        };
        let grantee = parts
            .iter()
            .position(|p| p.eq_ignore_ascii_case("FROM"))
            .and_then(|i| parts.get(i + 1))
            .map(|s| s.to_string())
            .unwrap_or_default();
        return Some(NodedbStatement::RevokePermission {
            permission,
            target_type,
            target_name,
            grantee,
        });
    }
    if upper.starts_with("SHOW PERMISSIONS") {
        let collection = parts.get(2).map(|s| s.to_string());
        return Some(NodedbStatement::ShowPermissions { collection });
    }
    if upper.starts_with("SHOW GRANTS") {
        let username = parts.get(2).map(|s| s.to_string());
        return Some(NodedbStatement::ShowGrants { username });
    }
    if upper.starts_with("SHOW TENANTS") {
        return Some(NodedbStatement::ShowTenants);
    }
    if upper.starts_with("SHOW AUDIT") {
        return Some(NodedbStatement::ShowAuditLog);
    }
    if upper.starts_with("SHOW CONSTRAINTS ") {
        let collection = parts.get(2)?.to_string();
        return Some(NodedbStatement::ShowConstraints { collection });
    }
    if upper.starts_with("SHOW TYPEGUARD") {
        let collection = parts.get(2)?.to_string();
        return Some(NodedbStatement::ShowTypeGuards { collection });
    }
    None
}

/// Parse `CREATE USER <name> WITH PASSWORD '<password>' [ROLE <role>] [TENANT <id>]`.
///
/// Extracts fields as primitive types; the handler converts role strings to
/// the `Role` enum and tenant IDs to `TenantId`.
fn parse_create_user(parts: &[&str], _trimmed: &str) -> NodedbStatement {
    // parts[0] = CREATE, parts[1] = USER, parts[2] = <name>
    let username = parts.get(2).map(|s| s.to_string()).unwrap_or_default();

    // Find PASSWORD token and extract the quoted string that follows.
    let password = parts
        .iter()
        .position(|p| p.eq_ignore_ascii_case("PASSWORD"))
        .and_then(|pi| extract_quoted_string_from_parts(parts, pi + 1))
        .unwrap_or_default();

    // ROLE <role> — find after PASSWORD section.
    let role = parts
        .iter()
        .position(|p| p.eq_ignore_ascii_case("ROLE"))
        .and_then(|ri| {
            // Make sure this ROLE keyword isn't before WITH/PASSWORD
            // (i.e., it appears after the password argument).
            let pw_pos = parts
                .iter()
                .position(|p| p.eq_ignore_ascii_case("PASSWORD"))
                .unwrap_or(0);
            if ri > pw_pos {
                parts.get(ri + 1).map(|s| s.to_lowercase())
            } else {
                None
            }
        });

    // TENANT <id>
    let tenant_id = parts
        .iter()
        .position(|p| p.eq_ignore_ascii_case("TENANT"))
        .and_then(|ti| parts.get(ti + 1))
        .and_then(|s| s.parse::<u32>().ok());

    NodedbStatement::CreateUser {
        username,
        password,
        role,
        tenant_id,
    }
}

/// Parse `ALTER USER <name> SET PASSWORD '<password>' | SET ROLE <role>`.
fn parse_alter_user(parts: &[&str], _trimmed: &str) -> NodedbStatement {
    // parts[0] = ALTER, parts[1] = USER, parts[2] = <name>, parts[3] = SET, parts[4] = action
    let username = parts.get(2).map(|s| s.to_string()).unwrap_or_default();
    let action = parts.get(4).map(|s| s.to_uppercase()).unwrap_or_default();
    let value = match action.as_str() {
        "PASSWORD" => extract_quoted_string_from_parts(parts, 5).unwrap_or_default(),
        "ROLE" => parts.get(5).map(|s| s.to_string()).unwrap_or_default(),
        _ => parts.get(5).map(|s| s.to_string()).unwrap_or_default(),
    };
    NodedbStatement::AlterUser {
        username,
        action,
        value,
    }
}

/// Extract a single-quoted string from parts starting at `start`.
/// Handles multi-token quoted strings like `'hello world'`.
fn extract_quoted_string_from_parts(parts: &[&str], start: usize) -> Option<String> {
    if start >= parts.len() {
        return None;
    }
    let first = parts[start];
    if !first.starts_with('\'') {
        return None;
    }
    if first.ends_with('\'') && first.len() > 1 {
        return Some(first[1..first.len() - 1].to_string());
    }
    // Multi-token: accumulate until closing quote.
    let mut result = first[1..].to_string();
    for &part in &parts[start + 1..] {
        result.push(' ');
        if let Some(stripped) = part.strip_suffix('\'') {
            result.push_str(stripped);
            return Some(result);
        }
        result.push_str(part);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(sql: &str) -> Option<NodedbStatement> {
        let upper = sql.to_uppercase();
        let parts: Vec<&str> = sql.split_whitespace().collect();
        try_parse(&upper, &parts, sql).map(|r| r.unwrap())
    }

    #[test]
    fn create_user_basic() {
        let stmt = parse("CREATE USER alice WITH PASSWORD 'secret' ROLE read_write").unwrap();
        if let NodedbStatement::CreateUser {
            username,
            password,
            role,
            tenant_id,
        } = stmt
        {
            assert_eq!(username, "alice");
            assert_eq!(password, "secret");
            assert_eq!(role.as_deref(), Some("read_write"));
            assert!(tenant_id.is_none());
        } else {
            panic!("expected CreateUser");
        }
    }

    #[test]
    fn create_user_no_role() {
        let stmt = parse("CREATE USER bob WITH PASSWORD 'pw123'").unwrap();
        if let NodedbStatement::CreateUser { username, role, .. } = stmt {
            assert_eq!(username, "bob");
            assert!(role.is_none());
        } else {
            panic!("expected CreateUser");
        }
    }

    #[test]
    fn create_user_with_tenant() {
        let stmt = parse("CREATE USER carol WITH PASSWORD 'pw' TENANT 42").unwrap();
        if let NodedbStatement::CreateUser { tenant_id, .. } = stmt {
            assert_eq!(tenant_id, Some(42));
        } else {
            panic!("expected CreateUser");
        }
    }

    #[test]
    fn alter_user_password() {
        let stmt = parse("ALTER USER alice SET PASSWORD 'newpass'").unwrap();
        if let NodedbStatement::AlterUser {
            username,
            action,
            value,
        } = stmt
        {
            assert_eq!(username, "alice");
            assert_eq!(action, "PASSWORD");
            assert_eq!(value, "newpass");
        } else {
            panic!("expected AlterUser");
        }
    }

    #[test]
    fn alter_user_role() {
        let stmt = parse("ALTER USER alice SET ROLE admin").unwrap();
        if let NodedbStatement::AlterUser {
            username,
            action,
            value,
        } = stmt
        {
            assert_eq!(username, "alice");
            assert_eq!(action, "ROLE");
            assert_eq!(value, "admin");
        } else {
            panic!("expected AlterUser");
        }
    }
}
