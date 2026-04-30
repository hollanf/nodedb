//! Parse users/roles/permissions/grants + audit/tenants/constraints/typeguards.

use crate::ddl_ast::statement::{AlterRoleOp, AlterUserOp, NodedbStatement};
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
    if upper.starts_with("ALTER ROLE ") {
        return Some(parse_alter_role(parts, trimmed));
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
        // SHOW PERMISSIONS [ON <collection>] [FOR <grantee>]
        let on_collection = parts
            .iter()
            .position(|p| p.eq_ignore_ascii_case("ON"))
            .and_then(|i| parts.get(i + 1))
            .map(|s| s.to_string());
        let for_grantee = parts
            .iter()
            .position(|p| p.eq_ignore_ascii_case("FOR"))
            .and_then(|i| parts.get(i + 1))
            .map(|s| s.to_string());
        return Some(NodedbStatement::ShowPermissions {
            on_collection,
            for_grantee,
        });
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
        .and_then(|s| s.parse::<u64>().ok());

    NodedbStatement::CreateUser {
        username,
        password,
        role,
        tenant_id,
    }
}

/// Parse all `ALTER USER <name> ...` forms.
///
/// Supported forms:
/// - `ALTER USER <name> SET PASSWORD '<password>'`
/// - `ALTER USER <name> SET ROLE <role>`
/// - `ALTER USER <name> MUST CHANGE PASSWORD`
/// - `ALTER USER <name> PASSWORD NEVER EXPIRES`
/// - `ALTER USER <name> PASSWORD EXPIRES '<iso8601>'`
/// - `ALTER USER <name> PASSWORD EXPIRES IN <N> DAYS`
fn parse_alter_user(parts: &[&str], _trimmed: &str) -> NodedbStatement {
    // parts[0] = ALTER, parts[1] = USER, parts[2] = <name>, parts[3] = sub-cmd
    let username = parts.get(2).map(|s| s.to_string()).unwrap_or_default();
    let sub_owned = parts.get(3).map(|s| s.to_uppercase()).unwrap_or_default();
    let sub = sub_owned.as_str();

    let op = match sub {
        "SET" => {
            // parts[4] = action
            let action = parts.get(4).map(|s| s.to_uppercase()).unwrap_or_default();
            match action.as_str() {
                "PASSWORD" => {
                    let password = extract_quoted_string_from_parts(parts, 5).unwrap_or_default();
                    AlterUserOp::SetPassword { password }
                }
                "ROLE" => {
                    let role = parts.get(5).map(|s| s.to_string()).unwrap_or_default();
                    AlterUserOp::SetRole { role }
                }
                _ => AlterUserOp::SetRole {
                    role: String::new(),
                },
            }
        }
        "MUST" => {
            // ALTER USER <name> MUST CHANGE PASSWORD
            AlterUserOp::MustChangePassword
        }
        "PASSWORD" => {
            // parts[4] = NEVER | EXPIRES
            let next = parts.get(4).map(|s| s.to_uppercase()).unwrap_or_default();
            match next.as_str() {
                "NEVER" => AlterUserOp::PasswordNeverExpires,
                "EXPIRES" => {
                    // parts[5] = '<iso8601>' or IN
                    let part5 = parts.get(5).map(|s| s.to_uppercase()).unwrap_or_default();
                    if part5 == "IN" {
                        // PASSWORD EXPIRES IN <N> DAYS
                        let days: u32 = parts.get(6).and_then(|s| s.parse().ok()).unwrap_or(0);
                        AlterUserOp::PasswordExpiresInDays { days }
                    } else {
                        // PASSWORD EXPIRES '<iso8601>'
                        let iso8601 =
                            extract_quoted_string_from_parts(parts, 5).unwrap_or_default();
                        AlterUserOp::PasswordExpiresAt { iso8601 }
                    }
                }
                _ => AlterUserOp::PasswordNeverExpires,
            }
        }
        _ => {
            // Unknown sub-command — fall back to a no-op SetRole to avoid panic.
            AlterUserOp::SetRole {
                role: String::new(),
            }
        }
    };

    NodedbStatement::AlterUser { username, op }
}

/// Parse `ALTER ROLE <name> GRANT/REVOKE/SET ...`.
///
/// Supported forms:
/// - `ALTER ROLE <name> GRANT <perm> ON [FUNCTION] <target>`
/// - `ALTER ROLE <name> REVOKE <perm> ON [FUNCTION] <target>`
/// - `ALTER ROLE <name> SET INHERIT <parent>`
fn parse_alter_role(parts: &[&str], _trimmed: &str) -> NodedbStatement {
    // parts[0] = ALTER, parts[1] = ROLE, parts[2] = <name>, parts[3] = sub-command
    let name = parts.get(2).map(|s| s.to_string()).unwrap_or_default();
    let sub_cmd = parts.get(3).map(|s| s.to_uppercase()).unwrap_or_default();

    let sub_op = match sub_cmd.as_str() {
        "GRANT" => {
            // ALTER ROLE <name> GRANT <perm> ON [FUNCTION] <target>
            let permission = parts.get(4).map(|s| s.to_string()).unwrap_or_default();
            // ON is at index 5
            let (target_type, target_name) = if parts
                .get(6)
                .map(|s| s.eq_ignore_ascii_case("FUNCTION"))
                .unwrap_or(false)
            {
                (
                    "FUNCTION".to_string(),
                    parts.get(7).map(|s| s.to_lowercase()).unwrap_or_default(),
                )
            } else {
                (
                    "COLLECTION".to_string(),
                    parts.get(6).map(|s| s.to_string()).unwrap_or_default(),
                )
            };
            AlterRoleOp::Grant {
                permission,
                target_type,
                target_name,
            }
        }
        "REVOKE" => {
            // ALTER ROLE <name> REVOKE <perm> ON [FUNCTION] <target>
            let permission = parts.get(4).map(|s| s.to_string()).unwrap_or_default();
            let (target_type, target_name) = if parts
                .get(6)
                .map(|s| s.eq_ignore_ascii_case("FUNCTION"))
                .unwrap_or(false)
            {
                (
                    "FUNCTION".to_string(),
                    parts.get(7).map(|s| s.to_lowercase()).unwrap_or_default(),
                )
            } else {
                (
                    "COLLECTION".to_string(),
                    parts.get(6).map(|s| s.to_string()).unwrap_or_default(),
                )
            };
            AlterRoleOp::Revoke {
                permission,
                target_type,
                target_name,
            }
        }
        _ => {
            // SET INHERIT <parent> (default / fallback)
            // parts[3] = SET, parts[4] = INHERIT, parts[5] = <parent>
            let parent = parts.get(5).map(|s| s.to_string()).unwrap_or_default();
            AlterRoleOp::SetInherit { parent }
        }
    };

    NodedbStatement::AlterRole { name, sub_op }
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
    fn alter_user_set_password() {
        let stmt = parse("ALTER USER alice SET PASSWORD 'newpass'").unwrap();
        assert_eq!(
            stmt,
            NodedbStatement::AlterUser {
                username: "alice".to_string(),
                op: AlterUserOp::SetPassword {
                    password: "newpass".to_string()
                },
            }
        );
    }

    #[test]
    fn alter_user_set_role() {
        let stmt = parse("ALTER USER alice SET ROLE admin").unwrap();
        assert_eq!(
            stmt,
            NodedbStatement::AlterUser {
                username: "alice".to_string(),
                op: AlterUserOp::SetRole {
                    role: "admin".to_string()
                },
            }
        );
    }

    #[test]
    fn alter_user_must_change_password() {
        let stmt = parse("ALTER USER alice MUST CHANGE PASSWORD").unwrap();
        assert_eq!(
            stmt,
            NodedbStatement::AlterUser {
                username: "alice".to_string(),
                op: AlterUserOp::MustChangePassword,
            }
        );
    }

    #[test]
    fn alter_user_password_never_expires() {
        let stmt = parse("ALTER USER alice PASSWORD NEVER EXPIRES").unwrap();
        assert_eq!(
            stmt,
            NodedbStatement::AlterUser {
                username: "alice".to_string(),
                op: AlterUserOp::PasswordNeverExpires,
            }
        );
    }

    #[test]
    fn alter_user_password_expires_at() {
        let stmt = parse("ALTER USER alice PASSWORD EXPIRES '2026-12-31T00:00:00Z'").unwrap();
        assert_eq!(
            stmt,
            NodedbStatement::AlterUser {
                username: "alice".to_string(),
                op: AlterUserOp::PasswordExpiresAt {
                    iso8601: "2026-12-31T00:00:00Z".to_string()
                },
            }
        );
    }

    #[test]
    fn alter_user_password_expires_in_days() {
        let stmt = parse("ALTER USER alice PASSWORD EXPIRES IN 90 DAYS").unwrap();
        assert_eq!(
            stmt,
            NodedbStatement::AlterUser {
                username: "alice".to_string(),
                op: AlterUserOp::PasswordExpiresInDays { days: 90 },
            }
        );
    }

    // ── ALTER ROLE tests ─────────────────────────────────────────────

    #[test]
    fn alter_role_set_inherit() {
        let stmt = parse("ALTER ROLE analyst SET INHERIT readonly").unwrap();
        if let NodedbStatement::AlterRole { name, sub_op } = stmt {
            assert_eq!(name, "analyst");
            assert_eq!(
                sub_op,
                AlterRoleOp::SetInherit {
                    parent: "readonly".to_string()
                }
            );
        } else {
            panic!("expected AlterRole");
        }
    }

    #[test]
    fn alter_role_grant_collection() {
        let stmt = parse("ALTER ROLE analyst GRANT READ ON my_collection").unwrap();
        if let NodedbStatement::AlterRole { name, sub_op } = stmt {
            assert_eq!(name, "analyst");
            assert_eq!(
                sub_op,
                AlterRoleOp::Grant {
                    permission: "READ".to_string(),
                    target_type: "COLLECTION".to_string(),
                    target_name: "my_collection".to_string(),
                }
            );
        } else {
            panic!("expected AlterRole");
        }
    }

    #[test]
    fn alter_role_grant_function() {
        let stmt = parse("ALTER ROLE analyst GRANT EXECUTE ON FUNCTION my_func").unwrap();
        if let NodedbStatement::AlterRole { name, sub_op } = stmt {
            assert_eq!(name, "analyst");
            assert_eq!(
                sub_op,
                AlterRoleOp::Grant {
                    permission: "EXECUTE".to_string(),
                    target_type: "FUNCTION".to_string(),
                    target_name: "my_func".to_string(),
                }
            );
        } else {
            panic!("expected AlterRole");
        }
    }

    #[test]
    fn alter_role_revoke_collection() {
        let stmt = parse("ALTER ROLE analyst REVOKE WRITE ON orders").unwrap();
        if let NodedbStatement::AlterRole { name, sub_op } = stmt {
            assert_eq!(name, "analyst");
            assert_eq!(
                sub_op,
                AlterRoleOp::Revoke {
                    permission: "WRITE".to_string(),
                    target_type: "COLLECTION".to_string(),
                    target_name: "orders".to_string(),
                }
            );
        } else {
            panic!("expected AlterRole");
        }
    }

    #[test]
    fn alter_role_revoke_function() {
        let stmt = parse("ALTER ROLE analyst REVOKE EXECUTE ON FUNCTION calc").unwrap();
        if let NodedbStatement::AlterRole { name, sub_op } = stmt {
            assert_eq!(name, "analyst");
            assert_eq!(
                sub_op,
                AlterRoleOp::Revoke {
                    permission: "EXECUTE".to_string(),
                    target_type: "FUNCTION".to_string(),
                    target_name: "calc".to_string(),
                }
            );
        } else {
            panic!("expected AlterRole");
        }
    }

    // ── SHOW PERMISSIONS tests ────────────────────────────────────────

    #[test]
    fn show_permissions_no_filter() {
        let stmt = parse("SHOW PERMISSIONS").unwrap();
        assert_eq!(
            stmt,
            NodedbStatement::ShowPermissions {
                on_collection: None,
                for_grantee: None,
            }
        );
    }

    #[test]
    fn show_permissions_on_collection() {
        let stmt = parse("SHOW PERMISSIONS ON orders").unwrap();
        assert_eq!(
            stmt,
            NodedbStatement::ShowPermissions {
                on_collection: Some("orders".to_string()),
                for_grantee: None,
            }
        );
    }

    #[test]
    fn show_permissions_for_user() {
        let stmt = parse("SHOW PERMISSIONS FOR alice").unwrap();
        assert_eq!(
            stmt,
            NodedbStatement::ShowPermissions {
                on_collection: None,
                for_grantee: Some("alice".to_string()),
            }
        );
    }

    #[test]
    fn show_permissions_on_and_for() {
        let stmt = parse("SHOW PERMISSIONS ON orders FOR alice").unwrap();
        assert_eq!(
            stmt,
            NodedbStatement::ShowPermissions {
                on_collection: Some("orders".to_string()),
                for_grantee: Some("alice".to_string()),
            }
        );
    }
}
