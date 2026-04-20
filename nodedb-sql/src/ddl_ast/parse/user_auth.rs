//! Parse users/roles/permissions/grants + audit/tenants/constraints/typeguards.

use crate::ddl_ast::statement::NodedbStatement;

pub(super) fn try_parse(upper: &str, parts: &[&str], trimmed: &str) -> Option<NodedbStatement> {
    if upper.starts_with("CREATE USER ") {
        return Some(NodedbStatement::CreateUser {
            raw_sql: trimmed.to_string(),
        });
    }
    if upper.starts_with("DROP USER ") {
        let username = parts.get(2)?.to_string();
        return Some(NodedbStatement::DropUser { username });
    }
    if upper.starts_with("ALTER USER ") {
        return Some(NodedbStatement::AlterUser {
            raw_sql: trimmed.to_string(),
        });
    }
    if upper.starts_with("SHOW USERS") {
        return Some(NodedbStatement::ShowUsers);
    }
    if upper.starts_with("GRANT ROLE ") {
        return Some(NodedbStatement::GrantRole {
            raw_sql: trimmed.to_string(),
        });
    }
    if upper.starts_with("REVOKE ROLE ") {
        return Some(NodedbStatement::RevokeRole {
            raw_sql: trimmed.to_string(),
        });
    }
    if upper.starts_with("GRANT ") {
        return Some(NodedbStatement::GrantPermission {
            raw_sql: trimmed.to_string(),
        });
    }
    if upper.starts_with("REVOKE ") {
        return Some(NodedbStatement::RevokePermission {
            raw_sql: trimmed.to_string(),
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
