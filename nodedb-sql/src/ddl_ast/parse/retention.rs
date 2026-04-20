//! Parse CREATE/DROP/ALTER/SHOW RETENTION POLICY.

use super::helpers::extract_name_after_if_exists;
use crate::ddl_ast::statement::NodedbStatement;

pub(super) fn try_parse(upper: &str, parts: &[&str], trimmed: &str) -> Option<NodedbStatement> {
    if upper.starts_with("CREATE RETENTION POLICY ") {
        return Some(NodedbStatement::CreateRetentionPolicy {
            raw_sql: trimmed.to_string(),
        });
    }
    if upper.starts_with("DROP RETENTION POLICY ") {
        let if_exists = upper.contains("IF EXISTS");
        let name = extract_name_after_if_exists(parts, "POLICY")?;
        return Some(NodedbStatement::DropRetentionPolicy { name, if_exists });
    }
    if upper.starts_with("ALTER RETENTION POLICY ") {
        return Some(NodedbStatement::AlterRetentionPolicy {
            raw_sql: trimmed.to_string(),
        });
    }
    if upper.starts_with("SHOW RETENTION POLIC") {
        return Some(NodedbStatement::ShowRetentionPolicies);
    }
    let _ = parts;
    None
}
