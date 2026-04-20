//! Parse CREATE/DROP/ALTER/SHOW ALERT + SHOW ALERT STATUS.

use super::helpers::extract_name_after_if_exists;
use crate::ddl_ast::statement::NodedbStatement;

pub(super) fn try_parse(upper: &str, parts: &[&str], trimmed: &str) -> Option<NodedbStatement> {
    if upper.starts_with("CREATE ALERT ") {
        return Some(NodedbStatement::CreateAlert {
            raw_sql: trimmed.to_string(),
        });
    }
    if upper.starts_with("DROP ALERT ") {
        let if_exists = upper.contains("IF EXISTS");
        let name = extract_name_after_if_exists(parts, "ALERT")?;
        return Some(NodedbStatement::DropAlert { name, if_exists });
    }
    if upper.starts_with("ALTER ALERT ") {
        return Some(NodedbStatement::AlterAlert {
            raw_sql: trimmed.to_string(),
        });
    }
    if upper.starts_with("SHOW ALERT STATUS ") {
        let name = parts.get(3)?.to_string();
        return Some(NodedbStatement::ShowAlertStatus { name });
    }
    if upper.starts_with("SHOW ALERT") && !upper.starts_with("SHOW ALERT STATUS") {
        return Some(NodedbStatement::ShowAlerts);
    }
    None
}
