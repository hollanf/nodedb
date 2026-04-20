//! Parse CREATE/DROP/ALTER/DESCRIBE/SHOW SEQUENCE.

use super::helpers::extract_name_after_if_exists;
use crate::ddl_ast::statement::NodedbStatement;

pub(super) fn try_parse(upper: &str, parts: &[&str], trimmed: &str) -> Option<NodedbStatement> {
    if upper.starts_with("CREATE SEQUENCE ") {
        let if_not_exists = upper.contains("IF NOT EXISTS");
        let name = extract_name_after_if_exists(parts, "SEQUENCE")?;
        return Some(NodedbStatement::CreateSequence {
            name,
            if_not_exists,
            raw_sql: trimmed.to_string(),
        });
    }
    if upper.starts_with("DROP SEQUENCE ") {
        let if_exists = upper.contains("IF EXISTS");
        let name = extract_name_after_if_exists(parts, "SEQUENCE")?;
        return Some(NodedbStatement::DropSequence { name, if_exists });
    }
    if upper.starts_with("ALTER SEQUENCE ") {
        return Some(NodedbStatement::AlterSequence {
            raw_sql: trimmed.to_string(),
        });
    }
    if upper.starts_with("DESCRIBE SEQUENCE ") {
        let name = parts.get(2)?.to_string();
        return Some(NodedbStatement::DescribeSequence { name });
    }
    if upper == "SHOW SEQUENCES" || upper.starts_with("SHOW SEQUENCES") {
        return Some(NodedbStatement::ShowSequences);
    }
    None
}
