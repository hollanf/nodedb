//! Parse CREATE/DROP CHANGE STREAM.

use super::helpers::extract_name_after_if_exists;
use crate::ddl_ast::statement::NodedbStatement;

pub(super) fn try_parse(upper: &str, parts: &[&str], trimmed: &str) -> Option<NodedbStatement> {
    if upper.starts_with("CREATE CHANGE STREAM ") {
        return Some(NodedbStatement::CreateChangeStream {
            raw_sql: trimmed.to_string(),
        });
    }
    if upper.starts_with("DROP CHANGE STREAM ") {
        let if_exists = upper.contains("IF EXISTS");
        let name = extract_name_after_if_exists(parts, "STREAM")?;
        return Some(NodedbStatement::DropChangeStream { name, if_exists });
    }
    None
}
