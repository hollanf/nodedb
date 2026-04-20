//! Parse CREATE INDEX / DROP INDEX / SHOW INDEX / REINDEX.

use super::helpers::extract_name_after_if_exists;
use crate::ddl_ast::statement::NodedbStatement;

pub(super) fn try_parse(upper: &str, parts: &[&str], trimmed: &str) -> Option<NodedbStatement> {
    if upper.starts_with("CREATE UNIQUE INDEX ") || upper.starts_with("CREATE UNIQUE IND") {
        return Some(NodedbStatement::CreateIndex {
            unique: true,
            raw_sql: trimmed.to_string(),
        });
    }
    if upper.starts_with("CREATE INDEX ") {
        return Some(NodedbStatement::CreateIndex {
            unique: false,
            raw_sql: trimmed.to_string(),
        });
    }
    if upper.starts_with("DROP INDEX ") {
        let if_exists = upper.contains("IF EXISTS");
        let name = extract_name_after_if_exists(parts, "INDEX")?;
        return Some(NodedbStatement::DropIndex {
            name,
            collection: None,
            if_exists,
        });
    }
    if upper.starts_with("SHOW INDEX") {
        let collection = parts.get(2).map(|s| s.to_string());
        return Some(NodedbStatement::ShowIndexes { collection });
    }
    if upper.starts_with("REINDEX ") {
        let collection = parts.get(1)?.to_string();
        return Some(NodedbStatement::Reindex { collection });
    }
    None
}
