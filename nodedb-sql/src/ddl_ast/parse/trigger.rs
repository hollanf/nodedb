//! Parse CREATE/DROP/ALTER/SHOW TRIGGER.

use super::helpers::{extract_after_keyword, extract_name_after_if_exists};
use crate::ddl_ast::statement::NodedbStatement;

pub(super) fn try_parse(upper: &str, parts: &[&str], trimmed: &str) -> Option<NodedbStatement> {
    if upper.starts_with("CREATE ") && upper.contains("TRIGGER ") {
        let or_replace = upper.contains("OR REPLACE");
        let deferred = upper.contains("DEFERRED");
        let sync = upper.contains("SYNC");
        return Some(NodedbStatement::CreateTrigger {
            or_replace,
            deferred,
            sync,
            raw_sql: trimmed.to_string(),
        });
    }
    if upper.starts_with("DROP TRIGGER ") {
        let if_exists = upper.contains("IF EXISTS");
        let name = extract_name_after_if_exists(parts, "TRIGGER")?;
        let collection = extract_after_keyword(parts, "ON").unwrap_or_default();
        return Some(NodedbStatement::DropTrigger {
            name,
            collection,
            if_exists,
        });
    }
    if upper.starts_with("ALTER TRIGGER ") {
        return Some(NodedbStatement::AlterTrigger {
            raw_sql: trimmed.to_string(),
        });
    }
    if upper.starts_with("SHOW TRIGGERS") {
        let collection = if upper.starts_with("SHOW TRIGGERS ON ") {
            parts.get(3).map(|s| s.to_string())
        } else {
            None
        };
        return Some(NodedbStatement::ShowTriggers { collection });
    }
    None
}
