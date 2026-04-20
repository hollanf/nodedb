//! Parse CREATE/DROP/SHOW RLS POLICY.

use super::helpers::{extract_after_keyword, extract_name_after_if_exists};
use crate::ddl_ast::statement::NodedbStatement;

pub(super) fn try_parse(upper: &str, parts: &[&str], trimmed: &str) -> Option<NodedbStatement> {
    if upper.starts_with("CREATE RLS POLICY ") {
        return Some(NodedbStatement::CreateRlsPolicy {
            raw_sql: trimmed.to_string(),
        });
    }
    if upper.starts_with("DROP RLS POLICY ") {
        let if_exists = upper.contains("IF EXISTS");
        let name = extract_name_after_if_exists(parts, "POLICY")?;
        let collection = extract_after_keyword(parts, "ON").unwrap_or_default();
        return Some(NodedbStatement::DropRlsPolicy {
            name,
            collection,
            if_exists,
        });
    }
    if upper.starts_with("SHOW RLS POLI") {
        let collection = parts.get(3).map(|s| s.to_string());
        return Some(NodedbStatement::ShowRlsPolicies { collection });
    }
    None
}
