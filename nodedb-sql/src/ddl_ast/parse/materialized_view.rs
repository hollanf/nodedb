//! Parse CREATE/DROP MATERIALIZED VIEW + CREATE/DROP CONTINUOUS AGGREGATE.

use super::helpers::extract_name_after_if_exists;
use crate::ddl_ast::statement::NodedbStatement;

pub(super) fn try_parse(upper: &str, parts: &[&str], trimmed: &str) -> Option<NodedbStatement> {
    if upper.starts_with("CREATE MATERIALIZED VIEW ") {
        return Some(NodedbStatement::CreateMaterializedView {
            raw_sql: trimmed.to_string(),
        });
    }
    if upper.starts_with("DROP MATERIALIZED VIEW ") {
        let if_exists = upper.contains("IF EXISTS");
        let name = extract_name_after_if_exists(parts, "VIEW")?;
        return Some(NodedbStatement::DropMaterializedView { name, if_exists });
    }
    if upper.starts_with("CREATE CONTINUOUS AGGREGATE ") {
        return Some(NodedbStatement::CreateContinuousAggregate {
            raw_sql: trimmed.to_string(),
        });
    }
    if upper.starts_with("DROP CONTINUOUS AGGREGATE ") {
        let if_exists = upper.contains("IF EXISTS");
        let name = extract_name_after_if_exists(parts, "AGGREGATE")?;
        return Some(NodedbStatement::DropContinuousAggregate { name, if_exists });
    }
    None
}
