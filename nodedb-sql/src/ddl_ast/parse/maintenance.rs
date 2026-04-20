//! Parse maintenance: ANALYZE, COMPACT, SHOW COMPACTION STATUS, SHOW STORAGE.

use crate::ddl_ast::statement::NodedbStatement;

pub(super) fn try_parse(upper: &str, parts: &[&str], _trimmed: &str) -> Option<NodedbStatement> {
    if upper.starts_with("ANALYZE") {
        let collection = parts.get(1).map(|s| s.to_string());
        return Some(NodedbStatement::Analyze { collection });
    }
    if upper.starts_with("COMPACT ") {
        let collection = parts.get(1)?.to_string();
        return Some(NodedbStatement::Compact { collection });
    }
    if upper.starts_with("SHOW COMPACTION ST") {
        return Some(NodedbStatement::ShowCompactionStatus);
    }
    if upper.starts_with("SHOW STORAGE") {
        let collection = parts.get(2).map(|s| s.to_string());
        return Some(NodedbStatement::ShowStorage { collection });
    }
    None
}
