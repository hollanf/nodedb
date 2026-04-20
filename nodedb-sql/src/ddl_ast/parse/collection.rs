//! Parse CREATE/DROP/ALTER/DESCRIBE/SHOW for COLLECTION (and TABLE alias).
//!
//! `DROP COLLECTION` extensions (sqlparser 0.61 does not tokenize
//! these, hence custom-handled upper-case keyword scan):
//! - `PURGE` — hard-delete, skipping the retention window
//! - `CASCADE` — recursively drop dependents
//! - `CASCADE FORCE` — cascade through dynamic-SQL schedules
//!
//! `UNDROP COLLECTION <name>` restores a soft-deleted record.

use super::helpers::{extract_name_after_if_exists, extract_name_after_keyword};
use crate::ddl_ast::statement::NodedbStatement;

pub(super) fn try_parse(upper: &str, parts: &[&str], trimmed: &str) -> Option<NodedbStatement> {
    if upper.starts_with("CREATE COLLECTION ") || upper.starts_with("CREATE TABLE ") {
        let if_not_exists = upper.contains("IF NOT EXISTS");
        let name = extract_name_after_keyword(parts, "COLLECTION")
            .or_else(|| extract_name_after_keyword(parts, "TABLE"))?;
        return Some(NodedbStatement::CreateCollection {
            name,
            if_not_exists,
            raw_sql: trimmed.to_string(),
        });
    }
    if upper.starts_with("UNDROP COLLECTION ") || upper.starts_with("UNDROP TABLE ") {
        let name = extract_name_after_keyword(parts, "COLLECTION")
            .or_else(|| extract_name_after_keyword(parts, "TABLE"))?;
        return Some(NodedbStatement::UndropCollection { name });
    }
    if upper.starts_with("DROP COLLECTION ") || upper.starts_with("DROP TABLE ") {
        let if_exists = upper.contains("IF EXISTS");
        let name = extract_name_after_if_exists(parts, "COLLECTION")
            .or_else(|| extract_name_after_if_exists(parts, "TABLE"))?;
        let purge = upper.contains(" PURGE");
        let cascade = upper.contains(" CASCADE");
        // `CASCADE FORCE` implies `cascade`; recognize both orderings.
        let cascade_force = upper.contains(" CASCADE FORCE") || upper.contains(" FORCE CASCADE");
        return Some(NodedbStatement::DropCollection {
            name,
            if_exists,
            purge,
            cascade: cascade || cascade_force,
            cascade_force,
        });
    }
    if upper.starts_with("ALTER COLLECTION ") || upper.starts_with("ALTER TABLE ") {
        let name = extract_name_after_keyword(parts, "COLLECTION")
            .or_else(|| extract_name_after_keyword(parts, "TABLE"))?;
        return Some(NodedbStatement::AlterCollection {
            name,
            raw_sql: trimmed.to_string(),
        });
    }
    if upper.starts_with("DESCRIBE ") && !upper.starts_with("DESCRIBE SEQUENCE") {
        let name = parts.get(1)?.to_string();
        return Some(NodedbStatement::DescribeCollection { name });
    }
    if upper == "\\D" || upper == "SHOW COLLECTIONS" || upper.starts_with("SHOW COLLECTIONS") {
        return Some(NodedbStatement::ShowCollections);
    }
    None
}
