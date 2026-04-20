//! Parse BACKUP TENANT / RESTORE TENANT.

use crate::ddl_ast::statement::NodedbStatement;

pub(super) fn try_parse(upper: &str, _parts: &[&str], trimmed: &str) -> Option<NodedbStatement> {
    if upper.starts_with("BACKUP TENANT ") {
        return Some(NodedbStatement::BackupTenant {
            raw_sql: trimmed.to_string(),
        });
    }
    if upper.starts_with("RESTORE TENANT ") {
        let dry_run = upper.ends_with(" DRY RUN") || upper.ends_with(" DRYRUN");
        return Some(NodedbStatement::RestoreTenant {
            dry_run,
            raw_sql: trimmed.to_string(),
        });
    }
    None
}
