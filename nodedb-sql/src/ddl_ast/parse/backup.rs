//! Parse BACKUP TENANT / RESTORE TENANT.

use crate::ddl_ast::statement::NodedbStatement;
use crate::error::SqlError;

pub(super) fn try_parse(
    upper: &str,
    parts: &[&str],
    _trimmed: &str,
) -> Option<Result<NodedbStatement, SqlError>> {
    if upper.starts_with("BACKUP TENANT ") {
        // BACKUP TENANT <tenant_id>
        let tenant_id = parts.get(2).map(|s| s.to_string()).unwrap_or_default();
        return Some(Ok(NodedbStatement::BackupTenant { tenant_id }));
    }
    if upper.starts_with("RESTORE TENANT ") {
        // RESTORE TENANT <tenant_id> FROM '<path>' [DRY RUN]
        let dry_run = upper.ends_with(" DRY RUN") || upper.ends_with(" DRYRUN");
        let tenant_id = parts.get(2).map(|s| s.to_string()).unwrap_or_default();
        return Some(Ok(NodedbStatement::RestoreTenant { dry_run, tenant_id }));
    }
    None
}
