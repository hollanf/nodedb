//! Parse CREATE/DROP/ALTER/SHOW SCHEDULE + SHOW SCHEDULE HISTORY.

use super::helpers::extract_name_after_if_exists;
use crate::ddl_ast::statement::NodedbStatement;

pub(super) fn try_parse(upper: &str, parts: &[&str], trimmed: &str) -> Option<NodedbStatement> {
    if upper.starts_with("CREATE SCHEDULE ") {
        return Some(NodedbStatement::CreateSchedule {
            raw_sql: trimmed.to_string(),
        });
    }
    if upper.starts_with("DROP SCHEDULE ") {
        let if_exists = upper.contains("IF EXISTS");
        let name = extract_name_after_if_exists(parts, "SCHEDULE")?;
        return Some(NodedbStatement::DropSchedule { name, if_exists });
    }
    if upper.starts_with("ALTER SCHEDULE ") {
        return Some(NodedbStatement::AlterSchedule {
            raw_sql: trimmed.to_string(),
        });
    }
    if upper.starts_with("SHOW SCHEDULE HISTORY ") {
        let name = parts.get(3)?.to_string();
        return Some(NodedbStatement::ShowScheduleHistory { name });
    }
    if upper == "SHOW SCHEDULES" || upper.starts_with("SHOW SCHEDULES") {
        return Some(NodedbStatement::ShowSchedules);
    }
    None
}
