//! Parse raw SQL into a [`NodedbStatement`].

use super::graph_parse;
use super::statement::NodedbStatement;

/// Try to parse a DDL statement from raw SQL. Returns `None` for
/// non-DDL queries (SELECT, INSERT, etc.) that should flow through
/// the normal planner.
pub fn parse(sql: &str) -> Option<NodedbStatement> {
    let trimmed = sql.trim();
    if trimmed.is_empty() {
        return None;
    }
    let upper = trimmed.to_uppercase();
    let parts: Vec<&str> = trimmed.split_whitespace().collect();
    if parts.is_empty() {
        return None;
    }

    // Graph DSL (`GRAPH ...`, `MATCH ...`) has its own tokenising
    // parser — delegate early so the string-prefix branches below
    // cannot accidentally shadow quoted values that contain DSL
    // keywords.
    if upper.starts_with("GRAPH ")
        || upper.starts_with("MATCH ")
        || upper.starts_with("OPTIONAL MATCH ")
    {
        return graph_parse::try_parse(trimmed);
    }

    // ── Collection lifecycle ─────────────────────────────────────
    if upper.starts_with("CREATE COLLECTION ") || upper.starts_with("CREATE TABLE ") {
        let if_not_exists = upper.contains("IF NOT EXISTS");
        let name = extract_name_after_keyword(&parts, "COLLECTION")
            .or_else(|| extract_name_after_keyword(&parts, "TABLE"))?;
        return Some(NodedbStatement::CreateCollection {
            name,
            if_not_exists,
            raw_sql: trimmed.to_string(),
        });
    }
    if upper.starts_with("DROP COLLECTION ") || upper.starts_with("DROP TABLE ") {
        let if_exists = upper.contains("IF EXISTS");
        let name = extract_name_after_if_exists(&parts, "COLLECTION")
            .or_else(|| extract_name_after_if_exists(&parts, "TABLE"))?;
        return Some(NodedbStatement::DropCollection { name, if_exists });
    }
    if upper.starts_with("ALTER COLLECTION ") || upper.starts_with("ALTER TABLE ") {
        let name = extract_name_after_keyword(&parts, "COLLECTION")
            .or_else(|| extract_name_after_keyword(&parts, "TABLE"))?;
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

    // ── Index ────────────────────────────────────────────────────
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
        let name = extract_name_after_if_exists(&parts, "INDEX")?;
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

    // ── Trigger ──────────────────────────────────────────────────
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
        let name = extract_name_after_if_exists(&parts, "TRIGGER")?;
        let collection = extract_after_keyword(&parts, "ON").unwrap_or_default();
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

    // ── Schedule ─────────────────────────────────────────────────
    if upper.starts_with("CREATE SCHEDULE ") {
        return Some(NodedbStatement::CreateSchedule {
            raw_sql: trimmed.to_string(),
        });
    }
    if upper.starts_with("DROP SCHEDULE ") {
        let if_exists = upper.contains("IF EXISTS");
        let name = extract_name_after_if_exists(&parts, "SCHEDULE")?;
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

    // ── Sequence ─────────────────────────────────────────────────
    if upper.starts_with("CREATE SEQUENCE ") {
        let if_not_exists = upper.contains("IF NOT EXISTS");
        let name = extract_name_after_if_exists(&parts, "SEQUENCE")?;
        return Some(NodedbStatement::CreateSequence {
            name,
            if_not_exists,
            raw_sql: trimmed.to_string(),
        });
    }
    if upper.starts_with("DROP SEQUENCE ") {
        let if_exists = upper.contains("IF EXISTS");
        let name = extract_name_after_if_exists(&parts, "SEQUENCE")?;
        return Some(NodedbStatement::DropSequence { name, if_exists });
    }
    if upper.starts_with("ALTER SEQUENCE ") {
        return Some(NodedbStatement::AlterSequence {
            raw_sql: trimmed.to_string(),
        });
    }
    if upper.starts_with("DESCRIBE SEQUENCE ") {
        let name = parts.get(2)?.to_string();
        return Some(NodedbStatement::DescribeSequence { name });
    }
    if upper == "SHOW SEQUENCES" || upper.starts_with("SHOW SEQUENCES") {
        return Some(NodedbStatement::ShowSequences);
    }

    // ── Alert ────────────────────────────────────────────────────
    if upper.starts_with("CREATE ALERT ") {
        return Some(NodedbStatement::CreateAlert {
            raw_sql: trimmed.to_string(),
        });
    }
    if upper.starts_with("DROP ALERT ") {
        let if_exists = upper.contains("IF EXISTS");
        let name = extract_name_after_if_exists(&parts, "ALERT")?;
        return Some(NodedbStatement::DropAlert { name, if_exists });
    }
    if upper.starts_with("ALTER ALERT ") {
        return Some(NodedbStatement::AlterAlert {
            raw_sql: trimmed.to_string(),
        });
    }
    if upper.starts_with("SHOW ALERT STATUS ") {
        let name = parts.get(3)?.to_string();
        return Some(NodedbStatement::ShowAlertStatus { name });
    }
    if upper.starts_with("SHOW ALERT") && !upper.starts_with("SHOW ALERT STATUS") {
        return Some(NodedbStatement::ShowAlerts);
    }

    // ── Retention policy ─────────────────────────────────────────
    if upper.starts_with("CREATE RETENTION POLICY ") {
        return Some(NodedbStatement::CreateRetentionPolicy {
            raw_sql: trimmed.to_string(),
        });
    }
    if upper.starts_with("DROP RETENTION POLICY ") {
        let if_exists = upper.contains("IF EXISTS");
        let name = extract_name_after_if_exists(&parts, "POLICY")?;
        return Some(NodedbStatement::DropRetentionPolicy { name, if_exists });
    }
    if upper.starts_with("ALTER RETENTION POLICY ") {
        return Some(NodedbStatement::AlterRetentionPolicy {
            raw_sql: trimmed.to_string(),
        });
    }
    if upper.starts_with("SHOW RETENTION POLIC") {
        return Some(NodedbStatement::ShowRetentionPolicies);
    }

    // ── Cluster admin ────────────────────────────────────────────
    if upper.starts_with("SHOW CLUSTER") {
        return Some(NodedbStatement::ShowCluster);
    }
    if upper.starts_with("SHOW MIGRATIONS") {
        return Some(NodedbStatement::ShowMigrations);
    }
    if upper.starts_with("SHOW RANGES") {
        return Some(NodedbStatement::ShowRanges);
    }
    if upper.starts_with("SHOW ROUTING") {
        return Some(NodedbStatement::ShowRouting);
    }
    if upper.starts_with("SHOW SCHEMA VERSION") {
        return Some(NodedbStatement::ShowSchemaVersion);
    }
    if upper.starts_with("SHOW PEER HEALTH") {
        return Some(NodedbStatement::ShowPeerHealth);
    }
    if upper.starts_with("REBALANCE") {
        return Some(NodedbStatement::Rebalance);
    }
    if upper.starts_with("SHOW RAFT GROUP ") {
        let id = parts.get(3)?.to_string();
        return Some(NodedbStatement::ShowRaftGroup { group_id: id });
    }
    if upper.starts_with("SHOW RAFT GROUPS") || upper.starts_with("SHOW RAFT") {
        return Some(NodedbStatement::ShowRaftGroups);
    }
    if upper.starts_with("ALTER RAFT GROUP ") {
        return Some(NodedbStatement::AlterRaftGroup {
            raw_sql: trimmed.to_string(),
        });
    }
    if upper.starts_with("REMOVE NODE ") {
        let id = parts.get(2)?.to_string();
        return Some(NodedbStatement::RemoveNode { node_id: id });
    }
    if upper.starts_with("SHOW NODE ") {
        let id = parts.get(2)?.to_string();
        return Some(NodedbStatement::ShowNode { node_id: id });
    }
    if upper.starts_with("SHOW NODES") {
        return Some(NodedbStatement::ShowNodes);
    }

    // ── Maintenance ──────────────────────────────────────────────
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

    // ── Backup / restore ─────────────────────────────────────────
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

    // ── User / auth ──────────────────────────────────────────────
    if upper.starts_with("CREATE USER ") {
        return Some(NodedbStatement::CreateUser {
            raw_sql: trimmed.to_string(),
        });
    }
    if upper.starts_with("DROP USER ") {
        let username = parts.get(2)?.to_string();
        return Some(NodedbStatement::DropUser { username });
    }
    if upper.starts_with("ALTER USER ") {
        return Some(NodedbStatement::AlterUser {
            raw_sql: trimmed.to_string(),
        });
    }
    if upper.starts_with("SHOW USERS") {
        return Some(NodedbStatement::ShowUsers);
    }
    if upper.starts_with("GRANT ROLE ") {
        return Some(NodedbStatement::GrantRole {
            raw_sql: trimmed.to_string(),
        });
    }
    if upper.starts_with("REVOKE ROLE ") {
        return Some(NodedbStatement::RevokeRole {
            raw_sql: trimmed.to_string(),
        });
    }
    if upper.starts_with("GRANT ") {
        return Some(NodedbStatement::GrantPermission {
            raw_sql: trimmed.to_string(),
        });
    }
    if upper.starts_with("REVOKE ") {
        return Some(NodedbStatement::RevokePermission {
            raw_sql: trimmed.to_string(),
        });
    }
    if upper.starts_with("SHOW PERMISSIONS") {
        let collection = parts.get(2).map(|s| s.to_string());
        return Some(NodedbStatement::ShowPermissions { collection });
    }
    if upper.starts_with("SHOW GRANTS") {
        let username = parts.get(2).map(|s| s.to_string());
        return Some(NodedbStatement::ShowGrants { username });
    }
    if upper.starts_with("SHOW TENANTS") {
        return Some(NodedbStatement::ShowTenants);
    }
    if upper.starts_with("SHOW AUDIT") {
        return Some(NodedbStatement::ShowAuditLog);
    }
    if upper.starts_with("SHOW CONSTRAINTS ") {
        let collection = parts.get(2)?.to_string();
        return Some(NodedbStatement::ShowConstraints { collection });
    }
    if upper.starts_with("SHOW TYPEGUARD") {
        let collection = parts.get(2)?.to_string();
        return Some(NodedbStatement::ShowTypeGuards { collection });
    }

    // ── Change stream ────────────────────────────────────────────
    if upper.starts_with("CREATE CHANGE STREAM ") {
        return Some(NodedbStatement::CreateChangeStream {
            raw_sql: trimmed.to_string(),
        });
    }
    if upper.starts_with("DROP CHANGE STREAM ") {
        let if_exists = upper.contains("IF EXISTS");
        let name = extract_name_after_if_exists(&parts, "STREAM")?;
        return Some(NodedbStatement::DropChangeStream { name, if_exists });
    }

    // ── RLS ──────────────────────────────────────────────────────
    if upper.starts_with("CREATE RLS POLICY ") {
        return Some(NodedbStatement::CreateRlsPolicy {
            raw_sql: trimmed.to_string(),
        });
    }
    if upper.starts_with("DROP RLS POLICY ") {
        let if_exists = upper.contains("IF EXISTS");
        let name = extract_name_after_if_exists(&parts, "POLICY")?;
        let collection = extract_after_keyword(&parts, "ON").unwrap_or_default();
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

    // ── Materialized view ────────────────────────────────────────
    if upper.starts_with("CREATE MATERIALIZED VIEW ") {
        return Some(NodedbStatement::CreateMaterializedView {
            raw_sql: trimmed.to_string(),
        });
    }
    if upper.starts_with("DROP MATERIALIZED VIEW ") {
        let if_exists = upper.contains("IF EXISTS");
        let name = extract_name_after_if_exists(&parts, "VIEW")?;
        return Some(NodedbStatement::DropMaterializedView { name, if_exists });
    }

    // ── Continuous aggregate ─────────────────────────────────────
    if upper.starts_with("CREATE CONTINUOUS AGGREGATE ") {
        return Some(NodedbStatement::CreateContinuousAggregate {
            raw_sql: trimmed.to_string(),
        });
    }
    if upper.starts_with("DROP CONTINUOUS AGGREGATE ") {
        let if_exists = upper.contains("IF EXISTS");
        let name = extract_name_after_if_exists(&parts, "AGGREGATE")?;
        return Some(NodedbStatement::DropContinuousAggregate { name, if_exists });
    }

    None
}

/// Extract the object name that follows a keyword (e.g. "COLLECTION"
/// in "CREATE COLLECTION users ..."). Handles IF NOT EXISTS by
/// skipping those tokens.
fn extract_name_after_keyword(parts: &[&str], keyword: &str) -> Option<String> {
    let kw_upper = keyword.to_uppercase();
    let pos = parts.iter().position(|p| p.to_uppercase() == kw_upper)?;
    let mut idx = pos + 1;
    // Skip IF NOT EXISTS tokens.
    if parts.get(idx).map(|s| s.to_uppercase()) == Some("IF".to_string()) {
        idx += 1; // NOT
        if parts.get(idx).map(|s| s.to_uppercase()) == Some("NOT".to_string()) {
            idx += 1; // EXISTS
        }
        if parts.get(idx).map(|s| s.to_uppercase()) == Some("EXISTS".to_string()) {
            idx += 1;
        }
    }
    parts.get(idx).map(|s| s.to_string())
}

/// Extract the object name for DROP-style commands where IF EXISTS
/// may appear between the keyword and the name.
fn extract_name_after_if_exists(parts: &[&str], keyword: &str) -> Option<String> {
    extract_name_after_keyword(parts, keyword)
}

/// Extract the token after a keyword like "ON" or "TO".
fn extract_after_keyword(parts: &[&str], keyword: &str) -> Option<String> {
    let kw_upper = keyword.to_uppercase();
    let pos = parts.iter().position(|p| p.to_uppercase() == kw_upper)?;
    parts.get(pos + 1).map(|s| s.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_create_collection() {
        let stmt = parse("CREATE COLLECTION users (id INT, name TEXT)").unwrap();
        match stmt {
            NodedbStatement::CreateCollection {
                name,
                if_not_exists,
                ..
            } => {
                assert_eq!(name, "users");
                assert!(!if_not_exists);
            }
            other => panic!("expected CreateCollection, got {other:?}"),
        }
    }

    #[test]
    fn parse_create_collection_if_not_exists() {
        let stmt = parse("CREATE COLLECTION IF NOT EXISTS users").unwrap();
        match stmt {
            NodedbStatement::CreateCollection {
                name,
                if_not_exists,
                ..
            } => {
                assert_eq!(name, "users");
                assert!(if_not_exists);
            }
            other => panic!("expected CreateCollection, got {other:?}"),
        }
    }

    #[test]
    fn parse_drop_collection() {
        let stmt = parse("DROP COLLECTION users").unwrap();
        assert_eq!(
            stmt,
            NodedbStatement::DropCollection {
                name: "users".into(),
                if_exists: false,
            }
        );
    }

    #[test]
    fn parse_drop_collection_if_exists() {
        let stmt = parse("DROP COLLECTION IF EXISTS users").unwrap();
        assert_eq!(
            stmt,
            NodedbStatement::DropCollection {
                name: "users".into(),
                if_exists: true,
            }
        );
    }

    #[test]
    fn parse_show_nodes() {
        assert_eq!(parse("SHOW NODES"), Some(NodedbStatement::ShowNodes));
    }

    #[test]
    fn parse_show_cluster() {
        assert_eq!(parse("SHOW CLUSTER"), Some(NodedbStatement::ShowCluster));
    }

    #[test]
    fn parse_create_trigger() {
        let stmt = parse("CREATE OR REPLACE SYNC TRIGGER on_insert ...").unwrap();
        match stmt {
            NodedbStatement::CreateTrigger {
                or_replace,
                sync,
                deferred,
                ..
            } => {
                assert!(or_replace);
                assert!(sync);
                assert!(!deferred);
            }
            other => panic!("expected CreateTrigger, got {other:?}"),
        }
    }

    #[test]
    fn parse_drop_index_if_exists() {
        let stmt = parse("DROP INDEX IF EXISTS idx_name").unwrap();
        match stmt {
            NodedbStatement::DropIndex {
                name, if_exists, ..
            } => {
                assert_eq!(name, "idx_name");
                assert!(if_exists);
            }
            other => panic!("expected DropIndex, got {other:?}"),
        }
    }

    #[test]
    fn parse_analyze() {
        assert_eq!(
            parse("ANALYZE users"),
            Some(NodedbStatement::Analyze {
                collection: Some("users".into()),
            })
        );
        assert_eq!(
            parse("ANALYZE"),
            Some(NodedbStatement::Analyze { collection: None })
        );
    }

    #[test]
    fn non_ddl_returns_none() {
        assert!(parse("SELECT * FROM users").is_none());
        assert!(parse("INSERT INTO users VALUES (1)").is_none());
    }

    #[test]
    fn parse_grant_role() {
        let stmt = parse("GRANT ROLE admin TO alice").unwrap();
        match stmt {
            NodedbStatement::GrantRole { raw_sql } => {
                assert!(raw_sql.contains("admin"));
            }
            other => panic!("expected GrantRole, got {other:?}"),
        }
    }

    #[test]
    fn parse_create_sequence_if_not_exists() {
        let stmt = parse("CREATE SEQUENCE IF NOT EXISTS my_seq START 1").unwrap();
        match stmt {
            NodedbStatement::CreateSequence {
                name,
                if_not_exists,
                ..
            } => {
                assert_eq!(name, "my_seq");
                assert!(if_not_exists);
            }
            other => panic!("expected CreateSequence, got {other:?}"),
        }
    }

    #[test]
    fn parse_restore_dry_run() {
        let stmt = parse("RESTORE TENANT 1 FROM '/tmp/backup' DRY RUN").unwrap();
        match stmt {
            NodedbStatement::RestoreTenant { dry_run, .. } => {
                assert!(dry_run);
            }
            other => panic!("expected RestoreTenant, got {other:?}"),
        }
    }
}
