//! Dispatcher: try each DDL family's `try_parse` in turn.

use super::{
    alert, backup, change_stream, cluster_admin, collection, index, maintenance, materialized_view,
    retention, rls, schedule, sequence, trigger, user_auth,
};
use crate::ddl_ast::graph_parse;
use crate::ddl_ast::statement::NodedbStatement;

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

    // Dispatch by family. Order matters only where prefixes overlap
    // (e.g. DESCRIBE vs DESCRIBE SEQUENCE — handled inside each
    // family's `try_parse`).
    collection::try_parse(&upper, &parts, trimmed)
        .or_else(|| index::try_parse(&upper, &parts, trimmed))
        .or_else(|| trigger::try_parse(&upper, &parts, trimmed))
        .or_else(|| schedule::try_parse(&upper, &parts, trimmed))
        .or_else(|| sequence::try_parse(&upper, &parts, trimmed))
        .or_else(|| alert::try_parse(&upper, &parts, trimmed))
        .or_else(|| retention::try_parse(&upper, &parts, trimmed))
        .or_else(|| cluster_admin::try_parse(&upper, &parts, trimmed))
        .or_else(|| maintenance::try_parse(&upper, &parts, trimmed))
        .or_else(|| backup::try_parse(&upper, &parts, trimmed))
        .or_else(|| user_auth::try_parse(&upper, &parts, trimmed))
        .or_else(|| change_stream::try_parse(&upper, &parts, trimmed))
        .or_else(|| rls::try_parse(&upper, &parts, trimmed))
        .or_else(|| materialized_view::try_parse(&upper, &parts, trimmed))
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
                purge: false,
                cascade: false,
                cascade_force: false,
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
                purge: false,
                cascade: false,
                cascade_force: false,
            }
        );
    }

    #[test]
    fn parse_drop_collection_purge() {
        let stmt = parse("DROP COLLECTION users PURGE").unwrap();
        assert_eq!(
            stmt,
            NodedbStatement::DropCollection {
                name: "users".into(),
                if_exists: false,
                purge: true,
                cascade: false,
                cascade_force: false,
            }
        );
    }

    #[test]
    fn parse_drop_collection_cascade() {
        let stmt = parse("DROP COLLECTION users CASCADE").unwrap();
        assert_eq!(
            stmt,
            NodedbStatement::DropCollection {
                name: "users".into(),
                if_exists: false,
                purge: false,
                cascade: true,
                cascade_force: false,
            }
        );
    }

    #[test]
    fn parse_drop_collection_purge_cascade() {
        let stmt = parse("DROP COLLECTION users PURGE CASCADE").unwrap();
        assert_eq!(
            stmt,
            NodedbStatement::DropCollection {
                name: "users".into(),
                if_exists: false,
                purge: true,
                cascade: true,
                cascade_force: false,
            }
        );
    }

    #[test]
    fn parse_drop_collection_cascade_force() {
        let stmt = parse("DROP COLLECTION users CASCADE FORCE").unwrap();
        assert_eq!(
            stmt,
            NodedbStatement::DropCollection {
                name: "users".into(),
                if_exists: false,
                purge: false,
                cascade: true,
                cascade_force: true,
            }
        );
    }

    #[test]
    fn parse_undrop_collection() {
        let stmt = parse("UNDROP COLLECTION users").unwrap();
        assert_eq!(
            stmt,
            NodedbStatement::UndropCollection {
                name: "users".into()
            }
        );
    }

    #[test]
    fn parse_undrop_table_alias() {
        let stmt = parse("UNDROP TABLE users").unwrap();
        assert_eq!(
            stmt,
            NodedbStatement::UndropCollection {
                name: "users".into()
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
