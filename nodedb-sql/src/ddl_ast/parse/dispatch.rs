//! Dispatcher: try each DDL family's `try_parse` in turn.

use super::{
    alert, backup, change_stream, cluster_admin, collection, index, maintenance, materialized_view,
    retention, rls, schedule, sequence, trigger, user_auth,
};
use crate::ddl_ast::graph_parse;
use crate::ddl_ast::statement::NodedbStatement;
use crate::error::SqlError;
use crate::parser::preprocess::lex;

/// Try to parse a DDL statement from raw SQL.
///
/// Returns `None` for non-DDL queries (SELECT, INSERT, etc.) that should
/// flow through the normal planner. Returns `Some(Err(...))` when the SQL
/// is structurally a DDL command but contains a reserved identifier that
/// would be misrouted by the dispatcher.
pub fn parse(sql: &str) -> Option<Result<NodedbStatement, SqlError>> {
    let trimmed = sql.trim();
    if trimmed.is_empty() {
        return None;
    }
    let upper = trimmed.to_uppercase();
    let parts: Vec<&str> = trimmed.split_whitespace().collect();
    if parts.is_empty() {
        return None;
    }

    // Graph DSL (`GRAPH ...`, `MATCH ...`, `OPTIONAL MATCH ...`) has its own
    // tokenising parser — delegate early using token-aware dispatch so that
    // leading block/line comments and quoted values containing DSL keywords
    // are never mistakenly matched.
    let first = lex::first_sql_word(trimmed).map(|w| w.to_uppercase());
    let is_graph = match first.as_deref() {
        Some("GRAPH") | Some("MATCH") => true,
        Some("OPTIONAL") => lex::second_sql_word(trimmed)
            .map(|w| w.eq_ignore_ascii_case("MATCH"))
            .unwrap_or(false),
        _ => false,
    };
    if is_graph {
        return graph_parse::try_parse(trimmed).map(Ok);
    }

    // Dispatch by family. Order matters only where prefixes overlap
    // (e.g. DESCRIBE vs DESCRIBE SEQUENCE — handled inside each
    // family's `try_parse`). A `Some(Err(...))` from any family
    // short-circuits the chain — reserved-identifier errors must not
    // be silently swallowed by the next family's `None` path.
    macro_rules! try_family {
        ($result:expr) => {{
            let r = $result;
            if r.is_some() {
                return r;
            }
        }};
    }

    try_family!(collection::try_parse(&upper, &parts, trimmed));
    try_family!(index::try_parse(&upper, &parts, trimmed));
    try_family!(trigger::try_parse(&upper, &parts, trimmed));
    try_family!(schedule::try_parse(&upper, &parts, trimmed));
    try_family!(sequence::try_parse(&upper, &parts, trimmed));
    try_family!(alert::try_parse(&upper, &parts, trimmed));
    try_family!(retention::try_parse(&upper, &parts, trimmed));
    try_family!(cluster_admin::try_parse(&upper, &parts, trimmed));
    try_family!(maintenance::try_parse(&upper, &parts, trimmed));
    try_family!(backup::try_parse(&upper, &parts, trimmed));
    try_family!(user_auth::try_parse(&upper, &parts, trimmed));
    try_family!(change_stream::try_parse(&upper, &parts, trimmed));
    try_family!(rls::try_parse(&upper, &parts, trimmed));
    try_family!(materialized_view::try_parse(&upper, &parts, trimmed));
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::SqlError;

    /// Parse and double-unwrap — panics if `None` or `Err`.
    fn ok(sql: &str) -> NodedbStatement {
        parse(sql)
            .expect("expected Some, got None")
            .expect("expected Ok, got Err")
    }

    /// Assert `parse` returns `Some(Err(SqlError::ReservedIdentifier { .. }))`.
    fn assert_reserved(sql: &str) {
        match parse(sql) {
            Some(Err(SqlError::ReservedIdentifier { .. })) => {}
            other => panic!("expected Some(Err(ReservedIdentifier)), got {other:?}"),
        }
    }

    #[test]
    fn parse_create_collection() {
        let stmt = ok("CREATE COLLECTION users (id INT, name TEXT)");
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
        let stmt = ok("CREATE COLLECTION IF NOT EXISTS users");
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
        let stmt = ok("DROP COLLECTION users");
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
        let stmt = ok("DROP COLLECTION IF EXISTS users");
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
        let stmt = ok("DROP COLLECTION users PURGE");
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
        let stmt = ok("DROP COLLECTION users CASCADE");
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
        let stmt = ok("DROP COLLECTION users PURGE CASCADE");
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
        let stmt = ok("DROP COLLECTION users CASCADE FORCE");
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
        let stmt = ok("UNDROP COLLECTION users");
        assert_eq!(
            stmt,
            NodedbStatement::UndropCollection {
                name: "users".into()
            }
        );
    }

    #[test]
    fn parse_undrop_table_alias() {
        let stmt = ok("UNDROP TABLE users");
        assert_eq!(
            stmt,
            NodedbStatement::UndropCollection {
                name: "users".into()
            }
        );
    }

    #[test]
    fn parse_show_nodes() {
        assert_eq!(parse("SHOW NODES"), Some(Ok(NodedbStatement::ShowNodes)));
    }

    #[test]
    fn parse_show_cluster() {
        assert_eq!(
            parse("SHOW CLUSTER"),
            Some(Ok(NodedbStatement::ShowCluster))
        );
    }

    #[test]
    fn parse_create_trigger() {
        let stmt = ok(
            "CREATE OR REPLACE SYNC TRIGGER on_insert AFTER INSERT ON orders FOR EACH ROW BEGIN RETURN; END",
        );
        match stmt {
            NodedbStatement::CreateTrigger {
                or_replace,
                execution_mode,
                timing,
                ..
            } => {
                assert!(or_replace);
                assert_eq!(execution_mode, "SYNC");
                assert_eq!(timing, "AFTER");
            }
            other => panic!("expected CreateTrigger, got {other:?}"),
        }
    }

    #[test]
    fn parse_drop_index_if_exists() {
        let stmt = ok("DROP INDEX IF EXISTS idx_name");
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
            Some(Ok(NodedbStatement::Analyze {
                collection: Some("users".into()),
            }))
        );
        assert_eq!(
            parse("ANALYZE"),
            Some(Ok(NodedbStatement::Analyze { collection: None }))
        );
    }

    #[test]
    fn parse_create_table_plain() {
        let stmt = ok("CREATE TABLE foo (id INT, name TEXT)");
        match stmt {
            NodedbStatement::CreateTable {
                name,
                if_not_exists,
                ..
            } => {
                assert_eq!(name, "foo");
                assert!(!if_not_exists);
            }
            other => panic!("expected CreateTable, got {other:?}"),
        }
    }

    #[test]
    fn parse_create_table_if_not_exists() {
        let stmt = ok("CREATE TABLE IF NOT EXISTS orders (id INT)");
        match stmt {
            NodedbStatement::CreateTable {
                name,
                if_not_exists,
                ..
            } => {
                assert_eq!(name, "orders");
                assert!(if_not_exists);
            }
            other => panic!("expected CreateTable, got {other:?}"),
        }
    }

    #[test]
    fn create_collection_is_not_create_table() {
        let stmt = ok("CREATE COLLECTION foo");
        assert!(matches!(stmt, NodedbStatement::CreateCollection { .. }));
    }

    #[test]
    fn non_ddl_returns_none() {
        assert!(parse("SELECT * FROM users").is_none());
        assert!(parse("INSERT INTO users VALUES (1)").is_none());
    }

    // ── graph dispatch (token-aware) ────────────────────────────────────────

    /// `MATCH` as first real token routes to the graph parser.
    #[test]
    fn graph_dispatch_match_plain() {
        let _ = parse("MATCH (a)-[]->(b) RETURN a");
    }

    /// `GRAPH` as first real token routes to the graph parser.
    #[test]
    fn graph_dispatch_graph_keyword() {
        let _ = parse("GRAPH something");
    }

    /// A leading block comment before `MATCH` must still route to graph.
    #[test]
    fn graph_dispatch_block_comment_before_match() {
        let _ = parse("/* hint */ MATCH (a) RETURN a");
    }

    /// `OPTIONAL MATCH` routes to the graph parser.
    #[test]
    fn graph_dispatch_optional_match() {
        let _ = parse("OPTIONAL MATCH (a) RETURN a");
    }

    /// `OPTIONAL` followed by something other than `MATCH` must NOT route to
    /// the graph parser (falls through to DDL families, which return None).
    #[test]
    fn graph_dispatch_optional_non_match_does_not_route() {
        assert!(parse("OPTIONAL FOO").is_none());
    }

    #[test]
    fn graph_dispatch_select_with_match_in_string() {
        assert!(parse("SELECT * FROM t WHERE name = 'MATCH'").is_none());
    }

    #[test]
    fn graph_dispatch_select_with_graph_in_string() {
        assert!(parse("SELECT * FROM t WHERE name = 'GRAPH'").is_none());
    }

    #[test]
    fn graph_dispatch_with_cte_does_not_route() {
        assert!(parse("WITH cte AS (SELECT 1) SELECT * FROM cte").is_none());
    }

    #[test]
    fn graph_dispatch_line_comment_match_then_select() {
        assert!(parse("-- MATCH (a)\nSELECT 1").is_none());
    }

    #[test]
    fn parse_grant_role() {
        let stmt = ok("GRANT ROLE admin TO alice");
        match stmt {
            NodedbStatement::GrantRole { role, username } => {
                assert_eq!(role, "admin");
                assert_eq!(username, "alice");
            }
            other => panic!("expected GrantRole, got {other:?}"),
        }
    }

    #[test]
    fn parse_create_sequence_if_not_exists() {
        let stmt = ok("CREATE SEQUENCE IF NOT EXISTS my_seq START 1");
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
        let stmt = ok("RESTORE TENANT 1 FROM '/tmp/backup' DRY RUN");
        match stmt {
            NodedbStatement::RestoreTenant { dry_run, tenant_id } => {
                assert!(dry_run);
                assert_eq!(tenant_id, "1");
            }
            other => panic!("expected RestoreTenant, got {other:?}"),
        }
    }

    // ── reserved identifier tests ─────────────────────────────────────────────

    #[test]
    fn create_table_reserved_name_is_err() {
        assert_reserved("CREATE TABLE match (id INT)");
    }

    #[test]
    fn create_table_quoted_reserved_name_is_ok() {
        let stmt = ok(r#"CREATE TABLE "match" (id INT)"#);
        match stmt {
            NodedbStatement::CreateTable { name, .. } => assert_eq!(name, "match"),
            other => panic!("expected CreateTable, got {other:?}"),
        }
    }

    #[test]
    fn create_collection_reserved_name_is_err() {
        assert_reserved("CREATE COLLECTION upsert (id INT)");
    }

    #[test]
    fn create_table_reserved_column_is_err() {
        assert_reserved("CREATE TABLE foo (graph INT)");
    }

    #[test]
    fn create_table_quoted_reserved_column_is_ok() {
        let stmt = ok(r#"CREATE TABLE foo ("graph" INT)"#);
        match stmt {
            NodedbStatement::CreateTable { columns, .. } => {
                assert_eq!(columns[0].0, "graph");
            }
            other => panic!("expected CreateTable, got {other:?}"),
        }
    }

    // One test per reserved word: rejected bare, accepted quoted.

    #[test]
    fn reserved_graph() {
        assert_reserved("CREATE TABLE graph (id INT)");
        let stmt = ok(r#"CREATE TABLE "graph" (id INT)"#);
        assert!(matches!(stmt, NodedbStatement::CreateTable { .. }));
    }

    #[test]
    fn reserved_match() {
        assert_reserved("CREATE TABLE match (id INT)");
        let stmt = ok(r#"CREATE TABLE "match" (id INT)"#);
        assert!(matches!(stmt, NodedbStatement::CreateTable { .. }));
    }

    #[test]
    fn reserved_optional() {
        assert_reserved("CREATE TABLE optional (id INT)");
        let stmt = ok(r#"CREATE TABLE "optional" (id INT)"#);
        assert!(matches!(stmt, NodedbStatement::CreateTable { .. }));
    }

    #[test]
    fn reserved_upsert() {
        assert_reserved("CREATE COLLECTION upsert (id INT)");
        let stmt = ok(r#"CREATE COLLECTION "upsert" (id INT)"#);
        assert!(matches!(stmt, NodedbStatement::CreateCollection { .. }));
    }

    #[test]
    fn reserved_undrop() {
        assert_reserved("CREATE TABLE undrop (id INT)");
        let stmt = ok(r#"CREATE TABLE "undrop" (id INT)"#);
        assert!(matches!(stmt, NodedbStatement::CreateTable { .. }));
    }

    #[test]
    fn reserved_purge() {
        assert_reserved("CREATE TABLE purge (id INT)");
        let stmt = ok(r#"CREATE TABLE "purge" (id INT)"#);
        assert!(matches!(stmt, NodedbStatement::CreateTable { .. }));
    }

    #[test]
    fn reserved_cascade() {
        assert_reserved("CREATE TABLE cascade (id INT)");
        let stmt = ok(r#"CREATE TABLE "cascade" (id INT)"#);
        assert!(matches!(stmt, NodedbStatement::CreateTable { .. }));
    }

    #[test]
    fn reserved_search() {
        assert_reserved("CREATE TABLE search (id INT)");
        let stmt = ok(r#"CREATE TABLE "search" (id INT)"#);
        assert!(matches!(stmt, NodedbStatement::CreateTable { .. }));
    }

    #[test]
    fn reserved_crdt() {
        assert_reserved("CREATE TABLE crdt (id INT)");
        let stmt = ok(r#"CREATE TABLE "crdt" (id INT)"#);
        assert!(matches!(stmt, NodedbStatement::CreateTable { .. }));
    }
}
