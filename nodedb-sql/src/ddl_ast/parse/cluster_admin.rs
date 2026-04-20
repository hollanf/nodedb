//! Parse cluster-admin: SHOW CLUSTER/NODES/RAFT/MIGRATIONS/RANGES/ROUTING,
//! REMOVE NODE, REBALANCE, ALTER RAFT GROUP.

use crate::ddl_ast::statement::NodedbStatement;

pub(super) fn try_parse(upper: &str, parts: &[&str], trimmed: &str) -> Option<NodedbStatement> {
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
    None
}
