use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

pub(super) async fn dispatch(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
    upper: &str,
    parts: &[&str],
) -> Option<PgWireResult<Vec<Response>>> {
    // BACKUP/RESTORE TENANT are fully dispatched via typed AST (ast.rs).

    // Schedules: CREATE/DROP/ALTER/SHOW SCHEDULE
    // CREATE SCHEDULE is fully dispatched via typed AST (ast.rs).
    if upper.starts_with("DROP SCHEDULE ") {
        return Some(super::super::schedule::drop_schedule(
            state, identity, parts,
        ));
    }
    // ALTER SCHEDULE is fully dispatched via typed AST (ast.rs).
    if upper.starts_with("SHOW SCHEDULE HISTORY ") {
        let name = parts.get(3).unwrap_or(&"");
        return Some(super::super::schedule::show_schedule_history(
            state, identity, name,
        ));
    }
    if upper.starts_with("SHOW SCHEDULE") {
        return Some(super::super::schedule::show_schedules(state, identity));
    }

    // Sequences: CREATE/DROP/ALTER/SHOW SEQUENCE
    // CREATE SEQUENCE is fully dispatched via typed AST (ast.rs).
    if upper.starts_with("DROP SEQUENCE ") {
        return Some(super::super::sequence::drop_sequence(
            state, identity, parts,
        ));
    }
    // ALTER SEQUENCE is fully dispatched via typed AST (ast.rs).
    if upper == "SHOW SEQUENCES" || upper.starts_with("SHOW SEQUENCES ") {
        return Some(super::super::sequence::show_sequences(state, identity));
    }

    // Maintenance: ANALYZE, COMPACT, REINDEX, SHOW STORAGE, SHOW COMPACTION
    if upper.starts_with("ANALYZE ") {
        return Some(super::super::maintenance::handle_analyze(state, identity, sql).await);
    }
    if upper.starts_with("COMPACT ") {
        return Some(super::super::maintenance::handle_compact(
            state, identity, parts,
        ));
    }
    if upper.starts_with("REINDEX ") {
        return Some(super::super::maintenance::handle_reindex(
            state, identity, parts,
        ));
    }
    if upper.starts_with("SHOW STORAGE ") {
        return Some(super::super::maintenance::handle_show_storage(
            state, identity, parts,
        ));
    }
    if upper == "SHOW COMPACTION STATUS" || upper.starts_with("SHOW COMPACTION STATUS ") {
        return Some(super::super::maintenance::handle_show_compaction_status(
            state, identity,
        ));
    }

    // Alert rules.
    // CREATE ALERT is fully dispatched via typed AST (ast.rs).
    if upper.starts_with("DROP ALERT ") {
        return Some(super::super::alert::drop_alert(state, identity, parts));
    }
    // ALTER ALERT is fully dispatched via typed AST (ast.rs).
    if upper.starts_with("SHOW ALERT STATUS ") {
        let name = parts.get(4).unwrap_or(&"");
        return Some(super::super::alert::show_alert_status(
            state, identity, name,
        ));
    }
    if upper.starts_with("SHOW ALERT") {
        return Some(super::super::alert::show_alerts(state, identity));
    }

    // Retention policies.
    // CREATE RETENTION POLICY is fully dispatched via typed AST (ast.rs).
    if upper.starts_with("DROP RETENTION POLICY ") {
        return Some(
            super::super::retention_policy::drop_retention_policy(state, identity, parts).await,
        );
    }
    // ALTER RETENTION POLICY is fully dispatched via typed AST (ast.rs).
    if upper.starts_with("SHOW RETENTION POLIC") {
        return Some(super::super::retention_policy::show_retention_policy(
            state, identity, parts,
        ));
    }

    // Continuous aggregates.
    // CREATE CONTINUOUS AGGREGATE is fully dispatched via typed AST (ast.rs).
    if upper.starts_with("DROP CONTINUOUS AGGREGATE ") {
        return Some(
            super::super::continuous_agg::drop_continuous_aggregate(state, identity, parts).await,
        );
    }
    if upper.starts_with("SHOW CONTINUOUS AGGREGATE") {
        return Some(
            super::super::continuous_agg::show_continuous_aggregates(state, identity, parts).await,
        );
    }

    // CONVERT COLLECTION.
    if upper.starts_with("CONVERT COLLECTION ")
        || upper.starts_with("CONVERT ") && upper.contains(" TO ")
    {
        return Some(super::super::convert::convert_collection(state, identity, sql).await);
    }

    // Materialized views (HTAP).
    // CREATE MATERIALIZED VIEW is fully dispatched via typed AST (ast.rs).
    if upper.starts_with("DROP MATERIALIZED VIEW ") {
        return Some(super::super::materialized_view::drop_materialized_view(
            state, identity, parts,
        ));
    }
    if upper.starts_with("REFRESH MATERIALIZED VIEW ") {
        return Some(
            super::super::materialized_view::refresh_materialized_view(state, identity, parts)
                .await,
        );
    }
    if upper.starts_with("SHOW MATERIALIZED VIEW") {
        return Some(super::super::materialized_view::show_materialized_views(
            state, identity, parts,
        ));
    }

    // Blacklist management.
    if upper.starts_with("BLACKLIST ") {
        return Some(super::super::blacklist_ddl::handle_blacklist(
            state, identity, parts,
        ));
    }
    if upper.starts_with("SHOW BLACKLIST") {
        return Some(super::super::blacklist_ddl::show_blacklist(
            state, identity, parts,
        ));
    }

    // Auth user management (JIT-provisioned users).
    if upper.starts_with("DEACTIVATE AUTH USER ") || upper.starts_with("ALTER AUTH USER ") {
        return Some(super::super::auth_user_ddl::handle_auth_user(
            state, identity, parts,
        ));
    }
    if upper.starts_with("PURGE AUTH USERS ") {
        return Some(super::super::auth_user_ddl::purge_auth_users(
            state, identity, parts,
        ));
    }
    if upper.starts_with("SHOW AUTH USERS") {
        return Some(super::super::auth_user_ddl::show_auth_users(
            state, identity, parts,
        ));
    }

    // Organization management.
    if upper.starts_with("CREATE ORG ")
        || upper.starts_with("ALTER ORG ")
        || upper.starts_with("DROP ORG ")
    {
        return Some(super::super::org_ddl::handle_org(state, identity, parts));
    }
    if upper.starts_with("SHOW ORGS") {
        return Some(super::super::org_ddl::show_orgs(state, identity, parts));
    }
    if upper.starts_with("SHOW MEMBERS OF ORG") {
        return Some(super::super::org_ddl::show_members(state, identity, parts));
    }

    // Scope management.
    if upper.starts_with("DEFINE SCOPE ") {
        return Some(super::super::scope_ddl::define_scope(
            state, identity, parts,
        ));
    }
    if upper.starts_with("DROP SCOPE ") {
        return Some(super::super::scope_ddl::drop_scope(state, identity, parts));
    }
    if upper.starts_with("GRANT SCOPE ") {
        return Some(super::super::scope_ddl::grant_scope(state, identity, parts));
    }
    if upper.starts_with("REVOKE SCOPE ") {
        return Some(super::super::scope_ddl::revoke_scope(
            state, identity, parts,
        ));
    }
    if upper.starts_with("ALTER SCOPE ") {
        return Some(super::super::scope_query_ddl::alter_scope(
            state, identity, parts,
        ));
    }
    if upper.starts_with("SHOW MY SCOPES") {
        return Some(super::super::scope_query_ddl::show_my_scopes(
            state, identity, parts,
        ));
    }
    if upper.starts_with("SHOW SCOPES FOR ") {
        return Some(super::super::scope_query_ddl::show_scopes_for(
            state, identity, parts,
        ));
    }
    if upper.starts_with("RENEW SCOPE ") {
        return Some(super::super::scope_ddl::renew_scope(state, identity, parts));
    }

    // EXPLAIN TIERS ON <collection> [RANGE <start> <end>]
    if upper.starts_with("EXPLAIN TIERS ") {
        return Some(super::helpers::explain_tiers(state, identity, parts));
    }

    // EXPLAIN PERMISSION / EXPLAIN SCOPE.
    if upper.starts_with("EXPLAIN PERMISSION ") {
        return Some(super::super::explain_ddl::explain_permission(
            state, identity, parts,
        ));
    }
    if upper.starts_with("EXPLAIN SCOPE ") {
        return Some(super::super::explain_ddl::explain_scope(
            state, identity, parts,
        ));
    }

    // Emergency response.
    if upper.starts_with("EMERGENCY LOCKDOWN") {
        return Some(super::super::emergency_ddl::emergency_lockdown(
            state, identity, parts,
        ));
    }
    if upper.starts_with("EMERGENCY UNLOCK") {
        return Some(super::super::emergency_ddl::emergency_unlock(
            state, identity, parts,
        ));
    }
    if upper.starts_with("BLACKLIST AUTH USERS WHERE") {
        return Some(super::super::emergency_ddl::bulk_blacklist(
            state, identity, parts,
        ));
    }

    // Auth-scoped API keys.
    if upper.starts_with("CREATE AUTH KEY ") {
        return Some(super::super::auth_key_ddl::create_auth_key(
            state, identity, parts,
        ));
    }
    if upper.starts_with("ROTATE AUTH KEY ") {
        return Some(super::super::auth_key_ddl::rotate_auth_key(
            state, identity, parts,
        ));
    }
    if upper.starts_with("LIST AUTH KEYS") {
        return Some(super::super::auth_key_ddl::list_auth_keys(
            state, identity, parts,
        ));
    }

    // Impersonation & delegation.
    if upper.starts_with("IMPERSONATE AUTH USER ") {
        return Some(super::super::impersonation_ddl::impersonate(
            state, identity, parts,
        ));
    }
    if upper.starts_with("STOP IMPERSONATION") {
        return Some(super::super::impersonation_ddl::stop_impersonation(
            state, identity, parts,
        ));
    }
    if upper.starts_with("DELEGATE AUTH USER ") {
        return Some(super::super::impersonation_ddl::delegate(
            state, identity, parts,
        ));
    }
    if upper.starts_with("REVOKE DELEGATION ") {
        return Some(super::super::impersonation_ddl::revoke_delegation(
            state, identity, parts,
        ));
    }
    if upper.starts_with("SHOW DELEGATIONS") {
        return Some(super::super::impersonation_ddl::show_delegations(
            state, identity, parts,
        ));
    }

    // Session management.
    if upper.starts_with("SHOW SESSIONS") {
        return Some(super::super::session_ddl::show_sessions(
            state, identity, parts,
        ));
    }
    if upper.starts_with("KILL SESSION ") {
        return Some(super::super::session_ddl::kill_session(
            state, identity, parts,
        ));
    }
    if upper.starts_with("KILL USER SESSIONS ") {
        return Some(super::super::session_ddl::kill_user_sessions(
            state, identity, parts,
        ));
    }
    if upper.starts_with("VERIFY AUDIT CHAIN") {
        return Some(super::super::session_ddl::verify_audit_chain(
            state, identity, parts,
        ));
    }

    // Usage metering.
    if upper.starts_with("DEFINE METERING DIMENSION ") {
        return Some(super::super::metering_ddl::define_dimension(
            state, identity, parts,
        ));
    }
    if upper.starts_with("SHOW USAGE FOR TENANT ") {
        return Some(super::super::metering_ddl::show_usage_for_tenant(
            state, identity, parts,
        ));
    }
    if upper.starts_with("EXPORT USAGE ") {
        return Some(super::super::metering_ddl::export_usage(
            state, identity, parts,
        ));
    }
    if upper.starts_with("SHOW USAGE ") {
        return Some(super::super::metering_ddl::show_usage(
            state, identity, parts,
        ));
    }
    if upper.starts_with("SHOW QUOTA ") {
        return Some(super::super::metering_ddl::show_quota(
            state, identity, parts,
        ));
    }

    if upper.starts_with("SHOW SCOPE GRANTS") {
        return Some(super::super::scope_ddl::show_scope_grants(
            state, identity, parts,
        ));
    }
    if upper.starts_with("SHOW SCOPE") {
        return Some(super::super::scope_ddl::show_scopes(state, identity, parts));
    }

    // API keys.
    if upper.starts_with("CREATE API KEY ") {
        return Some(super::super::apikey::create_api_key(state, identity, parts));
    }
    if upper.starts_with("REVOKE API KEY ") {
        return Some(super::super::apikey::revoke_api_key(state, identity, parts));
    }
    if upper.starts_with("LIST API KEYS") {
        return Some(super::super::apikey::list_api_keys(state, identity, parts));
    }

    // Cluster management & observability.
    if upper.starts_with("SHOW CLUSTER") {
        return Some(super::super::cluster::show_cluster(state, identity));
    }
    if upper.starts_with("SHOW RAFT GROUPS") {
        return Some(super::super::cluster::show_raft_groups(state, identity));
    }
    if upper.starts_with("SHOW RAFT GROUP ") {
        return Some(super::super::cluster::show_raft_group(
            state, identity, parts,
        ));
    }
    // ALTER RAFT GROUP is fully dispatched via typed AST (ast.rs).
    if upper.starts_with("SHOW MIGRATIONS") {
        return Some(super::super::cluster::show_migrations(state, identity));
    }
    if upper.starts_with("REBALANCE") {
        return Some(super::super::cluster::rebalance(state, identity));
    }
    if upper.starts_with("SHOW PEER HEALTH") {
        return Some(super::super::cluster::show_peer_health(state, identity));
    }
    if upper.starts_with("SHOW NODES") {
        return Some(super::super::cluster::show_nodes(state, identity));
    }
    if upper.starts_with("SHOW NODE ") {
        return Some(super::super::cluster::show_node(state, identity, parts));
    }
    if upper.starts_with("REMOVE NODE ") {
        return Some(super::super::cluster::remove_node(state, identity, parts));
    }
    if upper.starts_with("SHOW RANGES") {
        return Some(super::super::cluster::show_ranges(state, identity));
    }
    if upper.starts_with("SHOW ROUTING") {
        return Some(super::super::cluster::show_routing(state, identity));
    }
    if upper.starts_with("SHOW SCHEMA VERSION") {
        return Some(super::super::cluster::show_schema_version(state, identity));
    }

    // Introspection.
    if upper.starts_with("SHOW USERS") {
        return Some(super::super::inspect::show_users(state, identity));
    }
    if upper.starts_with("SHOW TENANTS") {
        return Some(super::super::inspect::show_tenants(state, identity));
    }
    if upper.starts_with("SHOW SESSION") {
        return Some(super::super::inspect::show_session(identity));
    }
    if upper.starts_with("TRUNCATE AUDIT")
        || upper.starts_with("DELETE AUDIT")
        || upper.starts_with("CLEAR AUDIT")
    {
        return Some(Err(super::super::super::types::sqlstate_error(
            "42501",
            "audit log cannot be manually truncated. Entries are pruned automatically by the retention policy (audit_retention_days in config).",
        )));
    }
    if upper.starts_with("EXPORT AUDIT") {
        return Some(super::super::inspect::export_audit_log(
            state, identity, parts,
        ));
    }
    if upper.starts_with("SHOW AUDIT LOG") || upper.starts_with("SHOW AUDIT_LOG") {
        return Some(super::super::inspect::show_audit_log(
            state, identity, parts,
        ));
    }
    if upper.starts_with("SHOW PERMISSIONS") {
        return Some(super::super::inspect::show_permissions(
            state, identity, parts,
        ));
    }
    if upper.starts_with("SHOW GRANTS") {
        return Some(super::super::inspect::show_grants(state, identity, parts));
    }

    None
}
