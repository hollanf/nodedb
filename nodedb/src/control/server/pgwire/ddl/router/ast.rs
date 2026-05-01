//! AST-based DDL dispatch — typed fast path.
//!
//! Runs before the legacy string-prefix routers. Handles
//! `IF [NOT] EXISTS` at the dispatch level so individual handlers
//! don't need to check. Falls through to legacy dispatch for
//! statements where the typed path is not yet wired (returns `None`).

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use nodedb_sql::ddl_ast::NodedbStatement;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::pgwire::ddl::alert::alter_alert;
use crate::control::server::pgwire::ddl::alert::{CreateAlertRequest, create_alert};
use crate::control::server::pgwire::ddl::change_stream::{
    alter_change_stream, create_change_stream,
};
use crate::control::server::pgwire::ddl::cluster::alter_raft_group;
use crate::control::server::pgwire::ddl::collection::alter::add_materialized_sum;
use crate::control::server::pgwire::ddl::collection::{
    CreateCollectionRequest, CreateIndexRequest, alter_collection_alter_column_type,
    alter_collection_drop_column, alter_collection_rename_column, alter_collection_set_append_only,
    alter_collection_set_last_value_cache, alter_collection_set_legal_hold,
    alter_collection_set_retention, alter_table_add_column, create_collection, create_index,
    create_table, dispatch_register_by_name,
};
use crate::control::server::pgwire::ddl::consumer_group::create_consumer_group;
use crate::control::server::pgwire::ddl::continuous_agg::{
    CreateContinuousAggregateRequest, create_continuous_aggregate,
};
use crate::control::server::pgwire::ddl::grant::permission::{grant_permission, revoke_permission};
use crate::control::server::pgwire::ddl::grant::role::{grant_role, revoke_role};
use crate::control::server::pgwire::ddl::inspect::show_permissions;
use crate::control::server::pgwire::ddl::materialized_view::create_materialized_view;
use crate::control::server::pgwire::ddl::ownership::alter_collection_owner;
use crate::control::server::pgwire::ddl::retention_policy::{
    alter_retention_policy, create_retention_policy,
};
use crate::control::server::pgwire::ddl::rls::{CreateRlsPolicyRequest, create_rls_policy};
use crate::control::server::pgwire::ddl::role::alter_role_typed;
use crate::control::server::pgwire::ddl::schedule::{
    CreateScheduleRequest, alter_schedule, create_schedule,
};
use crate::control::server::pgwire::ddl::sequence::{alter_sequence, create_sequence};
use crate::control::server::pgwire::ddl::trigger::{alter_trigger, create_trigger};
use crate::control::server::pgwire::ddl::user::{alter_user, create_user};
use crate::control::state::SharedState;
use nodedb_sql::ddl_ast::AlterCollectionOp;

/// Try to dispatch a parsed `NodedbStatement`. Returns `Some` if
/// fully handled, `None` if the statement should fall through to
/// the legacy dispatch.
pub(super) async fn try_dispatch(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    stmt: &NodedbStatement,
) -> Option<PgWireResult<Vec<Response>>> {
    match stmt {
        // ── IF NOT EXISTS: swallow duplicate-creation errors ──────
        NodedbStatement::CreateCollection {
            name,
            if_not_exists: true,
            ..
        } => {
            if collection_exists(state, identity, name) {
                return Some(Ok(vec![Response::Execution(Tag::new("CREATE COLLECTION"))]));
            }
            None // fall through to legacy CREATE handler
        }

        NodedbStatement::CreateTable {
            name,
            if_not_exists: true,
            ..
        } => {
            if collection_exists(state, identity, name) {
                return Some(Ok(vec![Response::Execution(Tag::new("CREATE TABLE"))]));
            }
            None // fall through to schema dispatcher
        }

        NodedbStatement::CreateSequence {
            name,
            if_not_exists: true,
            ..
        } => {
            if sequence_exists(state, identity, name) {
                return Some(Ok(vec![Response::Execution(Tag::new("CREATE SEQUENCE"))]));
            }
            None
        }

        // ── IF EXISTS: swallow not-found errors on DROP ──────────
        NodedbStatement::DropCollection {
            name,
            if_exists: true,
            ..
        } => {
            if !collection_exists(state, identity, name) {
                return Some(Ok(vec![Response::Execution(Tag::new("DROP COLLECTION"))]));
            }
            None
        }

        NodedbStatement::DropIndex {
            if_exists: true, ..
        } => None,

        NodedbStatement::DropTrigger {
            name,
            if_exists: true,
            ..
        } => {
            if !trigger_exists(state, identity, name) {
                return Some(Ok(vec![Response::Execution(Tag::new("DROP TRIGGER"))]));
            }
            None
        }

        NodedbStatement::DropSchedule {
            name,
            if_exists: true,
        } => {
            if !schedule_exists(state, identity, name) {
                return Some(Ok(vec![Response::Execution(Tag::new("DROP SCHEDULE"))]));
            }
            None
        }

        NodedbStatement::DropSequence {
            name,
            if_exists: true,
        } => {
            if !sequence_exists(state, identity, name) {
                return Some(Ok(vec![Response::Execution(Tag::new("DROP SEQUENCE"))]));
            }
            None
        }

        NodedbStatement::DropAlert {
            name,
            if_exists: true,
        } => {
            if !alert_exists(state, identity, name) {
                return Some(Ok(vec![Response::Execution(Tag::new("DROP ALERT"))]));
            }
            None
        }

        NodedbStatement::DropRetentionPolicy {
            name,
            if_exists: true,
        } => {
            if !retention_policy_exists(state, identity, name) {
                return Some(Ok(vec![Response::Execution(Tag::new(
                    "DROP RETENTION POLICY",
                ))]));
            }
            None
        }

        NodedbStatement::DropChangeStream {
            name,
            if_exists: true,
        } => {
            if !change_stream_exists(state, identity, name) {
                return Some(Ok(vec![Response::Execution(Tag::new(
                    "DROP CHANGE STREAM",
                ))]));
            }
            None
        }

        NodedbStatement::DropMaterializedView {
            name,
            if_exists: true,
        } => {
            if !materialized_view_exists(state, identity, name) {
                return Some(Ok(vec![Response::Execution(Tag::new(
                    "DROP MATERIALIZED VIEW",
                ))]));
            }
            None
        }

        NodedbStatement::DropContinuousAggregate {
            name,
            if_exists: true,
        } => {
            if !continuous_aggregate_exists(state, identity, name) {
                return Some(Ok(vec![Response::Execution(Tag::new(
                    "DROP CONTINUOUS AGGREGATE",
                ))]));
            }
            None
        }

        NodedbStatement::DropRlsPolicy {
            name,
            collection,
            if_exists: true,
        } => {
            let tid = identity.tenant_id.as_u64();
            if !state.rls.policy_exists(tid, collection, name) {
                return Some(Ok(vec![Response::Execution(Tag::new("DROP RLS POLICY"))]));
            }
            None
        }

        NodedbStatement::DropConsumerGroup {
            name,
            stream,
            if_exists: true,
        } => {
            let tid = identity.tenant_id.as_u64();
            if state.group_registry.get(tid, stream, name).is_none() {
                return Some(Ok(vec![Response::Execution(Tag::new(
                    "DROP CONSUMER GROUP",
                ))]));
            }
            None
        }

        // ── Batch 1: typed dispatch (no raw_sql) ─────────────────
        NodedbStatement::GrantRole { role, username } => {
            Some(grant_role(state, identity, role, username))
        }

        NodedbStatement::RevokeRole { role, username } => {
            Some(revoke_role(state, identity, role, username))
        }

        NodedbStatement::AlterAlert { name, action } => {
            Some(alter_alert(state, identity, name, action))
        }

        NodedbStatement::AlterChangeStream { name, action } => {
            Some(alter_change_stream(state, identity, name, action))
        }

        NodedbStatement::BackupTenant { .. } => {
            // Backup is delivered via COPY framing; the pgwire copy handler
            // intercepts the COPY command before DDL dispatch reaches here.
            // If this variant arrives via simple query, return the guidance error.
            Some(Err(super::super::super::types::sqlstate_error(
                "0A000",
                "use `COPY (BACKUP TENANT <id>) TO STDOUT` to stream backup bytes to the client",
            )))
        }

        NodedbStatement::RestoreTenant { .. } => {
            Some(Err(super::super::super::types::sqlstate_error(
                "0A000",
                "use `COPY tenant_restore(<id>) FROM STDIN` to stream backup bytes from the client",
            )))
        }

        NodedbStatement::AlterTrigger {
            name,
            action,
            new_owner,
        } => Some(alter_trigger(
            state,
            identity,
            name,
            action,
            new_owner.as_deref(),
        )),

        NodedbStatement::AlterRaftGroup {
            group_id,
            action,
            node_id,
        } => Some(alter_raft_group(state, identity, group_id, action, node_id)),

        // ── Batch 2: typed dispatch ───────────────────────────────
        NodedbStatement::GrantPermission {
            permission,
            target_type,
            target_name,
            grantee,
        } => Some(grant_permission(
            state,
            identity,
            permission,
            target_type,
            target_name,
            grantee,
        )),

        NodedbStatement::RevokePermission {
            permission,
            target_type,
            target_name,
            grantee,
        } => Some(revoke_permission(
            state,
            identity,
            permission,
            target_type,
            target_name,
            grantee,
        )),

        NodedbStatement::AlterSchedule {
            name,
            action,
            cron_expr,
        } => Some(alter_schedule(
            state,
            identity,
            name,
            action,
            cron_expr.as_deref(),
        )),

        NodedbStatement::AlterRetentionPolicy {
            name,
            action,
            set_key,
            set_value,
        } => Some(alter_retention_policy(
            state,
            identity,
            name,
            action,
            set_key.as_deref(),
            set_value.as_deref(),
        )),

        NodedbStatement::AlterSequence {
            name,
            action,
            with_value,
        } => Some(alter_sequence(
            state,
            identity,
            name,
            action,
            with_value.as_deref(),
        )),

        NodedbStatement::CreateConsumerGroup {
            group_name,
            stream_name,
        } => Some(create_consumer_group(
            state,
            identity,
            group_name,
            stream_name,
        )),

        // ── Batch 3: typed dispatch ───────────────────────────────
        NodedbStatement::CreateAlert {
            name,
            collection,
            where_filter,
            condition_raw,
            group_by,
            window_raw,
            fire_after,
            recover_after,
            severity,
            notify_targets_raw,
        } => Some(create_alert(
            state,
            identity,
            &CreateAlertRequest {
                name,
                collection,
                where_filter: where_filter.as_deref(),
                condition_raw,
                group_by,
                window_raw,
                fire_after: *fire_after,
                recover_after: *recover_after,
                severity,
                notify_targets_raw,
            },
        )),

        NodedbStatement::CreateTrigger {
            or_replace,
            execution_mode,
            name,
            timing,
            events_insert,
            events_update,
            events_delete,
            collection,
            granularity,
            when_condition,
            priority,
            security,
            body_sql,
        } => Some(create_trigger(
            state,
            identity,
            *or_replace,
            execution_mode,
            name,
            timing,
            *events_insert,
            *events_update,
            *events_delete,
            collection,
            granularity,
            when_condition.as_deref(),
            *priority,
            security,
            body_sql,
        )),

        NodedbStatement::CreateSchedule {
            name,
            cron_expr,
            body_sql,
            scope,
            missed_policy,
            allow_overlap,
        } => Some(create_schedule(
            state,
            identity,
            &CreateScheduleRequest {
                name,
                cron_expr,
                body_sql,
                scope,
                missed_policy,
                allow_overlap: *allow_overlap,
            },
        )),

        NodedbStatement::CreateChangeStream {
            name,
            collection,
            with_clause_raw,
        } => Some(create_change_stream(
            state,
            identity,
            name,
            collection,
            with_clause_raw,
        )),

        NodedbStatement::CreateMaterializedView {
            name,
            source,
            query_sql,
            refresh_mode,
        } => Some(
            create_materialized_view(state, identity, name, source, query_sql, refresh_mode).await,
        ),

        NodedbStatement::CreateContinuousAggregate {
            name,
            source,
            bucket_raw,
            aggregate_exprs_raw,
            group_by,
            with_clause_raw,
        } => Some(
            create_continuous_aggregate(
                state,
                identity,
                &CreateContinuousAggregateRequest {
                    name,
                    source,
                    bucket_raw,
                    aggregate_exprs_raw,
                    group_by,
                    with_clause_raw,
                },
            )
            .await,
        ),

        NodedbStatement::CreateRetentionPolicy {
            name,
            collection,
            body_raw,
            eval_interval_raw,
        } => Some(
            create_retention_policy(
                state,
                identity,
                name,
                collection,
                body_raw,
                eval_interval_raw.as_deref(),
            )
            .await,
        ),

        // ── Batch 4: typed dispatch ───────────────────────────────
        NodedbStatement::CreateUser {
            username,
            password,
            role,
            tenant_id,
        } => Some(create_user(
            state,
            identity,
            username,
            password,
            role.as_deref(),
            *tenant_id,
        )),

        NodedbStatement::AlterUser { username, op } => {
            Some(alter_user(state, identity, username, op))
        }

        // ── RLS policy typed dispatch ─────────────────────────────
        NodedbStatement::CreateRlsPolicy {
            name,
            collection,
            policy_type,
            predicate_raw,
            is_restrictive,
            on_deny_raw,
            tenant_id_override,
        } => Some(create_rls_policy(
            state,
            identity,
            &CreateRlsPolicyRequest {
                name,
                collection,
                policy_type_raw: policy_type,
                predicate_raw,
                is_restrictive: *is_restrictive,
                on_deny_raw: on_deny_raw.as_deref(),
                tenant_id_override: *tenant_id_override,
            },
        )),

        NodedbStatement::CreateSequence {
            name,
            if_not_exists: false,
            start,
            increment,
            min_value,
            max_value,
            cycle,
            cache,
            format_template_raw,
            reset_period_raw,
            gap_free,
            scope,
        } => Some(create_sequence(
            state,
            identity,
            name,
            *start,
            *increment,
            *min_value,
            *max_value,
            *cycle,
            *cache,
            format_template_raw.as_deref(),
            reset_period_raw.as_deref(),
            *gap_free,
            scope.as_deref(),
        )),

        NodedbStatement::CreateIndex {
            unique,
            index_name,
            collection,
            field,
            case_insensitive,
            where_condition,
        } => Some(
            create_index(
                state,
                identity,
                &CreateIndexRequest {
                    is_unique: *unique,
                    index_name_opt: index_name.as_deref(),
                    collection,
                    field,
                    case_insensitive: *case_insensitive,
                    where_condition: where_condition.as_deref(),
                },
            )
            .await,
        ),

        NodedbStatement::CreateCollection {
            name,
            if_not_exists: false,
            engine,
            columns,
            options,
            flags,
            balanced_raw,
        } => {
            let result = create_collection(
                state,
                identity,
                &CreateCollectionRequest {
                    name,
                    engine: engine.as_deref(),
                    columns,
                    options,
                    flags,
                    balanced_raw: balanced_raw.as_deref(),
                },
            );
            if result.is_ok() {
                dispatch_register_by_name(state, identity, name).await;
            }
            Some(result)
        }

        NodedbStatement::CreateTable {
            name,
            if_not_exists: false,
            engine,
            columns,
            options,
            flags,
            balanced_raw,
        } => {
            let result = create_table(
                state,
                identity,
                &CreateCollectionRequest {
                    name,
                    engine: engine.as_deref(),
                    columns,
                    options,
                    flags,
                    balanced_raw: balanced_raw.as_deref(),
                },
            )
            .await;
            if result.is_ok() {
                dispatch_register_by_name(state, identity, name).await;
            }
            Some(result)
        }

        NodedbStatement::AlterCollection { name, operation } => {
            Some(dispatch_alter_collection(state, identity, name, operation).await)
        }

        NodedbStatement::ShowPermissions {
            on_collection,
            for_grantee,
        } => Some(show_permissions(
            state,
            identity,
            on_collection.as_deref(),
            for_grantee.as_deref(),
        )),

        NodedbStatement::AlterRole { name, sub_op } => {
            Some(alter_role_typed(state, identity, name, sub_op))
        }

        // All other variants fall through to legacy dispatch.
        _ => None,
    }
}

async fn dispatch_alter_collection(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
    operation: &AlterCollectionOp,
) -> PgWireResult<Vec<Response>> {
    match operation {
        AlterCollectionOp::AddColumn {
            column_name,
            column_type,
            not_null,
            default_expr,
        } => {
            // Reconstruct a column definition string for parse_origin_column_def.
            let mut col_def = format!("{column_name} {column_type}");
            if *not_null {
                col_def.push_str(" NOT NULL");
            }
            if let Some(def) = default_expr {
                col_def.push_str(&format!(" DEFAULT {def}"));
            }
            alter_table_add_column(state, identity, name, &col_def).await
        }

        AlterCollectionOp::DropColumn { column_name } => {
            alter_collection_drop_column(state, identity, name, column_name).await
        }

        AlterCollectionOp::RenameColumn { old_name, new_name } => {
            alter_collection_rename_column(state, identity, name, old_name, new_name).await
        }

        AlterCollectionOp::AlterColumnType {
            column_name,
            new_type,
        } => alter_collection_alter_column_type(state, identity, name, column_name, new_type).await,

        AlterCollectionOp::OwnerTo { new_owner } => {
            alter_collection_owner(state, identity, name, new_owner)
        }

        AlterCollectionOp::SetRetention { value } => {
            alter_collection_set_retention(state, identity, name, value)
        }

        AlterCollectionOp::SetAppendOnly => alter_collection_set_append_only(state, identity, name),

        AlterCollectionOp::SetLastValueCache { enabled } => {
            alter_collection_set_last_value_cache(state, identity, name, *enabled)
        }

        AlterCollectionOp::SetLegalHold { enabled, tag } => {
            alter_collection_set_legal_hold(state, identity, name, *enabled, tag)
        }

        AlterCollectionOp::AddMaterializedSum {
            target_collection,
            target_column,
            source_collection,
            join_column,
            value_expr,
        } => add_materialized_sum(
            state,
            identity,
            target_collection,
            target_column,
            source_collection,
            join_column,
            value_expr,
        ),
    }
}

fn collection_exists(state: &SharedState, identity: &AuthenticatedIdentity, name: &str) -> bool {
    let Some(catalog) = state.credentials.catalog() else {
        return false;
    };
    let tid = identity.tenant_id.as_u64();
    matches!(catalog.get_collection(tid, name), Ok(Some(_)))
}

fn trigger_exists(state: &SharedState, identity: &AuthenticatedIdentity, name: &str) -> bool {
    let Some(catalog) = state.credentials.catalog() else {
        return false;
    };
    let tid = identity.tenant_id.as_u64();
    matches!(catalog.get_trigger(tid, name), Ok(Some(_)))
}

fn schedule_exists(state: &SharedState, identity: &AuthenticatedIdentity, name: &str) -> bool {
    let tid = identity.tenant_id.as_u64();
    state.schedule_registry.get(tid, name).is_some()
}

fn sequence_exists(state: &SharedState, identity: &AuthenticatedIdentity, name: &str) -> bool {
    let tid = identity.tenant_id.as_u64();
    state.sequence_registry.exists(tid, name)
}

fn alert_exists(state: &SharedState, identity: &AuthenticatedIdentity, name: &str) -> bool {
    let tid = identity.tenant_id.as_u64();
    state.alert_registry.get(tid, name).is_some()
}

fn retention_policy_exists(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
) -> bool {
    let tid = identity.tenant_id.as_u64();
    state.retention_policy_registry.get(tid, name).is_some()
}

fn change_stream_exists(state: &SharedState, identity: &AuthenticatedIdentity, name: &str) -> bool {
    let tid = identity.tenant_id.as_u64();
    state.stream_registry.get(tid, name).is_some()
}

fn materialized_view_exists(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
) -> bool {
    let tid = identity.tenant_id.as_u64();
    state.mv_registry.get_def(tid, name).is_some()
}

fn continuous_aggregate_exists(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
) -> bool {
    let tid = identity.tenant_id.as_u64();
    state.mv_registry.get_def(tid, name).is_some()
}
