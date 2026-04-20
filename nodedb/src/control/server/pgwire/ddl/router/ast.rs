//! AST-based DDL dispatch — typed fast path.
//!
//! Runs before the legacy string-prefix routers. Handles
//! `IF [NOT] EXISTS` at the dispatch level so individual handlers
//! don't need to check. Falls through to legacy dispatch for
//! `Other` variants and for statements where the typed path
//! delegates to the existing handler (via `raw_sql`).

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use nodedb_sql::ddl_ast::NodedbStatement;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

/// Try to dispatch a parsed `NodedbStatement`. Returns `Some` if
/// fully handled, `None` if the statement should fall through to
/// the legacy dispatch.
pub(super) fn try_dispatch(
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
        } => None, // legacy handler has its own check

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
            if_exists: true, ..
        } => {
            // RLS policy existence check would need collection context;
            // fall through to legacy handler which already handles this.
            None
        }

        NodedbStatement::DropConsumerGroup {
            if_exists: true, ..
        } => None, // legacy handler

        // All other variants fall through to legacy dispatch.
        _ => None,
    }
}

fn collection_exists(state: &SharedState, identity: &AuthenticatedIdentity, name: &str) -> bool {
    let Some(catalog) = state.credentials.catalog() else {
        return false;
    };
    let tid = identity.tenant_id.as_u32();
    matches!(catalog.get_collection(tid, name), Ok(Some(_)))
}

fn trigger_exists(state: &SharedState, identity: &AuthenticatedIdentity, name: &str) -> bool {
    let Some(catalog) = state.credentials.catalog() else {
        return false;
    };
    let tid = identity.tenant_id.as_u32();
    matches!(catalog.get_trigger(tid, name), Ok(Some(_)))
}

fn schedule_exists(state: &SharedState, identity: &AuthenticatedIdentity, name: &str) -> bool {
    let tid = identity.tenant_id.as_u32();
    state.schedule_registry.get(tid, name).is_some()
}

fn sequence_exists(state: &SharedState, identity: &AuthenticatedIdentity, name: &str) -> bool {
    let tid = identity.tenant_id.as_u32();
    state.sequence_registry.exists(tid, name)
}

fn alert_exists(state: &SharedState, identity: &AuthenticatedIdentity, name: &str) -> bool {
    let tid = identity.tenant_id.as_u32();
    state.alert_registry.get(tid, name).is_some()
}

fn retention_policy_exists(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
) -> bool {
    let tid = identity.tenant_id.as_u32();
    state.retention_policy_registry.get(tid, name).is_some()
}

fn change_stream_exists(state: &SharedState, identity: &AuthenticatedIdentity, name: &str) -> bool {
    let tid = identity.tenant_id.as_u32();
    state.stream_registry.get(tid, name).is_some()
}

fn materialized_view_exists(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
) -> bool {
    let tid = identity.tenant_id.as_u32();
    state.mv_registry.get_def(tid, name).is_some()
}

fn continuous_aggregate_exists(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
) -> bool {
    let tid = identity.tenant_id.as_u32();
    state.mv_registry.get_def(tid, name).is_some()
}
