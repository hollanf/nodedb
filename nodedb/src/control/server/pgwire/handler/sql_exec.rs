//! Main SQL execution entry point with DDL dispatch.
//!
//! Transaction commands (BEGIN/COMMIT/ROLLBACK/SAVEPOINT) are in `transaction_cmds.rs`.
//! Session commands (SET/SHOW/RESET/DISCARD) are in `session_cmds.rs`.
//! Cursor commands (DECLARE/FETCH/MOVE/CLOSE) are in `cursor_cmds.rs`.

use std::sync::Arc;

use pgwire::api::results::{DataRowEncoder, QueryResponse, Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::security::identity::AuthenticatedIdentity;

use super::super::session::TransactionState;
use super::super::types::text_field;
use super::core::NodeDbPgHandler;

impl NodeDbPgHandler {
    /// Execute a SQL query: session state → identity → DDL check → quota → plan → perms → dispatch.
    pub(super) async fn execute_sql(
        &self,
        identity: &AuthenticatedIdentity,
        addr: &std::net::SocketAddr,
        sql: &str,
    ) -> PgWireResult<Vec<Response>> {
        use super::super::types::error_to_sqlstate;

        let sql_trimmed = sql.trim();
        let upper = sql_trimmed.to_uppercase();

        self.sessions.ensure_session(*addr);

        if sql_trimmed.is_empty() || sql_trimmed == ";" {
            return Ok(vec![Response::EmptyQuery]);
        }

        // ── Transaction commands ──────────────────────────────────────

        if upper == "BEGIN" || upper == "BEGIN TRANSACTION" || upper == "START TRANSACTION" {
            return self.handle_begin(addr);
        }

        if upper == "COMMIT" || upper == "END" || upper == "END TRANSACTION" {
            return self.handle_commit(identity, addr).await;
        }

        if upper == "ROLLBACK" || upper == "ABORT" {
            return self.handle_rollback(identity, addr);
        }

        if let Some(result) = self.try_handle_deferred_offset(identity, addr, sql_trimmed, &upper) {
            return result;
        }

        if upper.starts_with("SAVEPOINT ") {
            return self.handle_savepoint(addr, sql_trimmed);
        }

        if upper.starts_with("RELEASE SAVEPOINT ") || upper.starts_with("RELEASE ") {
            return self.handle_release_savepoint(addr, sql_trimmed);
        }

        if upper.starts_with("ROLLBACK TO ") {
            return self.handle_rollback_to_savepoint(addr, sql_trimmed);
        }

        // ── Cursor commands ───────────────────────────────────────────

        if upper.starts_with("DECLARE ") && upper.contains(" CURSOR ") {
            let scrollable =
                upper.contains(" SCROLL CURSOR") && !upper.contains(" NO SCROLL CURSOR");
            let with_hold = upper.contains(" WITH HOLD ");
            let parts: Vec<&str> = sql_trimmed.split_whitespace().collect();
            let cursor_name = parts.get(1).unwrap_or(&"default").to_string();
            if let Some(for_pos) = upper.find(" FOR ") {
                let inner_sql = sql_trimmed[for_pos + 5..].trim();
                match self
                    .execute_query_for_cursor(addr, inner_sql, identity)
                    .await
                {
                    Ok(rows) => {
                        let spill_config =
                            super::super::session::cursor_spill::CursorSpillConfig::default();
                        let (rows, _truncated) =
                            super::super::session::cursor_spill::enforce_cursor_limit(
                                rows,
                                &spill_config,
                            );
                        self.sessions.declare_cursor(
                            addr,
                            cursor_name,
                            rows,
                            scrollable,
                            with_hold,
                        );
                        return Ok(vec![Response::Execution(Tag::new("DECLARE CURSOR"))]);
                    }
                    Err(e) => return Err(e),
                }
            }
            return Ok(vec![Response::Execution(Tag::new("DECLARE CURSOR"))]);
        }

        if upper.starts_with("FETCH ") {
            return self.handle_fetch(addr, sql_trimmed, &upper);
        }

        if upper.starts_with("MOVE ") {
            return self.handle_move(addr, &upper);
        }

        if upper.starts_with("CLOSE ") {
            let cursor_name = sql_trimmed
                .split_whitespace()
                .nth(1)
                .unwrap_or("default")
                .to_string();
            self.sessions.close_cursor(addr, &cursor_name);
            return Ok(vec![Response::Execution(Tag::new("CLOSE CURSOR"))]);
        }

        // ── Failed transaction guard ──────────────────────────────────

        if self.sessions.transaction_state(addr) == TransactionState::Failed {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "25P02".to_owned(),
                "current transaction is aborted, commands ignored until end of transaction block"
                    .to_owned(),
            ))));
        }

        // ── Session commands ──────────────────────────────────────────

        if upper.starts_with("SET ") {
            return self.handle_set(addr, sql_trimmed);
        }

        if upper == "SHOW CONNECTIONS" {
            let schema = Arc::new(vec![
                text_field("peer_address"),
                text_field("transaction_state"),
            ]);
            let sessions = self.sessions.all_sessions();
            let mut rows = Vec::with_capacity(sessions.len());
            let mut encoder = DataRowEncoder::new(schema.clone());
            for (addr_str, tx_state) in &sessions {
                encoder.encode_field(addr_str)?;
                encoder.encode_field(tx_state)?;
                rows.push(Ok(encoder.take_row()));
            }
            return Ok(vec![Response::Query(QueryResponse::new(
                schema,
                futures::stream::iter(rows),
            ))]);
        }

        if upper.starts_with("KILL CONNECTION ") {
            if !identity.is_superuser {
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42501".to_owned(),
                    "permission denied: only superuser can kill connections".to_owned(),
                ))));
            }
            let target = sql_trimmed[16..]
                .trim()
                .trim_matches('\'')
                .trim_matches('"');
            if let Ok(target_addr) = target.parse::<std::net::SocketAddr>() {
                self.sessions.remove(&target_addr);
                return Ok(vec![Response::Execution(Tag::new("KILL"))]);
            }
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42601".to_owned(),
                format!("invalid connection address: '{target}'. Use SHOW CONNECTIONS to list."),
            ))));
        }

        if upper.starts_with("SHOW ")
            && !upper.starts_with("SHOW USERS")
            && !upper.starts_with("SHOW TENANTS")
            && !upper.starts_with("SHOW SESSION")
            && !upper.starts_with("SHOW CLUSTER")
            && !upper.starts_with("SHOW RAFT")
            && !upper.starts_with("SHOW MIGRATIONS")
            && !upper.starts_with("SHOW PEER")
            && !upper.starts_with("SHOW NODES")
            && !upper.starts_with("SHOW NODE ")
            && !upper.starts_with("SHOW COLLECTIONS")
            && !upper.starts_with("SHOW AUDIT")
            && !upper.starts_with("SHOW PERMISSIONS")
            && !upper.starts_with("SHOW GRANTS")
            && upper != "SHOW CONNECTIONS"
            && !upper.starts_with("SHOW INDEXES")
        {
            return self.handle_show(addr, sql_trimmed);
        }

        if upper.starts_with("RESET ") {
            let param = sql_trimmed[6..].trim().to_lowercase();
            self.sessions.set_parameter(addr, param, String::new());
            return Ok(vec![Response::Execution(Tag::new("RESET"))]);
        }

        if upper == "DISCARD ALL" {
            self.sessions.remove(addr);
            self.sessions.ensure_session(*addr);
            return Ok(vec![Response::Execution(Tag::new("DISCARD ALL"))]);
        }

        // ── Prepared statements ───────────────────────────────────────

        if upper.starts_with("PREPARE ") {
            return self.handle_prepare(addr, sql_trimmed);
        }
        if upper.starts_with("EXECUTE ") {
            return self.handle_execute(identity, addr, sql_trimmed).await;
        }
        if upper.starts_with("DEALLOCATE ") {
            return self.handle_deallocate(addr, sql_trimmed);
        }

        if upper.starts_with("EXPLAIN ") {
            return self.handle_explain(identity, sql_trimmed).await;
        }

        // ── Special query forms ───────────────────────────────────────

        if upper.starts_with("LIVE SELECT ") {
            return self.handle_live_select(identity, addr, sql_trimmed);
        }

        if upper.starts_with("SELECT FACET_COUNTS") {
            return super::facet::execute_facet_counts_sql(self, identity, addr, sql_trimmed).await;
        }

        if upper.starts_with("SELECT SEARCH_WITH_FACETS") {
            return super::facet::execute_search_with_facets_sql(self, identity, addr, sql_trimmed)
                .await;
        }

        // ── DDL / Temp tables ─────────────────────────────────────────

        if upper.starts_with("CREATE TEMPORARY TABLE ") || upper.starts_with("CREATE TEMP TABLE ") {
            return super::super::ddl::temp_table::create_temp_table(
                &self.sessions,
                identity,
                addr,
                sql_trimmed,
            );
        }

        if let Some(result) = super::super::ddl::dispatch(&self.state, identity, sql_trimmed).await
        {
            return result;
        }

        // ── DataFusion-planned query execution ────────────────────────

        let tenant_id = identity.tenant_id;

        self.state.check_tenant_quota(tenant_id).map_err(|e| {
            let (severity, code, message) = error_to_sqlstate(&e);
            PgWireError::UserError(Box::new(ErrorInfo::new(
                severity.to_owned(),
                code.to_owned(),
                message,
            )))
        })?;

        self.state.tenant_request_start(tenant_id);
        let result = self
            .execute_planned_sql(identity, sql_trimmed, tenant_id, addr)
            .await;
        self.state.tenant_request_end(tenant_id);

        if result.is_err() {
            self.sessions.fail_transaction(addr);
        }

        result
    }

    /// Handle LIVE SELECT: create a change stream subscription.
    fn handle_live_select(
        &self,
        identity: &AuthenticatedIdentity,
        addr: &std::net::SocketAddr,
        sql: &str,
    ) -> PgWireResult<Vec<Response>> {
        let coll_name = super::super::ddl::sql_parse::extract_collection_after(sql, " FROM ")
            .ok_or_else(|| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42601".to_owned(),
                    "syntax: LIVE SELECT [*|fields] FROM <collection> [WHERE ...]".to_owned(),
                )))
            })?;

        let tenant_id = identity.tenant_id;
        let sub = self
            .state
            .change_stream
            .subscribe(Some(coll_name.clone()), Some(tenant_id));
        let sub_id = sub.id;
        let channel = format!("live_{coll_name}");

        self.sessions
            .add_live_subscription(addr, channel.clone(), sub);

        tracing::info!(
            sub_id,
            collection = coll_name,
            channel,
            "LIVE SELECT subscription created"
        );

        use futures::stream;
        let schema = Arc::new(vec![
            text_field("subscription_id"),
            text_field("channel"),
            text_field("collection"),
            text_field("status"),
        ]);
        let mut encoder = DataRowEncoder::new(schema.clone());
        let _ = encoder.encode_field(&sub_id.to_string());
        let _ = encoder.encode_field(&channel);
        let _ = encoder.encode_field(&coll_name);
        let _ = encoder.encode_field(&"active");
        let row = encoder.take_row();
        Ok(vec![Response::Query(QueryResponse::new(
            schema,
            stream::iter(vec![Ok(row)]),
        ))])
    }

    /// Execute a SELECT query and return results as JSON strings for cursor storage.
    pub(super) async fn execute_query_for_cursor(
        &self,
        _addr: &std::net::SocketAddr,
        sql: &str,
        identity: &AuthenticatedIdentity,
    ) -> PgWireResult<Vec<String>> {
        let tenant_id = identity.tenant_id;
        let query_ctx = crate::control::planner::context::QueryContext::for_state(
            &self.state,
            tenant_id.as_u32(),
        );

        if let Some(mode) = self.sessions.get_parameter(_addr, "rounding_mode") {
            query_ctx.set_rounding_mode(&mode);
        }

        let auth_ctx = crate::control::server::session_auth::build_auth_context(identity);
        let perm_cache = self.state.permission_cache.read().await;
        let sec = crate::control::planner::context::PlanSecurityContext {
            identity,
            auth: &auth_ctx,
            rls_store: &self.state.rls,
            permissions: &self.state.permissions,
            roles: &self.state.roles,
            permission_cache: Some(&*perm_cache),
        };
        let tasks = query_ctx
            .plan_sql_with_rls(sql, tenant_id, &sec)
            .await
            .map_err(|e| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42000".to_owned(),
                    e.to_string(),
                )))
            })?;

        let mut rows = Vec::new();
        for task in tasks {
            let resp = crate::control::server::dispatch_utils::dispatch_to_data_plane(
                &self.state,
                task.tenant_id,
                task.vshard_id,
                task.plan,
                0,
            )
            .await
            .map_err(|e| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "XX000".to_owned(),
                    e.to_string(),
                )))
            })?;

            if !resp.payload.is_empty() {
                let json =
                    crate::data::executor::response_codec::decode_payload_to_json(&resp.payload);
                rows.push(json);
            }
        }
        Ok(rows)
    }
}
