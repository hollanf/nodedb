//! Main SQL execution entry point with transaction handling and DDL dispatch.

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

        if upper == "BEGIN" || upper == "BEGIN TRANSACTION" || upper == "START TRANSACTION" {
            // Capture current WAL LSN as the snapshot point for this transaction.
            let snapshot_lsn = {
                let next = self.state.wal.next_lsn();
                crate::types::Lsn::new(next.as_u64().saturating_sub(1))
            };
            self.sessions.begin(addr, snapshot_lsn).map_err(|msg| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "25P02".to_owned(),
                    msg.to_owned(),
                )))
            })?;
            return Ok(vec![Response::Execution(Tag::new("BEGIN"))]);
        }

        if upper == "COMMIT" || upper == "END" || upper == "END TRANSACTION" {
            // Snapshot isolation: check for write conflicts before committing.
            // If any collection read during this transaction has been modified
            // since the snapshot LSN, reject with serialization failure.
            let read_set = self.sessions.take_read_set(addr);
            if let Some(snapshot_lsn) = self.sessions.snapshot_lsn(addr) {
                let current_lsn = self.state.wal.next_lsn();
                let current = crate::types::Lsn::new(current_lsn.as_u64().saturating_sub(1));
                for (_collection, _doc_id, read_lsn) in &read_set {
                    if current > *read_lsn && current > snapshot_lsn {
                        // WAL advanced past what we read — concurrent write detected.
                        self.sessions.rollback(addr).ok();
                        return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                            "ERROR".to_owned(),
                            "40001".to_owned(),
                            "could not serialize access due to concurrent update".to_owned(),
                        ))));
                    }
                }
            }

            let buffered = self.sessions.commit(addr).map_err(|msg| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "25000".to_owned(),
                    msg.to_owned(),
                )))
            })?;

            if !buffered.is_empty() {
                let tenant_id = identity.tenant_id;
                let vshard_id = buffered[0].vshard_id;

                let mut sub_records: Vec<(u16, Vec<u8>)> = Vec::with_capacity(buffered.len());
                for task in &buffered {
                    if let Some(entry) = crate::control::wal_replication::to_replicated_entry(
                        task.tenant_id,
                        task.vshard_id,
                        &task.plan,
                    ) {
                        let bytes = entry.to_bytes();
                        sub_records.push((nodedb_wal::record::RecordType::Put as u16, bytes));
                    }
                }

                if !sub_records.is_empty() {
                    let tx_payload = rmp_serde::to_vec_named(&sub_records).map_err(|e| {
                        PgWireError::UserError(Box::new(ErrorInfo::new(
                            "ERROR".to_owned(),
                            "XX000".to_owned(),
                            format!("transaction WAL serialization failed: {e}"),
                        )))
                    })?;
                    self.state
                        .wal
                        .append_transaction(tenant_id, vshard_id, &tx_payload)
                        .map_err(|e| {
                            PgWireError::UserError(Box::new(ErrorInfo::new(
                                "ERROR".to_owned(),
                                "XX000".to_owned(),
                                format!("transaction WAL append failed: {e}"),
                            )))
                        })?;
                }

                // Dispatch all writes as a single TransactionBatch for
                // atomic execution on the Data Plane.
                let plans: Vec<crate::bridge::envelope::PhysicalPlan> =
                    buffered.iter().map(|t| t.plan.clone()).collect();
                let batch_task = crate::control::planner::physical::PhysicalTask {
                    tenant_id,
                    vshard_id,
                    plan: crate::bridge::envelope::PhysicalPlan::TransactionBatch { plans },
                };
                if let Err(e) = self.dispatch_task_no_wal(batch_task).await {
                    tracing::warn!(error = %e, "transaction batch dispatch failed");
                    return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(),
                        "40001".to_owned(),
                        format!("transaction commit failed: {e}"),
                    ))));
                }
            }

            return Ok(vec![Response::Execution(Tag::new("COMMIT"))]);
        }

        if upper == "ROLLBACK" || upper == "ABORT" {
            let _ = self.sessions.rollback(addr);
            return Ok(vec![Response::Execution(Tag::new("ROLLBACK"))]);
        }

        if upper.starts_with("SAVEPOINT ") {
            return Ok(vec![Response::Execution(Tag::new("SAVEPOINT"))]);
        }

        if upper.starts_with("RELEASE SAVEPOINT ") || upper.starts_with("RELEASE ") {
            return Ok(vec![Response::Execution(Tag::new("RELEASE"))]);
        }

        if upper.starts_with("ROLLBACK TO ") {
            return Ok(vec![Response::Execution(Tag::new("ROLLBACK"))]);
        }

        if self.sessions.transaction_state(addr) == TransactionState::Failed {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "25P02".to_owned(),
                "current transaction is aborted, commands ignored until end of transaction block"
                    .to_owned(),
            ))));
        }

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

        if upper.starts_with("EXPLAIN ") {
            return self.handle_explain(identity, sql_trimmed).await;
        }

        if let Some(result) = super::super::ddl::dispatch(&self.state, identity, sql_trimmed) {
            return result;
        }

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
}
