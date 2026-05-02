//! Plan-and-dispatch entry points for SQL queries on the simple-query and
//! extended-query (prepared-statement) paths, plus per-task consistency
//! selection.

use std::sync::Arc;

use pgwire::api::results::{Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::planner::physical::{PhysicalTask, PostSetOp};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::types::{ReadConsistency, TenantId};

use super::super::super::types::{error_to_sqlstate, response_status_to_sqlstate};
use super::super::core::NodeDbPgHandler;
use super::super::plan::{describe_plan, extract_collection, payload_to_response};
use super::catalog::current_descriptor_version;
use super::kv_wrapping::maybe_wrap_kv_point_get;
use super::set_ops;

impl NodeDbPgHandler {
    /// Plan and dispatch SQL after quota and DDL checks have passed.
    ///
    /// When in a transaction block (BEGIN..COMMIT), write operations are
    /// buffered instead of dispatched. Read operations execute immediately.
    /// The buffer is dispatched atomically on COMMIT.
    ///
    /// This is the simple-query entry point (no bound parameters). After
    /// dispatching, the SELECT projection list is parsed from `sql` and
    /// each query response is re-encoded with one pgwire field per projected
    /// column. The extended-query path (`execute_planned_sql_with_params`)
    /// skips this step because `execute_prepared` applies column projection
    /// using the richer schema from the Describe phase.
    pub(in crate::control::server::pgwire::handler) async fn execute_planned_sql(
        &self,
        identity: &AuthenticatedIdentity,
        sql: &str,
        tenant_id: TenantId,
        addr: &std::net::SocketAddr,
    ) -> PgWireResult<Vec<Response>> {
        let responses = self
            .execute_planned_sql_inner(identity, sql, tenant_id, addr, &[])
            .await?;

        // Column projection: re-encode each query response with one pgwire
        // field per named column from the SELECT list.
        //
        // `payload_to_response` produces a single-column envelope (`result`
        // or `document`) with the full row as JSON. SQL clients (psql,
        // tokio-postgres simple_query, psycopg2, etc.) expect one field per
        // projected column. Parse the SELECT list and reproject if it
        // contains named (non-star) columns.
        let projection = {
            use super::super::projection::{
                fields_for_projection, lookup_keys_for_projection, needs_projection,
                parse_select_projection,
            };
            parse_select_projection(sql)
                .filter(|items| needs_projection(items))
                .map(|items| {
                    let fields = fields_for_projection(&items);
                    let keys = lookup_keys_for_projection(&items);
                    (fields, keys)
                })
        };

        if let Some((fields, keys)) = projection {
            let mut projected = Vec::with_capacity(responses.len());
            for resp in responses {
                projected.push(
                    super::super::projection::reproject_response(resp, &fields, &keys)
                        .await
                        .map_err(|e| {
                            PgWireError::UserError(Box::new(ErrorInfo::new(
                                "ERROR".to_owned(),
                                "XX000".to_owned(),
                                format!("column projection failed: {e}"),
                            )))
                        })?,
                );
            }
            return Ok(projected);
        }

        Ok(responses)
    }

    /// Execute planned SQL with bound parameters (prepared statement path).
    pub(in crate::control::server::pgwire::handler) async fn execute_planned_sql_with_params(
        &self,
        identity: &AuthenticatedIdentity,
        sql: &str,
        tenant_id: TenantId,
        addr: &std::net::SocketAddr,
        params: &[nodedb_sql::ParamValue],
    ) -> PgWireResult<Vec<Response>> {
        self.execute_planned_sql_inner(identity, sql, tenant_id, addr, params)
            .await
    }

    async fn execute_planned_sql_inner(
        &self,
        identity: &AuthenticatedIdentity,
        sql: &str,
        tenant_id: TenantId,
        addr: &std::net::SocketAddr,
        params: &[nodedb_sql::ParamValue],
    ) -> PgWireResult<Vec<Response>> {
        // Resolve opaque session handle if SET LOCAL nodedb.auth_session is set.
        // Bind the resolve to the caller's (tenant_id, peer IP) fingerprint so
        // a handle leaked cross-origin does not grant access. The
        // store also enforces per-connection rate limits + emits audit events
        // on miss spikes; a `RateLimited` outcome here becomes a
        // fatal pgwire error that closes the connection.
        let caller_fp = crate::control::security::session_handle::ClientFingerprint::from_peer(
            identity.tenant_id,
            addr,
        );
        let conn_key = addr.to_string();
        let mut auth_ctx =
            if let Some(handle) = self.sessions.get_parameter(addr, "nodedb.auth_session") {
                use crate::control::security::session_handle::ResolveOutcome;
                match self
                    .state
                    .session_handles
                    .resolve(&handle, &conn_key, &caller_fp)
                {
                    ResolveOutcome::Resolved(cached) => *cached,
                    ResolveOutcome::RateLimited => {
                        return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                            "FATAL".to_owned(),
                            "53300".to_owned(),
                            "session handle resolve rate limit exceeded on this \
                         connection — closing"
                                .to_owned(),
                        ))));
                    }
                    ResolveOutcome::Miss => {
                        crate::control::server::session_auth::build_auth_context_with_session(
                            identity,
                            &self.sessions,
                            addr,
                        )
                    }
                }
            } else {
                crate::control::server::session_auth::build_auth_context_with_session(
                    identity,
                    &self.sessions,
                    addr,
                )
            };

        // Extract per-query ON DENY override.
        let clean_sql =
            crate::control::server::session_auth::extract_and_apply_on_deny(sql, &mut auth_ctx);

        // Strip RETURNING clause before DataFusion planning.
        let (clean_sql, returning_spec) = super::super::returning::strip_returning(&clean_sql)
            .map_err(|e| {
                use super::super::super::types::error_to_sqlstate;
                let (severity, code, message) = error_to_sqlstate(&e);
                pgwire::error::PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new(
                    severity.to_owned(),
                    code.to_owned(),
                    message,
                )))
            })?;
        let has_returning = returning_spec.is_some();

        // Enforce general CHECK constraints for INSERT/UPDATE before planning.
        self.enforce_check_constraints_if_needed(&clean_sql, tenant_id)
            .await?;

        // Check plan cache before full planning. The cache
        // hits are per-descriptor-version: unrelated DDLs do
        // not invalidate unrelated cached plans.
        let cached_tasks = {
            let state = Arc::clone(&self.state);
            let tenant = tenant_id.as_u64();
            self.sessions.get_cached_plan(addr, &clean_sql, move |id| {
                current_descriptor_version(&state, tenant, id)
            })
        };

        // Produce `(tasks, lease_scope)` for both the cache-hit
        // and fresh-plan paths so the scope binding can outlive
        // the planning block. The parameterised path
        // (prepared statements with bound params) currently
        // skips the plan cache and the refcounted lease — that
        // is preserved by returning an empty scope for it.
        let (tasks, _plan_lease_scope) = if !params.is_empty() {
            let perm_cache = self.state.permission_cache.read().await;
            let sec = crate::control::planner::context::PlanSecurityContext {
                identity,
                auth: &auth_ctx,
                rls_store: &self.state.rls,
                permissions: &self.state.permissions,
                roles: &self.state.roles,
                permission_cache: Some(&*perm_cache),
            };
            let tasks = self
                .query_ctx
                .plan_sql_with_params_and_rls(&clean_sql, params, tenant_id, &sec)
                .await
                .map_err(|e| {
                    let (severity, code, message) = error_to_sqlstate(&e);
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        severity.to_owned(),
                        code.to_owned(),
                        message,
                    )))
                })?;
            (tasks, crate::control::lease::QueryLeaseScope::empty())
        } else if let Some((tasks, versions)) = cached_tasks {
            // Cache hit: tasks were compiled by a prior query
            // that has since dropped its scope. Re-acquire the
            // refcount for this query's duration so drain
            // still sees "leases held" while we execute.
            let scope = self.state.acquire_plan_lease_scope(&versions);
            (tasks, scope)
        } else {
            // Retry transparently on `RetryableSchemaChanged`
            // so pgwire clients see a stable view of DDL in
            // flight. The retry helper re-runs the entire
            // plan, which re-reads each descriptor and
            // records its current version. Each iteration
            // re-reads the permission cache because the
            // RwLock guard cannot be held across awaits
            // inside the closure.
            let (planned, versions) = super::super::retry::retry_on_schema_change(|| async {
                let perm_cache = self.state.permission_cache.read().await;
                let sec = crate::control::planner::context::PlanSecurityContext {
                    identity,
                    auth: &auth_ctx,
                    rls_store: &self.state.rls,
                    permissions: &self.state.permissions,
                    roles: &self.state.roles,
                    permission_cache: Some(&*perm_cache),
                };
                self.query_ctx
                    .plan_sql_with_rls_and_versions(&clean_sql, tenant_id, &sec, has_returning)
                    .await
            })
            .await
            .map_err(|e| {
                let (severity, code, message) = error_to_sqlstate(&e);
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    severity.to_owned(),
                    code.to_owned(),
                    message,
                )))
            })?;

            // Acquire the scope BEFORE writing the plan into
            // the cache so a concurrent cache-hit cannot race
            // us into an empty scope.
            let scope = self.state.acquire_plan_lease_scope(&versions);

            self.sessions
                .put_cached_plan(addr, &clean_sql, planned.clone(), versions);

            (planned, scope)
        };

        // Inject RETURNING spec into DML plans. The planner strips RETURNING
        // from the SQL before DataFusion sees it; the spec is re-attached here
        // so the Data Plane knows which columns to project and return.
        let tasks = if let Some(ref spec) = returning_spec {
            tasks
                .into_iter()
                .map(|mut task| {
                    inject_returning_spec(&mut task.plan, spec.clone());
                    task
                })
                .collect()
        } else {
            tasks
        };

        if tasks.is_empty() {
            return Ok(vec![Response::Execution(Tag::new("OK"))]);
        }

        let consistency = self.consistency_for_tasks(&tasks);

        // When all tasks target a remote leader, route through the gateway.
        // The gateway ships the pre-planned PhysicalPlan via ExecuteRequest
        // (plan bytes over QUIC) instead of the old SQL-string ForwardRequest.
        if self.should_forward_via_gateway(&tasks, consistency) {
            return self.dispatch_tasks_via_gateway(tasks, tenant_id).await;
        }

        let needs_set_op = tasks.iter().any(|t| t.post_set_op != PostSetOp::None);
        let mut dedup_payloads: Vec<Vec<u8>> = Vec::new();
        let mut dedup_set_op = PostSetOp::None;
        let mut responses = Vec::with_capacity(tasks.len());
        for mut task in tasks {
            if task.tenant_id != tenant_id {
                tracing::error!(
                    expected = %tenant_id,
                    actual = %task.tenant_id,
                    "SECURITY: task tenant_id mismatch — rejecting"
                );
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42501".to_owned(),
                    "tenant isolation violation: task targets wrong tenant".to_owned(),
                ))));
            }

            self.check_permission(identity, &task.plan)?;

            // ClusterArray plans are handled entirely on the Control Plane by the
            // ArrayCoordinator — they must never reach the SPSC bridge or
            // trigger/DML machinery. Intercept them here and short-circuit.
            if let crate::bridge::physical_plan::PhysicalPlan::ClusterArray(ref cluster_op) =
                task.plan
            {
                use crate::control::cluster::ClusterArrayExecutor;
                use crate::control::server::pgwire::handler::plan::PlanKind;

                let transport = self.state.cluster_transport.as_ref().ok_or_else(|| {
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(),
                        "XX000".to_owned(),
                        "cluster transport not available for ClusterArray dispatch".to_owned(),
                    )))
                })?;
                let routing = self.state.cluster_routing.as_ref().ok_or_else(|| {
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(),
                        "XX000".to_owned(),
                        "cluster routing not available for ClusterArray dispatch".to_owned(),
                    )))
                })?;
                let executor = ClusterArrayExecutor::new(
                    Arc::clone(transport),
                    Arc::clone(routing),
                    self.state.node_id,
                    Arc::clone(&self.state),
                );
                let payload_bytes = executor.execute(cluster_op).await.map_err(|e| {
                    let (severity, code, message) = error_to_sqlstate(&e);
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        severity.to_owned(),
                        code.to_owned(),
                        message,
                    )))
                })?;
                // Slice responses are wrapped in `ArraySliceResponse`; all
                // other cluster array ops return plain msgpack.
                let cluster_plan_kind = match cluster_op {
                    crate::bridge::physical_plan::ClusterArrayOp::Slice { .. } => {
                        PlanKind::ArraySlice
                    }
                    _ => PlanKind::MultiRow,
                };
                let shaped = payload_to_response(&payload_bytes, cluster_plan_kind);
                if let Some(notice) = shaped.notice {
                    self.sessions.push_notice(addr, notice);
                }
                responses.push(shaped.response);
                continue;
            }

            if self.sessions.transaction_state(addr)
                == crate::control::server::pgwire::session::TransactionState::InBlock
            {
                let is_write = crate::control::wal_replication::to_replicated_entry(
                    task.tenant_id,
                    task.vshard_id,
                    &task.plan,
                )
                .is_some();
                if is_write {
                    self.sessions.buffer_write(addr, task);
                    responses.push(Response::Execution(Tag::new("OK")));
                    continue;
                }
            }

            let plan_kind = describe_plan(&task.plan);
            let collection_for_si = extract_collection(&task.plan).map(String::from);
            let resp_post_set_op = task.post_set_op;
            // Clone the plan up front so response-shaping (which runs after
            // `dispatch_task` consumes `task`) still has it available.
            let plan_for_response = task.plan.clone();

            // --- Trigger interception for DML writes ---
            let mut dml_info = crate::control::trigger::dml_hook::classify_dml_write(&task.plan);

            // Fetch OLD row and fire BEFORE/INSTEAD OF triggers if applicable.
            // UPSERT sets `needs_existence_probe` so the probe decides whether
            // AFTER INSERT or AFTER UPDATE triggers fire — post-dispatch routing
            // branches on `info.event`, which we override here based on the
            // probe result before the BEFORE / AFTER hooks run.
            let old_row = if let Some(ref info) = dml_info
                && info.document_id.is_some()
                && (matches!(
                    info.event,
                    crate::control::trigger::DmlEvent::Update
                        | crate::control::trigger::DmlEvent::Delete
                ) || info.needs_existence_probe)
            {
                let doc_id = info.document_id.as_deref().unwrap_or("");
                let row = crate::control::trigger::dml_hook::fetch_old_row(
                    &self.state,
                    tenant_id,
                    &info.collection,
                    doc_id,
                )
                .await;
                if !row.is_empty() { Some(row) } else { None }
            } else {
                None
            };

            // Probe-driven reclassification: UPSERT onto an existing row is an
            // UPDATE for trigger purposes; onto a fresh key it's an INSERT.
            // `needs_existence_probe` is the signal that `event` was a
            // placeholder and must be refined from the probe result.
            if let Some(ref mut info) = dml_info
                && info.needs_existence_probe
            {
                info.event = if old_row.is_some() {
                    crate::control::trigger::DmlEvent::Update
                } else {
                    crate::control::trigger::DmlEvent::Insert
                };
            }

            if let Some(ref info) = dml_info {
                use crate::control::trigger::dml_hook::PreDispatchResult;
                match crate::control::trigger::dml_hook::fire_pre_dispatch_triggers(
                    &self.state,
                    identity,
                    tenant_id,
                    info,
                    &old_row,
                    0,
                )
                .await
                .map_err(|e| {
                    let (severity, code, message) = error_to_sqlstate(&e);
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        severity.to_owned(),
                        code.to_owned(),
                        message,
                    )))
                })? {
                    PreDispatchResult::Handled => {
                        responses.push(Response::Execution(Tag::new("OK")));
                        continue;
                    }
                    PreDispatchResult::Proceed {
                        mutated_fields: Some(fields),
                    } => {
                        crate::control::trigger::dml_hook::patch_task_with_mutated_fields(
                            &mut task, &fields,
                        );
                    }
                    PreDispatchResult::Proceed {
                        mutated_fields: None,
                    } => {}
                }
            }

            // Extract truncate restart_identity info before task is moved into dispatch.
            let truncate_restart_collection =
                if let crate::bridge::physical_plan::PhysicalPlan::Document(
                    crate::bridge::physical_plan::DocumentOp::Truncate {
                        collection,
                        restart_identity: true,
                    },
                ) = &task.plan
                {
                    Some(collection.clone())
                } else {
                    None
                };

            // --- Normal dispatch ---
            let resp = self.dispatch_task(task).await.map_err(|e| {
                let (severity, code, message) = error_to_sqlstate(&e);
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    severity.to_owned(),
                    code.to_owned(),
                    message,
                )))
            })?;

            if let Some((severity, code, message)) =
                response_status_to_sqlstate(resp.status, &resp.error_code)
            {
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    severity.to_owned(),
                    code.to_owned(),
                    message,
                ))));
            }

            // --- TRUNCATE RESTART IDENTITY ---
            if let Some(collection) = &truncate_restart_collection {
                self.state
                    .sequence_registry
                    .restart_sequences_for_collection(tenant_id.as_u64(), collection);
            }

            // --- SYNC AFTER triggers ---
            if let Some(ref info) = dml_info {
                crate::control::trigger::dml_hook::fire_post_dispatch_triggers(
                    &self.state,
                    identity,
                    tenant_id,
                    info,
                    &old_row,
                    0,
                )
                .await
                .map_err(|e| {
                    let (severity, code, message) = error_to_sqlstate(&e);
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        severity.to_owned(),
                        code.to_owned(),
                        message,
                    )))
                })?;

                self.state
                    .dml_counter
                    .record_dml(tenant_id.as_u64(), &info.collection);
            }

            // Track reads for snapshot isolation conflict detection.
            if self.sessions.transaction_state(addr)
                == crate::control::server::pgwire::session::TransactionState::InBlock
                && let Some(collection) = collection_for_si
            {
                self.sessions
                    .record_read(addr, collection, String::new(), resp.watermark_lsn);
            }

            if needs_set_op && resp_post_set_op != PostSetOp::None {
                dedup_payloads.push(resp.payload.to_vec());
                if dedup_set_op == PostSetOp::None {
                    dedup_set_op = resp_post_set_op;
                }
            } else {
                // KV point-get responses arrive as the stored value map
                // (`{col: val, ...}`) — the primary-key column isn't in the
                // stored value, so inject it before the pgwire layer turns
                // the map into a SQL row. Matches the convention used by
                // `execute_kv_scan`, which already injects `key` at the
                // engine level.
                let payload = maybe_wrap_kv_point_get(&plan_for_response, &resp.payload);
                let payload = crate::control::server::response_translate::translate_if_vector(
                    &payload,
                    &plan_for_response,
                    &self.state,
                );
                let shaped = payload_to_response(&payload, plan_kind);
                if let Some(notice) = shaped.notice {
                    self.sessions.push_notice(addr, notice);
                }
                responses.push(shaped.response);
            }
        }

        // Set operations: merge sub-query payloads.
        if needs_set_op && !dedup_payloads.is_empty() {
            responses.push(set_ops::apply_set_ops(&dedup_payloads, dedup_set_op));
        }

        Ok(responses)
    }

    /// Determine read consistency for a set of tasks.
    fn consistency_for_tasks(&self, tasks: &[PhysicalTask]) -> ReadConsistency {
        let has_writes = tasks.iter().any(|t| {
            crate::control::wal_replication::to_replicated_entry(t.tenant_id, t.vshard_id, &t.plan)
                .is_some()
        });

        if has_writes {
            ReadConsistency::Strong
        } else {
            ReadConsistency::BoundedStaleness(std::time::Duration::from_secs(5))
        }
    }
}

/// Inject a RETURNING spec into a DML physical plan variant.
///
/// Only `PointUpdate`, `BulkUpdate`, `PointDelete`, and `BulkDelete` are
/// affected. All other plan variants are left unchanged.
fn inject_returning_spec(
    plan: &mut crate::bridge::envelope::PhysicalPlan,
    spec: crate::bridge::physical_plan::ReturningSpec,
) {
    use crate::bridge::envelope::PhysicalPlan;
    use crate::bridge::physical_plan::DocumentOp;

    match plan {
        PhysicalPlan::Document(DocumentOp::PointUpdate { returning, .. }) => {
            *returning = Some(spec);
        }
        PhysicalPlan::Document(DocumentOp::BulkUpdate { returning, .. }) => {
            *returning = Some(spec);
        }
        PhysicalPlan::Document(DocumentOp::PointDelete { returning, .. }) => {
            *returning = Some(spec);
        }
        PhysicalPlan::Document(DocumentOp::BulkDelete { returning, .. }) => {
            *returning = Some(spec);
        }
        _ => {}
    }
}
