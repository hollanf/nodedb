use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::sink::Sink;
use futures::stream;

use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response, Tag};
use pgwire::api::stmt::NoopQueryParser;
use pgwire::api::{ClientInfo, ClientPortalStore};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;

use crate::bridge::envelope::{PhysicalPlan, Priority, Request};
use crate::config::auth::AuthMode;
use crate::control::planner::context::QueryContext;
use crate::control::planner::physical::PhysicalTask;
use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::{
    AuthMethod, AuthenticatedIdentity, Role, required_permission, role_grants_permission,
};
use crate::control::state::SharedState;
use crate::types::{ReadConsistency, RequestId, TenantId};

use super::types::{error_to_sqlstate, response_status_to_sqlstate, text_field};

/// Default request deadline: 30 seconds.
const DEFAULT_DEADLINE: Duration = Duration::from_secs(30);

/// PostgreSQL wire protocol handler for NodeDB.
///
/// Implements `SimpleQueryHandler` + `ExtendedQueryHandler`.
/// Receives SQL strings from clients (psql, drivers), resolves the
/// authenticated identity, checks permissions, plans via DataFusion,
/// dispatches to the Data Plane via SPSC, and returns results.
///
/// Lives on the Control Plane (Send + Sync).
pub struct NodeDbPgHandler {
    pub(crate) state: Arc<SharedState>,
    query_ctx: QueryContext,
    next_request_id: AtomicU64,
    query_parser: Arc<NoopQueryParser>,
    auth_mode: AuthMode,
}

impl NodeDbPgHandler {
    pub fn new(state: Arc<SharedState>, auth_mode: AuthMode) -> Self {
        Self {
            state,
            query_ctx: QueryContext::new(),
            next_request_id: AtomicU64::new(1_000_000),
            query_parser: Arc::new(NoopQueryParser::new()),
            auth_mode,
        }
    }

    fn next_request_id(&self) -> RequestId {
        RequestId::new(self.next_request_id.fetch_add(1, Ordering::Relaxed))
    }

    /// Resolve the authenticated identity from pgwire client metadata.
    ///
    /// In password mode: looks up the username (set during SCRAM handshake) in
    /// the credential store. In trust mode: returns a default superuser identity.
    fn resolve_identity<C: ClientInfo>(&self, client: &C) -> PgWireResult<AuthenticatedIdentity> {
        let username = client
            .metadata()
            .get("user")
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());

        match self.auth_mode {
            AuthMode::Trust => {
                // Trust mode: check if user exists in credential store, otherwise
                // return a default superuser identity for backward compatibility.
                if let Some(identity) = self
                    .state
                    .credentials
                    .to_identity(&username, AuthMethod::Trust)
                {
                    Ok(identity)
                } else {
                    Ok(AuthenticatedIdentity {
                        user_id: 0,
                        username,
                        tenant_id: TenantId::new(1),
                        auth_method: AuthMethod::Trust,
                        roles: vec![Role::Superuser],
                        is_superuser: true,
                    })
                }
            }
            AuthMode::Password | AuthMode::Certificate => {
                // Password/cert mode: user MUST exist (was authenticated by SCRAM).
                self.state
                    .credentials
                    .to_identity(&username, AuthMethod::ScramSha256)
                    .ok_or_else(|| {
                        PgWireError::UserError(Box::new(ErrorInfo::new(
                            "FATAL".to_owned(),
                            "28000".to_owned(),
                            format!(
                                "authenticated user '{username}' not found in credential store"
                            ),
                        )))
                    })
            }
        }
    }

    /// Check if the identity has permission for the given plan.
    fn check_permission(
        &self,
        identity: &AuthenticatedIdentity,
        plan: &PhysicalPlan,
    ) -> PgWireResult<()> {
        if identity.is_superuser {
            return Ok(());
        }

        let required = required_permission(plan);
        let has_permission = identity
            .roles
            .iter()
            .any(|role| role_grants_permission(role, required));

        if has_permission {
            Ok(())
        } else {
            // Audit the denial.
            self.state.audit_record(
                AuditEvent::AuthzDenied,
                Some(identity.tenant_id),
                &identity.username,
                &format!("permission {:?} denied", required),
            );

            Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42501".to_owned(),
                format!(
                    "permission denied: user '{}' lacks {:?} permission",
                    identity.username, required
                ),
            ))))
        }
    }

    /// Dispatch a single physical task and wait for the response.
    async fn dispatch_task(
        &self,
        task: PhysicalTask,
    ) -> crate::Result<crate::bridge::envelope::Response> {
        let request_id = self.next_request_id();
        let request = Request {
            request_id,
            tenant_id: task.tenant_id,
            vshard_id: task.vshard_id,
            plan: task.plan,
            deadline: Instant::now() + DEFAULT_DEADLINE,
            priority: Priority::Normal,
            trace_id: 0,
            consistency: ReadConsistency::Strong,
        };

        let rx = self.state.tracker.register(request_id);

        match self.state.dispatcher.lock() {
            Ok(mut d) => d.dispatch(request)?,
            Err(poisoned) => poisoned.into_inner().dispatch(request)?,
        };

        let response = tokio::time::timeout(DEFAULT_DEADLINE, rx)
            .await
            .map_err(|_| crate::Error::DeadlineExceeded { request_id })?
            .map_err(|_| crate::Error::Dispatch {
                detail: "response channel closed".into(),
            })?;

        Ok(response)
    }

    /// Try to handle a statement as a Control Plane DDL command.
    ///
    /// These commands are executed directly on the Control Plane without
    /// going through DataFusion or the Data Plane. Returns `None` if the
    /// statement is not a recognized DDL command.
    fn handle_ddl(
        &self,
        identity: &AuthenticatedIdentity,
        sql: &str,
    ) -> Option<PgWireResult<Vec<Response>>> {
        let upper = sql.to_uppercase();
        let parts: Vec<&str> = sql.split_whitespace().collect();

        // CREATE USER <name> WITH PASSWORD '<password>' [ROLE <role>] [TENANT <id>]
        if upper.starts_with("CREATE USER ") {
            return Some(self.handle_create_user(identity, &parts));
        }

        // DROP USER <name>
        if upper.starts_with("DROP USER ") {
            return Some(self.handle_drop_user(identity, &parts));
        }

        None
    }

    fn handle_create_user(
        &self,
        identity: &AuthenticatedIdentity,
        parts: &[&str],
    ) -> PgWireResult<Vec<Response>> {
        // Only superuser or tenant_admin can create users.
        if !identity.is_superuser && !identity.has_role(&Role::TenantAdmin) {
            return Err(sqlstate_error("42501", "permission denied: only superuser or tenant_admin can create users"));
        }

        // Parse: CREATE USER <name> WITH PASSWORD '<password>' [ROLE <role>] [TENANT <id>]
        if parts.len() < 6 {
            return Err(sqlstate_error("42601", "syntax: CREATE USER <name> WITH PASSWORD '<password>' [ROLE <role>] [TENANT <id>]"));
        }

        let username = parts[2];
        if !parts[3].eq_ignore_ascii_case("WITH") || !parts[4].eq_ignore_ascii_case("PASSWORD") {
            return Err(sqlstate_error("42601", "expected WITH PASSWORD after username"));
        }

        // Extract password (handle single-quoted strings).
        let password = extract_quoted_string(parts, 5)
            .ok_or_else(|| sqlstate_error("42601", "password must be a single-quoted string"))?;

        // Parse optional ROLE and TENANT.
        let mut role = Role::ReadWrite;
        let mut tenant_id = identity.tenant_id;
        let mut i = next_after_quoted(parts, 5);
        while i < parts.len() {
            match parts[i].to_uppercase().as_str() {
                "ROLE" if i + 1 < parts.len() => {
                    role = parts[i + 1].parse().unwrap_or(Role::ReadWrite);
                    i += 2;
                }
                "TENANT" if i + 1 < parts.len() => {
                    if !identity.is_superuser {
                        return Err(sqlstate_error("42501", "only superuser can assign tenants"));
                    }
                    let tid: u32 = parts[i + 1].parse().map_err(|_| {
                        sqlstate_error("42601", "TENANT must be a numeric ID")
                    })?;
                    tenant_id = TenantId::new(tid);
                    i += 2;
                }
                _ => i += 1,
            }
        }

        self.state
            .credentials
            .create_user(username, &password, tenant_id, vec![role])
            .map_err(|e| sqlstate_error("42710", &e.to_string()))?;

        self.state.audit_record(
            AuditEvent::PrivilegeChange,
            Some(tenant_id),
            &identity.username,
            &format!("created user '{username}' in tenant {tenant_id}"),
        );

        Ok(vec![Response::Execution(Tag::new("CREATE USER"))])
    }

    fn handle_drop_user(
        &self,
        identity: &AuthenticatedIdentity,
        parts: &[&str],
    ) -> PgWireResult<Vec<Response>> {
        if !identity.is_superuser && !identity.has_role(&Role::TenantAdmin) {
            return Err(sqlstate_error("42501", "permission denied: only superuser or tenant_admin can drop users"));
        }

        if parts.len() < 3 {
            return Err(sqlstate_error("42601", "syntax: DROP USER <name>"));
        }

        let username = parts[2];

        // Prevent dropping yourself.
        if username == identity.username {
            return Err(sqlstate_error("42501", "cannot drop your own user"));
        }

        let dropped = self
            .state
            .credentials
            .deactivate_user(username)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

        if dropped {
            self.state.audit_record(
                AuditEvent::PrivilegeChange,
                Some(identity.tenant_id),
                &identity.username,
                &format!("dropped user '{username}'"),
            );
            Ok(vec![Response::Execution(Tag::new("DROP USER"))])
        } else {
            Err(sqlstate_error("42704", &format!("user '{username}' does not exist")))
        }
    }

    /// Execute a SQL query end-to-end: resolve identity → DDL check → plan → quota → perms → dispatch.
    async fn execute_sql(
        &self,
        identity: &AuthenticatedIdentity,
        sql: &str,
    ) -> PgWireResult<Vec<Response>> {
        let sql_trimmed = sql.trim();

        // Handle SET commands that pgwire clients send during connection setup.
        if sql_trimmed.to_uppercase().starts_with("SET ") {
            return Ok(vec![Response::Execution(Tag::new("SET"))]);
        }

        // Handle DISCARD ALL (sent by connection poolers).
        if sql_trimmed.eq_ignore_ascii_case("DISCARD ALL") {
            return Ok(vec![Response::Execution(Tag::new("DISCARD ALL"))]);
        }

        // Handle empty/semicolon-only.
        if sql_trimmed.is_empty() || sql_trimmed == ";" {
            return Ok(vec![Response::EmptyQuery]);
        }

        // Try Control Plane DDL commands first (CREATE USER, DROP USER, etc.).
        if let Some(result) = self.handle_ddl(identity, sql_trimmed) {
            return result;
        }

        // Tenant derived from authenticated identity — never from client.
        let tenant_id = identity.tenant_id;

        // Tenant quota check before planning.
        self.state.check_tenant_quota(tenant_id).map_err(|e| {
            let (severity, code, message) = error_to_sqlstate(&e);
            PgWireError::UserError(Box::new(ErrorInfo::new(
                severity.to_owned(),
                code.to_owned(),
                message,
            )))
        })?;

        // Track request for quota accounting.
        self.state.tenant_request_start(tenant_id);

        // Plan via DataFusion.
        let result = self.execute_planned_sql(identity, sql, tenant_id).await;

        // Always decrement active request count.
        self.state.tenant_request_end(tenant_id);

        result
    }

    /// Plan and dispatch SQL after quota and DDL checks have passed.
    async fn execute_planned_sql(
        &self,
        identity: &AuthenticatedIdentity,
        sql: &str,
        tenant_id: TenantId,
    ) -> PgWireResult<Vec<Response>> {
        let tasks = self.query_ctx.plan_sql(sql, tenant_id).await.map_err(|e| {
            let (severity, code, message) = error_to_sqlstate(&e);
            PgWireError::UserError(Box::new(ErrorInfo::new(
                severity.to_owned(),
                code.to_owned(),
                message,
            )))
        })?;

        if tasks.is_empty() {
            return Ok(vec![Response::Execution(Tag::new("OK"))]);
        }

        let mut responses = Vec::with_capacity(tasks.len());
        for task in tasks {
            self.check_permission(identity, &task.plan)?;

            let plan_kind = describe_plan(&task.plan);
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

            let pg_response = payload_to_response(&resp.payload, plan_kind);
            responses.push(pg_response);
        }

        Ok(responses)
    }
}

// ── Plan classification ─────────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
enum PlanKind {
    SingleDocument,
    MultiRow,
    Execution,
}

fn describe_plan(plan: &PhysicalPlan) -> PlanKind {
    match plan {
        PhysicalPlan::PointGet { .. } | PhysicalPlan::CrdtRead { .. } => PlanKind::SingleDocument,
        PhysicalPlan::VectorSearch { .. }
        | PhysicalPlan::RangeScan { .. }
        | PhysicalPlan::GraphHop { .. }
        | PhysicalPlan::GraphNeighbors { .. }
        | PhysicalPlan::GraphPath { .. }
        | PhysicalPlan::GraphSubgraph { .. }
        | PhysicalPlan::GraphRagFusion { .. } => PlanKind::MultiRow,
        _ => PlanKind::Execution,
    }
}

fn payload_to_response(payload: &[u8], kind: PlanKind) -> Response {
    match kind {
        PlanKind::Execution => Response::Execution(Tag::new("OK")),
        PlanKind::SingleDocument | PlanKind::MultiRow => {
            let col_name = if matches!(kind, PlanKind::SingleDocument) {
                "document"
            } else {
                "result"
            };
            let schema = Arc::new(vec![text_field(col_name)]);
            if payload.is_empty() {
                Response::Query(QueryResponse::new(schema, stream::empty()))
            } else {
                let text = String::from_utf8_lossy(payload).into_owned();
                let mut encoder = DataRowEncoder::new(schema.clone());
                if let Err(e) = encoder.encode_field(&text) {
                    tracing::error!(error = %e, "failed to encode field");
                    return Response::Execution(Tag::new("ERROR"));
                }
                let row = encoder.take_row();
                Response::Query(QueryResponse::new(schema, stream::iter(vec![Ok(row)])))
            }
        }
    }
}

// ── DDL helpers ─────────────────────────────────────────────────────

/// Create a pgwire ErrorResponse with a SQLSTATE code.
fn sqlstate_error(code: &str, message: &str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        code.to_owned(),
        message.to_owned(),
    )))
}

/// Extract a single-quoted string from split parts starting at index `start`.
/// Handles passwords like 'hello world' spanning multiple parts.
fn extract_quoted_string(parts: &[&str], start: usize) -> Option<String> {
    if start >= parts.len() {
        return None;
    }

    let first = parts[start];
    if !first.starts_with('\'') {
        return None;
    }

    // Single part: 'password'
    if first.ends_with('\'') && first.len() > 1 {
        return Some(first[1..first.len() - 1].to_string());
    }

    // Multi-part: 'hello world'
    let mut result = first[1..].to_string();
    for &part in &parts[start + 1..] {
        result.push(' ');
        if part.ends_with('\'') {
            result.push_str(&part[..part.len() - 1]);
            return Some(result);
        }
        result.push_str(part);
    }

    None // Unterminated quote.
}

/// Find the index of the first part after a quoted string starting at `start`.
fn next_after_quoted(parts: &[&str], start: usize) -> usize {
    if start >= parts.len() {
        return parts.len();
    }

    let first = parts[start];
    if !first.starts_with('\'') {
        return start + 1;
    }

    if first.ends_with('\'') && first.len() > 1 {
        return start + 1;
    }

    for (i, part) in parts[start + 1..].iter().enumerate() {
        if part.ends_with('\'') {
            return start + 1 + i + 1;
        }
    }
    parts.len()
}

// ── SimpleQueryHandler ──────────────────────────────────────────────

#[async_trait]
impl SimpleQueryHandler for NodeDbPgHandler {
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let identity = self.resolve_identity(client)?;
        self.execute_sql(&identity, query).await
    }
}

// ── ExtendedQueryHandler ────────────────────────────────────────────

#[async_trait]
impl ExtendedQueryHandler for NodeDbPgHandler {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.query_parser.clone()
    }

    async fn do_query<C>(
        &self,
        client: &mut C,
        portal: &pgwire::api::portal::Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let identity = self.resolve_identity(client)?;
        let query = &portal.statement.statement;
        let mut results = self.execute_sql(&identity, query).await?;
        Ok(results.pop().unwrap_or(Response::EmptyQuery))
    }
}

// Trust mode: NoopStartupHandler (no authentication).
impl NoopStartupHandler for NodeDbPgHandler {
    // Record auth success (trust mode) after startup completes.
}
