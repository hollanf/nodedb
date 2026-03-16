use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::sink::Sink;
use futures::stream;

use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::auth::{
    AuthSource, DefaultServerParameterProvider, LoginInfo, Password, StartupHandler,
};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response, Tag};
use pgwire::api::stmt::NoopQueryParser;
use pgwire::api::{ClientInfo, ClientPortalStore, PgWireServerHandlers};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;

use crate::bridge::envelope::{PhysicalPlan, Priority, Request};
use crate::config::auth::AuthMode;
use crate::control::planner::context::QueryContext;
use crate::control::planner::physical::PhysicalTask;
use crate::control::security::audit::AuditEvent;
use crate::control::security::credential::CredentialStore;
use crate::control::state::SharedState;
use crate::types::{ReadConsistency, RequestId, TenantId};

use super::types::{error_to_sqlstate, response_status_to_sqlstate, text_field};

/// Default request deadline: 30 seconds.
const DEFAULT_DEADLINE: Duration = Duration::from_secs(30);

/// PostgreSQL wire protocol handler for NodeDB.
///
/// Implements `SimpleQueryHandler` + `ExtendedQueryHandler`.
/// Receives SQL strings from clients (psql, drivers), plans them via
/// DataFusion, dispatches to the Data Plane via SPSC, and returns results.
///
/// Lives on the Control Plane (Send + Sync).
pub struct NodeDbPgHandler {
    state: Arc<SharedState>,
    query_ctx: QueryContext,
    next_request_id: AtomicU64,
    query_parser: Arc<NoopQueryParser>,
}

impl NodeDbPgHandler {
    pub fn new(state: Arc<SharedState>) -> Self {
        Self {
            state,
            query_ctx: QueryContext::new(),
            next_request_id: AtomicU64::new(1_000_000),
            query_parser: Arc::new(NoopQueryParser::new()),
        }
    }

    fn next_request_id(&self) -> RequestId {
        RequestId::new(self.next_request_id.fetch_add(1, Ordering::Relaxed))
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

    /// Execute a SQL query end-to-end: parse → plan → dispatch → format as pgwire response.
    async fn execute_sql(&self, sql: &str) -> PgWireResult<Vec<Response>> {
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

        // TODO: derive tenant_id from authenticated session identity.
        let tenant_id = TenantId::new(1);

        // Plan via DataFusion.
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

        // Execute each task and collect results.
        let mut responses = Vec::with_capacity(tasks.len());
        for task in tasks {
            let plan_kind = describe_plan(&task.plan);
            let resp = self.dispatch_task(task).await.map_err(|e| {
                let (severity, code, message) = error_to_sqlstate(&e);
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    severity.to_owned(),
                    code.to_owned(),
                    message,
                )))
            })?;

            // Check for Data Plane errors.
            if let Some((severity, code, message)) =
                response_status_to_sqlstate(resp.status, &resp.error_code)
            {
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    severity.to_owned(),
                    code.to_owned(),
                    message,
                ))));
            }

            // Convert payload to pgwire response.
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

// ── SimpleQueryHandler ──────────────────────────────────────────────

#[async_trait]
impl SimpleQueryHandler for NodeDbPgHandler {
    async fn do_query<C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        self.execute_sql(query).await
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
        _client: &mut C,
        portal: &pgwire::api::portal::Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let query = &portal.statement.statement;
        let mut results = self.execute_sql(query).await?;
        Ok(results.pop().unwrap_or(Response::EmptyQuery))
    }
}

// Trust mode: NoopStartupHandler (no authentication).
impl NoopStartupHandler for NodeDbPgHandler {}

// ── AuthSource for SCRAM-SHA-256 ────────────────────────────────────

/// Bridges NodeDB's CredentialStore to pgwire's `AuthSource` trait.
///
/// When pgwire needs to verify a client's SCRAM-SHA-256 credentials,
/// it calls `get_password()` which looks up the SCRAM salt and salted
/// password from our credential store.
pub struct NodeDbAuthSource {
    credentials: Arc<CredentialStore>,
    state: Arc<SharedState>,
}

impl std::fmt::Debug for NodeDbAuthSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeDbAuthSource").finish()
    }
}

#[async_trait]
impl AuthSource for NodeDbAuthSource {
    async fn get_password(&self, login: &LoginInfo) -> PgWireResult<Password> {
        let username = login.user().unwrap_or("unknown");
        let source = login.host();

        match self.credentials.get_scram_credentials(username) {
            Some((salt, salted_password)) => Ok(Password::new(Some(salt), salted_password)),
            None => {
                // Record auth failure.
                self.state.audit_record(
                    AuditEvent::AuthFailure,
                    None,
                    source,
                    &format!("unknown user: {username}"),
                );
                Err(PgWireError::InvalidPassword(username.to_owned()))
            }
        }
    }
}

// ── Server parameter provider ───────────────────────────────────────

fn nodedb_parameter_provider() -> DefaultServerParameterProvider {
    let mut params = DefaultServerParameterProvider::default();
    params.server_version = format!("NodeDB 0.1.0 (pgwire {})", env!("CARGO_PKG_VERSION"));
    params
}

// ── Factory ─────────────────────────────────────────────────────────

/// Factory that wires together the pgwire handlers.
///
/// Supports both trust mode (NoopStartupHandler) and password mode
/// (SCRAM-SHA-256) based on the auth config.
pub struct NodeDbPgHandlerFactory {
    handler: Arc<NodeDbPgHandler>,
    auth_mode: AuthMode,
    credentials: Arc<CredentialStore>,
    state: Arc<SharedState>,
}

impl NodeDbPgHandlerFactory {
    pub fn new(state: Arc<SharedState>, auth_mode: AuthMode) -> Self {
        Self {
            handler: Arc::new(NodeDbPgHandler::new(Arc::clone(&state))),
            auth_mode,
            credentials: Arc::clone(&state.credentials),
            state,
        }
    }
}

impl PgWireServerHandlers for NodeDbPgHandlerFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.handler.clone()
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        self.handler.clone()
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        match self.auth_mode {
            AuthMode::Trust => {
                // No authentication — development only.
                Arc::new(TrustOrScramStartup::Trust(self.handler.clone()))
            }
            AuthMode::Password | AuthMode::Certificate => {
                // SCRAM-SHA-256 authentication.
                let auth_source = Arc::new(NodeDbAuthSource {
                    credentials: Arc::clone(&self.credentials),
                    state: Arc::clone(&self.state),
                });
                let scram = pgwire::api::auth::sasl::scram::ScramAuth::new(auth_source);
                let params = Arc::new(nodedb_parameter_provider());
                let sasl =
                    pgwire::api::auth::sasl::SASLAuthStartupHandler::new(params).with_scram(scram);
                Arc::new(TrustOrScramStartup::Scram(Box::new(sasl)))
            }
        }
    }
}

/// Enum dispatch for startup handler — avoids dyn trait object issues.
enum TrustOrScramStartup {
    Trust(Arc<NodeDbPgHandler>),
    Scram(Box<pgwire::api::auth::sasl::SASLAuthStartupHandler<DefaultServerParameterProvider>>),
}

#[async_trait]
impl StartupHandler for TrustOrScramStartup {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: pgwire::messages::PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        match self {
            TrustOrScramStartup::Trust(handler) => {
                // NoopStartupHandler is implemented for NodeDbPgHandler.
                <NodeDbPgHandler as StartupHandler>::on_startup(handler, client, message).await
            }
            TrustOrScramStartup::Scram(sasl) => sasl.on_startup(client, message).await,
        }
    }
}
