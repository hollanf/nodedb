use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::sink::Sink;

use pgwire::api::auth::{
    AuthSource, DefaultServerParameterProvider, LoginInfo, Password, StartupHandler,
};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::{ClientInfo, PgWireServerHandlers};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};

use crate::config::auth::AuthMode;
use crate::control::security::audit::AuditEvent;
use crate::control::security::credential::CredentialStore;
use crate::control::state::SharedState;

use super::handler::NodeDbPgHandler;

// ── AuthSource for SCRAM-SHA-256 ────────────────────────────────────

/// Bridges NodeDB's CredentialStore to pgwire's `AuthSource` trait.
pub struct NodeDbAuthSource {
    credentials: Arc<CredentialStore>,
    state: Arc<SharedState>,
}

impl Debug for NodeDbAuthSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeDbAuthSource").finish()
    }
}

#[async_trait]
impl AuthSource for NodeDbAuthSource {
    async fn get_password(&self, login: &LoginInfo) -> PgWireResult<Password> {
        let username = login.user().unwrap_or("unknown");
        let source = login.host();

        // Check lockout before returning credentials.
        if self.credentials.check_lockout(username).is_err() {
            self.state.audit_record(
                AuditEvent::AuthFailure,
                None,
                source,
                &format!("user '{username}' is locked out"),
            );
            return Err(PgWireError::InvalidPassword(format!(
                "{username} (account locked)"
            )));
        }

        match self.credentials.get_scram_credentials(username) {
            Some((salt, salted_password)) => Ok(Password::new(Some(salt), salted_password)),
            None => {
                self.credentials.record_login_failure(username);
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

// ── MD5 AuthSource ──────────────────────────────────────────────────

/// AuthSource for pgwire MD5 authentication.
///
/// Returns the stored MD5 hash + a random 4-byte salt for MD5 auth.
pub struct NodeDbMd5AuthSource {
    credentials: Arc<CredentialStore>,
    state: Arc<SharedState>,
}

impl Debug for NodeDbMd5AuthSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeDbMd5AuthSource").finish()
    }
}

#[async_trait]
impl AuthSource for NodeDbMd5AuthSource {
    async fn get_password(&self, login: &LoginInfo) -> PgWireResult<Password> {
        let username = login.user().unwrap_or("unknown");
        let source = login.host();

        if self.credentials.check_lockout(username).is_err() {
            self.state.audit_record(
                AuditEvent::AuthFailure,
                None,
                source,
                &format!("user '{username}' is locked out"),
            );
            return Err(PgWireError::InvalidPassword(format!(
                "{username} (account locked)"
            )));
        }

        match self.credentials.get_md5_hash(username) {
            Some(md5_hash) => {
                // Generate a random 4-byte salt for this connection.
                use argon2::password_hash::rand_core::{OsRng, RngCore};
                let mut salt = vec![0u8; 4];
                OsRng.fill_bytes(&mut salt);

                // Compute the expected wire-level response:
                // Client sends: "md5" + md5(md5(password + username) + salt)
                // We have md5_hash = md5(password + username) as hex string.
                // So we compute: "md5" + md5(md5_hash_hex + salt_bytes)
                let mut salted_input = Vec::with_capacity(md5_hash.len() + salt.len());
                salted_input.extend_from_slice(md5_hash.as_bytes());
                salted_input.extend_from_slice(&salt);
                let expected = format!("md5{:x}", md5::compute(&salted_input));

                Ok(Password::new(Some(salt), expected.into_bytes()))
            }
            None => {
                self.credentials.record_login_failure(username);
                self.state.audit_record(
                    AuditEvent::AuthFailure,
                    None,
                    source,
                    &format!("MD5 auth failed: user '{username}' not found or no MD5 hash"),
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
/// (SCRAM-SHA-256 via pgwire's SASL implementation).
pub struct NodeDbPgHandlerFactory {
    handler: Arc<NodeDbPgHandler>,
    auth_mode: AuthMode,
    credentials: Arc<CredentialStore>,
    state: Arc<SharedState>,
}

impl NodeDbPgHandlerFactory {
    pub fn new(state: Arc<SharedState>, auth_mode: AuthMode) -> Self {
        Self {
            handler: Arc::new(NodeDbPgHandler::new(Arc::clone(&state), auth_mode.clone())),
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
            AuthMode::Trust => Arc::new(AuthStartup::Trust(self.handler.clone())),
            AuthMode::Password | AuthMode::Certificate => {
                let auth_source = Arc::new(NodeDbAuthSource {
                    credentials: Arc::clone(&self.credentials),
                    state: Arc::clone(&self.state),
                });
                let scram = pgwire::api::auth::sasl::scram::ScramAuth::new(auth_source);
                let params = Arc::new(nodedb_parameter_provider());
                let sasl =
                    pgwire::api::auth::sasl::SASLAuthStartupHandler::new(params).with_scram(scram);
                Arc::new(AuthStartup::Scram {
                    sasl: Box::new(sasl),
                    state: Arc::clone(&self.state),
                })
            }
            AuthMode::Md5Password => {
                let auth_source = Arc::new(NodeDbMd5AuthSource {
                    credentials: Arc::clone(&self.credentials),
                    state: Arc::clone(&self.state),
                });
                let params = Arc::new(nodedb_parameter_provider());
                let md5 = pgwire::api::auth::md5pass::Md5PasswordAuthStartupHandler::new(
                    auth_source,
                    params,
                );
                Arc::new(AuthStartup::Md5 {
                    handler: Box::new(md5),
                    state: Arc::clone(&self.state),
                })
            }
        }
    }
}

// ── Startup handler dispatch ────────────────────────────────────────

/// Enum dispatch for startup handler — avoids dyn trait object issues.
enum AuthStartup {
    Trust(Arc<NodeDbPgHandler>),
    Scram {
        sasl: Box<pgwire::api::auth::sasl::SASLAuthStartupHandler<DefaultServerParameterProvider>>,
        state: Arc<SharedState>,
    },
    Md5 {
        handler: Box<
            pgwire::api::auth::md5pass::Md5PasswordAuthStartupHandler<
                NodeDbMd5AuthSource,
                DefaultServerParameterProvider,
            >,
        >,
        state: Arc<SharedState>,
    },
}

#[async_trait]
impl StartupHandler for AuthStartup {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + futures::sink::Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        match self {
            AuthStartup::Trust(handler) => {
                <NodeDbPgHandler as StartupHandler>::on_startup(handler, client, message).await?;

                let username = client
                    .metadata()
                    .get("user")
                    .cloned()
                    .unwrap_or_else(|| "unknown".to_string());
                let source = client.socket_addr().to_string();
                handler.state.audit_record(
                    AuditEvent::AuthSuccess,
                    None,
                    &source,
                    &format!("trust auth: {username}"),
                );
                Ok(())
            }
            AuthStartup::Scram { sasl, state } => {
                let was_in_auth = matches!(
                    client.state(),
                    pgwire::api::PgWireConnectionState::AuthenticationInProgress
                );

                let result = sasl.on_startup(client, message).await;

                let username = client
                    .metadata()
                    .get("user")
                    .cloned()
                    .unwrap_or_else(|| "unknown".to_string());
                let source = client.socket_addr().to_string();

                match &result {
                    Ok(())
                        if was_in_auth
                            && matches!(
                                client.state(),
                                pgwire::api::PgWireConnectionState::ReadyForQuery
                            ) =>
                    {
                        // SCRAM succeeded — reset lockout counter.
                        state.credentials.record_login_success(&username);
                        state.audit_record(
                            AuditEvent::AuthSuccess,
                            None,
                            &source,
                            &format!("SCRAM-SHA-256 auth: {username}"),
                        );
                    }
                    Err(_) if was_in_auth => {
                        // SCRAM failed — increment lockout counter.
                        state.credentials.record_login_failure(&username);
                        state.audit_record(
                            AuditEvent::AuthFailure,
                            None,
                            &source,
                            &format!("SCRAM-SHA-256 auth failed: {username}"),
                        );
                    }
                    _ => {}
                }

                result
            }
            AuthStartup::Md5 { handler, state } => {
                let was_in_auth = matches!(
                    client.state(),
                    pgwire::api::PgWireConnectionState::AuthenticationInProgress
                );

                let result = handler.on_startup(client, message).await;

                let username = client
                    .metadata()
                    .get("user")
                    .cloned()
                    .unwrap_or_else(|| "unknown".to_string());
                let source = client.socket_addr().to_string();

                match &result {
                    Ok(())
                        if was_in_auth
                            && matches!(
                                client.state(),
                                pgwire::api::PgWireConnectionState::ReadyForQuery
                            ) =>
                    {
                        state.credentials.record_login_success(&username);
                        state.audit_record(
                            AuditEvent::AuthSuccess,
                            None,
                            &source,
                            &format!("MD5 auth: {username}"),
                        );
                    }
                    Err(_) if was_in_auth => {
                        state.credentials.record_login_failure(&username);
                        state.audit_record(
                            AuditEvent::AuthFailure,
                            None,
                            &source,
                            &format!("MD5 auth failed: {username}"),
                        );
                    }
                    _ => {}
                }

                result
            }
        }
    }
}
