use pgwire::api::Type;
use pgwire::api::results::FieldFormat;
use pgwire::api::results::FieldInfo;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::bridge::envelope::{ErrorCode, Status};
use crate::control::security::identity::{AuthenticatedIdentity, Role};

/// Create a pgwire ErrorResponse with a SQLSTATE code.
pub fn sqlstate_error(code: &str, message: &str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        code.to_owned(),
        message.to_owned(),
    )))
}

/// Map a NodeDB `Error` to a PostgreSQL SQLSTATE code + message.
pub fn error_to_sqlstate(err: &crate::Error) -> (&'static str, &'static str, String) {
    match err {
        crate::Error::BadRequest { detail } => ("ERROR", "42601", detail.clone()),
        crate::Error::PlanError { detail } => ("ERROR", "42601", detail.clone()),
        crate::Error::CollectionNotFound { collection, .. } => (
            "ERROR",
            "42P01",
            format!("collection \"{collection}\" does not exist"),
        ),
        crate::Error::DocumentNotFound {
            collection,
            document_id,
        } => (
            "ERROR",
            "02000",
            format!("document \"{document_id}\" not found in \"{collection}\""),
        ),
        crate::Error::RejectedConstraint { detail, .. } => ("ERROR", "23505", detail.clone()),
        crate::Error::DeadlineExceeded { .. } => ("ERROR", "57014", err.to_string()),
        crate::Error::ConflictRetry { .. } => ("ERROR", "40001", err.to_string()),
        crate::Error::RejectedAuthz { .. } => ("ERROR", "42501", err.to_string()),
        crate::Error::MemoryExhausted { .. } => ("ERROR", "53200", err.to_string()),
        crate::Error::FanOutExceeded { .. } => ("ERROR", "54001", err.to_string()),
        crate::Error::NoLeader { .. } => ("ERROR", "55P03", err.to_string()),
        // SQLSTATE 01R01: custom — "not leader" with redirect hint.
        // The message contains the leader's address so clients can reconnect.
        crate::Error::NotLeader { leader_addr, .. } => (
            "ERROR",
            "01R01",
            format!("not leader; redirect to {leader_addr}"),
        ),
        _ => ("ERROR", "XX000", err.to_string()),
    }
}

/// Map a Data Plane `ErrorCode` to SQLSTATE.
pub fn error_code_to_sqlstate(code: &ErrorCode) -> (&'static str, &'static str, String) {
    match code {
        ErrorCode::DeadlineExceeded => ("ERROR", "57014", "query cancelled due to deadline".into()),
        ErrorCode::RejectedConstraint { constraint } => (
            "ERROR",
            "23505",
            format!("constraint violation: {constraint}"),
        ),
        ErrorCode::RejectedPrevalidation { reason } => (
            "ERROR",
            "23514",
            format!("pre-validation rejected: {reason}"),
        ),
        ErrorCode::NotFound => ("ERROR", "02000", "not found".into()),
        ErrorCode::RejectedAuthz => ("ERROR", "42501", "authorization denied".into()),
        ErrorCode::ConflictRetry => ("ERROR", "40001", "write conflict, retry".into()),
        ErrorCode::FanOutExceeded => ("ERROR", "54001", "fan-out limit exceeded".into()),
        ErrorCode::ResourcesExhausted => ("ERROR", "53200", "resources exhausted".into()),
        ErrorCode::Internal { detail } => ("ERROR", "XX000", detail.clone()),
    }
}

/// Build a FieldInfo for a text column in query results.
pub fn text_field(name: &str) -> FieldInfo {
    FieldInfo::new(name.to_owned(), None, None, Type::TEXT, FieldFormat::Text)
}

/// Build a FieldInfo for an int8 column.
pub fn int8_field(name: &str) -> FieldInfo {
    FieldInfo::new(name.to_owned(), None, None, Type::INT8, FieldFormat::Text)
}

/// Build a FieldInfo for a float8 column.
pub fn float8_field(name: &str) -> FieldInfo {
    FieldInfo::new(name.to_owned(), None, None, Type::FLOAT8, FieldFormat::Text)
}

/// Require that the identity is superuser or tenant_admin.
pub fn require_admin(identity: &AuthenticatedIdentity, action: &str) -> PgWireResult<()> {
    if identity.is_superuser || identity.has_role(&Role::TenantAdmin) {
        Ok(())
    } else {
        Err(sqlstate_error(
            "42501",
            &format!("permission denied: only superuser or tenant_admin can {action}"),
        ))
    }
}

/// Parse a role name string into a `Role`.
///
/// Known roles map to their enum variants; unknown names become `Role::Custom`.
pub fn parse_role(name: &str) -> Role {
    // Role::from_str is Infallible — unwrap is safe on Infallible.
    match name.parse() {
        Ok(role) => role,
        Err(e) => match e {},
    }
}

/// Map a Data Plane response status + error code to a SQLSTATE triple.
pub fn response_status_to_sqlstate(
    status: Status,
    error_code: &Option<ErrorCode>,
) -> Option<(&'static str, &'static str, String)> {
    match status {
        Status::Ok | Status::Partial => None,
        Status::Error => {
            if let Some(code) = error_code {
                Some(error_code_to_sqlstate(code))
            } else {
                Some(("ERROR", "XX000", "unknown data plane error".into()))
            }
        }
    }
}
