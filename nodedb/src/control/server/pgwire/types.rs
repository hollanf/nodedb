use nodedb_types::columnar::ColumnType;
use nodedb_types::error::sqlstate;
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
        crate::Error::BadRequest { detail } => ("ERROR", sqlstate::SYNTAX_ERROR, detail.clone()),
        crate::Error::PlanError { detail } => ("ERROR", sqlstate::SYNTAX_ERROR, detail.clone()),
        crate::Error::CollectionNotFound { collection, .. } => (
            "ERROR",
            sqlstate::UNDEFINED_TABLE,
            format!("collection \"{collection}\" does not exist"),
        ),
        crate::Error::CollectionDeactivated { collection, .. } => (
            "ERROR",
            // UNDEFINED_TABLE is the canonical pg code; the distinct message
            // carries the UNDROP hint so client UX can surface a restore
            // button without a custom sqlstate.
            sqlstate::UNDEFINED_TABLE,
            format!(
                "collection \"{collection}\" was dropped and is within its retention \
                 window; restore it with `UNDROP COLLECTION {collection}` before \
                 it is hard-deleted"
            ),
        ),
        crate::Error::DocumentNotFound {
            collection,
            document_id,
        } => (
            "ERROR",
            sqlstate::NO_DATA,
            format!("document \"{document_id}\" not found in \"{collection}\""),
        ),
        crate::Error::RejectedConstraint { detail, .. } => {
            ("ERROR", sqlstate::UNIQUE_VIOLATION, detail.clone())
        }
        crate::Error::DeadlineExceeded { .. } => {
            ("ERROR", sqlstate::QUERY_CANCELED, err.to_string())
        }
        crate::Error::ConflictRetry { .. } => {
            ("ERROR", sqlstate::SERIALIZATION_FAILURE, err.to_string())
        }
        crate::Error::RejectedAuthz { .. } => {
            ("ERROR", sqlstate::INSUFFICIENT_PRIVILEGE, err.to_string())
        }
        crate::Error::MemoryExhausted { .. } => {
            ("ERROR", sqlstate::OUT_OF_MEMORY, err.to_string())
        }
        crate::Error::FanOutExceeded { .. } => {
            ("ERROR", sqlstate::STATEMENT_TOO_COMPLEX, err.to_string())
        }
        crate::Error::NoLeader { .. } => {
            ("ERROR", sqlstate::LOCK_NOT_AVAILABLE, err.to_string())
        }
        // DATABASE_DROPPED (57P04) — the closest Postgres canonical code for
        // "try again later, different node". Client libraries that recognise
        // the 57P* family treat this as retryable transient unavailability,
        // which is exactly the semantics we want. The message carries the
        // hinted leader address so an operator inspecting logs can see the
        // redirect target.
        crate::Error::NotLeader { leader_addr, .. } => (
            "ERROR",
            sqlstate::DATABASE_DROPPED,
            format!("cluster in leader election; leader hint: {leader_addr}"),
        ),
        _ => ("ERROR", sqlstate::INTERNAL_ERROR, err.to_string()),
    }
}

/// Map a Data Plane `ErrorCode` to SQLSTATE.
pub fn error_code_to_sqlstate(code: &ErrorCode) -> (&'static str, &'static str, String) {
    match code {
        ErrorCode::DeadlineExceeded => {
            ("ERROR", sqlstate::QUERY_CANCELED, "query cancelled due to deadline".into())
        }
        ErrorCode::RejectedConstraint { constraint, detail } => (
            "ERROR",
            sqlstate::UNIQUE_VIOLATION,
            if detail.is_empty() {
                format!("constraint violation: {constraint}")
            } else {
                format!("constraint violation: {constraint}: {detail}")
            },
        ),
        ErrorCode::RejectedPrevalidation { reason } => (
            "ERROR",
            sqlstate::CHECK_VIOLATION,
            format!("pre-validation rejected: {reason}"),
        ),
        ErrorCode::NotFound => ("ERROR", sqlstate::NO_DATA, "not found".into()),
        ErrorCode::RejectedAuthz => {
            ("ERROR", sqlstate::INSUFFICIENT_PRIVILEGE, "authorization denied".into())
        }
        ErrorCode::ConflictRetry => {
            ("ERROR", sqlstate::SERIALIZATION_FAILURE, "write conflict, retry".into())
        }
        ErrorCode::FanOutExceeded => {
            ("ERROR", sqlstate::STATEMENT_TOO_COMPLEX, "fan-out limit exceeded".into())
        }
        ErrorCode::ResourcesExhausted => {
            ("ERROR", sqlstate::OUT_OF_MEMORY, "resources exhausted".into())
        }
        ErrorCode::RejectedDanglingEdge { missing_node } => (
            "ERROR",
            sqlstate::FOREIGN_KEY_VIOLATION,
            format!("edge rejected: node \"{missing_node}\" does not exist"),
        ),
        ErrorCode::DuplicateWrite => (
            "ERROR",
            sqlstate::UNIQUE_VIOLATION,
            "duplicate write detected via idempotency key".into(),
        ),
        ErrorCode::AppendOnlyViolation { collection } => (
            "ERROR",
            sqlstate::APPEND_ONLY_VIOLATION,
            format!("append-only violation: UPDATE/DELETE not allowed on {collection}"),
        ),
        ErrorCode::BalanceViolation { collection, detail } => (
            "ERROR",
            sqlstate::BALANCE_VIOLATION,
            format!("balance violation on {collection}: {detail}"),
        ),
        ErrorCode::PeriodLocked { collection } => (
            "ERROR",
            sqlstate::PERIOD_LOCKED,
            format!("period locked: writes rejected on {collection}"),
        ),
        ErrorCode::RetentionViolation { collection } => (
            "ERROR",
            sqlstate::RETENTION_VIOLATION,
            format!("retention violation: cannot delete from {collection}"),
        ),
        ErrorCode::LegalHoldActive { collection } => (
            "ERROR",
            sqlstate::LEGAL_HOLD_ACTIVE,
            format!("legal hold active: cannot delete from {collection}"),
        ),
        ErrorCode::StateTransitionViolation { collection, detail } => (
            "ERROR",
            sqlstate::STATE_TRANSITION_VIOLATION,
            format!("state transition violation on {collection}: {detail}"),
        ),
        ErrorCode::TransitionCheckViolation { collection } => (
            "ERROR",
            sqlstate::TRANSITION_CHECK_VIOLATION,
            format!("transition check violation on {collection}"),
        ),
        ErrorCode::TypeGuardViolation { collection, detail } => (
            "ERROR",
            sqlstate::TYPE_GUARD_VIOLATION,
            format!("type guard violation on {collection}: {detail}"),
        ),
        ErrorCode::TypeMismatch { collection, detail } => (
            "ERROR",
            sqlstate::CANNOT_COERCE,
            format!("type mismatch on {collection}: {detail}"),
        ),
        ErrorCode::OverflowError { collection } => (
            "ERROR",
            sqlstate::NUMERIC_VALUE_OUT_OF_RANGE,
            format!("arithmetic overflow on {collection}"),
        ),
        ErrorCode::InsufficientBalance { collection, detail } => (
            "ERROR",
            sqlstate::CHECK_VIOLATION,
            format!("insufficient balance on {collection}: {detail}"),
        ),
        ErrorCode::RateExceeded {
            gate,
            retry_after_ms,
        } => (
            "ERROR",
            sqlstate::STATEMENT_TOO_COMPLEX,
            format!("rate limit exceeded for {gate}, retry after {retry_after_ms}ms"),
        ),
        ErrorCode::CollectionDraining { collection } => (
            "ERROR",
            sqlstate::CANNOT_CONNECT_NOW,
            format!(
                "collection '{collection}' is draining for hard-delete; retry after purge completes"
            ),
        ),
        ErrorCode::Internal { detail } => ("ERROR", sqlstate::INTERNAL_ERROR, detail.clone()),
        ErrorCode::Unsupported { detail } => {
            ("ERROR", sqlstate::FEATURE_NOT_SUPPORTED, detail.clone())
        }
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

/// Build a FieldInfo for a float4 column.
pub fn float4_field(name: &str) -> FieldInfo {
    FieldInfo::new(name.to_owned(), None, None, Type::FLOAT4, FieldFormat::Text)
}

/// Build a FieldInfo for an int4 column.
pub fn int4_field(name: &str) -> FieldInfo {
    FieldInfo::new(name.to_owned(), None, None, Type::INT4, FieldFormat::Text)
}

/// Build a FieldInfo for an int2 column.
pub fn int2_field(name: &str) -> FieldInfo {
    FieldInfo::new(name.to_owned(), None, None, Type::INT2, FieldFormat::Text)
}

/// Build a FieldInfo for a bool column.
pub fn bool_field(name: &str) -> FieldInfo {
    FieldInfo::new(name.to_owned(), None, None, Type::BOOL, FieldFormat::Text)
}

/// Build a FieldInfo for a bytea column.
pub fn bytea_field(name: &str) -> FieldInfo {
    FieldInfo::new(name.to_owned(), None, None, Type::BYTEA, FieldFormat::Text)
}

/// Build a FieldInfo for a JSON column.
pub fn json_field(name: &str) -> FieldInfo {
    FieldInfo::new(name.to_owned(), None, None, Type::JSON, FieldFormat::Text)
}

/// Build a FieldInfo for a JSONB column.
pub fn jsonb_field(name: &str) -> FieldInfo {
    FieldInfo::new(name.to_owned(), None, None, Type::JSONB, FieldFormat::Text)
}

/// Build a FieldInfo for a timestamptz column.
pub fn timestamptz_field(name: &str) -> FieldInfo {
    FieldInfo::new(
        name.to_owned(),
        None,
        None,
        Type::TIMESTAMPTZ,
        FieldFormat::Text,
    )
}

/// Build a FieldInfo for a timestamp column.
pub fn timestamp_field(name: &str) -> FieldInfo {
    FieldInfo::new(
        name.to_owned(),
        None,
        None,
        Type::TIMESTAMP,
        FieldFormat::Text,
    )
}

/// Build a FieldInfo for a varchar column.
pub fn varchar_field(name: &str) -> FieldInfo {
    FieldInfo::new(
        name.to_owned(),
        None,
        None,
        Type::VARCHAR,
        FieldFormat::Text,
    )
}

/// Build a FieldInfo for a float4 array column (vector embeddings).
pub fn float4_array_field(name: &str) -> FieldInfo {
    FieldInfo::new(
        name.to_owned(),
        None,
        None,
        Type::FLOAT4_ARRAY,
        FieldFormat::Text,
    )
}

/// Build a FieldInfo for a float8 array column.
pub fn float8_array_field(name: &str) -> FieldInfo {
    FieldInfo::new(
        name.to_owned(),
        None,
        None,
        Type::FLOAT8_ARRAY,
        FieldFormat::Text,
    )
}

/// Map a NodeDB field type name to a pgwire `Type`.
///
/// Uses `ColumnType::from_str` + `ColumnType::to_pg_oid` as the single
/// authoritative OID mapping. Falls back to `Type::TEXT` only for names that
/// cannot be parsed as a known `ColumnType` (e.g. DataFusion aliases like
/// `"int4"` or `"float8[]"`).
pub fn type_name_to_pgwire(type_name: &str) -> Type {
    // Try to parse via the canonical ColumnType mapping first.
    if let Ok(ct) = type_name.parse::<ColumnType>() {
        return Type::from_oid(ct.to_pg_oid()).unwrap_or(Type::TEXT);
    }
    // Handle DataFusion / legacy aliases that ColumnType::from_str doesn't cover.
    match type_name.to_lowercase().as_str() {
        "int" | "int4" | "integer" => Type::INT4,
        "int2" | "smallint" => Type::INT2,
        "float4" | "real" => Type::FLOAT4,
        "float8" | "double" | "double precision" => Type::FLOAT8,
        "varchar" => Type::VARCHAR,
        "timestamptz" => Type::TIMESTAMPTZ,
        s if s.starts_with("float4[]") => Type::FLOAT4_ARRAY,
        "float8[]" => Type::FLOAT8_ARRAY,
        _ => Type::TEXT,
    }
}

/// Create a notice response (WARNING level).
pub fn notice_warning(message: &str) -> pgwire::messages::response::NoticeResponse {
    pgwire::messages::response::NoticeResponse::from(pgwire::error::ErrorInfo::new(
        "WARNING".to_owned(),
        sqlstate::WARNING.to_owned(),
        message.to_owned(),
    ))
}

/// Require that the identity is superuser or tenant_admin.
pub fn require_admin(identity: &AuthenticatedIdentity, action: &str) -> PgWireResult<()> {
    if identity.is_superuser || identity.has_role(&Role::TenantAdmin) {
        Ok(())
    } else {
        Err(sqlstate_error(
            sqlstate::INSUFFICIENT_PRIVILEGE,
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

/// Decode a hex string into bytes.
///
/// Returns `None` if the input has an odd number of characters or contains
/// characters that are not valid hexadecimal digits.
pub fn hex_decode(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    let mut bytes = Vec::with_capacity(s.len() / 2);
    for i in (0..s.len()).step_by(2) {
        let byte = u8::from_str_radix(&s[i..i + 2], 16).ok()?;
        bytes.push(byte);
    }
    Some(bytes)
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
