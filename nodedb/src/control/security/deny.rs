//! Access denial response configuration for RLS policies.
//!
//! Controls what happens when an RLS policy denies access:
//! - `SILENT` (default): row filtered, no error, no info leak.
//! - `ERROR`: structured error returned to the client with code/message/detail.
//!
//! ```sql
//! CREATE RLS POLICY read_own ON users FOR READ
//!     USING (user_id = $auth.id)
//!     ON DENY ERROR 'INSUFFICIENT_SCOPE' MESSAGE 'You can only read your own data'
//!
//! CREATE RLS POLICY admin_only ON config FOR READ
//!     USING ($auth.roles CONTAINS 'admin')
//!     ON DENY SILENT
//! ```

use nodedb_types::error::sqlstate;
use serde::{Deserialize, Serialize};

/// How the system responds when an RLS policy denies access.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum DenyMode {
    /// Row is silently filtered from results. No error, no info leak.
    /// This is the default and the most secure option — the client
    /// cannot distinguish "row doesn't exist" from "access denied".
    #[default]
    Silent,

    /// Structured error returned to the client.
    /// Contains code, message, and optional detail for programmatic handling.
    Error(DenyError),
}

/// Structured denial error returned when `DenyMode::Error` is configured.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DenyError {
    /// Application-level error code (e.g., "INSUFFICIENT_SCOPE", "ACCOUNT_SUSPENDED").
    pub code: String,
    /// Human-readable message.
    pub message: String,
    /// Optional machine-readable detail.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

/// Error code taxonomy for access denial.
///
/// These are the standard codes returned in `DenyError::code`.
/// Callers can also define custom codes via the DDL.
pub struct DenyCodes;

impl DenyCodes {
    /// No valid credentials presented.
    pub const UNAUTHORIZED: &'static str = "UNAUTHORIZED";
    /// Valid credentials but insufficient permissions/scope.
    pub const INSUFFICIENT_SCOPE: &'static str = "INSUFFICIENT_SCOPE";
    /// Account is suspended.
    pub const ACCOUNT_SUSPENDED: &'static str = "ACCOUNT_SUSPENDED";
    /// Account is banned.
    pub const ACCOUNT_BANNED: &'static str = "ACCOUNT_BANNED";
    /// Rate limit exceeded.
    pub const RATE_LIMITED: &'static str = "RATE_LIMITED";
    /// Usage quota exceeded.
    pub const QUOTA_EXCEEDED: &'static str = "QUOTA_EXCEEDED";
    /// RLS policy denied read access.
    pub const RLS_READ_DENIED: &'static str = "RLS_READ_DENIED";
    /// RLS policy denied write access.
    pub const RLS_WRITE_DENIED: &'static str = "RLS_WRITE_DENIED";
}

/// Map a `DenyMode::Error` to an HTTP status code.
pub fn deny_to_http_status(code: &str) -> u16 {
    match code {
        DenyCodes::UNAUTHORIZED => 401,
        DenyCodes::INSUFFICIENT_SCOPE
        | DenyCodes::ACCOUNT_SUSPENDED
        | DenyCodes::ACCOUNT_BANNED
        | DenyCodes::RLS_READ_DENIED
        | DenyCodes::RLS_WRITE_DENIED => 403,
        DenyCodes::RATE_LIMITED | DenyCodes::QUOTA_EXCEEDED => 429,
        _ => 403, // Default to forbidden for unknown codes.
    }
}

/// Map a `DenyMode::Error` to a PostgreSQL SQLSTATE code.
pub fn deny_to_sqlstate(code: &str) -> &'static str {
    match code {
        DenyCodes::UNAUTHORIZED => sqlstate::INVALID_AUTHORIZATION,
        DenyCodes::INSUFFICIENT_SCOPE => sqlstate::INSUFFICIENT_PRIVILEGE,
        DenyCodes::ACCOUNT_SUSPENDED => sqlstate::INSUFFICIENT_PRIVILEGE,
        DenyCodes::ACCOUNT_BANNED => sqlstate::INSUFFICIENT_PRIVILEGE,
        DenyCodes::RATE_LIMITED => sqlstate::TOO_MANY_CONNECTIONS,
        DenyCodes::QUOTA_EXCEEDED => sqlstate::CONFIGURATION_LIMIT_EXCEEDED,
        DenyCodes::RLS_READ_DENIED => sqlstate::INSUFFICIENT_PRIVILEGE,
        DenyCodes::RLS_WRITE_DENIED => sqlstate::INSUFFICIENT_PRIVILEGE,
        _ => sqlstate::INSUFFICIENT_PRIVILEGE,
    }
}

/// Build a structured JSON error response for access denial.
pub fn deny_to_json(deny: &DenyError, policy_name: &str, collection: &str) -> serde_json::Value {
    let mut resp = serde_json::json!({
        "error": {
            "code": deny.code,
            "message": deny.message,
            "policy": policy_name,
            "collection": collection,
        }
    });
    if let Some(ref detail) = deny.detail {
        resp["error"]["detail"] = serde_json::Value::String(detail.clone());
    }
    resp
}

/// Parse an `ON DENY` clause from DDL parts.
///
/// Syntax:
/// - `ON DENY SILENT`
/// - `ON DENY ERROR 'CODE' MESSAGE 'text' [DETAIL 'text']`
pub fn parse_on_deny(parts: &[&str]) -> Result<DenyMode, String> {
    if parts.is_empty() {
        return Ok(DenyMode::Silent);
    }

    let mode = parts[0].to_uppercase();
    match mode.as_str() {
        "SILENT" => Ok(DenyMode::Silent),
        "ERROR" => {
            if parts.len() < 2 {
                return Err(
                    "ON DENY ERROR requires a code: ON DENY ERROR 'CODE' MESSAGE '...'".into(),
                );
            }
            let code = parts[1].trim_matches('\'').to_string();

            let message = parts
                .iter()
                .position(|p| p.to_uppercase() == "MESSAGE")
                .and_then(|i| {
                    // Join everything after MESSAGE until DETAIL or end.
                    let msg_start = i + 1;
                    let msg_end = parts[msg_start..]
                        .iter()
                        .position(|p| p.to_uppercase() == "DETAIL")
                        .map(|j| msg_start + j)
                        .unwrap_or(parts.len());
                    if msg_start < msg_end {
                        Some(
                            parts[msg_start..msg_end]
                                .join(" ")
                                .trim_matches('\'')
                                .to_string(),
                        )
                    } else {
                        None
                    }
                })
                .unwrap_or_else(|| format!("Access denied by RLS policy (code: {code})"));

            let detail = parts
                .iter()
                .position(|p| p.to_uppercase() == "DETAIL")
                .and_then(|i| {
                    if i + 1 < parts.len() {
                        Some(parts[i + 1..].join(" ").trim_matches('\'').to_string())
                    } else {
                        None
                    }
                });

            Ok(DenyMode::Error(DenyError {
                code,
                message,
                detail,
            }))
        }
        other => Err(format!(
            "unknown ON DENY mode: '{other}'. Expected SILENT or ERROR"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_silent() {
        let mode = parse_on_deny(&["SILENT"]).unwrap();
        assert_eq!(mode, DenyMode::Silent);
    }

    #[test]
    fn parse_error_with_code_and_message() {
        let parts = vec![
            "ERROR",
            "'INSUFFICIENT_SCOPE'",
            "MESSAGE",
            "'You cannot access this'",
        ];
        let mode = parse_on_deny(&parts).unwrap();
        match mode {
            DenyMode::Error(err) => {
                assert_eq!(err.code, "INSUFFICIENT_SCOPE");
                assert_eq!(err.message, "You cannot access this");
                assert!(err.detail.is_none());
            }
            _ => panic!("expected Error mode"),
        }
    }

    #[test]
    fn parse_error_with_detail() {
        let parts = vec![
            "ERROR",
            "'RLS_READ_DENIED'",
            "MESSAGE",
            "'denied'",
            "DETAIL",
            "'policy: read_own'",
        ];
        let mode = parse_on_deny(&parts).unwrap();
        match mode {
            DenyMode::Error(err) => {
                assert_eq!(err.code, "RLS_READ_DENIED");
                assert_eq!(err.detail, Some("policy: read_own".into()));
            }
            _ => panic!("expected Error mode"),
        }
    }

    #[test]
    fn default_is_silent() {
        assert_eq!(DenyMode::default(), DenyMode::Silent);
    }

    #[test]
    fn http_status_mapping() {
        assert_eq!(deny_to_http_status(DenyCodes::UNAUTHORIZED), 401);
        assert_eq!(deny_to_http_status(DenyCodes::INSUFFICIENT_SCOPE), 403);
        assert_eq!(deny_to_http_status(DenyCodes::RATE_LIMITED), 429);
    }

    #[test]
    fn sqlstate_mapping() {
        assert_eq!(
            deny_to_sqlstate(DenyCodes::UNAUTHORIZED),
            sqlstate::INVALID_AUTHORIZATION
        );
        assert_eq!(
            deny_to_sqlstate(DenyCodes::INSUFFICIENT_SCOPE),
            sqlstate::INSUFFICIENT_PRIVILEGE
        );
        assert_eq!(
            deny_to_sqlstate(DenyCodes::RATE_LIMITED),
            sqlstate::TOO_MANY_CONNECTIONS
        );
    }

    #[test]
    fn json_response_structure() {
        let err = DenyError {
            code: "INSUFFICIENT_SCOPE".into(),
            message: "Access denied".into(),
            detail: Some("Requires admin role".into()),
        };
        let json = deny_to_json(&err, "admin_only", "config");
        assert_eq!(json["error"]["code"], "INSUFFICIENT_SCOPE");
        assert_eq!(json["error"]["policy"], "admin_only");
        assert_eq!(json["error"]["collection"], "config");
        assert_eq!(json["error"]["detail"], "Requires admin role");
    }
}
