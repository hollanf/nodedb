//! JWT exchange endpoint: validate JWT, JIT provision user, return `nda_` key.
//!
//! ```text
//! POST /api/auth/exchange-key
//! Authorization: Bearer <jwt>
//! Content-Type: application/json
//! { "scopes": ["profile:read"], "rate_limit_qps": 100, "expires_days": 30 }
//!
//! Response: { "api_key": "nda_...", "auth_user_id": "...", "expires_in": 2592000 }
//! ```

use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::IntoResponse;

use super::super::auth::{ApiError, AppState, resolve_auth};

/// `POST /api/auth/exchange-key` — Exchange a JWT for an `nda_` API key.
pub async fn exchange_key(
    headers: HeaderMap,
    State(state): State<AppState>,
    axum::Json(body): axum::Json<serde_json::Value>,
) -> Result<impl IntoResponse, ApiError> {
    let (identity, _auth_ctx) = resolve_auth(&headers, &state, "http")?;

    let auth_user_id = identity.user_id.to_string();
    let scopes: Vec<String> = body
        .get("scopes")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();
    let rate_limit_qps = body
        .get("rate_limit_qps")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let rate_limit_burst = body
        .get("rate_limit_burst")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let expires_days = body
        .get("expires_days")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);

    let token = state.shared.auth_api_keys.create_key(
        &auth_user_id,
        identity.tenant_id.as_u32(),
        scopes,
        rate_limit_qps,
        rate_limit_burst,
        expires_days,
    );

    let expires_in = if expires_days > 0 {
        expires_days * 86_400
    } else {
        0
    };

    state.shared.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("JWT exchanged for auth API key for user '{auth_user_id}'"),
    );

    Ok(axum::Json(serde_json::json!({
        "api_key": token,
        "auth_user_id": auth_user_id,
        "expires_in": expires_in,
    })))
}
