//! Authentication method types.

use serde::{Deserialize, Serialize};

/// Authentication method in an `Auth` request.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
#[serde(tag = "method", rename_all = "snake_case")]
#[non_exhaustive]
pub enum AuthMethod {
    #[serde(rename = "trust")]
    Trust {
        #[serde(default = "default_username")]
        username: String,
    },
    #[serde(rename = "password")]
    Password { username: String, password: String },
    #[serde(rename = "api_key")]
    ApiKey { token: String },
}

fn default_username() -> String {
    "admin".into()
}

/// Successful auth response payload.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct AuthResponse {
    pub username: String,
    pub tenant_id: u64,
}
