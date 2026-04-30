//! Native protocol request and response frame types.

use serde::{Deserialize, Serialize};

use crate::value::Value;

use super::auth::AuthResponse;
use super::opcodes::ResponseStatus;
use super::request_fields::RequestFields;

// ─── Request Frame ──────────────────────────────────────────────────

/// A request sent from client to server over the native protocol.
///
/// Serialized as MessagePack. The `op` field selects the handler,
/// `seq` correlates request to response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NativeRequest {
    /// Operation code.
    pub op: super::opcodes::OpCode,
    /// Client-assigned sequence number for request/response correlation.
    pub seq: u64,
    /// Operation-specific fields (flattened into the same map).
    #[serde(flatten)]
    pub fields: RequestFields,
}

impl zerompk::ToMessagePack for NativeRequest {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        writer.write_array_len(3)?;
        self.op.write(writer)?;
        writer.write_u64(self.seq)?;
        self.fields.write(writer)
    }
}

impl<'a> zerompk::FromMessagePack<'a> for NativeRequest {
    fn read<R: zerompk::Read<'a>>(reader: &mut R) -> zerompk::Result<Self> {
        let len = reader.read_array_len()?;
        if len != 3 {
            return Err(zerompk::Error::ArrayLengthMismatch {
                expected: 3,
                actual: len,
            });
        }
        let op = super::opcodes::OpCode::read(reader)?;
        let seq = reader.read_u64()?;
        let fields = RequestFields::read(reader)?;
        Ok(Self { op, seq, fields })
    }
}

// ─── Response Frame ─────────────────────────────────────────────────

/// A response sent from server to client over the native protocol.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct NativeResponse {
    /// Echoed from the request for correlation.
    pub seq: u64,
    /// Execution outcome.
    pub status: ResponseStatus,
    /// Column names (for query results).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub columns: Option<Vec<String>>,
    /// Row data (for query results). Each row is a Vec of Values.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows: Option<Vec<Vec<Value>>>,
    /// Number of rows affected (for writes).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows_affected: Option<u64>,
    /// WAL LSN watermark at time of computation.
    pub watermark_lsn: u64,
    /// Error details (if status == Error).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorPayload>,
    /// Auth response (if op == Auth and status == Ok).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth: Option<AuthResponse>,
    /// Advisory warnings (e.g. password expiry grace period, must_change_password).
    /// Empty in the common case; `#[serde(default)]` and `#[msgpack(default)]`
    /// keep this additive and backward-compatible with older clients.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[msgpack(default)]
    pub warnings: Vec<String>,
}

/// Error details in a response.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct ErrorPayload {
    /// SQLSTATE-style error code (e.g., "42P01" for undefined table).
    pub code: String,
    /// Human-readable error message.
    pub message: String,
}

impl NativeResponse {
    /// Create a successful response with no data.
    pub fn ok(seq: u64) -> Self {
        Self {
            seq,
            status: ResponseStatus::Ok,
            columns: None,
            rows: None,
            rows_affected: None,
            watermark_lsn: 0,
            error: None,
            auth: None,
            warnings: Vec::new(),
        }
    }

    /// Create a successful response from a `QueryResult`.
    pub fn from_query_result(seq: u64, qr: crate::result::QueryResult, lsn: u64) -> Self {
        Self {
            seq,
            status: ResponseStatus::Ok,
            columns: Some(qr.columns),
            rows: Some(qr.rows),
            rows_affected: Some(qr.rows_affected),
            watermark_lsn: lsn,
            error: None,
            auth: None,
            warnings: Vec::new(),
        }
    }

    /// Create an error response.
    pub fn error(seq: u64, code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            seq,
            status: ResponseStatus::Error,
            columns: None,
            rows: None,
            rows_affected: None,
            watermark_lsn: 0,
            error: Some(ErrorPayload {
                code: code.into(),
                message: message.into(),
            }),
            auth: None,
            warnings: Vec::new(),
        }
    }

    /// Create an auth success response.
    pub fn auth_ok(seq: u64, username: String, tenant_id: u64) -> Self {
        Self {
            seq,
            status: ResponseStatus::Ok,
            columns: None,
            rows: None,
            rows_affected: None,
            watermark_lsn: 0,
            error: None,
            auth: Some(AuthResponse {
                username,
                tenant_id,
            }),
            warnings: Vec::new(),
        }
    }

    /// Create a response with a single "status" column and one row.
    pub fn status_row(seq: u64, message: impl Into<String>) -> Self {
        Self {
            seq,
            status: ResponseStatus::Ok,
            columns: Some(vec!["status".into()]),
            rows: Some(vec![vec![Value::String(message.into())]]),
            rows_affected: Some(1),
            watermark_lsn: 0,
            error: None,
            auth: None,
            warnings: Vec::new(),
        }
    }
}
