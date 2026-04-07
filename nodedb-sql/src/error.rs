//! Error types for the nodedb-sql crate.

/// Errors produced during SQL parsing, resolution, or planning.
#[derive(Debug, thiserror::Error)]
pub enum SqlError {
    #[error("parse error: {detail}")]
    Parse { detail: String },

    #[error("unknown table: {name}")]
    UnknownTable { name: String },

    #[error("unknown column '{column}' in table '{table}'")]
    UnknownColumn { table: String, column: String },

    #[error("ambiguous column '{column}' — qualify with table name")]
    AmbiguousColumn { column: String },

    #[error("type mismatch: {detail}")]
    TypeMismatch { detail: String },

    #[error("unsupported: {detail}")]
    Unsupported { detail: String },

    #[error("invalid function call: {detail}")]
    InvalidFunction { detail: String },

    #[error("missing required field '{field}' for {context}")]
    MissingField { field: String, context: String },
}

impl From<sqlparser::parser::ParserError> for SqlError {
    fn from(e: sqlparser::parser::ParserError) -> Self {
        Self::Parse {
            detail: e.to_string(),
        }
    }
}

pub type Result<T> = std::result::Result<T, SqlError>;
