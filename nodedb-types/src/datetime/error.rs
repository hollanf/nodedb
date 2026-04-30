//! Error type for datetime and duration construction and arithmetic.

/// Error type for datetime and duration construction and arithmetic.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NdbDateTimeError {
    /// A multiplication would overflow `i64`.
    #[error("datetime overflow: input {input} {unit} overflows i64 microseconds")]
    Overflow { input: i64, unit: &'static str },

    /// Addition of two datetime/duration values would overflow `i64`.
    #[error("datetime overflow: addition overflows i64 microseconds")]
    AddOverflow,

    /// Subtraction of two datetime/duration values would overflow `i64`.
    #[error("datetime overflow: subtraction overflows i64 microseconds")]
    SubOverflow,
}

impl From<NdbDateTimeError> for crate::error::NodeDbError {
    fn from(e: NdbDateTimeError) -> Self {
        crate::error::NodeDbError::plan_error_at("datetime", e.to_string())
    }
}
