//! Shared error type for string-based identifier validation.

/// Maximum byte length for all string ID types.
///
/// IDs exceeding this are rejected by `try_new` to bound allocations and
/// prevent key-space abuse. The cap is generous enough for any realistic
/// identifier (UUIDs, URNs, DNS-qualified names, etc.).
pub const ID_MAX_LEN: usize = 1024;

/// Error returned when constructing a string-based ID type fails validation.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IdError {
    /// The supplied string was empty.
    #[error("ID must not be empty")]
    Empty,

    /// The supplied string exceeded the maximum allowed byte length.
    #[error("ID is too long: {len} bytes (max {max})")]
    TooLong {
        /// Actual byte length of the rejected string.
        len: usize,
        /// Maximum allowed byte length.
        max: usize,
    },

    /// The supplied string contained a NUL byte (`\0`), which is disallowed
    /// because storage backends treat NUL as a key terminator.
    #[error("ID must not contain NUL bytes")]
    ContainsNul,

    /// A requested ID length falls outside the allowed range for the generator.
    #[error("id length {requested} out of range [{min}, {max}]")]
    LengthOutOfRange {
        /// The length that was requested.
        requested: usize,
        /// The minimum allowed length.
        min: usize,
        /// The maximum allowed length.
        max: usize,
    },
}

/// Validate a candidate ID string against the shared rules.
///
/// Returns `Ok(())` if the string passes all checks, or the first
/// failing `IdError` variant otherwise.
pub(super) fn validate(id: &str) -> Result<(), IdError> {
    if id.is_empty() {
        return Err(IdError::Empty);
    }
    if id.len() > ID_MAX_LEN {
        return Err(IdError::TooLong {
            len: id.len(),
            max: ID_MAX_LEN,
        });
    }
    if id.contains('\0') {
        return Err(IdError::ContainsNul);
    }
    Ok(())
}
