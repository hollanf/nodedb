//! Document identifier.

use std::fmt;

use serde::{Deserialize, Serialize};

use super::error::{IdError, validate};

/// Identifies a document/row across all engines.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub struct DocumentId(String);

impl DocumentId {
    /// Construct a `DocumentId`, validating the input string.
    ///
    /// Returns `Err(IdError)` if the string is empty, exceeds
    /// [`ID_MAX_LEN`][super::error::ID_MAX_LEN] bytes, or contains a NUL byte.
    pub fn try_new(id: impl Into<String>) -> Result<Self, IdError> {
        let s = id.into();
        validate(&s)?;
        Ok(Self(s))
    }

    /// Construct without validation. Caller must guarantee the input was
    /// already validated by `try_new` (or came from a previously-validated
    /// source like deserialized wire bytes from a NodeDB server).
    pub fn from_validated(id: String) -> Self {
        Self(id)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for DocumentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::super::error::ID_MAX_LEN;
    use super::*;

    #[test]
    fn try_new_accepts_valid() {
        let d = DocumentId::try_new("doc-abc-123").expect("valid");
        assert_eq!(d.as_str(), "doc-abc-123");
    }

    #[test]
    fn try_new_rejects_empty() {
        assert_eq!(DocumentId::try_new(""), Err(IdError::Empty));
    }

    #[test]
    fn try_new_rejects_too_long() {
        let long = "x".repeat(ID_MAX_LEN + 1);
        assert!(matches!(
            DocumentId::try_new(long),
            Err(IdError::TooLong { .. })
        ));
    }

    #[test]
    fn try_new_rejects_nul() {
        assert_eq!(DocumentId::try_new("ab\0cd"), Err(IdError::ContainsNul));
    }

    #[test]
    fn try_new_accepts_max_length() {
        let exact = "a".repeat(ID_MAX_LEN);
        assert!(DocumentId::try_new(exact).is_ok());
    }

    #[test]
    fn try_new_accepts_unicode() {
        assert!(DocumentId::try_new("doc-ünïcödé-001").is_ok());
    }

    #[test]
    fn from_validated_does_not_validate() {
        let oversized = "z".repeat(ID_MAX_LEN * 2);
        let d = DocumentId::from_validated(oversized.clone());
        assert_eq!(d.as_str(), oversized);
    }
}
