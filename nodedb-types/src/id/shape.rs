//! Shape subscription identifier.

use std::borrow::Borrow;
use std::fmt;

use serde::{Deserialize, Serialize};

use super::error::{IdError, validate};

/// Identifies a shape subscription (globally unique per Origin).
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
pub struct ShapeId(String);

impl ShapeId {
    /// Construct a `ShapeId`, validating the input string.
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

impl fmt::Display for ShapeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Borrow<str> for ShapeId {
    fn borrow(&self) -> &str {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::super::error::ID_MAX_LEN;
    use super::*;

    #[test]
    fn try_new_accepts_valid() {
        let s = ShapeId::try_new("shape-001").expect("valid");
        assert_eq!(s.as_str(), "shape-001");
    }

    #[test]
    fn try_new_rejects_empty() {
        assert_eq!(ShapeId::try_new(""), Err(IdError::Empty));
    }

    #[test]
    fn try_new_rejects_too_long() {
        let long = "x".repeat(ID_MAX_LEN + 1);
        assert!(matches!(
            ShapeId::try_new(long),
            Err(IdError::TooLong { .. })
        ));
    }

    #[test]
    fn try_new_rejects_nul() {
        assert_eq!(ShapeId::try_new("ab\0cd"), Err(IdError::ContainsNul));
    }

    #[test]
    fn try_new_accepts_max_length() {
        let exact = "a".repeat(ID_MAX_LEN);
        assert!(ShapeId::try_new(exact).is_ok());
    }

    #[test]
    fn try_new_accepts_unicode() {
        assert!(ShapeId::try_new("形状:001").is_ok());
    }

    #[test]
    fn from_validated_does_not_validate() {
        let oversized = "z".repeat(ID_MAX_LEN * 2);
        let s = ShapeId::from_validated(oversized.clone());
        assert_eq!(s.as_str(), oversized);
    }
}
