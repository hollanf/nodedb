//! Graph node identifier.

use std::fmt;

use serde::{Deserialize, Serialize};

use super::error::{IdError, validate};

/// Identifies a graph node. Separate from `DocumentId` because graph nodes
/// can exist independently of documents (e.g., concept nodes in a knowledge graph).
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
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct NodeId(String);

impl NodeId {
    /// Construct a `NodeId`, validating the input string.
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

impl fmt::Display for NodeId {
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
        let n = NodeId::try_new("concept:rust").expect("valid");
        assert_eq!(n.as_str(), "concept:rust");
    }

    #[test]
    fn try_new_rejects_empty() {
        assert_eq!(NodeId::try_new(""), Err(IdError::Empty));
    }

    #[test]
    fn try_new_rejects_too_long() {
        let long = "x".repeat(ID_MAX_LEN + 1);
        assert!(matches!(
            NodeId::try_new(long),
            Err(IdError::TooLong { .. })
        ));
    }

    #[test]
    fn try_new_rejects_nul() {
        assert_eq!(NodeId::try_new("ab\0cd"), Err(IdError::ContainsNul));
    }

    #[test]
    fn try_new_accepts_max_length() {
        let exact = "a".repeat(ID_MAX_LEN);
        assert!(NodeId::try_new(exact).is_ok());
    }

    #[test]
    fn try_new_accepts_unicode() {
        assert!(NodeId::try_new("节点:rust").is_ok());
    }

    #[test]
    fn from_validated_does_not_validate() {
        let oversized = "z".repeat(ID_MAX_LEN * 2);
        let n = NodeId::from_validated(oversized.clone());
        assert_eq!(n.as_str(), oversized);
    }
}
