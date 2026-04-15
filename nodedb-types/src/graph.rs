//! Shared graph types used by both Origin and Lite CSR engines.

use serde::{Deserialize, Serialize};

/// Edge traversal direction.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(c_enum)]
pub enum Direction {
    /// Outgoing edges only.
    Out,
    /// Incoming edges only.
    In,
    /// Both directions.
    Both,
}

impl Direction {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Out => "out",
            Self::In => "in",
            Self::Both => "both",
        }
    }
}

impl std::fmt::Display for Direction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for Direction {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "out" | "outgoing" => Ok(Self::Out),
            "in" | "incoming" => Ok(Self::In),
            "both" | "any" => Ok(Self::Both),
            other => Err(format!("unknown direction: '{other}'")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn direction_roundtrip() {
        for dir in [Direction::Out, Direction::In, Direction::Both] {
            let s = dir.as_str();
            let parsed: Direction = s.parse().unwrap();
            assert_eq!(dir, parsed);
        }
    }

    #[test]
    fn direction_display() {
        assert_eq!(Direction::Out.to_string(), "out");
    }
}
