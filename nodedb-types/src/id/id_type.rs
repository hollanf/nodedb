//! Enum representing the detected or assigned format of a string-based ID.

/// The format of a detected or generated string-based identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IdType {
    /// A UUID (any version) in the standard `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` format.
    Uuid,
    /// A ULID — 26-character Crockford Base32, time-sortable.
    Ulid,
    /// A CUID2 — lowercase alphanumeric string, 4–32 characters, starts with a letter.
    Cuid2,
    /// A NanoID — URL-safe alphanumeric + `_-`, 10–64 characters.
    NanoId,
    /// Any other identifier format that does not match the known patterns.
    Custom,
}

impl IdType {
    /// Return the canonical string name used in SQL function output and JSON responses.
    pub fn as_str(&self) -> &'static str {
        match self {
            IdType::Uuid => "uuid",
            IdType::Ulid => "ulid",
            IdType::Cuid2 => "cuid2",
            IdType::NanoId => "nanoid",
            IdType::Custom => "custom",
        }
    }
}
