//! First-class DateTime and Duration types.
//!
//! `NdbDateTime` stores microseconds since Unix epoch (1970-01-01T00:00:00Z).
//! `NdbDuration` stores microseconds as a signed i64.
//!
//! Both serialize as strings (ISO 8601 for DateTime, human-readable for Duration)
//! for JSON compatibility. Internal representation is i64 for efficient comparison
//! and arithmetic.

pub mod duration;
pub mod error;
pub mod timestamp;

pub use duration::NdbDuration;
pub use error::NdbDateTimeError;
pub use timestamp::{DateTimeComponents, NdbDateTime};
