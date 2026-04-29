pub mod analyzer_config;
pub mod error;
pub mod fieldnorm;
pub mod stats;
pub mod writer;

pub use error::{FtsIndexError, MAX_INDEXABLE_SURROGATE};
pub use writer::FtsIndex;
