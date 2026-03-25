mod compaction;
pub mod index;
pub mod memory;
pub mod traversal;
pub mod weights;

pub use index::CsrIndex;
pub use weights::extract_weight_from_properties;
