pub mod traits;

#[cfg(feature = "remote")]
pub mod remote;

pub use traits::NodeDb;

#[cfg(feature = "remote")]
pub use remote::NodeDbRemote;

// Re-export core types so users only need `nodedb-client` in their Cargo.toml.
pub use nodedb_types::error::{NodeDbError, NodeDbResult};
pub use nodedb_types::{
    Document, EdgeFilter, EdgeId, MetadataFilter, NodeId, QueryResult, SearchResult, SubGraph,
    Value,
};
