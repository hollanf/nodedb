//! Collection DDL: CREATE COLLECTION, DROP COLLECTION, SHOW COLLECTIONS,
//! DESCRIBE, indexes, and ALTER commands.

pub mod alter;
pub mod create;
pub mod describe;
pub mod drop;
pub mod helpers;
pub mod index;

// Re-export all public functions so existing callers via `super::collection::*` continue to work.
pub use alter::{alter_collection_enforcement, alter_table_add_column};
pub use create::{create_collection, dispatch_register_if_needed};
pub use describe::{describe_collection, show_collections};
pub use drop::drop_collection;
pub use index::{create_index, drop_index, show_indexes};

// Re-export validate_document_schema from schema_validation (was re-exported from old collection.rs).
pub use super::schema_validation::validate_document_schema;

// Re-export parse_typed_schema for kv.rs which imports it as `super::collection::parse_typed_schema`.
pub(in crate::control::server::pgwire::ddl) use helpers::parse_typed_schema;
