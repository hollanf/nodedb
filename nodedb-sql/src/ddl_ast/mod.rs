//! Typed AST for NodeDB-specific DDL statements.
//!
//! Every DDL command the system supports is represented as a variant
//! of [`NodedbStatement`]. The DDL router matches on this enum
//! instead of string prefixes, so the compiler catches missing
//! handlers when a new DDL is added.
//!
//! The parser ([`parse`]) converts raw SQL into a `NodedbStatement`
//! using whitespace-split token matching — the same technique the
//! old string-prefix router used, but producing a typed output.

pub mod graph_parse;
pub mod parse;
pub mod statement;

pub use graph_parse::{FusionParams, parse_search_using_fusion};
pub use parse::parse;
pub use statement::{GraphDirection, GraphProperties, NodedbStatement};
