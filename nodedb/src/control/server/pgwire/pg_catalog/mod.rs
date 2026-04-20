//! Minimal `pg_catalog` virtual-table emulation.
//!
//! Generic Postgres clients (DBeaver, pgAdmin, SQLAlchemy, psql's
//! `\dt`) issue `SELECT` queries against `pg_catalog.*` tables to
//! discover schemas, types, and tables. Without a response they
//! either error out or show an empty catalog. This module intercepts
//! those queries and returns rows synthesised from NodeDB's own
//! `SystemCatalog` and credential store.
//!
//! The interception is pattern-based: we extract the first
//! `pg_catalog.<table>` (or bare `pg_<table>`) reference from the
//! `FROM` clause and delegate to the matching virtual table handler.
//! The result always returns ALL rows with a fixed column schema —
//! clients that send `WHERE` clauses filter client-side.

pub mod dispatch;
pub mod dropped_collections;
pub mod l2_cleanup_queue;
pub mod tables;

pub use dispatch::{extract_pg_catalog_table, pg_catalog_schema, try_pg_catalog};
