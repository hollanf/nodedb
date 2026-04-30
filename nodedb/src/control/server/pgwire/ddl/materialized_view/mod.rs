//! Materialized view DDL — split by concern.
//!
//! - [`create`] — `CREATE MATERIALIZED VIEW` (propose through raft)
//! - [`drop`]   — `DROP MATERIALIZED VIEW [IF EXISTS]` (propose through raft)
//! - [`refresh`] — `REFRESH MATERIALIZED VIEW` (Data Plane dispatch)
//! - [`show`]   — `SHOW MATERIALIZED VIEWS [FOR <source>]`

pub mod create;
pub mod drop;
pub mod refresh;
pub mod show;

pub use create::create_materialized_view;
pub use drop::drop_materialized_view;
pub use refresh::refresh_materialized_view;
pub use show::show_materialized_views;
