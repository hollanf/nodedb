//! Tenant DDL handlers.
//!
//! - [`create`] — `CREATE TENANT` (proposes `CatalogEntry::PutTenant`
//!   in phase 1k.6 to fix the pre-existing persistence bug).
//! - [`alter`] — `ALTER TENANT SET QUOTA` (in-memory; quota is not
//!   part of `StoredTenant` and replication of quotas is intentionally
//!   out of scope for batch 1k).
//! - [`drop`] — `DROP TENANT` (proposes `DeleteTenant`).
//! - [`purge`] — `PURGE TENANT <id> CONFIRM` (Data Plane meta op).
//! - [`show`] — `SHOW TENANT USAGE` / `SHOW TENANT QUOTA` reads.

pub mod alter;
pub mod create;
pub mod drop;
pub mod purge;
pub mod show;

pub use alter::alter_tenant;
pub use create::create_tenant;
pub use drop::drop_tenant;
pub use purge::purge_tenant;
pub use show::{show_tenant_quota, show_tenant_usage};
