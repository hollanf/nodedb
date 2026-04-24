//! Bitemporal audit-retention: per-collection configuration registry and
//! background enforcement loop that purges superseded versions older than
//! each collection's `audit_retain_ms` window, while preserving the single
//! latest version of every logical row.
//!
//! Population of the registry is the DDL layer's responsibility (e.g.
//! `CREATE COLLECTION ... WITH BITEMPORAL RETENTION (AUDIT_RETAIN = '7d')`).
//! Enforcement runs on the Event Plane as a Tokio background loop and
//! dispatches `MetaOp::TemporalPurge{EdgeStore,DocumentStrict,Columnar}`
//! to the owning Data Plane core. Successful dispatches emit a durable
//! `RecordType::TemporalPurge` audit record via `WalManager`.

pub mod enforcement;
pub mod registry;

pub use enforcement::spawn_bitemporal_retention_loop;
pub use registry::{BitemporalEngineKind, BitemporalRetentionRegistry, RegisterError};
