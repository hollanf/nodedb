//! `/v1/cluster/debug/*` authenticated dev-ops endpoints (J.5).
//!
//! Each handler dumps a different slice of in-memory cluster state:
//!
//! - [`raft`] — per-Raft-group status (role, term, commit index, ...).
//! - [`transport`] — connection cache + circuit-breaker state.
//! - [`catalog`] — `MetadataCache` snapshot (applied index, cluster
//!   version, topology/routing history, applied DDL count).
//! - [`leases`] — descriptor leases + active drains.
//!
//! Every handler requires a superuser identity **and** the
//! `observability.debug_endpoints_enabled` config flag — see
//! [`guard::ensure_debug_access`].

pub mod catalog;
pub mod guard;
pub mod leases;
pub mod quarantined_segments;
pub mod raft;
pub mod transport;
