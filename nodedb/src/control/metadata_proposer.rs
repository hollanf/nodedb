//! Synchronous `propose-and-wait-for-local-apply` helper for
//! replicated catalog DDL.
//!
//! This is the single entry point pgwire DDL handlers use to write a
//! `MetadataEntry` through the metadata raft group (group 0). It is
//! deliberately sync — pgwire DDL handlers today are not async, and
//! `tokio::task::block_in_place`-style workarounds inside tokio tasks
//! are a footgun. The blocking wait is implemented with
//! [`crate::control::cluster::AppliedIndexWatcher`] (a plain
//! `Condvar`), so it composes cleanly with tokio's current-thread
//! and multi-thread runtimes alike.
//!
//! Semantics:
//!
//! 1. If the node is in single-node / no-cluster mode, the proposer
//!    returns `Ok(0)` immediately. The caller's legacy direct-write
//!    path (e.g. `SystemCatalog::put_collection`) remains
//!    authoritative until batch 1c migrates pgwire handlers fully.
//! 2. If this node is the metadata-group leader, it proposes the
//!    entry, records the log index, and blocks until its local
//!    applied watermark reaches that index (5s default timeout).
//! 3. If this node is NOT the leader, the proposer returns
//!    `NodeDbError::Cluster { detail: "not metadata-group leader ..." }`.
//!    Phase C will replace this with a gateway-side redirect so every
//!    node can accept DDL. Until then, clients must target the leader.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use nodedb_cluster::{MetadataEntry, encode_entry};

use crate::control::cluster::applied_index_watcher::AppliedIndexWatcher;
use crate::control::state::SharedState;
use crate::error::Error;

/// Default upper bound on how long a single `propose_metadata_and_wait`
/// call will block before returning an error.
pub const DEFAULT_PROPOSE_TIMEOUT: Duration = Duration::from_secs(5);

/// Type-erased handle for proposing to the metadata raft group.
///
/// The concrete implementation ([`RaftLoopProposerHandle`]) wraps an
/// `Arc<RaftLoop<..>>`. The trait exists so `SharedState` can store a
/// non-generic handle — the `RaftLoop` type has a pair of generic
/// parameters (applier + forwarder) that would otherwise leak into
/// `SharedState`.
pub trait MetadataRaftHandle: Send + Sync {
    /// Propose a raw encoded `MetadataEntry` to the metadata group and
    /// return its assigned log index on success.
    fn propose(&self, bytes: Vec<u8>) -> Result<u64, Error>;

    /// The applied-index watcher backing this handle.
    fn watcher(&self) -> Arc<AppliedIndexWatcher>;
}

/// Concrete implementation that wraps the `nodedb-cluster` `RaftLoop`.
pub struct RaftLoopProposerHandle {
    raft_loop: Arc<
        nodedb_cluster::RaftLoop<
            crate::control::cluster::SpscCommitApplier,
            crate::control::LocalForwarder,
        >,
    >,
    watcher: OnceLock<Arc<AppliedIndexWatcher>>,
}

impl RaftLoopProposerHandle {
    pub fn new(
        raft_loop: Arc<
            nodedb_cluster::RaftLoop<
                crate::control::cluster::SpscCommitApplier,
                crate::control::LocalForwarder,
            >,
        >,
    ) -> Self {
        Self {
            raft_loop,
            watcher: OnceLock::new(),
        }
    }

    pub fn with_watcher(self, watcher: Arc<AppliedIndexWatcher>) -> Self {
        let _ = self.watcher.set(watcher);
        self
    }
}

impl MetadataRaftHandle for RaftLoopProposerHandle {
    fn propose(&self, bytes: Vec<u8>) -> Result<u64, Error> {
        self.raft_loop
            .propose_to_metadata_group(bytes)
            .map_err(|e| Error::Config {
                detail: format!("metadata propose: {e}"),
            })
    }

    fn watcher(&self) -> Arc<AppliedIndexWatcher> {
        self.watcher
            .get()
            .cloned()
            .unwrap_or_else(|| Arc::new(AppliedIndexWatcher::new()))
    }
}

/// Propose a `MetadataEntry` and block until the local applied-index
/// watcher confirms the entry has been applied on this node.
///
/// In single-node / no-cluster mode (no `metadata_raft` installed on
/// `SharedState`), returns `Ok(0)` immediately — the caller is expected
/// to fall back to the legacy direct-write path.
pub fn propose_metadata_and_wait(
    shared: &SharedState,
    entry: &MetadataEntry,
) -> Result<u64, Error> {
    propose_metadata_with_timeout(shared, entry, DEFAULT_PROPOSE_TIMEOUT)
}

/// Same as [`propose_metadata_and_wait`] but with an explicit timeout.
pub fn propose_metadata_with_timeout(
    shared: &SharedState,
    entry: &MetadataEntry,
    timeout: Duration,
) -> Result<u64, Error> {
    let Some(handle) = shared.metadata_raft.get() else {
        // Single-node / no-cluster mode: caller falls back to direct
        // `put_collection` / etc. Return `0` as the "no replicated
        // log index" sentinel.
        return Ok(0);
    };

    let bytes = encode_entry(entry).map_err(|e| Error::Config {
        detail: format!("metadata encode: {e}"),
    })?;

    let log_index = handle.propose(bytes)?;

    let watcher = shared.applied_index_watcher();
    if !watcher.wait_for(log_index, timeout) {
        return Err(Error::Config {
            detail: format!(
                "metadata propose timed out after {:?} waiting for log index {} (current: {})",
                timeout,
                log_index,
                watcher.current()
            ),
        });
    }

    Ok(log_index)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watcher_helper_returns_true_on_past_target() {
        let w = AppliedIndexWatcher::new();
        w.bump(10);
        assert!(w.wait_for(5, Duration::from_millis(1)));
    }
}
