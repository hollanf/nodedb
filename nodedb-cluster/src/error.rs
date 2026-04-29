use thiserror::Error;

pub type Result<T> = std::result::Result<T, ClusterError>;

/// Error emitted when applying or validating a `MigrationCheckpoint` entry.
#[derive(Debug, Error)]
pub enum MigrationCheckpointError {
    #[error(
        "crc32c mismatch on migration checkpoint for {migration_id}: expected {expected:#010x} got {actual:#010x}"
    )]
    Crc32cMismatch {
        migration_id: uuid::Uuid,
        expected: u32,
        actual: u32,
    },
    #[error("codec error persisting migration checkpoint: {detail}")]
    Codec { detail: String },
    #[error("storage error persisting migration checkpoint: {detail}")]
    Storage { detail: String },
}

/// Error emitted during in-flight migration recovery at startup.
#[derive(Debug, Error)]
pub enum MigrationRecoveryError {
    #[error("compensation failed for migration {migration_id} step {step}: {detail}")]
    CompensationFailed {
        migration_id: uuid::Uuid,
        step: usize,
        detail: String,
    },
    #[error("storage error during migration recovery: {detail}")]
    Storage { detail: String },
    #[error("codec error during migration recovery: {detail}")]
    Codec { detail: String },
}

#[derive(Debug, Error)]
pub enum ClusterError {
    #[error("raft error: {0}")]
    Raft(#[from] nodedb_raft::RaftError),

    #[error("vshard {vshard_id} not mapped to any raft group")]
    VShardNotMapped { vshard_id: u32 },

    #[error("raft group {group_id} not found on this node")]
    GroupNotFound { group_id: u64 },

    #[error("migration in progress for vshard {vshard_id}")]
    MigrationInProgress { vshard_id: u32 },

    #[error("migration refused: estimated pause {estimated_us}µs exceeds budget {budget_us}µs")]
    MigrationPauseBudgetExceeded { estimated_us: u64, budget_us: u64 },

    #[error("node {node_id} not reachable")]
    NodeUnreachable { node_id: u64 },

    #[error("ghost stub not found: node={node_id} on shard={shard_id}")]
    GhostNotFound { node_id: String, shard_id: u32 },

    #[error("transport error: {detail}")]
    Transport { detail: String },

    #[error("storage error: {detail}")]
    Storage { detail: String },

    #[error("codec error: {detail}")]
    Codec { detail: String },

    #[error("circuit open for node {node_id}: peer has {failures} consecutive failures")]
    CircuitOpen { node_id: u64, failures: u32 },

    #[error("raft group {group_id} disappeared while waiting for conf change commit")]
    JoinGroupDisappeared { group_id: u64 },

    #[error("conf change commit timeout on group {group_id} (waited for index {log_index})")]
    JoinCommitTimeout { group_id: u64, log_index: u64 },

    #[error("migration checkpoint error: {0}")]
    MigrationCheckpoint(#[from] MigrationCheckpointError),

    #[error("migration recovery error: {0}")]
    MigrationRecovery(#[from] MigrationRecoveryError),

    /// A shard RPC was routed to a node that no longer owns the target vShard.
    ///
    /// This surfaces when vShard ownership has transferred (rebalance or split
    /// cut-over) after the coordinator computed its routing plan. The coordinator
    /// must refresh its routing table and retry against the new owner.
    ///
    /// `expected_owner_node` is `Some` when the receiving shard knows who the
    /// current owner is, and `None` when it does not (e.g. during a brief
    /// transition window). Either way, the coordinator should re-derive the owner
    /// from its local routing table — `expected_owner_node` is advisory only.
    #[error(
        "vshard {vshard_id} misrouted: this node is no longer the owner\
         {}", if let Some(n) = expected_owner_node { format!("; current owner may be node {n}") } else { String::new() }
    )]
    WrongOwner {
        vshard_id: u32,
        expected_owner_node: Option<u64>,
    },
}
