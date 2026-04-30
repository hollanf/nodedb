//! Dead-Letter Queue (DLQ) for sync-rejected deltas.
//!
//! When a sync delta is rejected (RLS violation, rate limit, constraint
//! violation), the delta is persisted in the DLQ with full metadata for
//! later inspection, retry, or compensation.
//!
//! ## Entry Lifecycle
//!
//! 1. Delta rejected → `DlqEntry` created with reason, session metadata
//! 2. Entry stored in per-collection bounded queue
//! 3. Periodic sweep checks TTL expiry
//! 4. Expired entries handled per `DlqExpiryAction`:
//!    - `Drop`: silently discard
//!    - `Escalate`: forward to admin notification channel
//!    - `ForceApplyLww`: force-apply using last-writer-wins semantics
//!
//! ## Persistence
//!
//! In-memory with bounded capacity per collection. For durable persistence,
//! entries are flushed to the WAL as `RecordType::DlqEntry` records. On
//! restart, DLQ state is rebuilt from WAL replay.

use std::collections::{HashMap, VecDeque};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

// Re-export shared types from nodedb-types so existing `use super::dlq::*` imports work.
pub use nodedb_types::sync::compensation::CompensationHint;
pub use nodedb_types::sync::violation::ViolationType;

/// DLQ entry: a rejected sync delta with full forensic metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqEntry {
    /// Unique entry ID (monotonic within this node).
    pub entry_id: u64,
    /// Session that submitted the delta.
    pub session_id: String,
    /// Tenant context.
    pub tenant_id: u64,
    /// Username of the submitter.
    pub username: String,
    /// Target collection.
    pub collection: String,
    /// Target document ID.
    pub document_id: String,
    /// Original mutation ID from the client.
    pub mutation_id: u64,
    /// Client peer ID (CRDT identity).
    pub peer_id: u64,
    /// The rejected delta bytes.
    pub delta: Vec<u8>,
    /// Why the delta was rejected.
    pub violation_type: ViolationType,
    /// Compensation hint sent (or that would have been sent) to the client.
    pub compensation: Option<CompensationHint>,
    /// When the entry was created (epoch seconds).
    pub created_at: u64,
    /// Originating device/client metadata (user-agent, IP, etc.).
    pub device_metadata: DeviceMetadata,
    /// Number of retry attempts.
    pub retry_count: u32,
    /// Whether this entry has been acknowledged/resolved.
    pub resolved: bool,
}

/// Originating device metadata attached to DLQ entries.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DeviceMetadata {
    /// Client user-agent string (from handshake).
    pub client_version: String,
    /// Client IP address.
    pub remote_addr: String,
    /// Client peer ID for CRDT identity.
    pub peer_id: u64,
}

/// What to do when a DLQ entry expires.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum DlqExpiryAction {
    /// Silently discard the entry.
    #[default]
    Drop,
    /// Escalate to admin notification channel.
    Escalate,
    /// Force-apply using last-writer-wins semantics.
    ForceApplyLww,
}

/// Per-collection DLQ expiry policy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqExpiryPolicy {
    /// Time-to-live for DLQ entries in seconds.
    pub ttl_secs: u64,
    /// Action to take when entries expire.
    pub expiry_action: DlqExpiryAction,
}

impl Default for DlqExpiryPolicy {
    fn default() -> Self {
        Self {
            ttl_secs: 72 * 3600, // 72 hours.
            expiry_action: DlqExpiryAction::Drop,
        }
    }
}

/// DLQ configuration.
#[derive(Debug, Clone)]
pub struct DlqConfig {
    /// Maximum entries per collection before oldest are evicted.
    pub max_entries_per_collection: usize,
    /// Default expiry policy (used when no per-collection policy is set).
    pub default_policy: DlqExpiryPolicy,
}

impl Default for DlqConfig {
    fn default() -> Self {
        Self {
            max_entries_per_collection: 10_000,
            default_policy: DlqExpiryPolicy::default(),
        }
    }
}

/// Expired DLQ entry with the action to take.
#[derive(Debug, Clone)]
pub struct ExpiredEntry {
    /// The expired DLQ entry.
    pub entry: DlqEntry,
    /// The action determined by the expiry policy.
    pub action: DlqExpiryAction,
}

/// Parameters for enqueuing a rejected delta into the DLQ.
pub struct DlqEnqueueParams {
    pub session_id: String,
    pub tenant_id: u64,
    pub username: String,
    pub collection: String,
    pub document_id: String,
    pub mutation_id: u64,
    pub peer_id: u64,
    pub delta: Vec<u8>,
    pub violation_type: ViolationType,
    pub compensation: Option<CompensationHint>,
    pub device_metadata: DeviceMetadata,
}

/// Sync Dead-Letter Queue.
///
/// Stores rejected deltas per collection with bounded capacity and
/// configurable expiry policies.
pub struct SyncDlq {
    /// Per-collection queues: `"{tenant_id}:{collection}"` → entries.
    queues: HashMap<String, VecDeque<DlqEntry>>,
    /// Per-collection expiry policies.
    policies: HashMap<String, DlqExpiryPolicy>,
    /// Default config.
    config: DlqConfig,
    /// Monotonic entry ID.
    next_entry_id: u64,
    /// Total entries ever enqueued.
    total_enqueued: u64,
    /// Total entries expired.
    total_expired: u64,
    /// Total entries resolved.
    total_resolved: u64,
}

impl SyncDlq {
    pub fn new(config: DlqConfig) -> Self {
        Self {
            queues: HashMap::new(),
            policies: HashMap::new(),
            config,
            next_entry_id: 1,
            total_enqueued: 0,
            total_expired: 0,
            total_resolved: 0,
        }
    }

    /// Set the expiry policy for a specific collection.
    pub fn set_expiry_policy(&mut self, tenant_id: u64, collection: &str, policy: DlqExpiryPolicy) {
        let key = format!("{tenant_id}:{collection}");
        info!(
            %key,
            ttl_secs = policy.ttl_secs,
            action = ?policy.expiry_action,
            "DLQ expiry policy set"
        );
        self.policies.insert(key, policy);
    }

    /// Get the expiry policy for a collection (falls back to default).
    pub fn expiry_policy(&self, tenant_id: u64, collection: &str) -> &DlqExpiryPolicy {
        let key = format!("{tenant_id}:{collection}");
        self.policies
            .get(&key)
            .unwrap_or(&self.config.default_policy)
    }

    /// Parameters for enqueuing a rejected delta.
    pub fn enqueue(&mut self, params: DlqEnqueueParams) -> u64 {
        let entry_id = self.next_entry_id;
        self.next_entry_id += 1;

        let entry = DlqEntry {
            entry_id,
            session_id: params.session_id,
            tenant_id: params.tenant_id,
            username: params.username,
            collection: params.collection,
            document_id: params.document_id,
            mutation_id: params.mutation_id,
            peer_id: params.peer_id,
            delta: params.delta,
            violation_type: params.violation_type.clone(),
            compensation: params.compensation,
            created_at: now_epoch_secs(),
            device_metadata: params.device_metadata,
            retry_count: 0,
            resolved: false,
        };

        let key = format!("{}:{}", entry.tenant_id, entry.collection);
        let queue = self
            .queues
            .entry(key)
            .or_insert_with(|| VecDeque::with_capacity(128));

        // Evict oldest if at capacity.
        if queue.len() >= self.config.max_entries_per_collection
            && let Some(evicted) = queue.pop_front()
        {
            warn!(
                entry_id = evicted.entry_id,
                collection = %evicted.collection,
                "DLQ entry evicted (capacity)"
            );
        }

        debug!(
            entry_id,
            session = %entry.session_id,
            collection = %entry.collection,
            violation = %entry.violation_type,
            "DLQ entry enqueued"
        );

        queue.push_back(entry);
        self.total_enqueued += 1;

        entry_id
    }

    /// Get all unresolved entries for a collection.
    pub fn entries_for_collection(&self, tenant_id: u64, collection: &str) -> Vec<&DlqEntry> {
        let key = format!("{tenant_id}:{collection}");
        self.queues
            .get(&key)
            .map(|q| q.iter().filter(|e| !e.resolved).collect())
            .unwrap_or_default()
    }

    /// Get a specific entry by ID.
    pub fn get_entry(&self, entry_id: u64) -> Option<&DlqEntry> {
        self.queues
            .values()
            .flat_map(|q| q.iter())
            .find(|e| e.entry_id == entry_id)
    }

    /// Mark an entry as resolved (acknowledged/retried successfully).
    pub fn resolve_entry(&mut self, entry_id: u64) -> bool {
        for queue in self.queues.values_mut() {
            if let Some(entry) = queue.iter_mut().find(|e| e.entry_id == entry_id) {
                entry.resolved = true;
                self.total_resolved += 1;
                return true;
            }
        }
        false
    }

    /// Increment the retry count for an entry.
    pub fn increment_retry(&mut self, entry_id: u64) -> bool {
        for queue in self.queues.values_mut() {
            if let Some(entry) = queue.iter_mut().find(|e| e.entry_id == entry_id) {
                entry.retry_count += 1;
                return true;
            }
        }
        false
    }

    /// Sweep all queues for expired entries and return them with their actions.
    ///
    /// Called periodically by the sync maintenance loop. The caller is
    /// responsible for executing the expiry actions (drop, escalate, or
    /// force-apply).
    pub fn sweep_expired(&mut self) -> Vec<ExpiredEntry> {
        let now = now_epoch_secs();
        let mut expired = Vec::new();

        for (key, queue) in &mut self.queues {
            let policy = self
                .policies
                .get(key)
                .unwrap_or(&self.config.default_policy);
            let ttl = policy.ttl_secs;
            let action = policy.expiry_action;

            // Collect expired entries.
            let mut remaining = VecDeque::with_capacity(queue.len());
            for entry in queue.drain(..) {
                if !entry.resolved && now.saturating_sub(entry.created_at) >= ttl {
                    expired.push(ExpiredEntry { entry, action });
                } else {
                    remaining.push_back(entry);
                }
            }
            *queue = remaining;
        }

        self.total_expired += expired.len() as u64;

        if !expired.is_empty() {
            info!(
                count = expired.len(),
                "DLQ sweep: expired entries collected"
            );
        }

        expired
    }

    /// Total entries currently in the DLQ (including resolved).
    pub fn total_entries(&self) -> usize {
        self.queues.values().map(|q| q.len()).sum()
    }

    /// Total unresolved entries across all collections.
    pub fn unresolved_entries(&self) -> usize {
        self.queues
            .values()
            .flat_map(|q| q.iter())
            .filter(|e| !e.resolved)
            .count()
    }

    /// Total entries ever enqueued.
    pub fn total_enqueued(&self) -> u64 {
        self.total_enqueued
    }

    /// Total entries expired.
    pub fn total_expired(&self) -> u64 {
        self.total_expired
    }

    /// Total entries resolved.
    pub fn total_resolved(&self) -> u64 {
        self.total_resolved
    }

    /// Drain all entries for WAL persistence.
    ///
    /// Returns all entries (resolved and unresolved) for serialization
    /// into WAL records. After draining, the in-memory DLQ is empty.
    pub fn drain_for_persistence(&mut self) -> Vec<DlqEntry> {
        let mut all = Vec::new();
        for queue in self.queues.values_mut() {
            all.extend(queue.drain(..));
        }
        all
    }

    /// Restore entries from WAL replay.
    pub fn restore_entry(&mut self, entry: DlqEntry) {
        let key = format!("{}:{}", entry.tenant_id, entry.collection);
        if entry.entry_id >= self.next_entry_id {
            self.next_entry_id = entry.entry_id + 1;
        }
        self.queues
            .entry(key)
            .or_insert_with(|| VecDeque::with_capacity(128))
            .push_back(entry);
    }
}

fn now_epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_device() -> DeviceMetadata {
        DeviceMetadata {
            client_version: "0.1.0".into(),
            remote_addr: "10.0.0.1:5555".into(),
            peer_id: 99,
        }
    }

    /// Create default params, caller overrides specific fields.
    fn default_params() -> DlqEnqueueParams {
        DlqEnqueueParams {
            session_id: "s1".into(),
            tenant_id: 1,
            username: "alice".into(),
            collection: "orders".into(),
            document_id: "o1".into(),
            mutation_id: 1,
            peer_id: 1,
            delta: vec![],
            violation_type: ViolationType::PermissionDenied,
            compensation: None,
            device_metadata: make_device(),
        }
    }

    #[test]
    fn enqueue_and_retrieve() {
        let mut dlq = SyncDlq::new(DlqConfig::default());

        let id = dlq.enqueue(DlqEnqueueParams {
            session_id: "sess-1".into(),
            mutation_id: 42,
            delta: vec![1, 2, 3],
            compensation: Some(CompensationHint::PermissionDenied),
            ..default_params()
        });

        assert_eq!(id, 1);
        assert_eq!(dlq.total_entries(), 1);
        assert_eq!(dlq.unresolved_entries(), 1);

        let entries = dlq.entries_for_collection(1, "orders");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].session_id, "sess-1");
        assert_eq!(entries[0].username, "alice");
        assert_eq!(entries[0].mutation_id, 42);
    }

    #[test]
    fn resolve_entry() {
        let mut dlq = SyncDlq::new(DlqConfig::default());

        let id = dlq.enqueue(DlqEnqueueParams {
            username: "bob".into(),
            collection: "users".into(),
            document_id: "u1".into(),
            violation_type: ViolationType::RateLimited,
            ..default_params()
        });

        assert!(dlq.resolve_entry(id));
        assert_eq!(dlq.unresolved_entries(), 0);
        assert_eq!(dlq.total_resolved(), 1);

        let entries = dlq.entries_for_collection(1, "users");
        assert!(entries.is_empty());
    }

    #[test]
    fn capacity_eviction() {
        let config = DlqConfig {
            max_entries_per_collection: 3,
            ..Default::default()
        };
        let mut dlq = SyncDlq::new(config);

        for i in 0..5 {
            dlq.enqueue(DlqEnqueueParams {
                document_id: format!("o{i}"),
                mutation_id: i,
                ..default_params()
            });
        }

        assert_eq!(dlq.total_entries(), 3);
        let entries = dlq.entries_for_collection(1, "orders");
        assert_eq!(entries[0].document_id, "o2");
    }

    #[test]
    fn expiry_sweep_drop() {
        let config = DlqConfig {
            max_entries_per_collection: 100,
            default_policy: DlqExpiryPolicy {
                ttl_secs: 0,
                expiry_action: DlqExpiryAction::Drop,
            },
        };
        let mut dlq = SyncDlq::new(config);
        dlq.enqueue(default_params());

        let expired = dlq.sweep_expired();
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].action, DlqExpiryAction::Drop);
        assert_eq!(dlq.total_entries(), 0);
        assert_eq!(dlq.total_expired(), 1);
    }

    #[test]
    fn expiry_sweep_escalate() {
        let mut dlq = SyncDlq::new(DlqConfig::default());
        dlq.set_expiry_policy(
            1,
            "critical",
            DlqExpiryPolicy {
                ttl_secs: 0,
                expiry_action: DlqExpiryAction::Escalate,
            },
        );

        dlq.enqueue(DlqEnqueueParams {
            collection: "critical".into(),
            document_id: "c1".into(),
            delta: vec![1, 2, 3],
            violation_type: ViolationType::UniqueViolation {
                field: "email".into(),
                value: "a@b.com".into(),
            },
            compensation: Some(CompensationHint::UniqueViolation {
                field: "email".into(),
                conflicting_value: "a@b.com".into(),
            }),
            ..default_params()
        });

        let expired = dlq.sweep_expired();
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].action, DlqExpiryAction::Escalate);
    }

    #[test]
    fn non_expired_entries_survive_sweep() {
        let config = DlqConfig {
            max_entries_per_collection: 100,
            default_policy: DlqExpiryPolicy {
                ttl_secs: 3600,
                expiry_action: DlqExpiryAction::Drop,
            },
        };
        let mut dlq = SyncDlq::new(config);
        dlq.enqueue(default_params());

        let expired = dlq.sweep_expired();
        assert!(expired.is_empty());
        assert_eq!(dlq.total_entries(), 1);
    }

    #[test]
    fn resolved_entries_not_expired() {
        let config = DlqConfig {
            max_entries_per_collection: 100,
            default_policy: DlqExpiryPolicy {
                ttl_secs: 0,
                expiry_action: DlqExpiryAction::Drop,
            },
        };
        let mut dlq = SyncDlq::new(config);

        let id = dlq.enqueue(default_params());
        dlq.resolve_entry(id);

        let expired = dlq.sweep_expired();
        assert!(expired.is_empty());
        assert_eq!(dlq.total_entries(), 1);
    }

    #[test]
    fn drain_and_restore() {
        let mut dlq = SyncDlq::new(DlqConfig::default());

        dlq.enqueue(DlqEnqueueParams {
            delta: vec![10, 20],
            violation_type: ViolationType::RateLimited,
            ..default_params()
        });
        dlq.enqueue(DlqEnqueueParams {
            collection: "users".into(),
            document_id: "u1".into(),
            mutation_id: 2,
            delta: vec![30, 40],
            ..default_params()
        });

        let drained = dlq.drain_for_persistence();
        assert_eq!(drained.len(), 2);
        assert_eq!(dlq.total_entries(), 0);

        let mut dlq2 = SyncDlq::new(DlqConfig::default());
        for entry in drained {
            dlq2.restore_entry(entry);
        }
        assert_eq!(dlq2.total_entries(), 2);
        assert_eq!(dlq2.entries_for_collection(1, "orders").len(), 1);
        assert_eq!(dlq2.entries_for_collection(1, "users").len(), 1);
    }

    #[test]
    fn violation_type_display() {
        assert_eq!(
            ViolationType::PermissionDenied.to_string(),
            "permission_denied"
        );
        assert_eq!(ViolationType::RateLimited.to_string(), "rate_limited");
        assert_eq!(
            ViolationType::UniqueViolation {
                field: "email".into(),
                value: "x@y.com".into()
            }
            .to_string(),
            "unique:email=x@y.com"
        );
    }

    #[test]
    fn increment_retry_count() {
        let mut dlq = SyncDlq::new(DlqConfig::default());
        let id = dlq.enqueue(DlqEnqueueParams {
            violation_type: ViolationType::ConstraintViolation {
                detail: "test".into(),
            },
            ..default_params()
        });

        assert!(dlq.increment_retry(id));
        assert!(dlq.increment_retry(id));
        let entry = dlq.get_entry(id).unwrap();
        assert_eq!(entry.retry_count, 2);
    }

    #[test]
    fn per_collection_policy_overrides_default() {
        let mut dlq = SyncDlq::new(DlqConfig {
            default_policy: DlqExpiryPolicy {
                ttl_secs: 3600,
                expiry_action: DlqExpiryAction::Drop,
            },
            ..Default::default()
        });

        // Override for "important" collection.
        dlq.set_expiry_policy(
            1,
            "important",
            DlqExpiryPolicy {
                ttl_secs: 86400,
                expiry_action: DlqExpiryAction::Escalate,
            },
        );

        let default = dlq.expiry_policy(1, "regular");
        assert_eq!(default.ttl_secs, 3600);
        assert_eq!(default.expiry_action, DlqExpiryAction::Drop);

        let custom = dlq.expiry_policy(1, "important");
        assert_eq!(custom.ttl_secs, 86400);
        assert_eq!(custom.expiry_action, DlqExpiryAction::Escalate);
    }
}
