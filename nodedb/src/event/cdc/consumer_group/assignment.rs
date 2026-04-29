//! Partition assignment for consumer groups.
//!
//! Within a consumer group, partitions (vShards) are distributed across
//! connected consumers using range-based assignment. Each consumer gets
//! a contiguous range of partition IDs.
//!
//! On consumer join/leave, partitions are reassigned automatically.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::RwLock;

/// Tracks active consumers and their partition assignments per group.
pub struct ConsumerAssignments {
    /// (tenant_id, stream_name, group_name) → assignment state.
    groups: RwLock<HashMap<(u32, String, String), GroupAssignment>>,
}

/// Per-group assignment state.
struct GroupAssignment {
    /// Active consumer IDs (sorted for deterministic assignment).
    consumers: BTreeSet<String>,
    /// Known partition IDs in this stream (discovered as events arrive).
    partitions: BTreeSet<u32>,
    /// Current assignment: consumer_id → set of partition IDs.
    assignments: BTreeMap<String, Vec<u32>>,
}

impl GroupAssignment {
    fn new() -> Self {
        Self {
            consumers: BTreeSet::new(),
            partitions: BTreeSet::new(),
            assignments: BTreeMap::new(),
        }
    }

    /// Rebalance partitions across active consumers using range assignment.
    fn rebalance(&mut self) {
        self.assignments.clear();

        if self.consumers.is_empty() || self.partitions.is_empty() {
            return;
        }

        let consumers: Vec<&String> = self.consumers.iter().collect();
        let partitions: Vec<u32> = self.partitions.iter().copied().collect();
        let n_consumers = consumers.len();
        let n_partitions = partitions.len();

        // Range-based: each consumer gets floor(n/c) partitions, first (n%c) get one extra.
        let base = n_partitions / n_consumers;
        let remainder = n_partitions % n_consumers;

        let mut offset = 0;
        for (i, consumer) in consumers.iter().enumerate() {
            let count = base + if i < remainder { 1 } else { 0 };
            let assigned: Vec<u32> = partitions[offset..offset + count].to_vec();
            self.assignments.insert(consumer.to_string(), assigned);
            offset += count;
        }
    }
}

impl ConsumerAssignments {
    pub fn new() -> Self {
        Self {
            groups: RwLock::new(HashMap::new()),
        }
    }

    /// Register a consumer joining a group. Triggers rebalance.
    pub fn join(&self, tenant_id: u32, stream: &str, group: &str, consumer_id: &str) {
        let key = (tenant_id, stream.to_string(), group.to_string());
        let mut groups = self.groups.write().unwrap_or_else(|p| p.into_inner());
        let state = groups.entry(key).or_insert_with(GroupAssignment::new);
        state.consumers.insert(consumer_id.to_string());
        state.rebalance();

        tracing::debug!(
            stream,
            group,
            consumer_id,
            total_consumers = state.consumers.len(),
            "consumer joined, rebalanced"
        );
    }

    /// Deregister a consumer leaving a group. Triggers rebalance.
    pub fn leave(&self, tenant_id: u32, stream: &str, group: &str, consumer_id: &str) {
        let key = (tenant_id, stream.to_string(), group.to_string());
        let mut groups = self.groups.write().unwrap_or_else(|p| p.into_inner());
        if let Some(state) = groups.get_mut(&key) {
            state.consumers.remove(consumer_id);
            state.rebalance();

            tracing::debug!(
                stream,
                group,
                consumer_id,
                remaining = state.consumers.len(),
                "consumer left, rebalanced"
            );
        }
    }

    /// Register a partition as known (called when events with new partition IDs are seen).
    pub fn register_partition(&self, tenant_id: u32, stream: &str, group: &str, partition_id: u32) {
        let key = (tenant_id, stream.to_string(), group.to_string());
        let mut groups = self.groups.write().unwrap_or_else(|p| p.into_inner());
        if let Some(state) = groups.get_mut(&key)
            && state.partitions.insert(partition_id)
        {
            state.rebalance();
        }
    }

    /// Get the partitions assigned to a specific consumer.
    /// Returns None if the consumer is not registered (meaning: all partitions).
    pub fn assigned_partitions(
        &self,
        tenant_id: u32,
        stream: &str,
        group: &str,
        consumer_id: &str,
    ) -> Option<Vec<u32>> {
        let key = (tenant_id, stream.to_string(), group.to_string());
        let groups = self.groups.read().unwrap_or_else(|p| p.into_inner());
        groups
            .get(&key)
            .and_then(|state| state.assignments.get(consumer_id).cloned())
    }

    /// Number of active consumers in a group.
    pub fn consumer_count(&self, tenant_id: u32, stream: &str, group: &str) -> usize {
        let key = (tenant_id, stream.to_string(), group.to_string());
        let groups = self.groups.read().unwrap_or_else(|p| p.into_inner());
        groups.get(&key).map(|s| s.consumers.len()).unwrap_or(0)
    }
}

impl Default for ConsumerAssignments {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_consumer_gets_all_partitions() {
        let assignments = ConsumerAssignments::new();

        // Register partitions first by creating state.
        assignments.join(1, "s", "g", "c1");
        // Manually add partitions (normally discovered via events).
        {
            let mut groups = assignments.groups.write().unwrap();
            let state = groups.get_mut(&(1, "s".into(), "g".into())).unwrap();
            for p in 0..4 {
                state.partitions.insert(p);
            }
            state.rebalance();
        }

        let assigned = assignments.assigned_partitions(1, "s", "g", "c1").unwrap();
        assert_eq!(assigned, vec![0, 1, 2, 3]);
    }

    #[test]
    fn two_consumers_split_evenly() {
        let assignments = ConsumerAssignments::new();
        assignments.join(1, "s", "g", "c1");
        assignments.join(1, "s", "g", "c2");

        {
            let mut groups = assignments.groups.write().unwrap();
            let state = groups.get_mut(&(1, "s".into(), "g".into())).unwrap();
            for p in 0..4 {
                state.partitions.insert(p);
            }
            state.rebalance();
        }

        let c1 = assignments.assigned_partitions(1, "s", "g", "c1").unwrap();
        let c2 = assignments.assigned_partitions(1, "s", "g", "c2").unwrap();

        assert_eq!(c1.len(), 2);
        assert_eq!(c2.len(), 2);
        // No overlap.
        let all: BTreeSet<u32> = c1.iter().chain(c2.iter()).copied().collect();
        assert_eq!(all.len(), 4);
    }

    #[test]
    fn odd_partitions_distributed_fairly() {
        let assignments = ConsumerAssignments::new();
        assignments.join(1, "s", "g", "c1");
        assignments.join(1, "s", "g", "c2");

        {
            let mut groups = assignments.groups.write().unwrap();
            let state = groups.get_mut(&(1, "s".into(), "g".into())).unwrap();
            for p in 0..5 {
                state.partitions.insert(p);
            }
            state.rebalance();
        }

        let c1 = assignments.assigned_partitions(1, "s", "g", "c1").unwrap();
        let c2 = assignments.assigned_partitions(1, "s", "g", "c2").unwrap();

        // 5 partitions / 2 consumers → 3 + 2.
        assert!(c1.len() == 3 || c1.len() == 2);
        assert_eq!(c1.len() + c2.len(), 5);
    }

    #[test]
    fn leave_triggers_rebalance() {
        let assignments = ConsumerAssignments::new();
        assignments.join(1, "s", "g", "c1");
        assignments.join(1, "s", "g", "c2");

        {
            let mut groups = assignments.groups.write().unwrap();
            let state = groups.get_mut(&(1, "s".into(), "g".into())).unwrap();
            for p in 0..4 {
                state.partitions.insert(p);
            }
            state.rebalance();
        }

        // c2 leaves.
        assignments.leave(1, "s", "g", "c2");

        let c1 = assignments.assigned_partitions(1, "s", "g", "c1").unwrap();
        assert_eq!(c1.len(), 4); // c1 gets all partitions.
        assert!(assignments.assigned_partitions(1, "s", "g", "c2").is_none());
    }
}
