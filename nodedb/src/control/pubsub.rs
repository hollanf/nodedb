//! Pub/Sub topic registry with bounded in-memory message retention.
//!
//! Topics are named channels. Publishers push messages, subscribers consume
//! them. Messages are retained in a bounded ring buffer for backlog replay
//! on reconnect. Messages are **ephemeral** — lost on server restart.
//! For durable event streaming, use the Change Stream (CDC) instead.
//!
//! SQL interface:
//! - `CREATE TOPIC name` — create a topic
//! - `DROP TOPIC name` — delete a topic and all its messages
//! - `SHOW TOPICS` — list all topics
//! - `PUBLISH TO name 'payload'` — publish a message
//! - `SUBSCRIBE TO name [SINCE seq]` — subscribe (returns subscription_id)

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, RwLock};

use tracing::debug;

/// A single pub/sub message.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PubSubMessage {
    /// Monotonically increasing sequence number within the topic.
    pub seq: u64,
    /// Message payload (arbitrary JSON or text).
    pub payload: String,
    /// Timestamp (epoch milliseconds).
    pub timestamp_ms: u64,
    /// Publisher identity (username or "system").
    pub publisher: String,
}

/// Errors from topic operations.
#[derive(Debug)]
pub enum TopicError {
    /// Topic does not exist.
    NotFound(String),
    /// Topic already exists.
    AlreadyExists(String),
}

impl fmt::Display for TopicError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TopicError::NotFound(name) => write!(f, "topic '{name}' does not exist"),
            TopicError::AlreadyExists(name) => write!(f, "topic '{name}' already exists"),
        }
    }
}

/// A consumer group: named set of subscribers sharing load via round-robin.
struct ConsumerGroup {
    /// Members: sub_id → sender channel.
    members: Vec<(u64, tokio::sync::mpsc::Sender<PubSubMessage>)>,
    /// Round-robin index for next delivery.
    next_index: usize,
}

impl ConsumerGroup {
    fn new() -> Self {
        Self {
            members: Vec::new(),
            next_index: 0,
        }
    }

    /// Deliver message to exactly one member (round-robin). Remove dead members.
    fn deliver(&mut self, msg: &PubSubMessage) {
        if self.members.is_empty() {
            return;
        }
        let mut attempts = 0;
        while attempts < self.members.len() {
            let idx = self.next_index % self.members.len();
            self.next_index = idx + 1;
            if self.members[idx].1.try_send(msg.clone()).is_ok() {
                return;
            }
            // Dead member — remove and try next.
            self.members.remove(idx);
            if self.next_index > 0 {
                self.next_index -= 1;
            }
            attempts += 1;
        }
    }
}

/// A named topic with bounded message log and active subscribers.
struct Topic {
    messages: Vec<PubSubMessage>,
    next_seq: u64,
    max_messages: usize,
    /// Broadcast subscribers: all get every message.
    subscribers: HashMap<u64, tokio::sync::mpsc::Sender<PubSubMessage>>,
    /// Consumer groups: each message delivered to exactly one member per group.
    consumer_groups: HashMap<String, ConsumerGroup>,
}

impl Topic {
    fn new(max_messages: usize) -> Self {
        Self {
            messages: Vec::new(),
            next_seq: 1,
            max_messages,
            subscribers: HashMap::new(),
            consumer_groups: HashMap::new(),
        }
    }

    /// Get backlog of messages since a given sequence number.
    fn get_backlog(&self, since_seq: u64) -> Vec<PubSubMessage> {
        if since_seq > 0 {
            self.messages
                .iter()
                .filter(|m| m.seq >= since_seq)
                .cloned()
                .collect()
        } else {
            Vec::new()
        }
    }
}

/// Topic registry managing all pub/sub topics.
pub struct TopicRegistry {
    topics: RwLock<HashMap<String, Mutex<Topic>>>,
    next_sub_id: AtomicU64,
    default_max_messages: usize,
}

impl TopicRegistry {
    pub fn new(default_max_messages: usize) -> Self {
        Self {
            topics: RwLock::new(HashMap::new()),
            next_sub_id: AtomicU64::new(1),
            default_max_messages,
        }
    }

    /// Create a new topic.
    pub fn create_topic(&self, name: &str) -> Result<(), TopicError> {
        let mut topics = super::lock_utils::write_or_recover(self.topics.write(), "topics");
        if topics.contains_key(name) {
            return Err(TopicError::AlreadyExists(name.to_string()));
        }
        topics.insert(
            name.to_string(),
            Mutex::new(Topic::new(self.default_max_messages)),
        );
        debug!(topic = name, "topic created");
        Ok(())
    }

    /// Drop a topic and all its messages.
    pub fn drop_topic(&self, name: &str) -> Result<(), TopicError> {
        let mut topics = super::lock_utils::write_or_recover(self.topics.write(), "topics");
        if topics.remove(name).is_some() {
            debug!(topic = name, "topic dropped");
            Ok(())
        } else {
            Err(TopicError::NotFound(name.to_string()))
        }
    }

    /// List all topic names.
    pub fn list_topics(&self) -> Vec<String> {
        let topics = super::lock_utils::read_or_recover(self.topics.read(), "topics");
        topics.keys().cloned().collect()
    }

    /// Publish a message to a topic.
    ///
    /// Returns `(sequence_number, receivers)` where `receivers` is the count of
    /// broadcast subscribers + consumer groups that received the message.
    pub fn publish(
        &self,
        topic_name: &str,
        payload: String,
        publisher: &str,
    ) -> Result<(u64, usize), TopicError> {
        let topics = super::lock_utils::read_or_recover(self.topics.read(), "topics");
        let topic_mutex = topics
            .get(topic_name)
            .ok_or_else(|| TopicError::NotFound(topic_name.to_string()))?;
        let mut topic = super::lock_utils::lock_or_recover(topic_mutex.lock(), "topic");

        let seq = topic.next_seq;
        topic.next_seq += 1;

        let msg = PubSubMessage {
            seq,
            payload,
            timestamp_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
            publisher: publisher.to_string(),
        };

        if topic.messages.len() >= topic.max_messages {
            topic.messages.remove(0);
        }
        topic.messages.push(msg.clone());

        // Deliver to broadcast subscribers. Remove dead channels.
        let mut dead = Vec::new();
        for (&sub_id, sender) in &topic.subscribers {
            if sender.try_send(msg.clone()).is_err() {
                debug!(
                    topic = topic_name,
                    sub_id, "subscriber channel full or closed; removing"
                );
                dead.push(sub_id);
            }
        }
        for sub_id in dead {
            topic.subscribers.remove(&sub_id);
        }

        // Count live broadcast subscribers (after removing dead ones).
        let broadcast_count = topic.subscribers.len();

        // Deliver to consumer groups (one member per group, round-robin).
        let group_count = topic.consumer_groups.len();
        for group in topic.consumer_groups.values_mut() {
            group.deliver(&msg);
        }

        let receivers = broadcast_count + group_count;
        debug!(topic = topic_name, seq, receivers, "message published");
        Ok((seq, receivers))
    }

    /// Subscribe to a topic. Returns (subscription_id, receiver, backlog).
    ///
    /// `since_seq`: replay messages starting from this sequence number.
    /// 0 = no replay, just new messages.
    pub fn subscribe(
        &self,
        topic_name: &str,
        since_seq: u64,
    ) -> Result<
        (
            u64,
            tokio::sync::mpsc::Receiver<PubSubMessage>,
            Vec<PubSubMessage>,
        ),
        TopicError,
    > {
        let topics = super::lock_utils::read_or_recover(self.topics.read(), "topics");
        let topic_mutex = topics
            .get(topic_name)
            .ok_or_else(|| TopicError::NotFound(topic_name.to_string()))?;
        let mut topic = super::lock_utils::lock_or_recover(topic_mutex.lock(), "topic");

        let sub_id = self.next_sub_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = tokio::sync::mpsc::channel(256);

        let backlog = topic.get_backlog(since_seq);

        topic.subscribers.insert(sub_id, tx);
        debug!(
            topic = topic_name,
            sub_id,
            backlog = backlog.len(),
            "subscribed"
        );
        Ok((sub_id, rx, backlog))
    }

    /// Subscribe to a topic as a consumer group member.
    ///
    /// Messages are delivered to exactly ONE member of the group (round-robin).
    /// Multiple subscribers with the same `group_name` share the load.
    /// Returns (subscription_id, receiver, backlog).
    pub fn subscribe_group(
        &self,
        topic_name: &str,
        group_name: &str,
        since_seq: u64,
    ) -> Result<
        (
            u64,
            tokio::sync::mpsc::Receiver<PubSubMessage>,
            Vec<PubSubMessage>,
        ),
        TopicError,
    > {
        let topics = super::lock_utils::read_or_recover(self.topics.read(), "topics");
        let topic_mutex = topics
            .get(topic_name)
            .ok_or_else(|| TopicError::NotFound(topic_name.to_string()))?;
        let mut topic = super::lock_utils::lock_or_recover(topic_mutex.lock(), "topic");

        let sub_id = self.next_sub_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = tokio::sync::mpsc::channel(256);

        let backlog = topic.get_backlog(since_seq);

        let group = topic
            .consumer_groups
            .entry(group_name.to_string())
            .or_insert_with(ConsumerGroup::new);
        group.members.push((sub_id, tx));

        debug!(
            topic = topic_name,
            group = group_name,
            sub_id,
            members = group.members.len(),
            backlog = backlog.len(),
            "subscribed to consumer group"
        );
        Ok((sub_id, rx, backlog))
    }

    /// Unsubscribe from a topic.
    pub fn unsubscribe(&self, topic_name: &str, sub_id: u64) {
        let topics = super::lock_utils::read_or_recover(self.topics.read(), "topics");
        if let Some(topic_mutex) = topics.get(topic_name) {
            let mut topic = super::lock_utils::lock_or_recover(topic_mutex.lock(), "topic");
            topic.subscribers.remove(&sub_id);
            debug!(topic = topic_name, sub_id, "unsubscribed");
        }
    }

    /// Get topic stats.
    pub fn topic_stats(&self, topic_name: &str) -> Option<TopicStats> {
        let topics = super::lock_utils::read_or_recover(self.topics.read(), "topics");
        let topic_mutex = topics.get(topic_name)?;
        let topic = super::lock_utils::lock_or_recover(topic_mutex.lock(), "topic");
        Some(TopicStats {
            name: topic_name.to_string(),
            message_count: topic.messages.len(),
            subscriber_count: topic.subscribers.len(),
            consumer_group_count: topic.consumer_groups.len(),
            next_seq: topic.next_seq,
        })
    }
}

/// Topic statistics.
#[derive(Debug, Clone)]
pub struct TopicStats {
    pub name: String,
    pub message_count: usize,
    pub subscriber_count: usize,
    pub consumer_group_count: usize,
    pub next_seq: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_and_publish() {
        let registry = TopicRegistry::new(1000);
        assert!(registry.create_topic("orders").is_ok());
        assert!(registry.create_topic("orders").is_err()); // duplicate

        let (seq, _receivers) = registry
            .publish("orders", "order-1".into(), "admin")
            .unwrap();
        assert_eq!(seq, 1);

        let (seq, _receivers) = registry
            .publish("orders", "order-2".into(), "admin")
            .unwrap();
        assert_eq!(seq, 2);
    }

    #[test]
    fn subscribe_with_backlog() {
        let registry = TopicRegistry::new(1000);
        registry.create_topic("events").unwrap();
        registry
            .publish("events", "msg-1".into(), "system")
            .unwrap();
        registry
            .publish("events", "msg-2".into(), "system")
            .unwrap();
        registry
            .publish("events", "msg-3".into(), "system")
            .unwrap();

        let (sub_id, _rx, backlog) = registry.subscribe("events", 2).unwrap();
        assert!(sub_id > 0);
        assert_eq!(backlog.len(), 2);
        assert_eq!(backlog[0].payload, "msg-2");
        assert_eq!(backlog[1].payload, "msg-3");
    }

    #[test]
    fn publish_nonexistent_topic() {
        let registry = TopicRegistry::new(1000);
        assert!(registry.publish("ghost", "msg".into(), "admin").is_err());
    }

    #[test]
    fn drop_topic() {
        let registry = TopicRegistry::new(1000);
        registry.create_topic("temp").unwrap();
        assert!(registry.drop_topic("temp").is_ok());
        assert!(registry.drop_topic("temp").is_err()); // already gone
        assert!(registry.list_topics().is_empty());
    }

    #[test]
    fn bounded_message_log() {
        let registry = TopicRegistry::new(3);
        registry.create_topic("bounded").unwrap();
        registry.publish("bounded", "1".into(), "a").unwrap();
        registry.publish("bounded", "2".into(), "a").unwrap();
        registry.publish("bounded", "3".into(), "a").unwrap();
        registry.publish("bounded", "4".into(), "a").unwrap(); // evicts "1"

        let (_, _, backlog) = registry.subscribe("bounded", 1).unwrap();
        assert_eq!(backlog.len(), 3);
        assert_eq!(backlog[0].payload, "2");
    }

    #[test]
    fn topic_stats_test() {
        let registry = TopicRegistry::new(1000);
        registry.create_topic("stats").unwrap();
        registry.publish("stats", "a".into(), "admin").unwrap();
        registry.publish("stats", "b".into(), "admin").unwrap();

        let stats = registry.topic_stats("stats").unwrap();
        assert_eq!(stats.message_count, 2);
        assert_eq!(stats.next_seq, 3);
    }

    #[tokio::test]
    async fn consumer_group_round_robin() {
        let registry = TopicRegistry::new(1000);
        registry.create_topic("tasks").unwrap();

        // Two members in the same consumer group.
        let (_id1, mut rx1, _) = registry.subscribe_group("tasks", "workers", 0).unwrap();
        let (_id2, mut rx2, _) = registry.subscribe_group("tasks", "workers", 0).unwrap();

        // Publish 4 messages.
        registry.publish("tasks", "a".into(), "system").unwrap();
        registry.publish("tasks", "b".into(), "system").unwrap();
        registry.publish("tasks", "c".into(), "system").unwrap();
        registry.publish("tasks", "d".into(), "system").unwrap();

        // Each member should get ~2 messages (round-robin).
        let mut count1 = 0;
        let mut count2 = 0;
        while let Ok(msg) = rx1.try_recv() {
            count1 += 1;
            let _ = msg;
        }
        while let Ok(msg) = rx2.try_recv() {
            count2 += 1;
            let _ = msg;
        }
        assert_eq!(count1 + count2, 4);
        assert!(
            count1 >= 1 && count2 >= 1,
            "both members should get messages: c1={count1}, c2={count2}"
        );
    }

    #[test]
    fn consumer_group_stats() {
        let registry = TopicRegistry::new(1000);
        registry.create_topic("cg").unwrap();
        registry.subscribe_group("cg", "g1", 0).unwrap();
        registry.subscribe_group("cg", "g1", 0).unwrap();
        registry.subscribe_group("cg", "g2", 0).unwrap();

        let stats = registry.topic_stats("cg").unwrap();
        assert_eq!(stats.consumer_group_count, 2);
    }
}
