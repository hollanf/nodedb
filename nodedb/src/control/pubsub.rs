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

/// A named topic with bounded message log and active subscribers.
struct Topic {
    messages: Vec<PubSubMessage>,
    next_seq: u64,
    max_messages: usize,
    subscribers: HashMap<u64, tokio::sync::mpsc::Sender<PubSubMessage>>,
}

impl Topic {
    fn new(max_messages: usize) -> Self {
        Self {
            messages: Vec::new(),
            next_seq: 1,
            max_messages,
            subscribers: HashMap::new(),
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
        let mut topics = self.topics.write().unwrap_or_else(|p| p.into_inner());
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
        let mut topics = self.topics.write().unwrap_or_else(|p| p.into_inner());
        if topics.remove(name).is_some() {
            debug!(topic = name, "topic dropped");
            Ok(())
        } else {
            Err(TopicError::NotFound(name.to_string()))
        }
    }

    /// List all topic names.
    pub fn list_topics(&self) -> Vec<String> {
        let topics = self.topics.read().unwrap_or_else(|p| p.into_inner());
        topics.keys().cloned().collect()
    }

    /// Publish a message to a topic.
    pub fn publish(
        &self,
        topic_name: &str,
        payload: String,
        publisher: &str,
    ) -> Result<u64, TopicError> {
        let topics = self.topics.read().unwrap_or_else(|p| p.into_inner());
        let topic_mutex = topics
            .get(topic_name)
            .ok_or_else(|| TopicError::NotFound(topic_name.to_string()))?;
        let mut topic = topic_mutex.lock().unwrap_or_else(|p| p.into_inner());

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

        // Deliver to active subscribers. Remove dead channels.
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

        debug!(topic = topic_name, seq, "message published");
        Ok(seq)
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
        let topics = self.topics.read().unwrap_or_else(|p| p.into_inner());
        let topic_mutex = topics
            .get(topic_name)
            .ok_or_else(|| TopicError::NotFound(topic_name.to_string()))?;
        let mut topic = topic_mutex.lock().unwrap_or_else(|p| p.into_inner());

        let sub_id = self.next_sub_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = tokio::sync::mpsc::channel(256);

        let backlog: Vec<PubSubMessage> = if since_seq > 0 {
            topic
                .messages
                .iter()
                .filter(|m| m.seq >= since_seq)
                .cloned()
                .collect()
        } else {
            Vec::new()
        };

        topic.subscribers.insert(sub_id, tx);
        debug!(
            topic = topic_name,
            sub_id,
            backlog = backlog.len(),
            "subscribed"
        );
        Ok((sub_id, rx, backlog))
    }

    /// Unsubscribe from a topic.
    pub fn unsubscribe(&self, topic_name: &str, sub_id: u64) {
        let topics = self.topics.read().unwrap_or_else(|p| p.into_inner());
        if let Some(topic_mutex) = topics.get(topic_name) {
            let mut topic = topic_mutex.lock().unwrap_or_else(|p| p.into_inner());
            topic.subscribers.remove(&sub_id);
            debug!(topic = topic_name, sub_id, "unsubscribed");
        }
    }

    /// Get topic stats.
    pub fn topic_stats(&self, topic_name: &str) -> Option<TopicStats> {
        let topics = self.topics.read().unwrap_or_else(|p| p.into_inner());
        let topic_mutex = topics.get(topic_name)?;
        let topic = topic_mutex.lock().unwrap_or_else(|p| p.into_inner());
        Some(TopicStats {
            name: topic_name.to_string(),
            message_count: topic.messages.len(),
            subscriber_count: topic.subscribers.len(),
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

        let seq = registry.publish("orders", "order-1".into(), "admin");
        assert_eq!(seq.unwrap(), 1);

        let seq = registry.publish("orders", "order-2".into(), "admin");
        assert_eq!(seq.unwrap(), 2);
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
}
