use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use tokio::sync::oneshot;

/// Calvin transaction identity in the sequencer-assigned coordinate space.
///
/// `(epoch, position)` is the unique key the sequencer Raft state machine
/// stamps onto every admitted transaction; it is the join key between the
/// completion-awaiter side and the per-vshard ack side of the registry.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Hash)]
pub struct TxnId {
    pub epoch: u64,
    pub position: u32,
}

impl TxnId {
    pub fn new(epoch: u64, position: u32) -> Self {
        Self { epoch, position }
    }
}

struct PendingCompletion {
    expected_participants: usize,
    acked_vshards: BTreeSet<u32>,
    completion_tx: Option<oneshot::Sender<()>>,
}

impl PendingCompletion {
    fn new(expected_participants: usize) -> Self {
        Self {
            expected_participants,
            acked_vshards: BTreeSet::new(),
            completion_tx: None,
        }
    }

    fn is_complete(&self) -> bool {
        self.acked_vshards.len() >= self.expected_participants
    }
}

#[derive(Default)]
struct Inner {
    assignments: BTreeMap<u64, oneshot::Sender<(u64, u32, usize)>>,
    completions: BTreeMap<TxnId, PendingCompletion>,
}

#[derive(Default)]
pub struct CalvinCompletionRegistry {
    inner: Mutex<Inner>,
}

impl CalvinCompletionRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn register_submission(&self, inbox_seq: u64) -> oneshot::Receiver<(u64, u32, usize)> {
        let (tx, rx) = oneshot::channel();
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .assignments
            .insert(inbox_seq, tx);
        rx
    }

    pub fn note_assigned(&self, inbox_seq: u64, txn: TxnId, expected_participants: usize) {
        let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(tx) = inner.assignments.remove(&inbox_seq)
            && tx
                .send((txn.epoch, txn.position, expected_participants))
                .is_err()
        {
            tracing::warn!(
                inbox_seq,
                epoch = txn.epoch,
                position = txn.position,
                "calvin assignment receiver dropped before sequencer position arrived; \
                 client likely timed out on submit_with_retry"
            );
        }
        inner
            .completions
            .entry(txn)
            .or_insert_with(|| PendingCompletion::new(expected_participants));
    }

    pub fn register_completion(&self, txn: TxnId) -> oneshot::Receiver<()> {
        let (tx, rx) = oneshot::channel();
        let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let entry = inner
            .completions
            .entry(txn)
            .or_insert_with(|| PendingCompletion::new(0));
        if entry.is_complete() {
            inner.completions.remove(&txn);
            if tx.send(()).is_err() {
                tracing::warn!(
                    epoch = txn.epoch,
                    position = txn.position,
                    "calvin completion receiver dropped before all-acked signal; \
                     client likely timed out on completion wait"
                );
            }
        } else {
            entry.completion_tx = Some(tx);
        }
        rx
    }

    pub fn note_completion_ack(&self, txn: TxnId, vshard_id: u32) {
        let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let entry = inner
            .completions
            .entry(txn)
            .or_insert_with(|| PendingCompletion::new(0));
        entry.acked_vshards.insert(vshard_id);
        if entry.is_complete() {
            let tx = entry.completion_tx.take();
            inner.completions.remove(&txn);
            if let Some(tx) = tx
                && tx.send(()).is_err()
            {
                tracing::warn!(
                    epoch = txn.epoch,
                    position = txn.position,
                    vshard_id,
                    "calvin completion receiver dropped before final ack; \
                     client likely timed out on completion wait"
                );
            }
        }
    }

    /// Test-only: returns the number of pending completion entries.
    /// Used to verify entries are removed once all acks arrive (no leak).
    #[cfg(test)]
    pub fn pending_completions_len(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .completions
            .len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn completion_entry_removed_after_all_acks() {
        let reg = CalvinCompletionRegistry::new();
        let txn = TxnId::new(7, 0);
        reg.note_assigned(1, txn, 2);
        let rx = reg.register_completion(txn);
        assert_eq!(reg.pending_completions_len(), 1);
        reg.note_completion_ack(txn, 10);
        assert_eq!(reg.pending_completions_len(), 1);
        reg.note_completion_ack(txn, 20);
        rx.await.expect("completion fires");
        assert_eq!(
            reg.pending_completions_len(),
            0,
            "entry must be evicted once all expected vshards have acked"
        );
    }

    #[tokio::test]
    async fn completion_entry_removed_when_register_arrives_after_acks() {
        let reg = CalvinCompletionRegistry::new();
        let txn = TxnId::new(9, 3);
        reg.note_assigned(1, txn, 2);
        reg.note_completion_ack(txn, 10);
        // Entry remains: expected=2, only 1 ack received.
        assert_eq!(reg.pending_completions_len(), 1);
        let rx = reg.register_completion(txn);
        assert_eq!(reg.pending_completions_len(), 1);
        reg.note_completion_ack(txn, 20);
        rx.await.expect("completion fires once both acks arrived");
        assert_eq!(
            reg.pending_completions_len(),
            0,
            "entry must be evicted once awaiter is signalled"
        );
    }
}
