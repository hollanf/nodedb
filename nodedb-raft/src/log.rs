use crate::error::{RaftError, Result};
use crate::message::LogEntry;
use crate::storage::LogStorage;

/// In-memory Raft log backed by a pluggable `LogStorage`.
///
/// The log is 1-indexed. Index 0 is a sentinel (term 0, empty data).
/// Compacted entries are replaced by a snapshot; only entries after
/// `snapshot_index` are retained in memory.
pub struct RaftLog<S: LogStorage> {
    /// In-memory buffer of log entries (post-snapshot).
    entries: Vec<LogEntry>,
    /// Index of the last entry included in the snapshot.
    snapshot_index: u64,
    /// Term of the last entry included in the snapshot.
    snapshot_term: u64,
    /// Persistent storage backend.
    storage: S,
}

impl<S: LogStorage> RaftLog<S> {
    pub fn new(storage: S) -> Self {
        Self {
            entries: Vec::new(),
            snapshot_index: 0,
            snapshot_term: 0,
            storage,
        }
    }

    /// Restore from storage on startup.
    pub fn restore(&mut self) -> Result<()> {
        let (snap_index, snap_term) = self.storage.snapshot_metadata();
        self.snapshot_index = snap_index;
        self.snapshot_term = snap_term;
        self.entries = self.storage.load_entries_after(snap_index)?;
        Ok(())
    }

    /// Last log index (snapshot_index if log is empty).
    pub fn last_index(&self) -> u64 {
        self.entries
            .last()
            .map(|e| e.index)
            .unwrap_or(self.snapshot_index)
    }

    /// Last log term.
    pub fn last_term(&self) -> u64 {
        self.entries
            .last()
            .map(|e| e.term)
            .unwrap_or(self.snapshot_term)
    }

    /// Get the term at a given index.
    pub fn term_at(&self, index: u64) -> Option<u64> {
        if index == 0 {
            return Some(0);
        }
        if index == self.snapshot_index {
            return Some(self.snapshot_term);
        }
        if index < self.snapshot_index {
            return None; // Compacted.
        }
        self.entry_at(index).map(|e| e.term)
    }

    /// Get entry at a given index.
    pub fn entry_at(&self, index: u64) -> Option<&LogEntry> {
        if index <= self.snapshot_index || index > self.last_index() {
            return None;
        }
        let offset = (index - self.snapshot_index - 1) as usize;
        self.entries.get(offset)
    }

    /// Get entries in range [lo, hi] inclusive.
    pub fn entries_range(&self, lo: u64, hi: u64) -> Result<&[LogEntry]> {
        if lo <= self.snapshot_index {
            return Err(RaftError::LogCompacted {
                requested: lo,
                first_available: self.snapshot_index + 1,
            });
        }
        if lo > hi || lo > self.last_index() {
            return Ok(&[]);
        }
        let start = (lo - self.snapshot_index - 1) as usize;
        let end = ((hi.min(self.last_index()) - self.snapshot_index - 1) + 1) as usize;
        Ok(&self.entries[start..end])
    }

    /// Append new entries from a leader's AppendEntries RPC.
    ///
    /// Handles conflict detection per Raft paper §5.3:
    /// - If an existing entry conflicts with a new one (same index, different
    ///   terms), delete the existing entry and all that follow it.
    /// - Append any new entries not already in the log.
    pub fn append_entries(&mut self, _prev_index: u64, entries: &[LogEntry]) -> Result<()> {
        for entry in entries {
            if let Some(existing) = self.entry_at(entry.index) {
                if existing.term != entry.term {
                    // Conflict: truncate from this index onward.
                    self.truncate_from(entry.index);
                    self.entries.push(entry.clone());
                }
                // Same term = already present, skip.
            } else {
                self.entries.push(entry.clone());
            }
        }

        // Persist.
        if !entries.is_empty() {
            self.storage.append(entries)?;
        }
        Ok(())
    }

    /// Append a single entry proposed by the leader.
    pub fn append(&mut self, entry: LogEntry) -> Result<()> {
        self.storage.append(std::slice::from_ref(&entry))?;
        self.entries.push(entry);
        Ok(())
    }

    /// Truncate entries from `index` onward (inclusive).
    fn truncate_from(&mut self, index: u64) {
        if index <= self.snapshot_index {
            return;
        }
        let offset = (index - self.snapshot_index - 1) as usize;
        self.entries.truncate(offset);
        let _ = self.storage.truncate(index);
    }

    /// Apply a snapshot: discard all entries up to `last_included_index`.
    pub fn apply_snapshot(&mut self, last_included_index: u64, last_included_term: u64) {
        // Remove entries already covered by the snapshot.
        if last_included_index > self.snapshot_index {
            let new_start = last_included_index + 1;
            self.entries.retain(|e| e.index >= new_start);
            self.snapshot_index = last_included_index;
            self.snapshot_term = last_included_term;
            let _ = self
                .storage
                .compact(last_included_index, last_included_term);
        }
    }

    pub fn snapshot_index(&self) -> u64 {
        self.snapshot_index
    }

    pub fn snapshot_term(&self) -> u64 {
        self.snapshot_term
    }

    pub fn storage(&self) -> &S {
        &self.storage
    }

    pub fn storage_mut(&mut self) -> &mut S {
        &mut self.storage
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemStorage;

    fn make_entry(term: u64, index: u64) -> LogEntry {
        LogEntry {
            term,
            index,
            data: vec![],
        }
    }

    #[test]
    fn empty_log() {
        let log = RaftLog::new(MemStorage::new());
        assert_eq!(log.last_index(), 0);
        assert_eq!(log.last_term(), 0);
        assert_eq!(log.term_at(0), Some(0));
    }

    #[test]
    fn append_and_retrieve() {
        let mut log = RaftLog::new(MemStorage::new());
        log.append(make_entry(1, 1)).unwrap();
        log.append(make_entry(1, 2)).unwrap();
        log.append(make_entry(2, 3)).unwrap();

        assert_eq!(log.last_index(), 3);
        assert_eq!(log.last_term(), 2);
        assert_eq!(log.term_at(1), Some(1));
        assert_eq!(log.term_at(3), Some(2));
        assert!(log.entry_at(4).is_none());
    }

    #[test]
    fn entries_range() {
        let mut log = RaftLog::new(MemStorage::new());
        for i in 1..=5 {
            log.append(make_entry(1, i)).unwrap();
        }
        let range = log.entries_range(2, 4).unwrap();
        assert_eq!(range.len(), 3);
        assert_eq!(range[0].index, 2);
        assert_eq!(range[2].index, 4);
    }

    #[test]
    fn conflict_detection() {
        let mut log = RaftLog::new(MemStorage::new());
        log.append(make_entry(1, 1)).unwrap();
        log.append(make_entry(1, 2)).unwrap();
        log.append(make_entry(1, 3)).unwrap();

        // Leader sends entries starting at index 2 with different term.
        let new_entries = vec![make_entry(2, 2), make_entry(2, 3), make_entry(2, 4)];
        log.append_entries(1, &new_entries).unwrap();

        assert_eq!(log.last_index(), 4);
        assert_eq!(log.term_at(2), Some(2));
        assert_eq!(log.term_at(3), Some(2));
    }

    #[test]
    fn snapshot_compaction() {
        let mut log = RaftLog::new(MemStorage::new());
        for i in 1..=10 {
            log.append(make_entry(1, i)).unwrap();
        }

        log.apply_snapshot(5, 1);
        assert_eq!(log.snapshot_index(), 5);
        assert_eq!(log.last_index(), 10);
        // Compacted entries are gone.
        assert!(log.entry_at(3).is_none());
        assert!(log.entry_at(5).is_none()); // snapshot boundary
        assert!(log.entry_at(6).is_some());

        // Range query into compacted region fails.
        assert!(log.entries_range(3, 8).is_err());
    }
}
