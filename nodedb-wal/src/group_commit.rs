//! Group commit coordinator.
//!
//! Batches concurrent WAL write requests into a single fsync for maximum NVMe IOPS.
//!
//! ## How it works
//!
//! 1. Multiple threads submit `PendingWrite` to the commit queue.
//! 2. One thread becomes the "commit leader" (acquires the commit lock).
//! 3. The leader drains all pending writes, appends them to the WAL writer,
//!    issues a single `fsync`, and advances `durable_lsn`.
//! 4. Non-leader threads spin-wait on `durable_lsn` until their write is covered.
//!
//! This converts N fsyncs into 1 fsync, which is critical for NVMe performance.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::{Result, WalError};
use crate::writer::WalWriter;

/// A pending write request waiting to be committed.
#[derive(Debug)]
pub struct PendingWrite {
    /// Record type discriminant.
    pub record_type: u16,
    /// Tenant ID.
    pub tenant_id: u32,
    /// Virtual shard ID.
    pub vshard_id: u16,
    /// Payload bytes.
    pub payload: Vec<u8>,
}

/// Result delivered to each waiter after group commit completes.
#[derive(Debug, Clone, Copy)]
pub struct CommitResult {
    /// The LSN assigned to this record.
    pub lsn: u64,
    /// Whether the fsync succeeded (record is durable).
    pub durable: bool,
}

/// Thread-safe group commit coordinator.
///
/// Multiple threads call `submit()` which blocks until the record is durable.
/// Internally, one thread becomes the commit leader and batches all pending
/// writes into a single WAL flush.
pub struct GroupCommitter {
    /// Pending writes queue. The mutex also serves as leader election:
    /// the thread that holds it during commit is the leader.
    pending: Mutex<Vec<PendingWrite>>,

    /// The durable LSN — all records with LSN <= this value are on disk.
    durable_lsn: AtomicU64,

    /// Serializes commit operations (drain + write + fsync).
    /// Separate from `pending` so enqueue doesn't block on I/O.
    commit_lock: Mutex<()>,
}

impl GroupCommitter {
    /// Create a new group committer.
    pub fn new() -> Self {
        Self {
            pending: Mutex::new(Vec::with_capacity(1024)),
            durable_lsn: AtomicU64::new(0),
            commit_lock: Mutex::new(()),
        }
    }

    /// Submit a write and block until it's durable.
    ///
    /// Returns the assigned LSN once the batch containing this write has been
    /// fsynced to disk.
    pub fn submit(&self, writer: &Mutex<WalWriter>, write: PendingWrite) -> Result<CommitResult> {
        // Enqueue the write.
        {
            let mut pending = self.pending.lock().map_err(|_| WalError::LockPoisoned {
                context: "pending queue",
            })?;
            pending.push(write);
        }

        // Try to become the commit leader. If another thread holds the commit
        // lock, we block here until it finishes — then we check if our write
        // was already committed by that leader.
        let _commit_guard = self
            .commit_lock
            .lock()
            .map_err(|_| WalError::LockPoisoned {
                context: "commit lock",
            })?;

        // Drain pending writes. If the previous leader already committed our
        // write, the pending queue might be empty (or only contain writes from
        // threads that enqueued after us).
        let batch: Vec<PendingWrite> = {
            let mut pending = self.pending.lock().map_err(|_| WalError::LockPoisoned {
                context: "pending queue (drain)",
            })?;
            std::mem::take(&mut *pending)
        };

        if batch.is_empty() {
            // Previous leader committed our write. Return the durable LSN.
            let lsn = self.durable_lsn.load(Ordering::Acquire);
            return Ok(CommitResult { lsn, durable: true });
        }

        // We are the leader — append and fsync.
        let mut wal = writer.lock().map_err(|_| WalError::LockPoisoned {
            context: "WAL writer",
        })?;
        let mut last_lsn = 0;

        for w in &batch {
            last_lsn = wal.append(w.record_type, w.tenant_id, w.vshard_id, &w.payload)?;
        }

        wal.sync()?;
        drop(wal);

        self.durable_lsn.store(last_lsn, Ordering::Release);

        Ok(CommitResult {
            lsn: last_lsn,
            durable: true,
        })
    }

    /// Current durable LSN (all records <= this are on disk).
    pub fn durable_lsn(&self) -> u64 {
        self.durable_lsn.load(Ordering::Acquire)
    }
}

impl Default for GroupCommitter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reader::WalReader;
    use crate::record::RecordType;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn single_thread_group_commit() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");

        let writer = Mutex::new(WalWriter::open_without_direct_io(&path).unwrap());
        let gc = GroupCommitter::new();

        let result = gc
            .submit(
                &writer,
                PendingWrite {
                    record_type: RecordType::Put as u16,
                    tenant_id: 1,
                    vshard_id: 0,
                    payload: b"hello".to_vec(),
                },
            )
            .unwrap();

        assert!(result.durable);
        assert_eq!(result.lsn, 1);
        assert_eq!(gc.durable_lsn(), 1);

        // Verify the record is readable.
        let reader = WalReader::open(&path).unwrap();
        let records: Vec<_> = reader
            .records()
            .collect::<crate::error::Result<_>>()
            .unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].payload, b"hello");
    }

    #[test]
    fn concurrent_group_commit() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");

        let writer = Arc::new(Mutex::new(
            WalWriter::open_without_direct_io(&path).unwrap(),
        ));
        let gc = Arc::new(GroupCommitter::new());

        let mut handles = Vec::new();

        for i in 0..10 {
            let w = Arc::clone(&writer);
            let g = Arc::clone(&gc);
            handles.push(thread::spawn(move || {
                let payload = format!("record-{i}");
                let result = g
                    .submit(
                        &w,
                        PendingWrite {
                            record_type: RecordType::Put as u16,
                            tenant_id: 1,
                            vshard_id: 0,
                            payload: payload.into_bytes(),
                        },
                    )
                    .unwrap();
                assert!(result.durable);
                result.lsn
            }));
        }

        let lsns: Vec<u64> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        // All should have gotten durable results.
        assert!(lsns.iter().all(|l| *l > 0));

        // All 10 records should be in the WAL.
        let reader = WalReader::open(&path).unwrap();
        let records: Vec<_> = reader
            .records()
            .collect::<crate::error::Result<_>>()
            .unwrap();
        assert_eq!(records.len(), 10);
    }
}
