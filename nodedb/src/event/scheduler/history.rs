//! Job execution history: redb-persisted log of recent runs.

use std::collections::VecDeque;
use std::path::Path;

use redb::{Database, ReadableTable, TableDefinition};
use tracing::debug;

use super::types::JobRun;

/// redb table: monotonic run_id → MessagePack-serialized JobRun.
const JOB_HISTORY: TableDefinition<u64, &[u8]> = TableDefinition::new("job_history");

/// Maximum history entries retained per schedule.
const MAX_HISTORY_PER_SCHEDULE: usize = 100;

/// Maximum total history entries.
const MAX_TOTAL_HISTORY: usize = 10_000;

/// Job execution history store.
pub struct JobHistoryStore {
    db: Database,
    /// In-memory recent runs (newest at back).
    runs: std::sync::RwLock<VecDeque<JobRun>>,
    next_id: std::sync::atomic::AtomicU64,
}

impl JobHistoryStore {
    /// Open or create the history store at `{data_dir}/event_plane/job_history.redb`.
    pub fn open(data_dir: &Path) -> crate::Result<Self> {
        let dir = data_dir.join("event_plane");
        std::fs::create_dir_all(&dir).map_err(|e| crate::Error::Storage {
            engine: "event_plane".into(),
            detail: format!("create dir {}: {e}", dir.display()),
        })?;

        let path = dir.join("job_history.redb");
        let db = Database::create(&path).map_err(|e| crate::Error::Storage {
            engine: "event_plane".into(),
            detail: format!("open job_history db {}: {e}", path.display()),
        })?;

        // Ensure table exists and load recent entries.
        let mut runs = VecDeque::new();
        let mut max_id = 0u64;
        {
            let txn = db.begin_write().map_err(|e| crate::Error::Storage {
                engine: "event_plane".into(),
                detail: format!("begin_write: {e}"),
            })?;
            {
                let table = txn
                    .open_table(JOB_HISTORY)
                    .map_err(|e| crate::Error::Storage {
                        engine: "event_plane".into(),
                        detail: format!("open_table: {e}"),
                    })?;
                let mut range = table.range(0u64..).map_err(|e| crate::Error::Storage {
                    engine: "event_plane".into(),
                    detail: format!("range: {e}"),
                })?;
                while let Some(Ok((key_guard, value_guard))) = range.next() {
                    let id = key_guard.value();
                    if id > max_id {
                        max_id = id;
                    }
                    if let Ok(run) = zerompk::from_msgpack::<JobRun>(value_guard.value()) {
                        runs.push_back(run);
                    }
                }
            }
            txn.commit().map_err(|e| crate::Error::Storage {
                engine: "event_plane".into(),
                detail: format!("commit: {e}"),
            })?;
        }

        // Trim to max total.
        while runs.len() > MAX_TOTAL_HISTORY {
            runs.pop_front();
        }

        debug!(entries = runs.len(), "job history loaded");

        Ok(Self {
            db,
            runs: std::sync::RwLock::new(runs),
            next_id: std::sync::atomic::AtomicU64::new(max_id + 1),
        })
    }

    /// Record a completed job run.
    pub fn record(&self, run: JobRun) -> crate::Result<()> {
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let bytes = zerompk::to_msgpack_vec(&run).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("job_run: {e}"),
        })?;

        let txn = self.db.begin_write().map_err(|e| crate::Error::Storage {
            engine: "event_plane".into(),
            detail: format!("begin_write: {e}"),
        })?;
        {
            let mut table = txn
                .open_table(JOB_HISTORY)
                .map_err(|e| crate::Error::Storage {
                    engine: "event_plane".into(),
                    detail: format!("open_table: {e}"),
                })?;
            table
                .insert(id, bytes.as_slice())
                .map_err(|e| crate::Error::Storage {
                    engine: "event_plane".into(),
                    detail: format!("insert: {e}"),
                })?;
        }
        txn.commit().map_err(|e| crate::Error::Storage {
            engine: "event_plane".into(),
            detail: format!("commit: {e}"),
        })?;

        let mut runs = self.runs.write().unwrap_or_else(|p| p.into_inner());
        runs.push_back(run);
        while runs.len() > MAX_TOTAL_HISTORY {
            runs.pop_front();
        }

        Ok(())
    }

    /// Get the last N runs for a specific schedule.
    pub fn last_runs(&self, tenant_id: u64, schedule_name: &str, limit: usize) -> Vec<JobRun> {
        let runs = self.runs.read().unwrap_or_else(|p| p.into_inner());
        runs.iter()
            .rev()
            .filter(|r| r.tenant_id == tenant_id && r.schedule_name == schedule_name)
            .take(limit.min(MAX_HISTORY_PER_SCHEDULE))
            .cloned()
            .collect()
    }

    /// Get the most recent run for a schedule (for SHOW SCHEDULES).
    pub fn last_run(&self, tenant_id: u64, schedule_name: &str) -> Option<JobRun> {
        let runs = self.runs.read().unwrap_or_else(|p| p.into_inner());
        runs.iter()
            .rev()
            .find(|r| r.tenant_id == tenant_id && r.schedule_name == schedule_name)
            .cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_and_retrieve() {
        let dir = tempfile::tempdir().unwrap();
        let store = JobHistoryStore::open(dir.path()).unwrap();

        let run = JobRun {
            schedule_name: "cleanup".into(),
            tenant_id: 1,
            started_at: 1700000000000,
            duration_ms: 150,
            success: true,
            error: None,
        };
        store.record(run).unwrap();

        let last = store.last_run(1, "cleanup").unwrap();
        assert_eq!(last.schedule_name, "cleanup");
        assert!(last.success);
    }

    #[test]
    fn last_runs_filtered() {
        let dir = tempfile::tempdir().unwrap();
        let store = JobHistoryStore::open(dir.path()).unwrap();

        for i in 0..5 {
            store
                .record(JobRun {
                    schedule_name: "a".into(),
                    tenant_id: 1,
                    started_at: i,
                    duration_ms: 10,
                    success: true,
                    error: None,
                })
                .unwrap();
        }
        store
            .record(JobRun {
                schedule_name: "b".into(),
                tenant_id: 1,
                started_at: 100,
                duration_ms: 10,
                success: false,
                error: Some("timeout".into()),
            })
            .unwrap();

        let a_runs = store.last_runs(1, "a", 3);
        assert_eq!(a_runs.len(), 3);

        let b_runs = store.last_runs(1, "b", 10);
        assert_eq!(b_runs.len(), 1);
        assert!(!b_runs[0].success);
    }
}
