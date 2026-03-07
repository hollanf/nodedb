//! Layered snapshots and Point-In-Time Recovery (PITR).
//!
//! - **Layered Snapshots**: Base image + block-level deltas.
//! - **PITR**: Restore base image, then replay WAL to exact target timestamp.
//!
//! Snapshot operations emit begin/end markers with consistent LSN boundaries.
//! Restore supports dry-run validation before serving traffic.
//! PITR accepts absolute UTC timestamps and exposes resolved replay LSN.

use tracing::info;

use crate::types::Lsn;

/// Snapshot metadata stored alongside the snapshot data.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SnapshotMeta {
    /// Unique snapshot identifier.
    pub snapshot_id: u64,
    /// LSN at snapshot begin (inclusive).
    pub begin_lsn: Lsn,
    /// LSN at snapshot end (inclusive). All data up to this LSN is captured.
    pub end_lsn: Lsn,
    /// UTC timestamp when snapshot was initiated (microseconds since epoch).
    pub created_at_us: u64,
    /// Node that created this snapshot.
    pub created_by: String,
    /// Whether this is a base snapshot or a delta.
    pub kind: SnapshotKind,
    /// Parent snapshot ID (for deltas).
    pub parent_id: Option<u64>,
    /// Total uncompressed data size in bytes.
    pub data_bytes: u64,
}

/// Snapshot type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SnapshotKind {
    /// Full base image.
    Base,
    /// Block-level delta relative to a parent snapshot.
    Delta,
}

/// Result of a PITR target resolution.
#[derive(Debug, Clone)]
pub struct PitrTarget {
    /// The closest base snapshot to restore from.
    pub base_snapshot: SnapshotMeta,
    /// Delta snapshots to apply in order (oldest first).
    pub deltas: Vec<SnapshotMeta>,
    /// Target LSN resolved from the requested UTC timestamp.
    pub replay_lsn: Lsn,
    /// Number of WAL records to replay after snapshot restore.
    pub wal_records_to_replay: u64,
}

/// Snapshot catalog: tracks all available snapshots for restore planning.
#[derive(Debug, Clone)]
pub struct SnapshotCatalog {
    snapshots: Vec<SnapshotMeta>,
}

impl SnapshotCatalog {
    pub fn new() -> Self {
        Self {
            snapshots: Vec::new(),
        }
    }

    /// Register a completed snapshot.
    pub fn add(&mut self, meta: SnapshotMeta) {
        info!(
            id = meta.snapshot_id,
            kind = ?meta.kind,
            begin_lsn = meta.begin_lsn.as_u64(),
            end_lsn = meta.end_lsn.as_u64(),
            "registered snapshot"
        );
        self.snapshots.push(meta);
    }

    /// Find the best base snapshot for a given target LSN.
    ///
    /// Returns the most recent base snapshot whose `end_lsn <= target_lsn`.
    pub fn find_base(&self, target_lsn: Lsn) -> Option<&SnapshotMeta> {
        self.snapshots
            .iter()
            .filter(|s| s.kind == SnapshotKind::Base && s.end_lsn <= target_lsn)
            .max_by_key(|s| s.end_lsn)
    }

    /// Find all delta snapshots between a base and target LSN.
    pub fn find_deltas(&self, base_lsn: Lsn, target_lsn: Lsn) -> Vec<&SnapshotMeta> {
        let mut deltas: Vec<_> = self
            .snapshots
            .iter()
            .filter(|s| {
                s.kind == SnapshotKind::Delta && s.begin_lsn >= base_lsn && s.end_lsn <= target_lsn
            })
            .collect();
        deltas.sort_by_key(|s| s.begin_lsn);
        deltas
    }

    /// Resolve a PITR target from an absolute UTC timestamp.
    ///
    /// The `lsn_for_timestamp` callback resolves the UTC timestamp to an LSN
    /// (typically by scanning WAL metadata).
    pub fn resolve_pitr<F>(
        &self,
        target_timestamp_us: u64,
        lsn_for_timestamp: F,
    ) -> Option<PitrTarget>
    where
        F: Fn(u64) -> Option<Lsn>,
    {
        let replay_lsn = lsn_for_timestamp(target_timestamp_us)?;
        let base = self.find_base(replay_lsn)?;
        let deltas: Vec<_> = self
            .find_deltas(base.end_lsn, replay_lsn)
            .into_iter()
            .cloned()
            .collect();

        let last_snapshot_lsn = deltas.last().map(|d| d.end_lsn).unwrap_or(base.end_lsn);
        let wal_records = replay_lsn
            .as_u64()
            .saturating_sub(last_snapshot_lsn.as_u64());

        Some(PitrTarget {
            base_snapshot: base.clone(),
            deltas,
            replay_lsn,
            wal_records_to_replay: wal_records,
        })
    }

    pub fn len(&self) -> usize {
        self.snapshots.len()
    }

    pub fn is_empty(&self) -> bool {
        self.snapshots.is_empty()
    }

    /// All snapshots sorted by end_lsn.
    pub fn all(&self) -> &[SnapshotMeta] {
        &self.snapshots
    }
}

impl Default for SnapshotCatalog {
    fn default() -> Self {
        Self::new()
    }
}

/// Dry-run result for restore validation.
#[derive(Debug, Clone)]
pub struct RestoreDryRun {
    /// Whether the restore plan is valid.
    pub valid: bool,
    /// Human-readable description of what would happen.
    pub plan_description: String,
    /// Estimated time for restore (microseconds).
    pub estimated_duration_us: u64,
    /// Number of snapshot files to read.
    pub files_to_read: usize,
    /// Number of WAL records to replay.
    pub wal_records: u64,
    /// Issues found during validation.
    pub issues: Vec<String>,
}

/// Validate a restore plan without executing it.
pub fn dry_run_restore(target: &PitrTarget) -> RestoreDryRun {
    let mut issues = Vec::new();
    let files_to_read = 1 + target.deltas.len(); // base + deltas

    // Validate delta chain continuity.
    let mut expected_lsn = target.base_snapshot.end_lsn;
    for delta in &target.deltas {
        if delta.begin_lsn > expected_lsn {
            issues.push(format!(
                "gap in delta chain: expected begin_lsn <= {}, got {}",
                expected_lsn.as_u64(),
                delta.begin_lsn.as_u64()
            ));
        }
        expected_lsn = delta.end_lsn;
    }

    // Check that replay LSN is reachable.
    if target.replay_lsn < target.base_snapshot.begin_lsn {
        issues.push(format!(
            "replay LSN {} is before base snapshot begin {}",
            target.replay_lsn.as_u64(),
            target.base_snapshot.begin_lsn.as_u64()
        ));
    }

    let plan_description = format!(
        "Restore base snapshot #{} (LSN {}-{}), apply {} deltas, replay {} WAL records to LSN {}",
        target.base_snapshot.snapshot_id,
        target.base_snapshot.begin_lsn.as_u64(),
        target.base_snapshot.end_lsn.as_u64(),
        target.deltas.len(),
        target.wal_records_to_replay,
        target.replay_lsn.as_u64(),
    );

    // Rough estimate: 100MB/s for snapshot reads + 10K WAL records/sec.
    let total_snapshot_bytes: u64 =
        target.base_snapshot.data_bytes + target.deltas.iter().map(|d| d.data_bytes).sum::<u64>();
    let snapshot_us = (total_snapshot_bytes as f64 / 100_000_000.0 * 1_000_000.0) as u64;
    let wal_us = target.wal_records_to_replay * 100; // 100us per record

    RestoreDryRun {
        valid: issues.is_empty(),
        plan_description,
        estimated_duration_us: snapshot_us + wal_us,
        files_to_read,
        wal_records: target.wal_records_to_replay,
        issues,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_snapshot(id: u64, end_lsn: u64) -> SnapshotMeta {
        SnapshotMeta {
            snapshot_id: id,
            begin_lsn: Lsn::new(1),
            end_lsn: Lsn::new(end_lsn),
            created_at_us: 1_700_000_000_000_000,
            created_by: "node-1".into(),
            kind: SnapshotKind::Base,
            parent_id: None,
            data_bytes: 1_000_000,
        }
    }

    fn delta_snapshot(id: u64, begin: u64, end: u64, parent: u64) -> SnapshotMeta {
        SnapshotMeta {
            snapshot_id: id,
            begin_lsn: Lsn::new(begin),
            end_lsn: Lsn::new(end),
            created_at_us: 1_700_000_000_000_000 + end * 1000,
            created_by: "node-1".into(),
            kind: SnapshotKind::Delta,
            parent_id: Some(parent),
            data_bytes: 100_000,
        }
    }

    #[test]
    fn empty_catalog() {
        let cat = SnapshotCatalog::new();
        assert!(cat.is_empty());
        assert!(cat.find_base(Lsn::new(100)).is_none());
    }

    #[test]
    fn find_base_snapshot() {
        let mut cat = SnapshotCatalog::new();
        cat.add(base_snapshot(1, 100));
        cat.add(base_snapshot(2, 500));

        // Target LSN 300: should pick base #1 (end_lsn=100 <= 300).
        let base = cat.find_base(Lsn::new(300)).unwrap();
        assert_eq!(base.snapshot_id, 1);

        // Target LSN 600: should pick base #2 (end_lsn=500 <= 600).
        let base = cat.find_base(Lsn::new(600)).unwrap();
        assert_eq!(base.snapshot_id, 2);

        // Target LSN 50: no base covers it.
        assert!(cat.find_base(Lsn::new(50)).is_none());
    }

    #[test]
    fn find_deltas_in_range() {
        let mut cat = SnapshotCatalog::new();
        cat.add(base_snapshot(1, 100));
        cat.add(delta_snapshot(2, 100, 200, 1));
        cat.add(delta_snapshot(3, 200, 300, 1));
        cat.add(delta_snapshot(4, 300, 400, 1));

        let deltas = cat.find_deltas(Lsn::new(100), Lsn::new(350));
        assert_eq!(deltas.len(), 2); // #2 and #3 (end_lsn <= 350)
        assert_eq!(deltas[0].snapshot_id, 2);
        assert_eq!(deltas[1].snapshot_id, 3);
    }

    #[test]
    fn resolve_pitr() {
        let mut cat = SnapshotCatalog::new();
        cat.add(base_snapshot(1, 100));
        cat.add(delta_snapshot(2, 100, 200, 1));

        // Timestamp resolves to LSN 250.
        let target = cat
            .resolve_pitr(1_700_000_000_250_000, |_| Some(Lsn::new(250)))
            .unwrap();

        assert_eq!(target.base_snapshot.snapshot_id, 1);
        assert_eq!(target.deltas.len(), 1);
        assert_eq!(target.deltas[0].snapshot_id, 2);
        assert_eq!(target.replay_lsn, Lsn::new(250));
        assert_eq!(target.wal_records_to_replay, 50); // 250 - 200
    }

    #[test]
    fn dry_run_valid() {
        let target = PitrTarget {
            base_snapshot: base_snapshot(1, 100),
            deltas: vec![delta_snapshot(2, 100, 200, 1)],
            replay_lsn: Lsn::new(250),
            wal_records_to_replay: 50,
        };

        let result = dry_run_restore(&target);
        assert!(result.valid);
        assert!(result.issues.is_empty());
        assert_eq!(result.files_to_read, 2);
        assert_eq!(result.wal_records, 50);
        assert!(result.plan_description.contains("base snapshot #1"));
    }

    #[test]
    fn dry_run_detects_gap() {
        let target = PitrTarget {
            base_snapshot: base_snapshot(1, 100),
            deltas: vec![delta_snapshot(2, 150, 200, 1)], // gap: 100..150
            replay_lsn: Lsn::new(250),
            wal_records_to_replay: 50,
        };

        let result = dry_run_restore(&target);
        assert!(!result.valid);
        assert!(!result.issues.is_empty());
        assert!(result.issues[0].contains("gap"));
    }

    #[test]
    fn pitr_no_deltas_needed() {
        let mut cat = SnapshotCatalog::new();
        cat.add(base_snapshot(1, 100));

        let target = cat
            .resolve_pitr(1_700_000_000_110_000, |_| Some(Lsn::new(110)))
            .unwrap();

        assert!(target.deltas.is_empty());
        assert_eq!(target.wal_records_to_replay, 10); // 110 - 100
    }
}
