//! Column statistics for cost-based query optimization.
//!
//! Maintains per-collection, per-field statistics in redb metadata tables,
//! updated incrementally on writes. Used by the CBO to select join strategies,
//! estimate result cardinality, and choose scan methods.

use std::sync::Arc;

use redb::{Database, TableDefinition};
use serde::{Deserialize, Serialize};

/// Redb table for column statistics.
/// Key: "{tenant}:{collection}:{field}" → Value: serialized ColumnStats.
const COLUMN_STATS: TableDefinition<&str, &[u8]> = TableDefinition::new("column_stats");

/// Statistics for a single column in a collection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnStats {
    /// Total number of documents observed (including those without this field).
    pub row_count: u64,
    /// Number of documents that have this field with a non-null value.
    pub non_null_count: u64,
    /// Number of null values (field absent or explicitly null).
    pub null_count: u64,
    /// Approximate number of distinct values (HyperLogLog estimate).
    pub distinct_count: u64,
    /// Minimum value observed (as JSON string for cross-type comparison).
    pub min_value: Option<String>,
    /// Maximum value observed (as JSON string for cross-type comparison).
    pub max_value: Option<String>,
    /// HyperLogLog registers for cardinality estimation.
    /// 256 registers (m=256) give ~6.5% standard error, good enough for CBO.
    pub hll_registers: Vec<u8>,
}

/// Number of HLL registers. 256 = 2^8, giving ~6.5% standard error.
const HLL_M: usize = 256;
const HLL_P: u32 = 8; // log2(HLL_M)

impl ColumnStats {
    /// Create empty statistics for a new column.
    pub fn new() -> Self {
        Self {
            row_count: 0,
            non_null_count: 0,
            null_count: 0,
            distinct_count: 0,
            min_value: None,
            max_value: None,
            hll_registers: vec![0u8; HLL_M],
        }
    }

    /// Update statistics with a new observed value.
    ///
    /// Call this on every write (PointPut) for each field in the document.
    pub fn observe(&mut self, value: Option<&serde_json::Value>) {
        self.row_count += 1;

        match value {
            None | Some(serde_json::Value::Null) => {
                self.null_count += 1;
            }
            Some(val) => {
                self.non_null_count += 1;

                // Update min/max.
                let val_str = match val {
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                };
                match &self.min_value {
                    None => self.min_value = Some(val_str.clone()),
                    Some(min) if val_str < *min => self.min_value = Some(val_str.clone()),
                    _ => {}
                }
                match &self.max_value {
                    None => self.max_value = Some(val_str.clone()),
                    Some(max) if val_str > *max => self.max_value = Some(val_str.clone()),
                    _ => {}
                }

                // Update HyperLogLog for cardinality estimation.
                let hash = Self::hash_value(&val_str);
                let register_idx = (hash as usize) & (HLL_M - 1);
                let remaining = hash >> HLL_P;
                let leading_zeros = if remaining == 0 {
                    (64 - HLL_P) as u8
                } else {
                    remaining.trailing_zeros() as u8 + 1
                };
                if leading_zeros > self.hll_registers[register_idx] {
                    self.hll_registers[register_idx] = leading_zeros;
                }

                // Re-estimate distinct count from HLL registers.
                self.distinct_count = self.hll_estimate();
            }
        }
    }

    /// HyperLogLog cardinality estimate.
    fn hll_estimate(&self) -> u64 {
        let m = HLL_M as f64;
        // Alpha constant for m=256.
        let alpha = 0.7213 / (1.0 + 1.079 / m);
        let raw: f64 = alpha * m * m
            / self
                .hll_registers
                .iter()
                .map(|&r| 2.0_f64.powi(-(r as i32)))
                .sum::<f64>();

        if raw <= 2.5 * m {
            // Small range correction.
            let zeros = self.hll_registers.iter().filter(|&&r| r == 0).count() as f64;
            if zeros > 0.0 {
                (m * (m / zeros).ln()) as u64
            } else {
                raw as u64
            }
        } else {
            raw as u64
        }
    }

    /// Simple hash function for HLL (FNV-1a 64-bit).
    fn hash_value(s: &str) -> u64 {
        let mut hash: u64 = 0xcbf29ce484222325;
        for byte in s.as_bytes() {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash
    }

    /// Selectivity estimate for equality predicate (1 / distinct_count).
    pub fn eq_selectivity(&self) -> f64 {
        if self.distinct_count == 0 {
            1.0
        } else {
            1.0 / self.distinct_count as f64
        }
    }

    /// Selectivity estimate for range predicate (heuristic: 0.33).
    pub fn range_selectivity(&self) -> f64 {
        0.33
    }
}

impl Default for ColumnStats {
    fn default() -> Self {
        Self::new()
    }
}

/// Column statistics store backed by redb.
pub struct StatsStore {
    db: Arc<Database>,
}

impl StatsStore {
    /// Open or create the stats store sharing a redb database.
    pub fn open(db: Arc<Database>) -> crate::Result<Self> {
        // Ensure the table exists.
        let write_txn = db.begin_write().map_err(|e| crate::Error::Storage {
            engine: "stats".into(),
            detail: format!("open write txn: {e}"),
        })?;
        {
            let _ = write_txn.open_table(COLUMN_STATS);
        }
        write_txn.commit().map_err(|e| crate::Error::Storage {
            engine: "stats".into(),
            detail: format!("commit: {e}"),
        })?;
        Ok(Self { db })
    }

    /// Load statistics for a column.
    pub fn get(
        &self,
        tenant_id: u32,
        collection: &str,
        field: &str,
    ) -> crate::Result<Option<ColumnStats>> {
        let key = format!("{tenant_id}:{collection}:{field}");
        let read_txn = self.db.begin_read().map_err(|e| crate::Error::Storage {
            engine: "stats".into(),
            detail: format!("read txn: {e}"),
        })?;
        let table = read_txn
            .open_table(COLUMN_STATS)
            .map_err(|e| crate::Error::Storage {
                engine: "stats".into(),
                detail: format!("open table: {e}"),
            })?;
        match table.get(key.as_str()) {
            Ok(Some(guard)) => {
                let bytes = guard.value();
                let stats: ColumnStats =
                    rmp_serde::from_slice(bytes).map_err(|e| crate::Error::Storage {
                        engine: "stats".into(),
                        detail: format!("deserialize: {e}"),
                    })?;
                Ok(Some(stats))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(crate::Error::Storage {
                engine: "stats".into(),
                detail: format!("get: {e}"),
            }),
        }
    }

    /// Persist updated statistics for a column.
    pub fn put(
        &self,
        tenant_id: u32,
        collection: &str,
        field: &str,
        stats: &ColumnStats,
    ) -> crate::Result<()> {
        let key = format!("{tenant_id}:{collection}:{field}");
        let bytes = rmp_serde::to_vec_named(stats).map_err(|e| crate::Error::Storage {
            engine: "stats".into(),
            detail: format!("serialize: {e}"),
        })?;
        let write_txn = self.db.begin_write().map_err(|e| crate::Error::Storage {
            engine: "stats".into(),
            detail: format!("write txn: {e}"),
        })?;
        {
            let mut table =
                write_txn
                    .open_table(COLUMN_STATS)
                    .map_err(|e| crate::Error::Storage {
                        engine: "stats".into(),
                        detail: format!("open table: {e}"),
                    })?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| crate::Error::Storage {
                    engine: "stats".into(),
                    detail: format!("insert: {e}"),
                })?;
        }
        write_txn.commit().map_err(|e| crate::Error::Storage {
            engine: "stats".into(),
            detail: format!("commit: {e}"),
        })?;
        Ok(())
    }

    /// Update statistics incrementally for a document's fields.
    ///
    /// Called on every PointPut. Loads existing stats for each field,
    /// observes the new value, and persists.
    pub fn observe_document(
        &self,
        tenant_id: u32,
        collection: &str,
        doc: &serde_json::Value,
    ) -> crate::Result<()> {
        if let Some(obj) = doc.as_object() {
            for (field, value) in obj {
                let mut stats = self.get(tenant_id, collection, field)?.unwrap_or_default();
                stats.observe(Some(value));
                self.put(tenant_id, collection, field, &stats)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hll_cardinality_estimate() {
        let mut stats = ColumnStats::new();
        for i in 0..1000 {
            stats.observe(Some(&serde_json::Value::String(format!("value_{i}"))));
        }
        // HLL with 256 registers should be within ~20% of 1000.
        assert!(
            stats.distinct_count > 700,
            "too low: {}",
            stats.distinct_count
        );
        assert!(
            stats.distinct_count < 1400,
            "too high: {}",
            stats.distinct_count
        );
    }

    #[test]
    fn min_max_tracking() {
        let mut stats = ColumnStats::new();
        for v in &["charlie", "alice", "bob"] {
            stats.observe(Some(&serde_json::Value::String(v.to_string())));
        }
        assert_eq!(stats.min_value.as_deref(), Some("alice"));
        assert_eq!(stats.max_value.as_deref(), Some("charlie"));
        assert_eq!(stats.non_null_count, 3);
        assert_eq!(stats.null_count, 0);
    }

    #[test]
    fn null_tracking() {
        let mut stats = ColumnStats::new();
        stats.observe(None);
        stats.observe(Some(&serde_json::Value::Null));
        stats.observe(Some(&serde_json::Value::String("val".into())));
        assert_eq!(stats.null_count, 2);
        assert_eq!(stats.non_null_count, 1);
        assert_eq!(stats.row_count, 3);
    }

    #[test]
    fn eq_selectivity() {
        let mut stats = ColumnStats::new();
        for i in 0..100 {
            stats.observe(Some(&serde_json::Value::String(format!("v{i}"))));
        }
        let sel = stats.eq_selectivity();
        assert!(sel > 0.005 && sel < 0.02, "selectivity: {sel}");
    }

    #[test]
    fn stats_store_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(Database::create(dir.path().join("stats.redb")).unwrap());
        let store = StatsStore::open(db).unwrap();

        let mut stats = ColumnStats::new();
        stats.observe(Some(&serde_json::Value::String("hello".into())));
        store.put(1, "users", "name", &stats).unwrap();

        let loaded = store.get(1, "users", "name").unwrap().unwrap();
        assert_eq!(loaded.row_count, 1);
        assert_eq!(loaded.non_null_count, 1);
        assert_eq!(loaded.min_value, Some("hello".to_string()));
    }
}
