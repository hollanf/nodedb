//! Per-session temporary table registry.
//!
//! Temporary tables are stored as DataFusion MemTables, registered in
//! the session's DataFusion context per-query. They shadow permanent
//! tables with the same name. Auto-dropped on disconnect.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::datasource::MemTable;

use super::store::SessionStore;

/// A session-local temporary table entry.
///
/// Holds the Arrow schema, ON COMMIT behavior, and the backing DataFusion
/// `MemTable` that stores the actual data as Arrow RecordBatches. The MemTable
/// supports SELECT scans and INSERT INTO appends (via DataFusion's `insert_into`).
///
/// Registered per-query in the DataFusion context by `execute_planned_sql` to
/// shadow permanent tables with the same name. Auto-dropped on session disconnect
/// via `TempTableRegistry::clear()`.
pub struct TempTableEntry {
    /// Arrow schema of the table columns.
    pub schema: SchemaRef,
    /// Transaction-end behavior for this temp table's data.
    pub on_commit: OnCommitAction,
    /// DataFusion MemTable backing this temp table.
    /// Supports SELECT reads and INSERT INTO appends.
    pub mem_table: Arc<MemTable>,
}

// MemTable is Debug but we need a manual impl since Arc<MemTable> isn't Clone-Debug-friendly everywhere.
impl std::fmt::Debug for TempTableEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TempTableEntry")
            .field("schema", &self.schema)
            .field("on_commit", &self.on_commit)
            .finish()
    }
}

/// Transaction-end behavior for temporary table data.
///
/// PostgreSQL-compatible semantics:
/// - `PreserveRows` (default): data persists across transactions.
/// - `DeleteRows`: rows are deleted on COMMIT, table remains.
/// - `Drop`: table is dropped entirely on COMMIT.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnCommitAction {
    /// Keep rows (default).
    PreserveRows,
    /// Delete all rows but keep the table.
    DeleteRows,
    /// Drop the entire table.
    Drop,
}

/// Per-session temp table registry.
pub struct TempTableRegistry {
    /// Table name → entry (metadata + MemTable).
    tables: HashMap<String, TempTableEntry>,
}

impl TempTableRegistry {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    /// Register a temp table with a backing MemTable.
    pub fn register(&mut self, name: String, entry: TempTableEntry) {
        self.tables.insert(name, entry);
    }

    /// Check if a temp table exists.
    pub fn exists(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    /// Get a temp table's MemTable for registration in a query context.
    pub fn get_mem_table(&self, name: &str) -> Option<Arc<MemTable>> {
        self.tables.get(name).map(|e| Arc::clone(&e.mem_table))
    }

    /// Get all temp table names and their MemTables (for query context registration).
    pub fn all_mem_tables(&self) -> Vec<(String, Arc<MemTable>)> {
        self.tables
            .iter()
            .map(|(name, entry)| (name.clone(), Arc::clone(&entry.mem_table)))
            .collect()
    }

    /// Remove a temp table.
    pub fn remove(&mut self, name: &str) -> bool {
        self.tables.remove(name).is_some()
    }

    /// List all temp table names.
    pub fn names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    /// Apply ON COMMIT actions. Returns names of tables to drop.
    pub fn on_commit(&mut self) -> Vec<String> {
        let mut to_drop = Vec::new();
        for (name, entry) in &self.tables {
            if entry.on_commit == OnCommitAction::Drop {
                to_drop.push(name.clone());
            }
        }
        for name in &to_drop {
            self.tables.remove(name);
        }
        to_drop
    }

    /// Clear all temp tables (session disconnect).
    pub fn clear(&mut self) {
        self.tables.clear();
    }
}

impl Default for TempTableRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ── SessionStore methods for temp tables ───────────────────────────

impl SessionStore {
    /// Register a temporary table in the session.
    pub fn register_temp_table(&self, addr: &SocketAddr, name: String, entry: TempTableEntry) {
        self.write_session(addr, |session| {
            session.temp_tables.register(name, entry);
        });
    }

    /// Check if a temp table exists in the session.
    pub fn has_temp_table(&self, addr: &SocketAddr, name: &str) -> bool {
        self.read_session(addr, |s| s.temp_tables.exists(name))
            .unwrap_or(false)
    }

    /// Remove a temp table from the session.
    pub fn remove_temp_table(&self, addr: &SocketAddr, name: &str) -> bool {
        self.write_session(addr, |session| session.temp_tables.remove(name))
            .unwrap_or(false)
    }

    /// Get all temp table names for the session.
    pub fn temp_table_names(&self, addr: &SocketAddr) -> Vec<String> {
        self.read_session(addr, |s| s.temp_tables.names())
            .unwrap_or_default()
    }

    /// Get all temp table MemTables for query context registration.
    pub fn temp_mem_tables(&self, addr: &SocketAddr) -> Vec<(String, Arc<MemTable>)> {
        self.read_session(addr, |s| s.temp_tables.all_mem_tables())
            .unwrap_or_default()
    }
}
