//! Bitemporal write paths on `EdgeStore`:
//! `put_edge_versioned`, `soft_delete_edge`, `gdpr_erase_edge`.

use super::keys::{
    EdgeRef, GDPR_ERASURE_SENTINEL, TOMBSTONE_SENTINEL, is_sentinel, versioned_edge_key,
};
use super::payload::EdgeValuePayload;
use crate::engine::graph::edge_store::store::{EDGES, EdgeStore, REVERSE_EDGES, redb_err};

impl EdgeStore {
    /// Write a new version of an edge at `system_from`. Maintains
    /// the reverse index with the same suffix so inbound traversal can
    /// version-scan symmetrically.
    ///
    /// Does NOT close prior versions' `system_until` — Ceiling infers the
    /// closed-open interval at read time from the next-newer version's
    /// `system_from`.
    pub fn put_edge_versioned(
        &self,
        edge: EdgeRef<'_>,
        properties: &[u8],
        system_from: i64,
        valid_from_ms: i64,
        valid_until_ms: i64,
    ) -> crate::Result<()> {
        let fwd = versioned_edge_key(edge.collection, edge.src, edge.label, edge.dst, system_from)?;
        let rev = versioned_edge_key(edge.collection, edge.dst, edge.label, edge.src, system_from)?;
        let payload =
            EdgeValuePayload::new(valid_from_ms, valid_until_ms, properties.to_vec()).encode()?;
        let t = edge.tid.as_u32();

        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("begin_write", e))?;
        {
            let mut edges = write_txn
                .open_table(EDGES)
                .map_err(|e| redb_err("open edges", e))?;
            edges
                .insert((t, fwd.as_str()), payload.as_slice())
                .map_err(|e| redb_err("insert versioned edge", e))?;

            let mut rev_t = write_txn
                .open_table(REVERSE_EDGES)
                .map_err(|e| redb_err("open reverse", e))?;
            rev_t
                .insert((t, rev.as_str()), &[] as &[u8])
                .map_err(|e| redb_err("insert reverse", e))?;
        }
        write_txn.commit().map_err(|e| redb_err("commit", e))?;
        Ok(())
    }

    /// Append a tombstone version at `system_from`.
    pub fn soft_delete_edge(&self, edge: EdgeRef<'_>, system_from: i64) -> crate::Result<()> {
        self.write_sentinel(edge, system_from, TOMBSTONE_SENTINEL)
    }

    /// Append a GDPR-erasure version — distinct from a soft-delete so audits
    /// can distinguish user-visible removal from regulatory erasure.
    pub fn gdpr_erase_edge(&self, edge: EdgeRef<'_>, system_from: i64) -> crate::Result<()> {
        self.write_sentinel(edge, system_from, GDPR_ERASURE_SENTINEL)
    }

    fn write_sentinel(
        &self,
        edge: EdgeRef<'_>,
        system_from: i64,
        sentinel: &[u8],
    ) -> crate::Result<()> {
        debug_assert!(
            is_sentinel(sentinel),
            "write_sentinel called with non-sentinel bytes"
        );
        let fwd = versioned_edge_key(edge.collection, edge.src, edge.label, edge.dst, system_from)?;
        let rev = versioned_edge_key(edge.collection, edge.dst, edge.label, edge.src, system_from)?;
        let t = edge.tid.as_u32();

        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("begin_write", e))?;
        {
            let mut edges = write_txn
                .open_table(EDGES)
                .map_err(|e| redb_err("open edges", e))?;
            edges
                .insert((t, fwd.as_str()), sentinel)
                .map_err(|e| redb_err("insert sentinel edge", e))?;

            let mut rev_t = write_txn
                .open_table(REVERSE_EDGES)
                .map_err(|e| redb_err("open reverse", e))?;
            rev_t
                .insert((t, rev.as_str()), sentinel)
                .map_err(|e| redb_err("insert sentinel reverse", e))?;
        }
        write_txn
            .commit()
            .map_err(|e| redb_err("commit sentinel", e))?;
        Ok(())
    }
}
