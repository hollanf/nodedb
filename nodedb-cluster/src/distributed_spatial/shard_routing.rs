//! Spatial-aware shard routing.
//!
//! Maintains per-shard bounding box metadata — the spatial extent of all
//! geometries on each shard. On spatial query, only fan out to shards
//! whose bounding box overlaps the query region.
//!
//! Updated at flush time: each shard reports its spatial extent to the
//! routing table. Bounding box union is cheap (min of mins, max of maxes).

use nodedb_types::BoundingBox;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Per-shard spatial extent for a collection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardSpatialExtent {
    pub shard_id: u32,
    /// Bounding box covering all geometries in this shard for this collection.
    /// None if the shard has no spatial data for this collection.
    pub extent: Option<BoundingBox>,
    /// Number of spatial entries on this shard.
    pub entry_count: usize,
}

/// Per-collection spatial routing metadata.
///
/// Tracks which shards have spatial data and their extent. Used by the
/// coordinator to avoid fanning out to shards that can't possibly match.
pub struct SpatialRoutingTable {
    /// collection_name → shard_id → extent.
    extents: HashMap<String, HashMap<u32, ShardSpatialExtent>>,
}

impl SpatialRoutingTable {
    pub fn new() -> Self {
        Self {
            extents: HashMap::new(),
        }
    }

    /// Update the spatial extent for a shard.
    pub fn update_extent(
        &mut self,
        collection: &str,
        shard_id: u32,
        extent: Option<BoundingBox>,
        entry_count: usize,
    ) {
        let entry = ShardSpatialExtent {
            shard_id,
            extent,
            entry_count,
        };
        self.extents
            .entry(collection.to_string())
            .or_default()
            .insert(shard_id, entry);
    }

    /// Find which shards might contain results for a spatial query.
    ///
    /// Returns shard IDs whose extent overlaps the query bounding box.
    /// If no extent data is available for a shard, it is conservatively
    /// included (we don't know what's there, so we must check).
    pub fn route_query(
        &self,
        collection: &str,
        query_bbox: &BoundingBox,
        all_shard_ids: &[u32],
    ) -> Vec<u32> {
        let Some(shard_extents) = self.extents.get(collection) else {
            // No extent data at all — fan out to all shards (conservative).
            return all_shard_ids.to_vec();
        };

        let mut target_shards = Vec::new();
        for &shard_id in all_shard_ids {
            match shard_extents.get(&shard_id) {
                Some(ext) => {
                    match &ext.extent {
                        Some(bbox) if !bbox.intersects(query_bbox) => {
                            // Skip — shard extent doesn't overlap query.
                        }
                        _ => target_shards.push(shard_id),
                    }
                }
                None => {
                    // No extent data for this shard — include conservatively.
                    target_shards.push(shard_id);
                }
            }
        }
        target_shards
    }

    /// Get the total number of spatial entries across all shards for a collection.
    pub fn total_entries(&self, collection: &str) -> usize {
        self.extents
            .get(collection)
            .map(|shards| shards.values().map(|e| e.entry_count).sum())
            .unwrap_or(0)
    }
}

impl Default for SpatialRoutingTable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_filters_non_overlapping() {
        let mut table = SpatialRoutingTable::new();
        // Shard 0: buildings in NYC area.
        table.update_extent(
            "buildings",
            0,
            Some(BoundingBox::new(-74.1, 40.6, -73.9, 40.8)),
            1000,
        );
        // Shard 1: buildings in London area.
        table.update_extent(
            "buildings",
            1,
            Some(BoundingBox::new(-0.2, 51.4, 0.1, 51.6)),
            500,
        );
        // Shard 2: buildings in Tokyo.
        table.update_extent(
            "buildings",
            2,
            Some(BoundingBox::new(139.6, 35.6, 139.8, 35.8)),
            300,
        );

        // Query near NYC — should only route to shard 0.
        let query = BoundingBox::new(-74.05, 40.7, -73.95, 40.8);
        let targets = table.route_query("buildings", &query, &[0, 1, 2]);
        assert_eq!(targets, vec![0]);
    }

    #[test]
    fn route_includes_unknown_shards() {
        let mut table = SpatialRoutingTable::new();
        table.update_extent(
            "buildings",
            0,
            Some(BoundingBox::new(0.0, 0.0, 10.0, 10.0)),
            100,
        );
        // Shard 1 has no extent data.

        let query = BoundingBox::new(5.0, 5.0, 15.0, 15.0);
        let targets = table.route_query("buildings", &query, &[0, 1]);
        // Both shards included: 0 overlaps, 1 is unknown.
        assert_eq!(targets.len(), 2);
    }

    #[test]
    fn route_no_extent_data_fans_out_all() {
        let table = SpatialRoutingTable::new();
        let targets =
            table.route_query("unknown", &BoundingBox::new(0.0, 0.0, 1.0, 1.0), &[0, 1, 2]);
        assert_eq!(targets, vec![0, 1, 2]);
    }

    #[test]
    fn total_entries() {
        let mut table = SpatialRoutingTable::new();
        table.update_extent("col", 0, Some(BoundingBox::new(0.0, 0.0, 1.0, 1.0)), 100);
        table.update_extent("col", 1, Some(BoundingBox::new(2.0, 2.0, 3.0, 3.0)), 200);
        assert_eq!(table.total_entries("col"), 300);
    }
}
