//! Spatial query handler: R-tree index scan with predicate refinement.
//!
//! Documents with geometry fields are auto-indexed into per-field R-trees
//! on insert (see `handlers/point.rs`). Spatial queries use the R-tree for
//! fast bbox candidate selection, then refine with exact predicates.
//!
//! Internal document representation: `nodedb_types::Value` (no JSON intermediary).

use sonic_rs;
use tracing::debug;

use super::super::response_codec;
use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::physical_plan::SpatialPredicate;
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use nodedb_types::Value;

impl CoreLoop {
    /// Execute a spatial scan using the R-tree index.
    ///
    /// 1. Parse query geometry from GeoJSON bytes
    /// 2. R-tree range search for bbox candidates
    /// 3. Exact predicate refinement (extract geometry, apply ST_*)
    /// 4. Return matching documents up to limit
    #[allow(clippy::too_many_arguments)]
    pub(in crate::data::executor) fn execute_spatial_scan(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        field: &str,
        predicate: &SpatialPredicate,
        query_geometry_bytes: &[u8],
        distance_meters: f64,
        attribute_filters: &[u8],
        limit: usize,
        projection: &[String],
        rls_filters: &[u8],
    ) -> Response {
        debug!(
            core = self.core_id,
            %collection,
            %field,
            predicate = ?predicate,
            "spatial scan"
        );

        // 1. Parse query geometry.
        let query_geom: nodedb_types::geometry::Geometry =
            match sonic_rs::from_slice(query_geometry_bytes) {
                Ok(g) => g,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("invalid query geometry GeoJSON: {e}"),
                        },
                    );
                }
            };

        // 2. Deserialize attribute and RLS filters.
        let attr_filters: Vec<ScanFilter> = if attribute_filters.is_empty() {
            Vec::new()
        } else {
            zerompk::from_msgpack(attribute_filters).unwrap_or_default()
        };
        let row_level_filters: Vec<ScanFilter> = if rls_filters.is_empty() {
            Vec::new()
        } else {
            zerompk::from_msgpack(rls_filters).unwrap_or_default()
        };

        // 3. Compute search bbox (expand by distance for ST_DWithin).
        let query_bbox = nodedb_types::bbox::geometry_bbox(&query_geom);
        let search_bbox = if distance_meters > 0.0 {
            expand_bbox(&query_bbox, distance_meters)
        } else {
            query_bbox
        };

        let tid_id = crate::types::TenantId::new(tid);
        let spatial_key = (tid_id, collection.to_string(), field.to_string());
        let has_index = self.spatial_indexes.contains_key(&spatial_key);
        let limit = if limit == 0 { 1000 } else { limit };

        // No R-tree: full scan with predicate post-filter.
        if !has_index {
            return self.spatial_full_scan(
                task,
                tid,
                collection,
                field,
                predicate,
                &query_geom,
                distance_meters,
                limit,
                projection,
                &attr_filters,
                &row_level_filters,
            );
        }

        let rtree = match self.spatial_indexes.get(&spatial_key) {
            Some(rt) => rt,
            None => {
                return match response_codec::encode_value_vec(&[]) {
                    Ok(payload) => self.response_with_payload(task, payload),
                    Err(e) => self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    ),
                };
            }
        };

        // 4. R-tree range search → candidate entry IDs.
        let candidates = rtree.search(&search_bbox);
        debug!(
            core = self.core_id,
            candidates = candidates.len(),
            "spatial R-tree candidates"
        );

        // Pre-load all docs from scan_collection (handles both sparse and columnar).
        let all_docs: std::collections::HashMap<String, Vec<u8>> = self
            .scan_collection(tid, collection, 10_000)
            .unwrap_or_default()
            .into_iter()
            .collect();

        // 5. Exact predicate refinement.
        let mut results = Vec::new();

        for entry in &candidates {
            if results.len() >= limit {
                break;
            }

            let doc_id = match self.spatial_doc_map.get(&(
                tid_id,
                collection.to_string(),
                field.to_string(),
                entry.id,
            )) {
                Some(id) => id.clone(),
                None => continue,
            };

            let doc_bytes = match all_docs.get(&doc_id) {
                Some(b) => b,
                None => continue,
            };

            let doc = match super::super::doc_format::decode_document_value(doc_bytes) {
                Some(d) => d,
                None => continue,
            };

            let doc_geom = match extract_geometry(&doc, field) {
                Some(g) => g,
                None => continue,
            };

            if !apply_predicate(predicate, &query_geom, &doc_geom, distance_meters) {
                continue;
            }

            if !attr_filters.iter().all(|f| f.matches_value(&doc)) {
                continue;
            }
            if !row_level_filters.iter().all(|f| f.matches_value(&doc)) {
                continue;
            }

            results.push(project_doc(&doc, &doc_id, projection));
        }

        match response_codec::encode_value_vec(&results) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// Full scan when no R-tree exists for the field.
    #[allow(clippy::too_many_arguments)]
    fn spatial_full_scan(
        &self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        field: &str,
        predicate: &SpatialPredicate,
        query_geom: &nodedb_types::geometry::Geometry,
        distance_meters: f64,
        limit: usize,
        projection: &[String],
        attr_filters: &[ScanFilter],
        rls_filters: &[ScanFilter],
    ) -> Response {
        debug!(core = self.core_id, %collection, "spatial full scan (no R-tree index yet)");

        let scan_limit = limit * 10;
        let entries = match self.scan_collection(tid, collection, scan_limit) {
            Ok(e) => e,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };

        let mut results = Vec::new();
        for (doc_id, doc_bytes) in &entries {
            if results.len() >= limit {
                break;
            }

            let doc = match super::super::doc_format::decode_document_value(doc_bytes) {
                Some(d) => d,
                None => continue,
            };

            let doc_geom = match extract_geometry(&doc, field) {
                Some(g) => g,
                None => continue,
            };

            if !apply_predicate(predicate, query_geom, &doc_geom, distance_meters) {
                continue;
            }

            if !attr_filters.iter().all(|f| f.matches_value(&doc)) {
                continue;
            }
            if !rls_filters.iter().all(|f| f.matches_value(&doc)) {
                continue;
            }

            results.push(project_doc(&doc, doc_id, projection));
        }

        match response_codec::encode_value_vec(&results) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }
}

/// Extract geometry from a document field.
///
/// Handles three storage forms:
/// - `Value::Geometry(g)` — native geometry (columnar path preserves type)
/// - `Value::String(s)` — GeoJSON string (from SQL ST_Point → serialized)
/// - `Value::Object(_)` — GeoJSON object (from schemaless doc storage)
fn extract_geometry(doc: &Value, field: &str) -> Option<nodedb_types::geometry::Geometry> {
    let field_val = doc.get(field)?;
    match field_val {
        Value::Geometry(g) => Some(g.clone()),
        Value::String(s) => sonic_rs::from_str(s).ok(),
        Value::Object(map) => {
            // GeoJSON object stored as Value::Object — serialize to JSON then parse.
            let json = serde_json::Value::from(Value::Object(map.clone()));
            serde_json::from_value(json).ok()
        }
        _ => None,
    }
}

/// Apply the spatial predicate.
fn apply_predicate(
    predicate: &SpatialPredicate,
    query: &nodedb_types::geometry::Geometry,
    doc: &nodedb_types::geometry::Geometry,
    distance_meters: f64,
) -> bool {
    match predicate {
        SpatialPredicate::DWithin => {
            crate::engine::spatial::st_dwithin(query, doc, distance_meters)
        }
        SpatialPredicate::Contains => crate::engine::spatial::st_contains(query, doc),
        SpatialPredicate::Intersects => crate::engine::spatial::st_intersects(query, doc),
        SpatialPredicate::Within => crate::engine::spatial::st_within(doc, query),
    }
}

/// Apply projection to a document, returning `nodedb_types::Value`.
fn project_doc(doc: &Value, doc_id: &str, projection: &[String]) -> Value {
    if projection.is_empty() {
        // Add id if not present.
        if let Value::Object(mut map) = doc.clone() {
            map.entry("id".to_string())
                .or_insert(Value::String(doc_id.to_string()));
            Value::Object(map)
        } else {
            doc.clone()
        }
    } else {
        let mut map = std::collections::HashMap::new();
        map.insert("id".to_string(), Value::String(doc_id.to_string()));
        for col in projection {
            if let Some(v) = doc.get(col) {
                map.insert(col.clone(), v.clone());
            }
        }
        Value::Object(map)
    }
}

/// Expand a bounding box by a distance in meters.
fn expand_bbox(bbox: &nodedb_types::BoundingBox, meters: f64) -> nodedb_types::BoundingBox {
    let lat_delta = meters / 111_320.0;
    let avg_lat = ((bbox.min_lat + bbox.max_lat) / 2.0).to_radians();
    let lng_delta = meters / (111_320.0 * avg_lat.cos().max(0.001));

    nodedb_types::BoundingBox::new(
        bbox.min_lng - lng_delta,
        bbox.min_lat - lat_delta,
        bbox.max_lng + lng_delta,
        bbox.max_lat + lat_delta,
    )
}
