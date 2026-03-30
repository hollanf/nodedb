//! Text search plan builders.

use std::sync::Arc;

use nodedb_types::protocol::TextFields;

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::TextOp;

pub(crate) fn build_search(fields: &TextFields, collection: &str) -> crate::Result<PhysicalPlan> {
    let query_text = fields
        .query_text
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'query_text'".to_string(),
        })?;
    let top_k = fields.top_k.unwrap_or(10) as usize;
    let fuzzy = fields.fuzzy.unwrap_or(false);

    Ok(PhysicalPlan::Text(TextOp::Search {
        collection: collection.to_string(),
        query: query_text.clone(),
        top_k,
        fuzzy,
        rls_filters: Vec::new(),
    }))
}

pub(crate) fn build_hybrid_search(
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let query_vector = fields
        .query_vector
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'query_vector'".to_string(),
        })?;
    let query_text = fields
        .query_text
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'query_text'".to_string(),
        })?;
    let top_k = fields.top_k.unwrap_or(10) as usize;
    let vector_weight = fields.vector_weight.unwrap_or(0.5) as f32;
    let ef_search = fields.ef_search.unwrap_or(0) as usize;
    let fuzzy = fields.fuzzy.unwrap_or(false);

    Ok(PhysicalPlan::Text(TextOp::HybridSearch {
        collection: collection.to_string(),
        query_vector: Arc::from(query_vector.as_slice()),
        query_text: query_text.clone(),
        top_k,
        ef_search,
        fuzzy,
        vector_weight,
        filter_bitmap: None,
        rls_filters: Vec::new(),
    }))
}
