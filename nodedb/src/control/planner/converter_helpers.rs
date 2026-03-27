//! Converter helpers: sort, aggregate, and timeseries plan conversion.
//!
//! Extracted from `converter.rs` to keep per-file code under the 500-line limit.

use datafusion::prelude::*;

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::{DocumentOp, QueryOp, TextOp, VectorOp};
use crate::control::planner::physical::PhysicalTask;
use crate::types::{TenantId, VShardId};

use super::converter::PlanConverter;
use super::extract::expr_to_scan_filters;
use super::search::{
    extract_table_name, try_extract_hybrid_search, try_extract_text_search,
    try_extract_vector_search,
};

impl PlanConverter {
    /// Convert a Sort logical plan into physical tasks.
    ///
    /// Handles: vector search, text search, hybrid search, index-ordered scan,
    /// and generic sort-key propagation.
    pub(super) fn convert_sort(
        &self,
        sort: &datafusion::logical_expr::Sort,
        tenant_id: TenantId,
    ) -> crate::Result<Vec<PhysicalTask>> {
        // Check for vector_distance() function in sort expression.
        if let Some((collection, query_vector, top_k)) =
            try_extract_vector_search(&sort.expr, &sort.input, sort.fetch)?
        {
            let vshard = VShardId::from_collection(&collection);
            return Ok(vec![PhysicalTask {
                tenant_id,
                vshard_id: vshard,
                plan: PhysicalPlan::Vector(VectorOp::Search {
                    collection,
                    query_vector: std::sync::Arc::from(query_vector.as_slice()),
                    top_k,
                    ef_search: 0,
                    filter_bitmap: None,
                    field_name: String::new(),
                }),
            }]);
        }

        // Check for bm25_score() in sort expression → TextSearch.
        if let Some((collection, query_text, top_k)) =
            try_extract_text_search(&sort.expr, &sort.input, sort.fetch)
        {
            let vshard = VShardId::from_collection(&collection);
            return Ok(vec![PhysicalTask {
                tenant_id,
                vshard_id: vshard,
                plan: PhysicalPlan::Text(TextOp::Search {
                    collection,
                    query: query_text,
                    top_k,
                    fuzzy: true,
                }),
            }]);
        }

        // Check for rrf_score(vector_distance(...), bm25_score(...), k1, k2) → HybridSearch.
        if let Some((collection, query_vector, query_text, top_k, vector_k, text_k)) =
            try_extract_hybrid_search(&sort.expr, &sort.input, sort.fetch)?
        {
            let vshard = VShardId::from_collection(&collection);
            let vector_weight = (text_k / (vector_k + text_k).max(0.001)) as f32;
            return Ok(vec![PhysicalTask {
                tenant_id,
                vshard_id: vshard,
                plan: PhysicalPlan::Text(TextOp::HybridSearch {
                    collection,
                    query_vector: std::sync::Arc::from(query_vector.as_slice()),
                    query_text,
                    top_k,
                    ef_search: 0,
                    fuzzy: true,
                    vector_weight,
                    filter_bitmap: None,
                }),
            }]);
        }

        // Index-ordered scan optimization (single ASC field with LIMIT).
        if sort.expr.len() == 1
            && sort.expr[0].asc
            && let Expr::Column(col) = &sort.expr[0].expr
        {
            let sort_field = &col.name;
            if sort_field != "id"
                && sort_field != "document_id"
                && let Some(collection) = extract_table_name(&sort.input)
            {
                let limit = sort.fetch.unwrap_or(1000);
                let vshard = VShardId::from_collection(&collection);
                return Ok(vec![PhysicalTask {
                    tenant_id,
                    vshard_id: vshard,
                    plan: PhysicalPlan::Document(DocumentOp::RangeScan {
                        collection,
                        field: sort_field.clone(),
                        lower: None,
                        upper: None,
                        limit,
                    }),
                }]);
            }
        }

        let mut tasks = self.convert(&sort.input, tenant_id)?;

        // Extract sort expressions as (field, ascending) pairs.
        let mut extracted_keys = Vec::new();
        for sort_expr in &sort.expr {
            if let Expr::Column(col) = &sort_expr.expr {
                extracted_keys.push((col.name.clone(), sort_expr.asc));
            }
        }
        if !extracted_keys.is_empty() {
            for task in &mut tasks {
                if let PhysicalPlan::Document(DocumentOp::Scan { sort_keys, .. }) = &mut task.plan {
                    *sort_keys = extracted_keys.clone();
                }
            }
        }

        // Propagate sort's fetch as additional limit.
        if let Some(fetch) = sort.fetch {
            for task in &mut tasks {
                if let PhysicalPlan::Document(DocumentOp::Scan { limit, .. }) = &mut task.plan {
                    *limit = fetch;
                }
            }
        }

        Ok(tasks)
    }
}

/// Convert a DataFusion `Aggregate` logical plan into a `QueryOp::Aggregate`.
pub(super) fn convert_aggregate(
    agg: &datafusion::logical_expr::Aggregate,
    tenant_id: TenantId,
) -> crate::Result<Vec<PhysicalTask>> {
    let collection = extract_table_name(&agg.input).ok_or_else(|| crate::Error::PlanError {
        detail: "GROUP BY requires a table scan input".into(),
    })?;
    let vshard = VShardId::from_collection(&collection);

    // Extract GROUP BY fields (all group expressions).
    let group_by: Vec<String> = agg
        .group_expr
        .iter()
        .filter_map(|e| {
            if let Expr::Column(col) = e {
                Some(col.name.clone())
            } else {
                None
            }
        })
        .collect();

    // Extract aggregate functions.
    let mut aggregates = Vec::new();
    for expr in &agg.aggr_expr {
        if let Expr::AggregateFunction(func) = expr {
            let mut op = func.func.name().to_lowercase();
            let field = func
                .params
                .args
                .first()
                .map(|a| match a {
                    Expr::Column(col) => col.name.clone(),
                    Expr::Literal(..) => "*".into(),
                    _ => format!("{a}"),
                })
                .unwrap_or_else(|| "*".into());

            // Handle DISTINCT modifier: COUNT(DISTINCT col) → "count_distinct".
            if func.params.distinct {
                op = format!("{op}_distinct");
            }

            aggregates.push((op, field));
        }
    }

    // Extract filters from input if it's a Filter plan.
    let filter_bytes =
        if let datafusion::logical_expr::LogicalPlan::Filter(filter) = agg.input.as_ref() {
            let filters = expr_to_scan_filters(&filter.predicate);
            rmp_serde::to_vec_named(&filters).unwrap_or_default()
        } else {
            Vec::new()
        };

    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        plan: PhysicalPlan::Query(QueryOp::Aggregate {
            collection,
            group_by,
            aggregates,
            filters: filter_bytes,
            having: Vec::new(),
            limit: 10000,
            sub_group_by: Vec::new(),
            sub_aggregates: Vec::new(),
        }),
    }])
}

/// Extract time-range predicates from filter expressions for timeseries routing.
///
/// Looks for patterns like `timestamp >= X AND timestamp <= Y` and extracts
/// the range bounds. Non-timestamp filters are serialized as generic filters.
/// Returns `((min_ts, max_ts), filter_bytes)`.
pub(super) fn extract_timeseries_filters(filters: &[Expr]) -> crate::Result<((i64, i64), Vec<u8>)> {
    let mut min_ts = 0i64;
    let mut max_ts = i64::MAX;
    let mut remaining = Vec::new();

    for f in filters {
        if let Some((bound, is_lower)) = try_extract_timestamp_bound(f) {
            if is_lower {
                min_ts = min_ts.max(bound);
            } else {
                max_ts = max_ts.min(bound);
            }
        } else {
            remaining.extend(expr_to_scan_filters(f));
        }
    }

    let filter_bytes = if remaining.is_empty() {
        Vec::new()
    } else {
        rmp_serde::to_vec_named(&remaining).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("ts filter serialization: {e}"),
        })?
    };

    Ok(((min_ts, max_ts), filter_bytes))
}

/// Try to extract a timestamp bound from a comparison expression.
///
/// Returns `Some((bound_value_ms, is_lower_bound))` if the expression
/// is `timestamp >= X` or `timestamp <= X` (or `>`, `<`).
fn try_extract_timestamp_bound(expr: &Expr) -> Option<(i64, bool)> {
    match expr {
        Expr::BinaryExpr(bin) => {
            // Check if left side is a column named "timestamp" or "ts".
            let col_name = match bin.left.as_ref() {
                Expr::Column(col) => col.name.to_lowercase(),
                _ => return None,
            };
            if col_name != "timestamp" && col_name != "ts" && col_name != "time" {
                return None;
            }

            // Extract literal value.
            let val = match bin.right.as_ref() {
                Expr::Literal(datafusion::scalar::ScalarValue::Int64(Some(v)), _) => *v,
                Expr::Literal(datafusion::scalar::ScalarValue::Float64(Some(v)), _) => *v as i64,
                _ => return None,
            };

            match bin.op {
                datafusion::logical_expr::Operator::GtEq
                | datafusion::logical_expr::Operator::Gt => {
                    Some((val, true)) // lower bound
                }
                datafusion::logical_expr::Operator::LtEq
                | datafusion::logical_expr::Operator::Lt => {
                    Some((val, false)) // upper bound
                }
                _ => None,
            }
        }
        _ => None,
    }
}
