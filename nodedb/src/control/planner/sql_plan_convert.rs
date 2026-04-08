//! Convert nodedb-sql SqlPlan IR to NodeDB PhysicalPlan + PhysicalTask.
//!
//! This is the Origin-specific mapping layer. It adds vShard routing,
//! serializes filters to msgpack, and handles broadcast join decisions.

use nodedb_sql::types::{
    AggregateExpr, EngineType, Filter, FilterExpr, Projection, SortKey, SqlExpr, SqlPlan, SqlValue,
};

use std::sync::Arc;

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::*;
use crate::engine::timeseries::retention_policy::RetentionPolicyRegistry;
use crate::types::{TenantId, VShardId};

use super::physical::{PhysicalTask, PostSetOp};

/// Conversion context holding optional references needed during plan conversion.
pub struct ConvertContext {
    pub retention_registry: Option<Arc<RetentionPolicyRegistry>>,
}

/// Convert a list of SqlPlans to PhysicalTasks.
pub fn convert(
    plans: &[SqlPlan],
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let mut tasks = Vec::new();
    for plan in plans {
        tasks.extend(convert_one(plan, tenant_id, ctx)?);
    }
    Ok(tasks)
}

fn convert_one(
    plan: &SqlPlan,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    match plan {
        SqlPlan::ConstantResult { columns, values } => {
            // Build a single-row result as Value::Object → zerompk.
            let mut map = std::collections::HashMap::new();
            for (col, val) in columns.iter().zip(values.iter()) {
                map.insert(col.clone(), sql_value_to_nodedb_value(val));
            }
            let row = nodedb_types::Value::Object(map);
            let payload =
                nodedb_types::value_to_msgpack(&row).map_err(|e| crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("constant result: {e}"),
                })?;
            Ok(vec![PhysicalTask {
                tenant_id,
                vshard_id: VShardId::from_collection(""),
                plan: PhysicalPlan::Meta(MetaOp::RawResponse { payload }),
                post_set_op: PostSetOp::None,
            }])
        }

        SqlPlan::Scan {
            collection,
            engine,
            filters,
            projection,
            sort_keys,
            limit,
            offset,
            distinct,
            window_functions,
        } => {
            let filter_bytes = serialize_filters(filters)?;
            let proj_names = extract_projection_names(projection);
            let sort = convert_sort_keys(sort_keys);
            let vshard = VShardId::from_collection(collection);
            let computed_bytes = extract_computed_columns(projection)?;
            let window_bytes = serialize_window_functions(window_functions)?;

            let physical = match engine {
                EngineType::Timeseries => {
                    let time_range = extract_time_range(filters);
                    PhysicalPlan::Timeseries(TimeseriesOp::Scan {
                        collection: collection.clone(),
                        time_range,
                        projection: proj_names,
                        limit: limit.unwrap_or(10000),
                        filters: filter_bytes,
                        bucket_interval_ms: 0,
                        group_by: Vec::new(),
                        aggregates: Vec::new(),
                        gap_fill: String::new(),
                        rls_filters: Vec::new(),
                    })
                }
                EngineType::Columnar | EngineType::Spatial => {
                    PhysicalPlan::Columnar(ColumnarOp::Scan {
                        collection: collection.clone(),
                        projection: proj_names,
                        limit: limit.unwrap_or(10000),
                        filters: filter_bytes,
                        rls_filters: Vec::new(),
                    })
                }
                EngineType::KeyValue => PhysicalPlan::Kv(KvOp::Scan {
                    collection: collection.clone(),
                    cursor: Vec::new(),
                    count: limit.unwrap_or(10000),
                    filters: filter_bytes,
                    match_pattern: None,
                }),
                _ => PhysicalPlan::Document(DocumentOp::Scan {
                    collection: collection.clone(),
                    limit: limit.unwrap_or(10000),
                    offset: *offset,
                    sort_keys: sort,
                    filters: filter_bytes,
                    distinct: *distinct,
                    projection: proj_names,
                    computed_columns: computed_bytes,
                    window_functions: window_bytes,
                }),
            };
            Ok(vec![PhysicalTask {
                tenant_id,
                vshard_id: vshard,
                plan: physical,
                post_set_op: PostSetOp::None,
            }])
        }

        SqlPlan::PointGet {
            collection,
            engine,
            key_column: _, // Column name is implicit (id/key) per engine type
            key_value,
        } => {
            let vshard = VShardId::from_collection(collection);
            let physical = match engine {
                EngineType::KeyValue => PhysicalPlan::Kv(KvOp::Get {
                    collection: collection.clone(),
                    key: sql_value_to_bytes(key_value),
                    rls_filters: Vec::new(),
                }),
                _ => PhysicalPlan::Document(DocumentOp::PointGet {
                    collection: collection.clone(),
                    document_id: sql_value_to_string(key_value),
                    rls_filters: Vec::new(),
                }),
            };
            Ok(vec![PhysicalTask {
                tenant_id,
                vshard_id: vshard,
                plan: physical,
                post_set_op: PostSetOp::None,
            }])
        }

        SqlPlan::Insert {
            collection,
            engine,
            rows,
            column_defaults,
        } => convert_insert(collection, engine, rows, column_defaults, tenant_id),

        SqlPlan::KvInsert {
            collection,
            entries,
        } => {
            let vshard = VShardId::from_collection(collection);
            let mut tasks = Vec::with_capacity(entries.len());
            for (key_val, value_cols) in entries {
                let key = sql_value_to_bytes(key_val);
                // Value is the payload columns as msgpack map.
                let value = if value_cols.len() == 1 && value_cols[0].0 == "value" {
                    // Simple (key, value) form — value is raw bytes.
                    sql_value_to_bytes(&value_cols[0].1)
                } else {
                    // Typed columns form — write standard msgpack map directly.
                    let mut buf = Vec::with_capacity(value_cols.len() * 32);
                    write_msgpack_map_header(&mut buf, value_cols.len());
                    for (col, val) in value_cols {
                        write_msgpack_str(&mut buf, col);
                        write_msgpack_value(&mut buf, val);
                    }
                    buf
                };
                tasks.push(PhysicalTask {
                    tenant_id,
                    vshard_id: vshard,
                    plan: PhysicalPlan::Kv(KvOp::Put {
                        collection: collection.into(),
                        key,
                        value,
                        ttl_ms: 0,
                    }),
                    post_set_op: PostSetOp::None,
                });
            }
            Ok(tasks)
        }

        SqlPlan::Update {
            collection,
            engine,
            assignments,
            filters,
            target_keys,
            returning,
        } => convert_update(
            collection,
            engine,
            assignments,
            filters,
            target_keys,
            *returning,
            tenant_id,
        ),

        SqlPlan::Delete {
            collection,
            engine,
            filters,
            target_keys,
        } => convert_delete(collection, engine, filters, target_keys, tenant_id),

        SqlPlan::Truncate { collection } => {
            let vshard = VShardId::from_collection(collection);
            Ok(vec![PhysicalTask {
                tenant_id,
                vshard_id: vshard,
                plan: PhysicalPlan::Document(DocumentOp::Truncate {
                    collection: collection.clone(),
                }),
                post_set_op: PostSetOp::None,
            }])
        }

        SqlPlan::Join {
            left,
            right,
            on,
            join_type,
            limit,
            projection,
            filters,
            ..
        } => {
            let mut left_collection = extract_collection_name(left);
            let mut right_collection = extract_collection_name(right);
            let proj_names = extract_projection_names(projection);
            let filter_bytes = serialize_filters(filters)?;

            // RIGHT JOIN → swap sides and convert to LEFT JOIN.
            // This avoids the broadcast-join problem where per-core unmatched
            // right-side emission causes N× duplication across cores.
            let mut on_keys = on.to_vec();
            let effective_join_type = if join_type.as_str() == "right" {
                std::mem::swap(&mut left_collection, &mut right_collection);
                on_keys = on_keys.into_iter().map(|(l, r)| (r, l)).collect();
                "left".to_string()
            } else {
                join_type.as_str().to_string()
            };

            let vshard = VShardId::from_collection(&left_collection);

            Ok(vec![PhysicalTask {
                tenant_id,
                vshard_id: vshard,
                plan: PhysicalPlan::Query(QueryOp::HashJoin {
                    left_collection,
                    right_collection,
                    on: on_keys,
                    join_type: effective_join_type,
                    limit: *limit,
                    post_group_by: Vec::new(),
                    post_aggregates: Vec::new(),
                    projection: proj_names,
                    post_filters: filter_bytes,
                }),
                post_set_op: PostSetOp::None,
            }])
        }

        SqlPlan::Aggregate {
            input,
            group_by,
            aggregates,
            having,
            limit,
        } => convert_aggregate(input, group_by, aggregates, having, *limit, tenant_id),

        SqlPlan::TimeseriesScan {
            collection,
            time_range,
            bucket_interval_ms,
            group_by,
            aggregates,
            filters,
            projection,
            gap_fill,
            limit,
            tiered,
        } => {
            let filter_bytes = serialize_filters(filters)?;
            let agg_pairs: Vec<(String, String)> =
                aggregates.iter().map(agg_expr_to_pair).collect();

            // AUTO_TIER: split query across retention tiers if enabled.
            if *tiered
                && let Some(registry) = &ctx.retention_registry
                && let Some(policy) = registry.get(tenant_id.as_u32(), collection)
                && policy.auto_tier
            {
                return Ok(super::auto_tier::plan_tiered_scan(
                    &policy,
                    tenant_id,
                    *time_range,
                    filter_bytes,
                    group_by.clone(),
                    agg_pairs,
                    gap_fill.clone(),
                ));
            }

            let proj_names = extract_projection_names(projection);
            let vshard = VShardId::from_collection(collection);
            Ok(vec![PhysicalTask {
                tenant_id,
                vshard_id: vshard,
                plan: PhysicalPlan::Timeseries(TimeseriesOp::Scan {
                    collection: collection.clone(),
                    time_range: *time_range,
                    projection: proj_names,
                    limit: *limit,
                    filters: filter_bytes,
                    bucket_interval_ms: *bucket_interval_ms,
                    group_by: group_by.clone(),
                    aggregates: agg_pairs,
                    gap_fill: gap_fill.clone(),
                    rls_filters: Vec::new(),
                }),
                post_set_op: PostSetOp::None,
            }])
        }

        SqlPlan::TimeseriesIngest { collection, rows } => {
            let vshard = VShardId::from_collection(collection);
            let payload = serialize_ingest_rows(rows)?;
            Ok(vec![PhysicalTask {
                tenant_id,
                vshard_id: vshard,
                plan: PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
                    collection: collection.clone(),
                    payload,
                    format: "json".into(),
                    wal_lsn: None,
                }),
                post_set_op: PostSetOp::None,
            }])
        }

        SqlPlan::VectorSearch {
            collection,
            field,
            query_vector,
            top_k,
            ef_search,
            filters,
        } => {
            let vshard = VShardId::from_collection(collection);
            let filter_bytes = serialize_filters(filters)?;
            Ok(vec![PhysicalTask {
                tenant_id,
                vshard_id: vshard,
                plan: PhysicalPlan::Vector(VectorOp::Search {
                    collection: collection.clone(),
                    query_vector: query_vector.clone().into(),
                    top_k: *top_k,
                    ef_search: *ef_search,
                    filter_bitmap: None,
                    field_name: field.clone(),
                    rls_filters: filter_bytes,
                }),
                post_set_op: PostSetOp::None,
            }])
        }

        SqlPlan::TextSearch {
            collection,
            query,
            top_k,
            fuzzy,
            ..
        } => {
            let vshard = VShardId::from_collection(collection);
            Ok(vec![PhysicalTask {
                tenant_id,
                vshard_id: vshard,
                plan: PhysicalPlan::Text(TextOp::Search {
                    collection: collection.clone(),
                    query: query.clone(),
                    top_k: *top_k,
                    fuzzy: *fuzzy,
                    rls_filters: Vec::new(),
                }),
                post_set_op: PostSetOp::None,
            }])
        }

        SqlPlan::HybridSearch {
            collection,
            query_vector,
            query_text,
            top_k,
            ef_search,
            vector_weight,
            fuzzy,
        } => {
            let vshard = VShardId::from_collection(collection);
            Ok(vec![PhysicalTask {
                tenant_id,
                vshard_id: vshard,
                plan: PhysicalPlan::Text(TextOp::HybridSearch {
                    collection: collection.clone(),
                    query_vector: query_vector.clone().into(),
                    query_text: query_text.clone(),
                    top_k: *top_k,
                    ef_search: *ef_search,
                    fuzzy: *fuzzy,
                    vector_weight: *vector_weight,
                    filter_bitmap: None,
                    rls_filters: Vec::new(),
                }),
                post_set_op: PostSetOp::None,
            }])
        }

        SqlPlan::SpatialScan {
            collection,
            field,
            predicate,
            query_geometry,
            distance_meters,
            attribute_filters,
            limit,
            projection,
        } => {
            let vshard = VShardId::from_collection(collection);
            let attr_bytes = serialize_filters(attribute_filters)?;
            let proj_names = extract_projection_names(projection);
            let sp = match predicate {
                nodedb_sql::types::SpatialPredicate::DWithin => SpatialPredicate::DWithin,
                nodedb_sql::types::SpatialPredicate::Contains => SpatialPredicate::Contains,
                nodedb_sql::types::SpatialPredicate::Intersects => SpatialPredicate::Intersects,
                nodedb_sql::types::SpatialPredicate::Within => SpatialPredicate::Within,
            };
            Ok(vec![PhysicalTask {
                tenant_id,
                vshard_id: vshard,
                plan: PhysicalPlan::Spatial(SpatialOp::Scan {
                    collection: collection.clone(),
                    field: field.clone(),
                    predicate: sp,
                    query_geometry: query_geometry.clone(),
                    distance_meters: *distance_meters,
                    attribute_filters: attr_bytes,
                    limit: *limit,
                    projection: proj_names,
                    rls_filters: Vec::new(),
                }),
                post_set_op: PostSetOp::None,
            }])
        }

        SqlPlan::Union { inputs, distinct } => {
            let mut all_tasks = Vec::new();
            for input in inputs {
                all_tasks.extend(convert_one(input, tenant_id, ctx)?);
            }
            if *distinct {
                for task in &mut all_tasks {
                    task.post_set_op = PostSetOp::UnionDistinct;
                }
            }
            Ok(all_tasks)
        }

        SqlPlan::Intersect { left, right, all } => {
            let mut left_tasks = convert_one(left, tenant_id, ctx)?;
            let mut right_tasks = convert_one(right, tenant_id, ctx)?;
            let op = if *all {
                PostSetOp::IntersectAll
            } else {
                PostSetOp::Intersect
            };
            for task in &mut left_tasks {
                task.post_set_op = op;
            }
            for task in &mut right_tasks {
                task.post_set_op = op;
            }
            left_tasks.extend(right_tasks);
            Ok(left_tasks)
        }

        SqlPlan::Except { left, right, all } => {
            let mut left_tasks = convert_one(left, tenant_id, ctx)?;
            let mut right_tasks = convert_one(right, tenant_id, ctx)?;
            let op = if *all {
                PostSetOp::ExceptAll
            } else {
                PostSetOp::Except
            };
            for task in &mut left_tasks {
                task.post_set_op = op;
            }
            for task in &mut right_tasks {
                task.post_set_op = op;
            }
            left_tasks.extend(right_tasks);
            Ok(left_tasks)
        }

        SqlPlan::InsertSelect { target, source, .. } => {
            // Execute the source query, then insert results into target.
            let source_tasks = convert_one(source, tenant_id, ctx)?;
            // For now, return source tasks — the routing layer reads results
            // and inserts them into the target collection.
            // TODO: implement proper two-phase insert-select execution.
            let _ = target;
            Ok(source_tasks)
        }

        SqlPlan::RecursiveScan {
            collection,
            base_filters,
            recursive_filters,
            max_iterations,
            distinct,
            limit,
        } => {
            let vshard = VShardId::from_collection(collection);
            Ok(vec![PhysicalTask {
                tenant_id,
                vshard_id: vshard,
                plan: PhysicalPlan::Query(QueryOp::RecursiveScan {
                    collection: collection.clone(),
                    base_filters: serialize_filters(base_filters)?,
                    recursive_filters: serialize_filters(recursive_filters)?,
                    max_iterations: *max_iterations,
                    distinct: *distinct,
                    limit: *limit,
                }),
                post_set_op: PostSetOp::None,
            }])
        }

        _ => Err(crate::Error::PlanError {
            detail: format!("unsupported SqlPlan variant: {plan:?}"),
        }),
    }
}

// ── DML conversions ──

fn convert_insert(
    collection: &str,
    engine: &EngineType,
    rows: &[Vec<(String, SqlValue)>],
    column_defaults: &[(String, String)],
    tenant_id: TenantId,
) -> crate::Result<Vec<PhysicalTask>> {
    let vshard = VShardId::from_collection(collection);
    let mut tasks = Vec::new();
    let mut columnar_rows: Vec<&Vec<(String, SqlValue)>> = Vec::new();

    for row in rows {
        let doc_id = row
            .iter()
            .find(|(k, _)| k == "id" || k == "document_id" || k == "key")
            .map(|(_, v)| sql_value_to_string(v))
            .unwrap_or_default();

        match engine {
            EngineType::KeyValue => {
                return Err(crate::Error::PlanError {
                    detail: "KV INSERT must use SqlPlan::KvInsert path".into(),
                });
            }
            EngineType::Columnar | EngineType::Spatial => {
                // Columnar/Spatial INSERT: batch into a single ColumnarOp::Insert.
                // Accumulate rows and emit after the loop.
                columnar_rows.push(row);
            }
            EngineType::DocumentSchemaless | EngineType::DocumentStrict => {
                let value_bytes = row_to_msgpack(row)?;
                tasks.push(PhysicalTask {
                    tenant_id,
                    vshard_id: vshard,
                    plan: PhysicalPlan::Document(DocumentOp::PointPut {
                        collection: collection.into(),
                        document_id: doc_id,
                        value: value_bytes,
                    }),
                    post_set_op: PostSetOp::None,
                });
            }
            other => {
                return Err(crate::Error::PlanError {
                    detail: format!(
                        "INSERT into '{collection}': unexpected engine type {other:?}, expected Document/Columnar/Spatial"
                    ),
                });
            }
        }
    }

    // Emit batched ColumnarOp::Insert for columnar/spatial collections.
    if !columnar_rows.is_empty() {
        let payload = rows_to_msgpack_array(&columnar_rows, column_defaults)?;
        tasks.push(PhysicalTask {
            tenant_id,
            vshard_id: vshard,
            plan: PhysicalPlan::Columnar(ColumnarOp::Insert {
                collection: collection.into(),
                payload,
                format: "msgpack".into(),
            }),
            post_set_op: PostSetOp::None,
        });
    }

    Ok(tasks)
}

fn convert_update(
    collection: &str,
    engine: &EngineType,
    assignments: &[(String, SqlExpr)],
    filters: &[Filter],
    target_keys: &[SqlValue],
    returning: bool,
    tenant_id: TenantId,
) -> crate::Result<Vec<PhysicalTask>> {
    let vshard = VShardId::from_collection(collection);
    let filter_bytes = serialize_filters(filters)?;
    let updates = assignments_to_bytes(assignments)?;

    // KV engine: route to FieldSet for point updates.
    if matches!(engine, EngineType::KeyValue) && !target_keys.is_empty() {
        let mut tasks = Vec::new();
        for key in target_keys {
            let field_updates: Vec<(String, Vec<u8>)> = assignments
                .iter()
                .filter_map(|(field, expr)| {
                    if let SqlExpr::Literal(val) = expr {
                        Some((field.clone(), sql_value_to_msgpack(val)))
                    } else {
                        None
                    }
                })
                .collect();
            tasks.push(PhysicalTask {
                tenant_id,
                vshard_id: vshard,
                plan: PhysicalPlan::Kv(KvOp::FieldSet {
                    collection: collection.into(),
                    key: sql_value_to_bytes(key),
                    updates: field_updates,
                }),
                post_set_op: PostSetOp::None,
            });
        }
        return Ok(tasks);
    }

    if !target_keys.is_empty() {
        // Point updates (document engine).
        let mut tasks = Vec::new();
        for key in target_keys {
            tasks.push(PhysicalTask {
                tenant_id,
                vshard_id: vshard,
                plan: PhysicalPlan::Document(DocumentOp::PointUpdate {
                    collection: collection.into(),
                    document_id: sql_value_to_string(key),
                    updates: updates.clone(),
                    returning,
                }),
                post_set_op: PostSetOp::None,
            });
        }
        Ok(tasks)
    } else {
        Ok(vec![PhysicalTask {
            tenant_id,
            vshard_id: vshard,
            plan: PhysicalPlan::Document(DocumentOp::BulkUpdate {
                collection: collection.into(),
                filters: filter_bytes,
                updates,
                returning,
            }),
            post_set_op: PostSetOp::None,
        }])
    }
}

fn convert_delete(
    collection: &str,
    engine: &EngineType,
    filters: &[Filter],
    target_keys: &[SqlValue],
    tenant_id: TenantId,
) -> crate::Result<Vec<PhysicalTask>> {
    let vshard = VShardId::from_collection(collection);

    // KV engine: route to KvOp::Delete.
    if matches!(engine, EngineType::KeyValue) && !target_keys.is_empty() {
        let keys: Vec<Vec<u8>> = target_keys.iter().map(sql_value_to_bytes).collect();
        return Ok(vec![PhysicalTask {
            tenant_id,
            vshard_id: vshard,
            plan: PhysicalPlan::Kv(KvOp::Delete {
                collection: collection.into(),
                keys,
            }),
            post_set_op: PostSetOp::None,
        }]);
    }

    if !target_keys.is_empty() {
        let mut tasks = Vec::new();
        for key in target_keys {
            tasks.push(PhysicalTask {
                tenant_id,
                vshard_id: vshard,
                plan: PhysicalPlan::Document(DocumentOp::PointDelete {
                    collection: collection.into(),
                    document_id: sql_value_to_string(key),
                }),
                post_set_op: PostSetOp::None,
            });
        }
        Ok(tasks)
    } else {
        let filter_bytes = serialize_filters(filters)?;
        Ok(vec![PhysicalTask {
            tenant_id,
            vshard_id: vshard,
            plan: PhysicalPlan::Document(DocumentOp::BulkDelete {
                collection: collection.into(),
                filters: filter_bytes,
            }),
            post_set_op: PostSetOp::None,
        }])
    }
}

fn convert_aggregate(
    input: &SqlPlan,
    group_by: &[SqlExpr],
    aggregates: &[AggregateExpr],
    having: &[Filter],
    limit: usize,
    tenant_id: TenantId,
) -> crate::Result<Vec<PhysicalTask>> {
    // Check if aggregating over a join.
    if let SqlPlan::Join {
        left,
        right,
        on,
        join_type,
        limit: join_limit,
        ..
    } = input
    {
        let mut left_collection = extract_collection_name(left);
        let mut right_collection = extract_collection_name(right);

        let group_strs = group_by_to_strings(group_by);
        let agg_pairs = aggregates.iter().map(agg_expr_to_pair).collect();

        // RIGHT JOIN → swap sides and convert to LEFT JOIN.
        let mut on_keys = on.to_vec();
        let effective_join_type = if join_type.as_str() == "right" {
            std::mem::swap(&mut left_collection, &mut right_collection);
            on_keys = on_keys.into_iter().map(|(l, r)| (r, l)).collect();
            "left".to_string()
        } else {
            join_type.as_str().to_string()
        };

        let vshard = VShardId::from_collection(&left_collection);

        return Ok(vec![PhysicalTask {
            tenant_id,
            vshard_id: vshard,
            plan: PhysicalPlan::Query(QueryOp::HashJoin {
                left_collection,
                right_collection,
                on: on_keys,
                join_type: effective_join_type,
                limit: *join_limit,
                post_group_by: group_strs,
                post_aggregates: agg_pairs,
                projection: Vec::new(),
                post_filters: Vec::new(),
            }),
            post_set_op: PostSetOp::None,
        }]);
    }

    // Standard aggregate on a single collection.
    let collection = extract_collection_name(input);
    let vshard = VShardId::from_collection(&collection);
    let (filters_ref, engine) = match input {
        SqlPlan::Scan {
            filters, engine, ..
        } => (filters.as_slice(), Some(*engine)),
        _ => (&[][..], None),
    };
    let filter_bytes = serialize_filters(filters_ref)?;
    let having_bytes = serialize_filters(having)?;

    let group_strs = group_by_to_strings(group_by);
    let agg_pairs: Vec<(String, String)> = aggregates.iter().map(agg_expr_to_pair).collect();

    // Timeseries aggregates: route through TimeseriesOp::Scan with time_range + aggregates.
    if engine == Some(EngineType::Timeseries) {
        let time_range = extract_time_range(filters_ref);
        return Ok(vec![PhysicalTask {
            tenant_id,
            vshard_id: vshard,
            plan: PhysicalPlan::Timeseries(TimeseriesOp::Scan {
                collection,
                time_range,
                projection: Vec::new(),
                limit,
                filters: filter_bytes,
                bucket_interval_ms: 0,
                group_by: group_strs,
                aggregates: agg_pairs,
                gap_fill: String::new(),
                rls_filters: Vec::new(),
            }),
            post_set_op: PostSetOp::None,
        }]);
    }

    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        plan: PhysicalPlan::Query(QueryOp::Aggregate {
            collection,
            group_by: group_strs,
            aggregates: agg_pairs,
            filters: filter_bytes,
            having: having_bytes,
            limit,
            sub_group_by: Vec::new(),
            sub_aggregates: Vec::new(),
        }),
        post_set_op: PostSetOp::None,
    }])
}

// ── Helpers ──

fn extract_collection_name(plan: &SqlPlan) -> String {
    match plan {
        SqlPlan::Scan { collection, .. } => collection.clone(),
        SqlPlan::PointGet { collection, .. } => collection.clone(),
        SqlPlan::Join { left, .. } => extract_collection_name(left),
        SqlPlan::Aggregate { input, .. } => extract_collection_name(input),
        _ => String::new(),
    }
}

/// Convert an `AggregateExpr` to the `(op, field)` pair the executor expects.
///
/// The field is extracted from the first argument (e.g. `elapsed_ms` from
/// `AVG(elapsed_ms)`). Wildcard args produce `"*"`. Falls back to `"*"` when
/// there are no arguments (bare `COUNT`).
fn agg_expr_to_pair(a: &AggregateExpr) -> (String, String) {
    let field = a
        .args
        .first()
        .map(|arg| match arg {
            SqlExpr::Column { name, .. } => name.clone(),
            SqlExpr::Wildcard => "*".into(),
            _ => format!("{arg:?}"),
        })
        .unwrap_or_else(|| "*".into());
    (a.function.clone(), field)
}

fn group_by_to_strings(exprs: &[SqlExpr]) -> Vec<String> {
    exprs
        .iter()
        .filter_map(|e| match e {
            SqlExpr::Column { name, .. } => Some(name.clone()),
            _ => None,
        })
        .collect()
}

fn extract_projection_names(proj: &[Projection]) -> Vec<String> {
    proj.iter()
        .filter_map(|p| match p {
            Projection::Column(name) => Some(name.clone()),
            _ => None,
        })
        .collect()
}

fn extract_computed_columns(proj: &[Projection]) -> crate::Result<Vec<u8>> {
    // Collect computed projections (alias + expression string).
    let computed: Vec<(String, String)> = proj
        .iter()
        .filter_map(|p| match p {
            Projection::Computed { expr, alias } => Some((alias.clone(), format!("{expr:?}"))),
            _ => None,
        })
        .collect();
    if computed.is_empty() {
        return Ok(Vec::new());
    }
    zerompk::to_msgpack_vec(&computed).map_err(|e| crate::Error::Internal {
        detail: format!("serialize computed columns: {e}"),
    })
}

fn serialize_window_functions(specs: &[nodedb_sql::types::WindowSpec]) -> crate::Result<Vec<u8>> {
    if specs.is_empty() {
        return Ok(Vec::new());
    }
    let bridge_specs: Vec<crate::bridge::window_func::WindowFuncSpec> = specs
        .iter()
        .map(|s| crate::bridge::window_func::WindowFuncSpec {
            alias: s.alias.clone(),
            func_name: s.function.clone(),
            args: s.args.iter().map(sql_expr_to_bridge_expr).collect(),
            partition_by: s
                .partition_by
                .iter()
                .filter_map(|e| match e {
                    SqlExpr::Column { name, .. } => Some(name.clone()),
                    _ => None,
                })
                .collect(),
            order_by: s
                .order_by
                .iter()
                .filter_map(|k| match &k.expr {
                    SqlExpr::Column { name, .. } => Some((name.clone(), k.ascending)),
                    _ => None,
                })
                .collect(),
            frame: crate::bridge::window_func::WindowFrame::default(),
        })
        .collect();
    zerompk::to_msgpack_vec(&bridge_specs).map_err(|e| crate::Error::Internal {
        detail: format!("serialize window functions: {e}"),
    })
}

/// Convert a `nodedb_sql::types::SqlExpr` (parser AST) to a
/// `nodedb_query::expr::SqlExpr` (bridge evaluation type).
fn sql_expr_to_bridge_expr(expr: &SqlExpr) -> crate::bridge::expr_eval::SqlExpr {
    use crate::bridge::expr_eval::SqlExpr as BExpr;
    match expr {
        SqlExpr::Column { name, .. } => BExpr::Column(name.clone()),
        SqlExpr::Literal(v) => BExpr::Literal(sql_value_to_json(v)),
        SqlExpr::Wildcard => BExpr::Column("*".into()),
        _ => BExpr::Literal(serde_json::Value::Null),
    }
}

fn convert_sort_keys(keys: &[SortKey]) -> Vec<(String, bool)> {
    keys.iter()
        .filter_map(|k| match &k.expr {
            SqlExpr::Column { name, .. } => Some((name.clone(), k.ascending)),
            _ => None,
        })
        .collect()
}

/// Convert SqlPlan filters to ScanFilter msgpack bytes.
fn serialize_filters(filters: &[Filter]) -> crate::Result<Vec<u8>> {
    if filters.is_empty() {
        return Ok(Vec::new());
    }
    let scan_filters: Vec<nodedb_query::scan_filter::ScanFilter> = filters
        .iter()
        .flat_map(|f| filter_to_scan_filters(&f.expr))
        .collect();
    if scan_filters.is_empty() {
        return Ok(Vec::new());
    }
    zerompk::to_msgpack_vec(&scan_filters).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("filter serialization: {e}"),
    })
}

fn filter_to_scan_filters(expr: &FilterExpr) -> Vec<nodedb_query::scan_filter::ScanFilter> {
    use nodedb_query::scan_filter::{FilterOp, ScanFilter};

    match expr {
        FilterExpr::Comparison { field, op, value } => {
            let filter_op = match op {
                nodedb_sql::types::CompareOp::Eq => FilterOp::Eq,
                nodedb_sql::types::CompareOp::Ne => FilterOp::Ne,
                nodedb_sql::types::CompareOp::Gt => FilterOp::Gt,
                nodedb_sql::types::CompareOp::Ge => FilterOp::Gte,
                nodedb_sql::types::CompareOp::Lt => FilterOp::Lt,
                nodedb_sql::types::CompareOp::Le => FilterOp::Lte,
            };
            vec![ScanFilter {
                field: field.clone(),
                op: filter_op,
                value: sql_value_to_nodedb_value(value),
                clauses: Vec::new(),
            }]
        }
        FilterExpr::Like { field, pattern } => {
            vec![ScanFilter {
                field: field.clone(),
                op: FilterOp::Like,
                value: nodedb_types::Value::String(pattern.clone()),
                clauses: Vec::new(),
            }]
        }
        FilterExpr::InList { field, values } => {
            let arr = values.iter().map(sql_value_to_nodedb_value).collect();
            vec![ScanFilter {
                field: field.clone(),
                op: FilterOp::In,
                value: nodedb_types::Value::Array(arr),
                clauses: Vec::new(),
            }]
        }
        FilterExpr::IsNull { field } => {
            vec![ScanFilter {
                field: field.clone(),
                op: FilterOp::IsNull,
                value: nodedb_types::Value::Null,
                clauses: Vec::new(),
            }]
        }
        FilterExpr::IsNotNull { field } => {
            vec![ScanFilter {
                field: field.clone(),
                op: FilterOp::IsNotNull,
                value: nodedb_types::Value::Null,
                clauses: Vec::new(),
            }]
        }
        FilterExpr::And(filters) => filters
            .iter()
            .flat_map(|f| filter_to_scan_filters(&f.expr))
            .collect(),
        FilterExpr::Or(filters) => {
            let clauses: Vec<Vec<ScanFilter>> = filters
                .iter()
                .map(|f| filter_to_scan_filters(&f.expr))
                .collect();
            vec![ScanFilter {
                field: String::new(),
                op: FilterOp::Or,
                value: nodedb_types::Value::Null,
                clauses,
            }]
        }
        FilterExpr::Expr(sql_expr) => {
            // Convert SqlExpr to ScanFilter via pattern matching.
            sql_expr_to_scan_filters(sql_expr)
        }
        _ => vec![ScanFilter {
            field: String::new(),
            op: FilterOp::MatchAll,
            value: nodedb_types::Value::Null,
            clauses: Vec::new(),
        }],
    }
}

/// Convert a raw SqlExpr (from WHERE clause) to ScanFilter list.
fn sql_expr_to_scan_filters(expr: &SqlExpr) -> Vec<nodedb_query::scan_filter::ScanFilter> {
    use nodedb_query::scan_filter::{FilterOp, ScanFilter};

    match expr {
        SqlExpr::BinaryOp {
            left,
            op: nodedb_sql::types::BinaryOp::And,
            right,
        } => {
            let mut filters = sql_expr_to_scan_filters(left);
            filters.extend(sql_expr_to_scan_filters(right));
            filters
        }
        SqlExpr::BinaryOp {
            left,
            op: nodedb_sql::types::BinaryOp::Or,
            right,
        } => {
            let left_filters = sql_expr_to_scan_filters(left);
            let right_filters = sql_expr_to_scan_filters(right);
            vec![ScanFilter {
                field: String::new(),
                op: FilterOp::Or,
                value: nodedb_types::Value::Null,
                clauses: vec![left_filters, right_filters],
            }]
        }
        SqlExpr::BinaryOp { left, op, right } => {
            let field = match left.as_ref() {
                SqlExpr::Column { name, .. } => name.clone(),
                _ => return vec![match_all()],
            };
            let value = match right.as_ref() {
                SqlExpr::Literal(v) => sql_value_to_nodedb_value(v),
                _ => return vec![match_all()],
            };
            let filter_op = match op {
                nodedb_sql::types::BinaryOp::Eq => FilterOp::Eq,
                nodedb_sql::types::BinaryOp::Ne => FilterOp::Ne,
                nodedb_sql::types::BinaryOp::Gt => FilterOp::Gt,
                nodedb_sql::types::BinaryOp::Ge => FilterOp::Gte,
                nodedb_sql::types::BinaryOp::Lt => FilterOp::Lt,
                nodedb_sql::types::BinaryOp::Le => FilterOp::Lte,
                _ => return vec![match_all()],
            };
            vec![ScanFilter {
                field,
                op: filter_op,
                value,
                clauses: Vec::new(),
            }]
        }
        SqlExpr::IsNull { expr, negated } => {
            let field = match expr.as_ref() {
                SqlExpr::Column { name, .. } => name.clone(),
                _ => return vec![match_all()],
            };
            let op = if *negated {
                FilterOp::IsNotNull
            } else {
                FilterOp::IsNull
            };
            vec![ScanFilter {
                field,
                op,
                value: nodedb_types::Value::Null,
                clauses: Vec::new(),
            }]
        }
        SqlExpr::InList {
            expr,
            list,
            negated,
        } => {
            let field = match expr.as_ref() {
                SqlExpr::Column { name, .. } => name.clone(),
                _ => return vec![match_all()],
            };
            let values: Vec<nodedb_types::Value> = list
                .iter()
                .filter_map(|e| match e {
                    SqlExpr::Literal(v) => Some(sql_value_to_nodedb_value(v)),
                    _ => None,
                })
                .collect();
            vec![ScanFilter {
                field,
                op: if *negated {
                    FilterOp::NotIn
                } else {
                    FilterOp::In
                },
                value: nodedb_types::Value::Array(values),
                clauses: Vec::new(),
            }]
        }
        SqlExpr::Like {
            expr,
            pattern,
            negated,
        } => {
            let field = match expr.as_ref() {
                SqlExpr::Column { name, .. } => name.clone(),
                _ => return vec![match_all()],
            };
            let pat = match pattern.as_ref() {
                SqlExpr::Literal(SqlValue::String(s)) => s.clone(),
                _ => return vec![match_all()],
            };
            vec![ScanFilter {
                field,
                op: if *negated {
                    FilterOp::NotLike
                } else {
                    FilterOp::Like
                },
                value: nodedb_types::Value::String(pat),
                clauses: Vec::new(),
            }]
        }
        SqlExpr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let field = match expr.as_ref() {
                SqlExpr::Column { name, .. } => name.clone(),
                _ => return vec![match_all()],
            };
            let low_val = match low.as_ref() {
                SqlExpr::Literal(v) => sql_value_to_nodedb_value(v),
                _ => return vec![match_all()],
            };
            let high_val = match high.as_ref() {
                SqlExpr::Literal(v) => sql_value_to_nodedb_value(v),
                _ => return vec![match_all()],
            };
            if *negated {
                // NOT BETWEEN → lt OR gt (outside the range)
                // clauses is Vec<Vec<ScanFilter>> — each inner vec is an AND-group,
                // outer is OR. So: [[field < low], [field > high]].
                vec![ScanFilter {
                    field: String::new(),
                    op: FilterOp::Or,
                    value: nodedb_types::Value::Null,
                    clauses: vec![
                        vec![ScanFilter {
                            field: field.clone(),
                            op: FilterOp::Lt,
                            value: low_val,
                            clauses: Vec::new(),
                        }],
                        vec![ScanFilter {
                            field,
                            op: FilterOp::Gt,
                            value: high_val,
                            clauses: Vec::new(),
                        }],
                    ],
                }]
            } else {
                // BETWEEN → gte AND lte
                vec![
                    ScanFilter {
                        field: field.clone(),
                        op: FilterOp::Gte,
                        value: low_val,
                        clauses: Vec::new(),
                    },
                    ScanFilter {
                        field,
                        op: FilterOp::Lte,
                        value: high_val,
                        clauses: Vec::new(),
                    },
                ]
            }
        }
        _ => vec![match_all()],
    }
}

fn match_all() -> nodedb_query::scan_filter::ScanFilter {
    nodedb_query::scan_filter::ScanFilter {
        field: String::new(),
        op: nodedb_query::scan_filter::FilterOp::MatchAll,
        value: nodedb_types::Value::Null,
        clauses: Vec::new(),
    }
}

fn sql_value_to_nodedb_value(v: &SqlValue) -> nodedb_types::Value {
    match v {
        SqlValue::Int(i) => nodedb_types::Value::Integer(*i),
        SqlValue::Float(f) => nodedb_types::Value::Float(*f),
        SqlValue::String(s) => nodedb_types::Value::String(s.clone()),
        SqlValue::Bool(b) => nodedb_types::Value::Bool(*b),
        SqlValue::Null => nodedb_types::Value::Null,
        SqlValue::Array(arr) => {
            nodedb_types::Value::Array(arr.iter().map(sql_value_to_nodedb_value).collect())
        }
        SqlValue::Bytes(b) => nodedb_types::Value::Bytes(b.clone()),
    }
}

fn sql_value_to_string(v: &SqlValue) -> String {
    match v {
        SqlValue::String(s) => s.clone(),
        SqlValue::Int(i) => i.to_string(),
        SqlValue::Float(f) => f.to_string(),
        SqlValue::Bool(b) => b.to_string(),
        _ => String::new(),
    }
}

fn rows_to_msgpack_array(
    rows: &[&Vec<(String, SqlValue)>],
    column_defaults: &[(String, String)],
) -> crate::Result<Vec<u8>> {
    let arr: Vec<nodedb_types::Value> = rows
        .iter()
        .map(|row| {
            let mut map = std::collections::HashMap::new();
            for (key, val) in row.iter() {
                map.insert(key.clone(), sql_value_to_nodedb_value(val));
            }
            // Apply column defaults for missing fields.
            for (col_name, default_expr) in column_defaults {
                if !map.contains_key(col_name)
                    && let Some(val) = evaluate_default_expr(default_expr)
                {
                    map.insert(col_name.clone(), val);
                }
            }
            nodedb_types::Value::Object(map)
        })
        .collect();
    let val = nodedb_types::Value::Array(arr);
    nodedb_types::value_to_msgpack(&val).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("columnar row batch: {e}"),
    })
}

/// Evaluate a column DEFAULT expression at insert time.
///
/// Supports ID generation functions and literal values:
///   - `UUID_V7` / `UUIDV7` → time-sortable UUID v7
///   - `UUID_V4` / `UUIDV4` / `UUID` → random UUID v4
///   - `ULID` → time-sortable ULID
///   - `CUID2` → collision-resistant unique ID
///   - `NANOID` → URL-friendly 21-char ID
///   - `NANOID(N)` → URL-friendly N-char ID
///   - Integer/float literals → numeric values
///   - Quoted strings → string values
fn evaluate_default_expr(expr: &str) -> Option<nodedb_types::Value> {
    let upper = expr.trim().to_uppercase();
    match upper.as_str() {
        "UUID_V7" | "UUIDV7" => Some(nodedb_types::Value::String(nodedb_types::id_gen::uuid_v7())),
        "UUID_V4" | "UUIDV4" | "UUID" => {
            Some(nodedb_types::Value::String(nodedb_types::id_gen::uuid_v4()))
        }
        "ULID" => Some(nodedb_types::Value::String(nodedb_types::id_gen::ulid())),
        "CUID2" => Some(nodedb_types::Value::String(nodedb_types::id_gen::cuid2())),
        "NANOID" => Some(nodedb_types::Value::String(nodedb_types::id_gen::nanoid())),
        _ => {
            // NANOID(N) — custom length
            if upper.starts_with("NANOID(") && upper.ends_with(')') {
                let len_str = &upper[7..upper.len() - 1];
                if let Ok(len) = len_str.parse::<usize>() {
                    return Some(nodedb_types::Value::String(
                        nodedb_types::id_gen::nanoid_with_length(len),
                    ));
                }
            }
            // CUID2(N) — custom length
            if upper.starts_with("CUID2(") && upper.ends_with(')') {
                let len_str = &upper[6..upper.len() - 1];
                if let Ok(len) = len_str.parse::<usize>() {
                    return Some(nodedb_types::Value::String(
                        nodedb_types::id_gen::cuid2_with_length(len),
                    ));
                }
            }
            // Numeric literal
            if let Ok(i) = expr.trim().parse::<i64>() {
                return Some(nodedb_types::Value::Integer(i));
            }
            if let Ok(f) = expr.trim().parse::<f64>() {
                return Some(nodedb_types::Value::Float(f));
            }
            // Quoted string literal
            let trimmed = expr.trim();
            if (trimmed.starts_with('\'') && trimmed.ends_with('\''))
                || (trimmed.starts_with('"') && trimmed.ends_with('"'))
            {
                return Some(nodedb_types::Value::String(
                    trimmed[1..trimmed.len() - 1].to_string(),
                ));
            }
            None
        }
    }
}

fn sql_value_to_bytes(v: &SqlValue) -> Vec<u8> {
    match v {
        SqlValue::String(s) => s.as_bytes().to_vec(),
        SqlValue::Bytes(b) => b.clone(),
        SqlValue::Int(i) => i.to_string().as_bytes().to_vec(),
        _ => sql_value_to_string(v).into_bytes(),
    }
}

/// Encode a SQL value as standard msgpack for field-level updates.
fn sql_value_to_msgpack(v: &SqlValue) -> Vec<u8> {
    let mut buf = Vec::with_capacity(16);
    write_msgpack_value(&mut buf, v);
    buf
}

fn row_to_msgpack(row: &[(String, SqlValue)]) -> crate::Result<Vec<u8>> {
    // Write standard msgpack map directly from SqlValue — no JSON or zerompk intermediary.
    let mut buf = Vec::with_capacity(row.len() * 32);
    write_msgpack_map_header(&mut buf, row.len());
    for (key, val) in row {
        write_msgpack_str(&mut buf, key);
        write_msgpack_value(&mut buf, val);
    }
    Ok(buf)
}

fn write_msgpack_map_header(buf: &mut Vec<u8>, len: usize) {
    if len < 16 {
        buf.push(0x80 | len as u8);
    } else if len <= u16::MAX as usize {
        buf.push(0xDE);
        buf.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        buf.push(0xDF);
        buf.extend_from_slice(&(len as u32).to_be_bytes());
    }
}

fn write_msgpack_str(buf: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    let len = bytes.len();
    if len < 32 {
        buf.push(0xA0 | len as u8);
    } else if len <= u8::MAX as usize {
        buf.push(0xD9);
        buf.push(len as u8);
    } else if len <= u16::MAX as usize {
        buf.push(0xDA);
        buf.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        buf.push(0xDB);
        buf.extend_from_slice(&(len as u32).to_be_bytes());
    }
    buf.extend_from_slice(bytes);
}

fn write_msgpack_value(buf: &mut Vec<u8>, val: &SqlValue) {
    match val {
        SqlValue::Null => buf.push(0xC0),
        SqlValue::Bool(true) => buf.push(0xC3),
        SqlValue::Bool(false) => buf.push(0xC2),
        SqlValue::Int(i) => {
            let i = *i;
            if (0..=127).contains(&i) {
                buf.push(i as u8);
            } else if (-32..0).contains(&i) {
                buf.push(i as u8); // negative fixint
            } else if i >= i8::MIN as i64 && i <= i8::MAX as i64 {
                buf.push(0xD0);
                buf.push(i as i8 as u8);
            } else if i >= i16::MIN as i64 && i <= i16::MAX as i64 {
                buf.push(0xD1);
                buf.extend_from_slice(&(i as i16).to_be_bytes());
            } else if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
                buf.push(0xD2);
                buf.extend_from_slice(&(i as i32).to_be_bytes());
            } else {
                buf.push(0xD3);
                buf.extend_from_slice(&i.to_be_bytes());
            }
        }
        SqlValue::Float(f) => {
            buf.push(0xCB);
            buf.extend_from_slice(&f.to_be_bytes());
        }
        SqlValue::String(s) => write_msgpack_str(buf, s),
        SqlValue::Array(arr) => {
            let len = arr.len();
            if len < 16 {
                buf.push(0x90 | len as u8);
            } else if len <= u16::MAX as usize {
                buf.push(0xDC);
                buf.extend_from_slice(&(len as u16).to_be_bytes());
            } else {
                buf.push(0xDD);
                buf.extend_from_slice(&(len as u32).to_be_bytes());
            }
            for item in arr {
                write_msgpack_value(buf, item);
            }
        }
        SqlValue::Bytes(b) => {
            let len = b.len();
            if len <= u8::MAX as usize {
                buf.push(0xC4);
                buf.push(len as u8);
            } else if len <= u16::MAX as usize {
                buf.push(0xC5);
                buf.extend_from_slice(&(len as u16).to_be_bytes());
            } else {
                buf.push(0xC6);
                buf.extend_from_slice(&(len as u32).to_be_bytes());
            }
            buf.extend_from_slice(b);
        }
    }
}

fn assignments_to_bytes(
    assignments: &[(String, SqlExpr)],
) -> crate::Result<Vec<(String, Vec<u8>)>> {
    let mut result = Vec::new();
    for (field, expr) in assignments {
        let bytes = match expr {
            SqlExpr::Literal(v) => sql_value_to_msgpack(v),
            _ => {
                // Non-literal expression — encode as string.
                let mut buf = Vec::new();
                write_msgpack_str(&mut buf, &format!("{expr:?}"));
                buf
            }
        };
        result.push((field.clone(), bytes));
    }
    Ok(result)
}

fn serialize_ingest_rows(rows: &[Vec<(String, SqlValue)>]) -> crate::Result<Vec<u8>> {
    let json_rows: Vec<serde_json::Value> = rows
        .iter()
        .map(|row| {
            let mut map = serde_json::Map::new();
            for (key, val) in row {
                map.insert(key.clone(), sql_value_to_json(val));
            }
            serde_json::Value::Object(map)
        })
        .collect();
    sonic_rs::to_vec(&json_rows).map_err(|e| crate::Error::Serialization {
        format: "json".into(),
        detail: format!("ingest serialization: {e}"),
    })
}

fn sql_value_to_json(v: &SqlValue) -> serde_json::Value {
    match v {
        SqlValue::Int(i) => serde_json::Value::Number((*i).into()),
        SqlValue::Float(f) => serde_json::json!(*f),
        SqlValue::String(s) => serde_json::Value::String(s.clone()),
        SqlValue::Bool(b) => serde_json::Value::Bool(*b),
        SqlValue::Null => serde_json::Value::Null,
        SqlValue::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(sql_value_to_json).collect())
        }
        SqlValue::Bytes(b) => serde_json::Value::String(base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            b,
        )),
    }
}

/// Extract (min_ts_ms, max_ts_ms) time bounds from WHERE filters on timestamp columns.
///
/// Recognizes patterns like `ts >= '2024-01-01' AND ts <= '2024-01-02'` and converts
/// timestamp strings to epoch milliseconds.
fn extract_time_range(filters: &[Filter]) -> (i64, i64) {
    let mut min_ts = i64::MIN;
    let mut max_ts = i64::MAX;

    for filter in filters {
        extract_time_bounds_from_filter(&filter.expr, &mut min_ts, &mut max_ts);
    }

    (min_ts, max_ts)
}

fn extract_time_bounds_from_filter(expr: &FilterExpr, min_ts: &mut i64, max_ts: &mut i64) {
    match expr {
        FilterExpr::Comparison { field, op, value } if is_time_field(field) => {
            if let Some(ms) = sql_value_to_timestamp_ms(value) {
                match op {
                    nodedb_sql::types::CompareOp::Ge | nodedb_sql::types::CompareOp::Gt => {
                        if ms > *min_ts {
                            *min_ts = ms;
                        }
                    }
                    nodedb_sql::types::CompareOp::Le | nodedb_sql::types::CompareOp::Lt => {
                        if ms < *max_ts {
                            *max_ts = ms;
                        }
                    }
                    nodedb_sql::types::CompareOp::Eq => {
                        *min_ts = ms;
                        *max_ts = ms;
                    }
                    _ => {}
                }
            }
        }
        FilterExpr::Between { field, low, high } if is_time_field(field) => {
            if let Some(lo) = sql_value_to_timestamp_ms(low) {
                *min_ts = lo;
            }
            if let Some(hi) = sql_value_to_timestamp_ms(high) {
                *max_ts = hi;
            }
        }
        FilterExpr::And(children) => {
            for child in children {
                extract_time_bounds_from_filter(&child.expr, min_ts, max_ts);
            }
        }
        // Expr-based filters: walk the SqlExpr tree for timestamp comparisons.
        FilterExpr::Expr(sql_expr) => {
            extract_time_bounds_from_expr(sql_expr, min_ts, max_ts);
        }
        _ => {}
    }
}

fn extract_time_bounds_from_expr(expr: &SqlExpr, min_ts: &mut i64, max_ts: &mut i64) {
    let SqlExpr::BinaryOp { left, op, right } = expr else {
        return;
    };
    match op {
        nodedb_sql::types::BinaryOp::And => {
            extract_time_bounds_from_expr(left, min_ts, max_ts);
            extract_time_bounds_from_expr(right, min_ts, max_ts);
        }
        nodedb_sql::types::BinaryOp::Ge | nodedb_sql::types::BinaryOp::Gt => {
            if let Some(field) = expr_column_name(left)
                && is_time_field(&field)
                && let Some(ms) = expr_to_timestamp_ms(right)
                && ms > *min_ts
            {
                *min_ts = ms;
            }
        }
        nodedb_sql::types::BinaryOp::Le | nodedb_sql::types::BinaryOp::Lt => {
            if let Some(field) = expr_column_name(left)
                && is_time_field(&field)
                && let Some(ms) = expr_to_timestamp_ms(right)
                && ms < *max_ts
            {
                *max_ts = ms;
            }
        }
        _ => {}
    }
}

fn is_time_field(name: &str) -> bool {
    let lower = name.to_lowercase();
    lower == "ts"
        || lower == "timestamp"
        || lower == "time"
        || lower == "created_at"
        || lower.ends_with("_at")
        || lower.ends_with("_time")
        || lower.ends_with("_ts")
}

fn expr_column_name(expr: &SqlExpr) -> Option<String> {
    match expr {
        SqlExpr::Column { name, .. } => Some(name.clone()),
        _ => None,
    }
}

fn expr_to_timestamp_ms(expr: &SqlExpr) -> Option<i64> {
    match expr {
        SqlExpr::Literal(val) => sql_value_to_timestamp_ms(val),
        _ => None,
    }
}

fn sql_value_to_timestamp_ms(val: &SqlValue) -> Option<i64> {
    match val {
        SqlValue::Int(ms) => Some(*ms),
        SqlValue::String(s) => parse_timestamp_to_ms(s),
        _ => None,
    }
}

fn parse_timestamp_to_ms(s: &str) -> Option<i64> {
    // Try common timestamp formats.
    // "2024-01-01 00:00:00" → epoch ms
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
        return Some(dt.and_utc().timestamp_millis());
    }
    // "2024-01-01T00:00:00" (ISO 8601)
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
        return Some(dt.and_utc().timestamp_millis());
    }
    // "2024-01-01" (date only)
    if let Ok(d) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return Some(d.and_hms_opt(0, 0, 0)?.and_utc().timestamp_millis());
    }
    // Raw milliseconds as string
    s.parse::<i64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_to_msgpack_produces_standard_format() {
        let row = vec![
            ("count".to_string(), SqlValue::Int(1)),
            ("label".to_string(), SqlValue::String("homepage".into())),
        ];
        let bytes = row_to_msgpack(&row).unwrap();
        // First byte should be a fixmap header (0x82 = map with 2 entries).
        assert_eq!(
            bytes[0], 0x82,
            "expected fixmap(2), got 0x{:02X}. bytes={bytes:?}",
            bytes[0]
        );
        // Should be decodable by json_from_msgpack (standard msgpack parser).
        let json = nodedb_types::json_from_msgpack(&bytes).unwrap();
        let obj = json.as_object().unwrap();
        assert_eq!(obj["count"], 1);
        assert_eq!(obj["label"], "homepage");
    }

    #[test]
    fn write_msgpack_value_int() {
        let mut buf = Vec::new();
        write_msgpack_value(&mut buf, &SqlValue::Int(42));
        // 42 fits in positive fixint (0x00-0x7F).
        assert_eq!(buf, vec![42]);
    }

    #[test]
    fn write_msgpack_value_string() {
        let mut buf = Vec::new();
        write_msgpack_value(&mut buf, &SqlValue::String("hi".into()));
        // fixstr: 0xA0 | 2 = 0xA2, then "hi".
        assert_eq!(buf, vec![0xA2, b'h', b'i']);
    }
}
