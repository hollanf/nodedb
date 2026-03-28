//! Graph DSL commands: GRAPH INSERT EDGE, GRAPH DELETE EDGE,
//! GRAPH TRAVERSE, GRAPH NEIGHBORS, GRAPH PATH.

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response, Tag};
use pgwire::error::PgWireResult;

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::GraphOp;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::dispatch_utils;
use crate::control::state::SharedState;
use crate::data::executor::response_codec;
use crate::types::VShardId;

use super::super::types::{sqlstate_error, text_field};

/// GRAPH INSERT EDGE FROM '<src>' TO '<dst>' TYPE '<label>' [ON <collection>] [PROPERTIES '<json>']
pub async fn insert_edge(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    _parts: &[&str],
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    // Parse: GRAPH INSERT EDGE FROM 'src' TO 'dst' TYPE 'label' ...
    let upper = sql.to_uppercase();
    let src = extract_quoted_after(&upper, sql, "FROM")
        .ok_or_else(|| sqlstate_error("42601", "missing FROM '<node_id>'"))?;
    let dst = extract_quoted_after(&upper, sql, "TO")
        .ok_or_else(|| sqlstate_error("42601", "missing TO '<node_id>'"))?;
    let label = extract_quoted_after(&upper, sql, "TYPE")
        .ok_or_else(|| sqlstate_error("42601", "missing TYPE '<edge_label>'"))?;
    let properties = extract_quoted_after(&upper, sql, "PROPERTIES").unwrap_or_default();

    let tenant_id = identity.tenant_id;
    let vshard_id = VShardId::from_key(src.as_bytes());

    let plan = PhysicalPlan::Graph(GraphOp::EdgePut {
        src_id: src,
        label: label.clone(),
        dst_id: dst,
        properties: properties.into_bytes(),
    });

    dispatch_utils::wal_append_if_write(&state.wal, tenant_id, vshard_id, &plan)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    match dispatch_utils::dispatch_to_data_plane(state, tenant_id, vshard_id, plan, 0).await {
        Ok(_) => Ok(vec![Response::Execution(Tag::new("INSERT EDGE"))]),
        Err(e) => Err(sqlstate_error("XX000", &e.to_string())),
    }
}

/// GRAPH DELETE EDGE FROM '<src>' TO '<dst>' TYPE '<label>'
pub async fn delete_edge(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    _parts: &[&str],
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let upper = sql.to_uppercase();
    let src = extract_quoted_after(&upper, sql, "FROM")
        .ok_or_else(|| sqlstate_error("42601", "missing FROM '<node_id>'"))?;
    let dst = extract_quoted_after(&upper, sql, "TO")
        .ok_or_else(|| sqlstate_error("42601", "missing TO '<node_id>'"))?;
    let label = extract_quoted_after(&upper, sql, "TYPE")
        .ok_or_else(|| sqlstate_error("42601", "missing TYPE '<edge_label>'"))?;

    let tenant_id = identity.tenant_id;
    let vshard_id = VShardId::from_key(src.as_bytes());

    let plan = PhysicalPlan::Graph(GraphOp::EdgeDelete {
        src_id: src,
        label,
        dst_id: dst,
    });

    dispatch_utils::wal_append_if_write(&state.wal, tenant_id, vshard_id, &plan)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    match dispatch_utils::dispatch_to_data_plane(state, tenant_id, vshard_id, plan, 0).await {
        Ok(_) => Ok(vec![Response::Execution(Tag::new("DELETE EDGE"))]),
        Err(e) => Err(sqlstate_error("XX000", &e.to_string())),
    }
}

/// GRAPH TRAVERSE FROM '<node_id>' [DEPTH <n>] [LABEL '<label>'] [DIRECTION in|out|both]
pub async fn traverse(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    _parts: &[&str],
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let upper = sql.to_uppercase();
    let start = extract_quoted_after(&upper, sql, "FROM")
        .ok_or_else(|| sqlstate_error("42601", "missing FROM '<node_id>'"))?;
    let depth = extract_number_after(&upper, "DEPTH").unwrap_or(2);
    let label = extract_quoted_after(&upper, sql, "LABEL");
    let direction = extract_word_after(&upper, "DIRECTION").unwrap_or("out".into());

    let dir = match direction.as_str() {
        "IN" | "in" => crate::engine::graph::edge_store::Direction::In,
        "BOTH" | "both" => crate::engine::graph::edge_store::Direction::Both,
        _ => crate::engine::graph::edge_store::Direction::Out,
    };

    let tenant_id = identity.tenant_id;

    // Cross-core BFS: Control Plane orchestrates hop-by-hop traversal
    // across all cores, collecting neighbors at each depth level.
    match dispatch_utils::cross_core_bfs(state, tenant_id, vec![start], label, dir, depth).await {
        Ok(resp) => payload_to_query_response(&resp.payload),
        Err(e) => Err(sqlstate_error("XX000", &e.to_string())),
    }
}

/// GRAPH NEIGHBORS OF '<node_id>' [LABEL '<label>'] [DIRECTION in|out|both]
pub async fn neighbors(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    _parts: &[&str],
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let upper = sql.to_uppercase();
    let node = extract_quoted_after(&upper, sql, "OF")
        .ok_or_else(|| sqlstate_error("42601", "missing OF '<node_id>'"))?;
    let label = extract_quoted_after(&upper, sql, "LABEL");
    let direction = extract_word_after(&upper, "DIRECTION").unwrap_or("out".into());

    let dir = match direction.as_str() {
        "IN" | "in" => crate::engine::graph::edge_store::Direction::In,
        "BOTH" | "both" => crate::engine::graph::edge_store::Direction::Both,
        _ => crate::engine::graph::edge_store::Direction::Out,
    };

    let tenant_id = identity.tenant_id;

    let plan = PhysicalPlan::Graph(GraphOp::Neighbors {
        node_id: node,
        edge_label: label,
        direction: dir,
        rls_filters: Vec::new(),
    });

    // Broadcast to all cores — edges may be distributed across cores.
    match dispatch_utils::broadcast_to_all_cores(state, tenant_id, plan, 0).await {
        Ok(resp) => payload_to_query_response(&resp.payload),
        Err(e) => Err(sqlstate_error("XX000", &e.to_string())),
    }
}

/// GRAPH PATH FROM '<src>' TO '<dst>' [MAX_DEPTH <n>] [LABEL '<label>']
pub async fn shortest_path(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    _parts: &[&str],
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let upper = sql.to_uppercase();
    let src = extract_quoted_after(&upper, sql, "FROM")
        .ok_or_else(|| sqlstate_error("42601", "missing FROM '<node_id>'"))?;
    let dst = extract_quoted_after(&upper, sql, "TO")
        .ok_or_else(|| sqlstate_error("42601", "missing TO '<node_id>'"))?;
    let max_depth = extract_number_after(&upper, "MAX_DEPTH").unwrap_or(10);
    let label = extract_quoted_after(&upper, sql, "LABEL");

    let tenant_id = identity.tenant_id;

    // Cross-core BFS from src. If dst is discovered, we found a path.
    // Use cross_core_bfs which traverses across all cores.
    let dir = crate::engine::graph::edge_store::Direction::Out;
    match dispatch_utils::cross_core_bfs(state, tenant_id, vec![src.clone()], label, dir, max_depth)
        .await
    {
        Ok(resp) => {
            // Check if dst was discovered in the BFS result.
            let json_text =
                crate::data::executor::response_codec::decode_payload_to_json(&resp.payload);
            if let Ok(nodes) = serde_json::from_str::<Vec<String>>(&json_text)
                && nodes.contains(&dst)
            {
                // Path exists — return the discovered nodes as the path.
                let payload = serde_json::to_vec(&nodes)
                    .map_err(|e| sqlstate_error("XX000", &format!("serialize path: {e}")))?;
                let path_resp = crate::bridge::envelope::Response {
                    request_id: crate::types::RequestId::new(0),
                    status: crate::bridge::envelope::Status::Ok,
                    attempt: 1,
                    partial: false,
                    payload: crate::bridge::envelope::Payload::from_vec(payload),
                    watermark_lsn: crate::types::Lsn::ZERO,
                    error_code: None,
                };
                return payload_to_query_response(&path_resp.payload);
            }
            // dst not found — empty result.
            let empty = crate::bridge::envelope::Payload::from_vec(b"[]".to_vec());
            payload_to_query_response(&empty)
        }
        Err(e) => Err(sqlstate_error("XX000", &e.to_string())),
    }
}

/// GRAPH ALGO <algorithm> ON <collection> [params...]
///
/// Supported algorithms and their parameters:
/// ```text
/// GRAPH ALGO PAGERANK   ON <collection> [DAMPING 0.85] [ITERATIONS 20] [TOLERANCE 1e-7]
/// GRAPH ALGO WCC        ON <collection>
/// GRAPH ALGO COMMUNITY  ON <collection> [ITERATIONS 10]
/// GRAPH ALGO LCC        ON <collection>
/// GRAPH ALGO SSSP       ON <collection> FROM '<source_node>'
/// ```
///
/// Returns a multi-row result set. Results are broadcast to all Data Plane
/// cores; each core runs the algorithm on its local CSR and returns results.
pub async fn algo(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    _parts: &[&str],
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let upper = sql.to_uppercase();

    // Parse algorithm name: third word after "GRAPH ALGO".
    let algo_name = upper
        .strip_prefix("GRAPH ALGO ")
        .and_then(|rest| rest.split_whitespace().next())
        .ok_or_else(|| sqlstate_error("42601", "GRAPH ALGO requires an algorithm name"))?;

    let algorithm = match algo_name {
        "PAGERANK" => crate::engine::graph::algo::GraphAlgorithm::PageRank,
        "WCC" => crate::engine::graph::algo::GraphAlgorithm::Wcc,
        "COMMUNITY" => crate::engine::graph::algo::GraphAlgorithm::LabelPropagation,
        "LCC" => crate::engine::graph::algo::GraphAlgorithm::Lcc,
        "SSSP" => crate::engine::graph::algo::GraphAlgorithm::Sssp,
        "BETWEENNESS" => crate::engine::graph::algo::GraphAlgorithm::Betweenness,
        "CLOSENESS" => crate::engine::graph::algo::GraphAlgorithm::Closeness,
        "HARMONIC" => crate::engine::graph::algo::GraphAlgorithm::Harmonic,
        "DEGREE" => crate::engine::graph::algo::GraphAlgorithm::Degree,
        "LOUVAIN" => crate::engine::graph::algo::GraphAlgorithm::Louvain,
        "TRIANGLES" => crate::engine::graph::algo::GraphAlgorithm::Triangles,
        "DIAMETER" => crate::engine::graph::algo::GraphAlgorithm::Diameter,
        "KCORE" => crate::engine::graph::algo::GraphAlgorithm::KCore,
        _ => {
            return Err(sqlstate_error(
                "42601",
                &format!("unknown graph algorithm '{algo_name}'"),
            ));
        }
    };

    // Parse collection: word after "ON".
    let collection = extract_word_after(&upper, " ON ")
        .map(|s| s.to_lowercase())
        .ok_or_else(|| sqlstate_error("42601", "GRAPH ALGO requires ON <collection>"))?;

    // Parse optional parameters.
    let params = crate::engine::graph::algo::AlgoParams {
        collection: collection.clone(),
        damping: extract_float_after(&upper, "DAMPING"),
        max_iterations: extract_number_after(&upper, "ITERATIONS"),
        tolerance: extract_float_after(&upper, "TOLERANCE"),
        source_node: extract_quoted_after(&upper, sql, "FROM"),
        sample_size: extract_number_after(&upper, "SAMPLE"),
        direction: extract_word_after(&upper, "DIRECTION"),
        resolution: extract_float_after(&upper, "RESOLUTION"),
        mode: extract_word_after(&upper, "MODE"),
    };

    let tenant_id = identity.tenant_id;

    let plan = PhysicalPlan::Graph(GraphOp::Algo { algorithm, params });

    // Broadcast to all cores — the algorithm runs on each core's local CSR.
    // Results from all cores are merged into a single JSON array response.
    match dispatch_utils::broadcast_to_all_cores(state, tenant_id, plan, 0).await {
        Ok(resp) => algo_payload_to_query_response(&resp.payload, algorithm),
        Err(e) => Err(sqlstate_error("XX000", &e.to_string())),
    }
}

/// Convert a graph algorithm result payload to a typed pgwire QueryResponse.
///
/// Uses the algorithm's result schema to create properly-named columns
/// (e.g., `node_id`, `rank` for PageRank) instead of a single `result` column.
fn algo_payload_to_query_response(
    payload: &crate::bridge::envelope::Payload,
    algorithm: crate::engine::graph::algo::GraphAlgorithm,
) -> PgWireResult<Vec<Response>> {
    use crate::engine::graph::algo::params::AlgoColumnType;

    // Build schema from algorithm definition (consistent for empty and non-empty results).
    let result_schema = algorithm.result_schema();
    let schema = Arc::new(
        result_schema
            .iter()
            .map(|&(name, _)| text_field(name))
            .collect::<Vec<_>>(),
    );

    if payload.is_empty() {
        return Ok(vec![Response::Query(QueryResponse::new(
            schema,
            stream::empty(),
        ))]);
    }

    let json_text = response_codec::decode_payload_to_json(payload);

    // Parse the JSON array of result objects.
    let rows: Vec<serde_json::Value> = serde_json::from_str(&json_text)
        .map_err(|e| sqlstate_error("XX000", &format!("invalid algorithm result JSON: {e}")))?;

    let mut pgwire_rows = Vec::with_capacity(rows.len());
    for row in &rows {
        let mut encoder = DataRowEncoder::new(schema.clone());
        for &(col_name, col_type) in result_schema {
            let field = row.get(col_name).unwrap_or(&serde_json::Value::Null);
            let val_str = match col_type {
                AlgoColumnType::Text => field.as_str().unwrap_or("").to_string(),
                AlgoColumnType::Float64 => match field.as_f64() {
                    Some(v) => format!("{v}"),
                    None => "Infinity".to_string(),
                },
                AlgoColumnType::Int64 => field.as_i64().map_or("0".into(), |v| v.to_string()),
            };
            encoder
                .encode_field(&val_str)
                .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        }
        pgwire_rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(pgwire_rows),
    ))])
}

// ─── Helpers ───────────────────────────────────────────────────────

/// Convert a Data Plane payload to a pgwire QueryResponse.
fn payload_to_query_response(
    payload: &crate::bridge::envelope::Payload,
) -> PgWireResult<Vec<Response>> {
    if payload.is_empty() {
        let schema = Arc::new(vec![text_field("result")]);
        return Ok(vec![Response::Query(QueryResponse::new(
            schema,
            stream::empty(),
        ))]);
    }

    let json_text = response_codec::decode_payload_to_json(payload);
    let schema = Arc::new(vec![text_field("result")]);
    let mut encoder = DataRowEncoder::new(schema.clone());
    encoder
        .encode_field(&json_text)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    let row = encoder.take_row();
    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(vec![Ok(row)]),
    ))])
}

/// Extract a single-quoted value after a keyword.
/// e.g., `FROM 'alice'` → `Some("alice")`
fn extract_quoted_after(upper: &str, original: &str, keyword: &str) -> Option<String> {
    let kw_pos = upper.find(keyword)?;
    let after = &original[kw_pos + keyword.len()..];
    let trimmed = after.trim_start();
    if let Some(content) = trimmed.strip_prefix('\'') {
        let end = content.find('\'')?;
        Some(content[..end].to_string())
    } else {
        // Unquoted word.
        let word = trimmed.split_whitespace().next()?;
        Some(word.to_string())
    }
}

/// Extract a number after a keyword. e.g., `DEPTH 3` → `Some(3)`
fn extract_number_after(upper: &str, keyword: &str) -> Option<usize> {
    let kw_pos = upper.find(keyword)?;
    let after = &upper[kw_pos + keyword.len()..];
    let word = after.split_whitespace().next()?;
    word.parse().ok()
}

/// Extract an unquoted word after a keyword. e.g., `DIRECTION out` → `Some("out")`
fn extract_word_after(upper: &str, keyword: &str) -> Option<String> {
    let kw_pos = upper.find(keyword)?;
    let after = &upper[kw_pos + keyword.len()..];
    let word = after.split_whitespace().next()?;
    Some(word.to_string())
}

/// Extract a float after a keyword. e.g., `DAMPING 0.85` → `Some(0.85)`
/// Handles scientific notation: `TOLERANCE 1E-7` → `Some(1e-7)`.
fn extract_float_after(upper: &str, keyword: &str) -> Option<f64> {
    let kw_pos = upper.find(keyword)?;
    let after = &upper[kw_pos + keyword.len()..];
    let word = after.split_whitespace().next()?;
    word.parse().ok()
}
