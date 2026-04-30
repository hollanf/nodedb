//! DDL handlers for continuous aggregates.
//!
//! - `CREATE CONTINUOUS AGGREGATE <name> ON <source> BUCKET '5m' AGGREGATE sum(col) AS alias [, ...] [GROUP BY col, ...] [WITH (...)]`
//! - `DROP CONTINUOUS AGGREGATE <name>`
//! - `SHOW CONTINUOUS AGGREGATES [FOR <source>]`

use std::sync::Arc;
use std::time::Duration;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::MetaOp;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::engine::timeseries::continuous_agg::{
    AggFunction, AggregateExpr, AggregateInfo, ContinuousAggregateDef, RefreshPolicy,
};

use super::super::types::{int8_field, sqlstate_error, text_field};

const KW_CONTINUOUS_AGGREGATE: &str = "CONTINUOUS AGGREGATE ";
const KW_ON: &str = " ON ";
const KW_BUCKET: &str = "BUCKET";
const KW_AGGREGATE: &str = "AGGREGATE ";
const KW_GROUP_BY: &str = "GROUP BY ";
const KW_AS: &str = " AS ";

/// CREATE CONTINUOUS AGGREGATE <name> ON <source> BUCKET '<interval>'
///   AGGREGATE <func>(col) [AS alias], ...
///   [GROUP BY col, ...]
///   [WITH (refresh_policy = 'on_flush', retention = '7d')]
/// Parsed `CREATE CONTINUOUS AGGREGATE` request.
///
/// `aggregate_exprs_raw` is the raw text after AGGREGATE keyword.
/// `with_clause_raw` is the raw inner text of the trailing WITH(...), or empty.
#[derive(Clone, Copy)]
pub struct CreateContinuousAggregateRequest<'a> {
    pub name: &'a str,
    pub source: &'a str,
    pub bucket_raw: &'a str,
    pub aggregate_exprs_raw: &'a str,
    pub group_by: &'a [String],
    pub with_clause_raw: &'a str,
}

/// Handle `CREATE CONTINUOUS AGGREGATE`.
pub async fn create_continuous_aggregate(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    req: &CreateContinuousAggregateRequest<'_>,
) -> PgWireResult<Vec<Response>> {
    let CreateContinuousAggregateRequest {
        name,
        source,
        bucket_raw,
        aggregate_exprs_raw,
        group_by,
        with_clause_raw,
    } = *req;
    // Reconstruct minimal SQL for parse_create_sql reuse.
    // This avoids duplicating the complex AggregateExpr parsing logic.
    let reconstructed = format!(
        "CREATE CONTINUOUS AGGREGATE {name} ON {source} BUCKET '{bucket_raw}' AGGREGATE {aggregate_exprs_raw}"
    );
    let def_from_parts = parse_create_sql(&reconstructed)?;

    // Apply group_by and with_clause_raw overrides.
    let (refresh_policy, retention_period_ms) = if with_clause_raw.is_empty() {
        (
            def_from_parts.refresh_policy,
            def_from_parts.retention_period_ms,
        )
    } else {
        let fake_with_sql = format!("dummy WITH ({with_clause_raw})");
        let (rp, ret) = extract_with_options(&fake_with_sql.to_uppercase(), &fake_with_sql);
        (rp, ret)
    };

    let def = ContinuousAggregateDef {
        name: def_from_parts.name,
        source: def_from_parts.source,
        bucket_interval: def_from_parts.bucket_interval,
        bucket_interval_ms: def_from_parts.bucket_interval_ms,
        group_by: group_by.to_vec(),
        aggregates: def_from_parts.aggregates,
        refresh_policy,
        retention_period_ms,
        stale: false,
    };

    // Validate source collection exists and is timeseries.
    let tenant_id = identity.tenant_id;
    if let Some(catalog) = state.credentials.catalog() {
        match catalog.get_collection(tenant_id.as_u64(), &def.source) {
            Ok(Some(coll)) if coll.collection_type.is_timeseries() => {}
            Ok(Some(_)) => {
                return Err(sqlstate_error(
                    "42809",
                    &format!("'{}' is not a timeseries collection", def.source),
                ));
            }
            _ => {
                return Err(sqlstate_error(
                    "42P01",
                    &format!("collection '{}' does not exist", def.source),
                ));
            }
        }
    }

    // Dispatch registration to Data Plane.
    let plan = PhysicalPlan::Meta(MetaOp::RegisterContinuousAggregate { def: def.clone() });
    super::sync_dispatch::dispatch_async(
        state,
        tenant_id,
        &def.source,
        plan,
        Duration::from_secs(5),
    )
    .await
    .map_err(|e| sqlstate_error("XX000", &format!("dispatch failed: {e}")))?;

    tracing::info!(
        name = def.name,
        source = def.source,
        interval = def.bucket_interval,
        tenant = tenant_id.as_u64(),
        "continuous aggregate created"
    );

    Ok(vec![Response::Execution(pgwire::api::results::Tag::new(
        "CREATE CONTINUOUS AGGREGATE",
    ))])
}

/// DROP CONTINUOUS AGGREGATE <name>
pub async fn drop_continuous_aggregate(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if parts.len() < 4 {
        return Err(sqlstate_error(
            "42601",
            "syntax: DROP CONTINUOUS AGGREGATE <name>",
        ));
    }

    let name = parts[3].to_lowercase();
    let tenant_id = identity.tenant_id;

    let plan = PhysicalPlan::Meta(MetaOp::UnregisterContinuousAggregate { name: name.clone() });

    super::sync_dispatch::dispatch_async(state, tenant_id, &name, plan, Duration::from_secs(5))
        .await
        .map_err(|e| sqlstate_error("XX000", &format!("dispatch failed: {e}")))?;

    tracing::info!(name, "continuous aggregate dropped");

    Ok(vec![Response::Execution(pgwire::api::results::Tag::new(
        "DROP CONTINUOUS AGGREGATE",
    ))])
}

/// SHOW CONTINUOUS AGGREGATES [FOR <source>]
///
/// Dispatches to the Data Plane to query `manager.list_aggregates()` and
/// returns the results as pgwire rows.
pub async fn show_continuous_aggregates(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    let source_filter = if parts.len() >= 5 && parts[3].to_uppercase() == "FOR" {
        Some(parts[4].to_lowercase())
    } else {
        None
    };

    let tenant_id = identity.tenant_id;

    // Dispatch to Data Plane to get aggregate list.
    // Use "__system" as collection for vShard routing (meta operation).
    let plan = PhysicalPlan::Meta(MetaOp::ListContinuousAggregates);
    let payload = super::sync_dispatch::dispatch_async(
        state,
        tenant_id,
        "__system",
        plan,
        Duration::from_secs(5),
    )
    .await
    .unwrap_or_default();

    let infos: Vec<AggregateInfo> = sonic_rs::from_slice(&payload).unwrap_or_default();

    let schema = Arc::new(vec![
        text_field("name"),
        text_field("source"),
        text_field("bucket_interval"),
        text_field("refresh_policy"),
        int8_field("watermark_ts"),
        int8_field("rows_aggregated"),
        int8_field("materialized_buckets"),
        text_field("stale"),
    ]);

    let mut rows = Vec::new();
    for info in &infos {
        // Apply source filter if specified.
        if let Some(ref filter) = source_filter
            && info.source != *filter
        {
            continue;
        }

        let mut encoder = DataRowEncoder::new(schema.clone());
        encoder
            .encode_field(&info.name)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&info.source)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&info.bucket_interval)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&format!("{:?}", info.refresh_policy))
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&info.watermark_ts)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&(info.rows_aggregated as i64))
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&(info.materialized_buckets as i64))
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&info.stale.to_string())
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}

// ── SQL Parsing ──────────────────────────────────────────────────────────

/// Parse CREATE CONTINUOUS AGGREGATE SQL.
///
/// Syntax:
/// ```text
/// CREATE CONTINUOUS AGGREGATE <name> ON <source>
///   BUCKET '<interval>'
///   AGGREGATE <func>(col) [AS alias], ...
///   [GROUP BY col, ...]
///   [WITH (refresh_policy = '...', retention = '...')]
/// ```
fn parse_create_sql(sql: &str) -> PgWireResult<ContinuousAggregateDef> {
    let upper = sql.to_uppercase();

    // Extract name: word after "CONTINUOUS AGGREGATE"
    let ca_pos = upper
        .find(KW_CONTINUOUS_AGGREGATE)
        .ok_or_else(|| sqlstate_error("42601", "expected CONTINUOUS AGGREGATE keyword"))?;
    let after_ca_start = ca_pos + KW_CONTINUOUS_AGGREGATE.len();
    let after_ca = sql[after_ca_start..].trim_start();
    let name = after_ca
        .split_whitespace()
        .next()
        .ok_or_else(|| sqlstate_error("42601", "missing aggregate name"))?
        .to_lowercase();

    // Extract source: word after "ON"
    let on_pos = upper[after_ca_start..]
        .find(KW_ON)
        .ok_or_else(|| sqlstate_error("42601", "expected ON <source> clause"))?;
    let after_on_start = after_ca_start + on_pos + KW_ON.len();
    let after_on = sql[after_on_start..].trim_start();
    let source = after_on
        .split_whitespace()
        .next()
        .ok_or_else(|| sqlstate_error("42601", "missing source collection name"))?
        .to_lowercase();

    // Extract bucket interval: between BUCKET ' and '
    let bucket_interval = extract_quoted_value(&upper, sql, KW_BUCKET)
        .ok_or_else(|| sqlstate_error("42601", "expected BUCKET '<interval>' clause"))?;

    let bucket_interval_ms = nodedb_types::kv_parsing::parse_interval_to_ms(&bucket_interval)
        .map_err(|e| sqlstate_error("42601", &format!("invalid bucket interval: {e}")))?
        as i64;

    // Extract aggregates: between AGGREGATE and GROUP BY / WITH / end
    let aggregates = extract_aggregates(&upper, sql)?;
    if aggregates.is_empty() {
        return Err(sqlstate_error(
            "42601",
            "expected AGGREGATE <func>(col), ... clause",
        ));
    }

    // Extract GROUP BY columns (optional).
    let group_by = extract_group_by(&upper, sql);

    // Extract WITH options (optional).
    let (refresh_policy, retention_period_ms) = extract_with_options(&upper, sql);

    Ok(ContinuousAggregateDef {
        name,
        source,
        bucket_interval,
        bucket_interval_ms,
        group_by,
        aggregates,
        refresh_policy,
        retention_period_ms,
        stale: false,
    })
}

/// Extract a quoted value after a keyword: `KEYWORD 'value'`.
fn extract_quoted_value(upper: &str, sql: &str, keyword: &str) -> Option<String> {
    let pos = upper.find(keyword)?;
    let after = sql[pos + keyword.len()..].trim_start();
    let start = after.find('\'')?;
    let end = after[start + 1..].find('\'')?;
    Some(after[start + 1..start + 1 + end].to_string())
}

/// Extract aggregate expressions from AGGREGATE clause.
///
/// Parses: `AGGREGATE sum(value) AS value_sum, count(*) AS row_count, avg(cpu)`
fn extract_aggregates(upper: &str, sql: &str) -> PgWireResult<Vec<AggregateExpr>> {
    // Find standalone AGGREGATE keyword. Skip past "CONTINUOUS AGGREGATE" by
    // searching after the BUCKET clause (which always precedes AGGREGATE).
    let search_start = upper.find(KW_BUCKET).unwrap_or(0);
    let agg_pos = match upper[search_start..].find(KW_AGGREGATE) {
        Some(p) => search_start + p,
        None => return Ok(Vec::new()),
    };
    let after_agg = &sql[agg_pos + KW_AGGREGATE.len()..];

    // Find end: GROUP BY, WITH, or end of string.
    let end_pos = [KW_GROUP_BY, "WITH (", "WITH("]
        .iter()
        .filter_map(|kw| upper[agg_pos + KW_AGGREGATE.len()..].find(kw))
        .min()
        .unwrap_or(after_agg.len());

    let agg_str = after_agg[..end_pos].trim().trim_end_matches(',');
    let mut exprs = Vec::new();

    for part in agg_str.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let expr = parse_single_aggregate(part)?;
        exprs.push(expr);
    }

    Ok(exprs)
}

/// Parse a single aggregate expression: `func(col) [AS alias]`.
fn parse_single_aggregate(s: &str) -> PgWireResult<AggregateExpr> {
    let upper = s.to_uppercase();

    // Split on AS for alias.
    let (func_part, alias) = if let Some(as_pos) = upper.find(KW_AS) {
        (
            &s[..as_pos],
            Some(s[as_pos + KW_AS.len()..].trim().to_lowercase()),
        )
    } else {
        (s, None)
    };
    let func_part = func_part.trim();

    // Parse func(col).
    let open = func_part.find('(').ok_or_else(|| {
        sqlstate_error("42601", &format!("expected function(column) syntax: {s}"))
    })?;
    let close = func_part
        .rfind(')')
        .ok_or_else(|| sqlstate_error("42601", &format!("missing closing parenthesis: {s}")))?;

    let func_name = func_part[..open].trim().to_lowercase();
    let col_name = func_part[open + 1..close].trim().to_lowercase();

    let function = match func_name.as_str() {
        "sum" => AggFunction::Sum,
        "count" => AggFunction::Count,
        "min" => AggFunction::Min,
        "max" => AggFunction::Max,
        "avg" => AggFunction::Avg,
        "first" => AggFunction::First,
        "last" => AggFunction::Last,
        "count_distinct" => AggFunction::CountDistinct,
        other => {
            return Err(sqlstate_error(
                "42601",
                &format!("unknown aggregate function: {other}"),
            ));
        }
    };

    let output_column = alias.unwrap_or_else(|| {
        if col_name == "*" {
            func_name.clone()
        } else {
            format!("{func_name}_{col_name}")
        }
    });

    Ok(AggregateExpr {
        function,
        source_column: col_name,
        output_column,
    })
}

/// Extract GROUP BY columns.
fn extract_group_by(upper: &str, sql: &str) -> Vec<String> {
    let gb_pos = match upper.find(KW_GROUP_BY) {
        Some(p) => p,
        None => return Vec::new(),
    };
    let after_gb = &sql[gb_pos + KW_GROUP_BY.len()..];

    // Find end: WITH or end of string.
    let end_pos = ["WITH (", "WITH("]
        .iter()
        .filter_map(|kw| upper[gb_pos + KW_GROUP_BY.len()..].find(kw))
        .min()
        .unwrap_or(after_gb.len());

    after_gb[..end_pos]
        .split(',')
        .map(|s| s.trim().to_lowercase())
        .filter(|s| !s.is_empty())
        .collect()
}

/// Extract WITH options: refresh_policy and retention.
fn extract_with_options(upper: &str, sql: &str) -> (RefreshPolicy, u64) {
    let mut refresh = RefreshPolicy::OnFlush;
    let mut retention_ms = 0u64;

    let with_pos = match upper.rfind("WITH") {
        Some(p) => p,
        None => return (refresh, retention_ms),
    };
    let after_with = sql[with_pos + 4..].trim_start();
    let open = match after_with.find('(') {
        Some(p) => p,
        None => return (refresh, retention_ms),
    };
    let close = match after_with.rfind(')') {
        Some(p) => p,
        None => return (refresh, retention_ms),
    };
    if close <= open {
        return (refresh, retention_ms);
    }

    let inner = &after_with[open + 1..close];
    for pair in inner.split(',') {
        let pair = pair.trim();
        if let Some(eq) = pair.find('=') {
            let key = pair[..eq].trim().to_lowercase();
            let val = pair[eq + 1..].trim().trim_matches('\'').trim_matches('"');
            match key.as_str() {
                "refresh_policy" | "refresh" => {
                    refresh = match val.to_lowercase().as_str() {
                        "on_flush" | "onflush" => RefreshPolicy::OnFlush,
                        "on_seal" | "onseal" => RefreshPolicy::OnSeal,
                        "manual" => RefreshPolicy::Manual,
                        other => {
                            if let Ok(ms) = nodedb_types::kv_parsing::parse_interval_to_ms(other) {
                                RefreshPolicy::Periodic(ms)
                            } else {
                                RefreshPolicy::OnFlush
                            }
                        }
                    };
                }
                "retention" | "retention_period" => {
                    if let Ok(ms) = nodedb_types::kv_parsing::parse_interval_to_ms(val) {
                        retention_ms = ms;
                    }
                }
                _ => {}
            }
        }
    }

    (refresh, retention_ms)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_create_basic() {
        let sql = "CREATE CONTINUOUS AGGREGATE metrics_1m ON metrics \
                    BUCKET '1m' \
                    AGGREGATE sum(value) AS value_sum, count(*) AS row_count";
        let def = parse_create_sql(sql).expect("parse_create_sql failed");
        assert_eq!(def.name, "metrics_1m");
        assert_eq!(def.source, "metrics");
        assert_eq!(def.bucket_interval, "1m");
        assert_eq!(def.bucket_interval_ms, 60_000);
        assert_eq!(def.aggregates.len(), 2);
        assert_eq!(def.aggregates[0].output_column, "value_sum");
        assert_eq!(def.aggregates[1].output_column, "row_count");
        assert!(matches!(def.aggregates[0].function, AggFunction::Sum));
        assert!(matches!(def.aggregates[1].function, AggFunction::Count));
    }

    #[test]
    fn parse_create_with_group_by_and_options() {
        let sql = "CREATE CONTINUOUS AGGREGATE cpu_5m ON cpu_metrics \
                    BUCKET '5m' \
                    AGGREGATE avg(cpu) AS cpu_avg, max(cpu) AS cpu_max \
                    GROUP BY host, region \
                    WITH (refresh_policy = 'on_flush', retention = '7d')";
        let def = parse_create_sql(sql).unwrap();
        assert_eq!(def.name, "cpu_5m");
        assert_eq!(def.source, "cpu_metrics");
        assert_eq!(def.bucket_interval_ms, 300_000);
        assert_eq!(def.group_by, vec!["host", "region"]);
        assert_eq!(def.refresh_policy, RefreshPolicy::OnFlush);
        assert_eq!(def.retention_period_ms, 604_800_000); // 7d
    }

    #[test]
    fn parse_create_auto_alias() {
        let sql = "CREATE CONTINUOUS AGGREGATE m1 ON metrics \
                    BUCKET '1h' \
                    AGGREGATE min(value), max(value)";
        let def = parse_create_sql(sql).unwrap();
        assert_eq!(def.aggregates[0].output_column, "min_value");
        assert_eq!(def.aggregates[1].output_column, "max_value");
    }

    #[test]
    fn parse_create_manual_refresh() {
        let sql = "CREATE CONTINUOUS AGGREGATE m1 ON metrics \
                    BUCKET '1d' \
                    AGGREGATE sum(val) \
                    WITH (refresh_policy = 'manual')";
        let def = parse_create_sql(sql).unwrap();
        assert_eq!(def.refresh_policy, RefreshPolicy::Manual);
    }

    #[test]
    fn parse_create_periodic_refresh() {
        let sql = "CREATE CONTINUOUS AGGREGATE m1 ON metrics \
                    BUCKET '1h' \
                    AGGREGATE count(*) \
                    WITH (refresh = '5m')";
        let def = parse_create_sql(sql).unwrap();
        assert_eq!(def.refresh_policy, RefreshPolicy::Periodic(300_000));
    }

    #[test]
    fn parse_missing_bucket_errors() {
        let sql = "CREATE CONTINUOUS AGGREGATE m1 ON metrics AGGREGATE sum(val)";
        assert!(parse_create_sql(sql).is_err());
    }
}
