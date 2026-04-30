//! `CREATE MATERIALIZED VIEW ... STREAMING AS` DDL handler.
//!
//! Syntax:
//! ```sql
//! CREATE MATERIALIZED VIEW <name> STREAMING AS
//!   SELECT <agg_func>(<expr>) AS <alias>, ...
//!   FROM <stream>
//!   [WHERE <filter>]
//!   GROUP BY <col1>, <col2>, ...
//! ```

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::event::streaming_mv::types::{AggDef, AggFunction, StreamingMvDef};

use super::super::super::types::{require_admin, sqlstate_error};

pub fn create_streaming_mv(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    require_admin(identity, "create streaming materialized views")?;

    let parsed = parse_streaming_mv(sql)?;
    let tenant_id = identity.tenant_id.as_u64();

    // Verify source stream exists.
    if state
        .stream_registry
        .get(tenant_id, &parsed.source_stream)
        .is_none()
    {
        return Err(sqlstate_error(
            "42704",
            &format!("change stream '{}' does not exist", parsed.source_stream),
        ));
    }

    // Check for duplicate.
    if state.mv_registry.get_def(tenant_id, &parsed.name).is_some() {
        return Err(sqlstate_error(
            "42710",
            &format!("streaming MV '{}' already exists", parsed.name),
        ));
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| sqlstate_error("XX000", "system clock error"))?
        .as_secs();

    let mv_name = parsed.name.clone();
    let source_stream = parsed.source_stream.clone();

    let def = StreamingMvDef {
        tenant_id,
        name: parsed.name,
        source_stream: parsed.source_stream,
        group_by_columns: parsed.group_by_columns,
        aggregates: parsed.aggregates,
        filter_expr: parsed.filter_expr,
        owner: identity.username.clone(),
        created_at: now,
    };

    let catalog = state
        .credentials
        .catalog()
        .as_ref()
        .ok_or_else(|| sqlstate_error("XX000", "system catalog not available"))?;

    catalog
        .put_streaming_mv(&def)
        .map_err(|e| sqlstate_error("XX000", &format!("catalog write: {e}")))?;

    state.mv_registry.register(def);

    // Backfill: process all events currently in the source stream's buffer
    // to bootstrap the MV with historical data.
    if let Some(mv_state) = state.mv_registry.get_state(tenant_id, &mv_name)
        && let Some(buffer) = state.cdc_router.get_buffer(tenant_id, &source_stream)
    {
        crate::event::streaming_mv::processor::backfill_from_buffer(&mv_state, &buffer);
    }

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("CREATE MATERIALIZED VIEW {mv_name} STREAMING"),
    );

    Ok(vec![Response::Execution(Tag::new(
        "CREATE MATERIALIZED VIEW",
    ))])
}

struct ParsedStreamingMv {
    name: String,
    source_stream: String,
    group_by_columns: Vec<String>,
    aggregates: Vec<AggDef>,
    filter_expr: Option<String>,
}

/// Parse `CREATE MATERIALIZED VIEW <name> STREAMING AS SELECT ... FROM <stream> GROUP BY ...`
fn parse_streaming_mv(sql: &str) -> PgWireResult<ParsedStreamingMv> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_uppercase();

    // Extract MV name: between "CREATE MATERIALIZED VIEW" and "STREAMING"
    let mv_start = "CREATE MATERIALIZED VIEW ";
    if !upper.starts_with(mv_start) {
        return Err(sqlstate_error("42601", "expected CREATE MATERIALIZED VIEW"));
    }

    let streaming_pos = upper
        .find(" STREAMING ")
        .ok_or_else(|| sqlstate_error("42601", "expected STREAMING keyword"))?;
    let name = trimmed[mv_start.len()..streaming_pos].trim().to_lowercase();

    // Extract the SELECT query after "STREAMING AS"
    let as_pos = upper
        .find(" AS ")
        .ok_or_else(|| sqlstate_error("42601", "expected AS keyword after STREAMING"))?;
    let query = trimmed[as_pos + 4..].trim();
    let query_upper = query.to_uppercase();

    // Extract FROM <stream>
    let from_pos = query_upper
        .find(" FROM ")
        .ok_or_else(|| sqlstate_error("42601", "expected FROM clause"))?;
    let after_from = query[from_pos + 6..].trim();
    let source_stream = after_from
        .split_whitespace()
        .next()
        .unwrap_or("")
        .to_lowercase();

    // Extract GROUP BY columns
    let group_by_columns = if let Some(gb_pos) = query_upper.find(" GROUP BY ") {
        let gb_str = query[gb_pos + 10..].trim();
        gb_str
            .split(',')
            .map(|s| s.trim().to_lowercase())
            .filter(|s| !s.is_empty())
            .collect()
    } else {
        Vec::new()
    };

    // Extract WHERE filter (between FROM <stream> and GROUP BY)
    let filter_expr = if let Some(where_pos) = query_upper.find(" WHERE ") {
        let end = query_upper.find(" GROUP BY ").unwrap_or(query.len());
        if where_pos < end {
            Some(query[where_pos + 7..end].trim().to_string())
        } else {
            None
        }
    } else {
        None
    };

    // Extract SELECT list (between SELECT and FROM)
    let select_start = if query_upper.starts_with("SELECT ") {
        7
    } else {
        return Err(sqlstate_error("42601", "expected SELECT"));
    };
    let select_list = query[select_start..from_pos].trim();

    // Parse aggregates from SELECT list
    let aggregates = parse_select_aggregates(select_list)?;

    if aggregates.is_empty() {
        return Err(sqlstate_error(
            "42601",
            "streaming MV requires at least one aggregate function (COUNT, SUM, MIN, MAX, AVG)",
        ));
    }

    Ok(ParsedStreamingMv {
        name,
        source_stream,
        group_by_columns,
        aggregates,
        filter_expr,
    })
}

/// Parse aggregate functions from a SELECT list.
///
/// Supports: `count(*) AS cnt`, `sum(field) AS total`, `min(field)`, etc.
fn parse_select_aggregates(select_list: &str) -> PgWireResult<Vec<AggDef>> {
    let mut aggregates = Vec::new();

    for item in select_list.split(',') {
        let item = item.trim();
        if item.is_empty() || item == "*" {
            continue;
        }

        // Split on AS to get alias.
        let (expr_part, alias) = if let Some(as_pos) = item.to_uppercase().rfind(" AS ") {
            (
                item[..as_pos].trim(),
                item[as_pos + 4..].trim().to_lowercase(),
            )
        } else {
            (item, item.to_lowercase().replace(['(', ')', '*', ' '], "_"))
        };

        // Parse aggregate function: func(args)
        let expr_upper = expr_part.to_uppercase();
        let func = if expr_upper.starts_with("COUNT(") {
            Some(AggFunction::Count)
        } else if expr_upper.starts_with("SUM(") {
            Some(AggFunction::Sum)
        } else if expr_upper.starts_with("MIN(") {
            Some(AggFunction::Min)
        } else if expr_upper.starts_with("MAX(") {
            Some(AggFunction::Max)
        } else if expr_upper.starts_with("AVG(") {
            Some(AggFunction::Avg)
        } else {
            None // Not an aggregate — skip (could be a GROUP BY column reference).
        };

        if let Some(function) = func {
            // Extract the input expression between parentheses.
            let inner = expr_part
                .split_once('(')
                .and_then(|(_, rest)| rest.rsplit_once(')'))
                .map(|(inner, _)| inner.trim().to_string())
                .unwrap_or_default();

            let input_expr = if inner == "*" {
                String::new() // COUNT(*)
            } else {
                inner
            };

            aggregates.push(AggDef {
                output_name: alias,
                function,
                input_expr,
            });
        }
    }

    Ok(aggregates)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_basic_streaming_mv() {
        let sql = "CREATE MATERIALIZED VIEW order_stats STREAMING AS \
                    SELECT event_type, count(*) AS cnt \
                    FROM orders_stream \
                    GROUP BY event_type";
        let parsed = parse_streaming_mv(sql).unwrap();
        assert_eq!(parsed.name, "order_stats");
        assert_eq!(parsed.source_stream, "orders_stream");
        assert_eq!(parsed.group_by_columns, vec!["event_type"]);
        assert_eq!(parsed.aggregates.len(), 1);
        assert_eq!(parsed.aggregates[0].function, AggFunction::Count);
        assert_eq!(parsed.aggregates[0].output_name, "cnt");
    }

    #[test]
    fn parse_multi_aggregate() {
        let sql = "CREATE MATERIALIZED VIEW stats STREAMING AS \
                    SELECT count(*) AS cnt, sum(total) AS revenue \
                    FROM orders_stream \
                    GROUP BY event_type";
        let parsed = parse_streaming_mv(sql).unwrap();
        assert_eq!(parsed.aggregates.len(), 2);
        assert_eq!(parsed.aggregates[1].function, AggFunction::Sum);
        assert_eq!(parsed.aggregates[1].input_expr, "total");
    }

    #[test]
    fn parse_with_where() {
        let sql = "CREATE MATERIALIZED VIEW insert_stats STREAMING AS \
                    SELECT count(*) AS cnt \
                    FROM orders_stream \
                    WHERE event_type = 'INSERT' \
                    GROUP BY collection";
        let parsed = parse_streaming_mv(sql).unwrap();
        assert!(parsed.filter_expr.is_some());
        assert!(parsed.filter_expr.unwrap().contains("event_type"));
    }
}
