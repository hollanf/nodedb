//! Expression evaluation helpers for the statement executor.
//!
//! Evaluates SQL expressions and converts results to Rust types (bool, i64,
//! nodedb_types::Value). Used by ASSIGN, IF/WHILE conditions, FOR bounds,
//! and OUT parameter capture.
//!
//! Uses DataFusion for complex expression evaluation (arithmetic, CASE, etc.).
//! Literal fast paths avoid DataFusion for simple constants.

use crate::control::state::SharedState;
use crate::types::TenantId;

use super::arrow_conv::arrow_scalar_to_value;

/// Evaluate a SQL boolean condition.
///
/// Fast path for constant TRUE/FALSE/1/0/NULL. Falls back to DataFusion
/// evaluation for complex expressions.
pub async fn evaluate_condition(
    state: &SharedState,
    tenant_id: TenantId,
    condition_sql: &str,
) -> crate::Result<bool> {
    if let Some(result) = crate::control::trigger::try_eval_simple_condition(condition_sql) {
        return Ok(result);
    }

    let batches = eval_sql_expr(state, tenant_id, condition_sql, "condition").await?;

    for batch in &batches {
        if batch.num_rows() > 0 {
            let col = batch.column(0);
            if let Some(bool_arr) = col
                .as_any()
                .downcast_ref::<arrow::array::BooleanArray>()
            {
                return Ok(bool_arr.value(0));
            }
            if let Some(int_arr) = col
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
            {
                return Ok(int_arr.value(0) != 0);
            }
        }
    }

    Ok(false)
}

/// Evaluate a SQL expression as an integer.
///
/// Fast path for literal integers. Falls back to DataFusion evaluation.
pub async fn evaluate_int(
    state: &SharedState,
    tenant_id: TenantId,
    sql: &str,
) -> crate::Result<i64> {
    if let Ok(v) = sql.trim().parse::<i64>() {
        return Ok(v);
    }

    let batches = eval_sql_expr(state, tenant_id, sql, "integer").await?;

    for batch in &batches {
        if batch.num_rows() > 0 {
            let col = batch.column(0);
            if let Some(arr) = col
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
            {
                return Ok(arr.value(0));
            }
            if let Some(arr) = col
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
            {
                return Ok(arr.value(0) as i64);
            }
        }
    }

    Err(crate::Error::PlanError {
        detail: format!("could not evaluate '{sql}' as integer"),
    })
}

/// Evaluate a SQL expression and return the result as a `nodedb_types::Value`.
///
/// Fast path for literal values (NULL, TRUE, FALSE, integers, floats, strings).
/// Falls back to DataFusion for complex expressions.
pub async fn evaluate_to_value(
    state: &SharedState,
    tenant_id: TenantId,
    expr: &str,
) -> crate::Result<nodedb_types::Value> {
    let trimmed = expr.trim();
    if trimmed.eq_ignore_ascii_case("NULL") {
        return Ok(nodedb_types::Value::Null);
    }
    if trimmed.eq_ignore_ascii_case("TRUE") {
        return Ok(nodedb_types::Value::Bool(true));
    }
    if trimmed.eq_ignore_ascii_case("FALSE") {
        return Ok(nodedb_types::Value::Bool(false));
    }
    if let Ok(n) = trimmed.parse::<i64>() {
        return Ok(nodedb_types::Value::Integer(n));
    }
    if let Ok(n) = trimmed.parse::<f64>() {
        return Ok(nodedb_types::Value::Float(n));
    }
    // String literal: 'value'
    if trimmed.starts_with('\'') && trimmed.ends_with('\'') && trimmed.len() >= 2 {
        let inner = &trimmed[1..trimmed.len() - 1];
        return Ok(nodedb_types::Value::String(inner.replace("''", "'")));
    }

    // Complex expression: evaluate via DataFusion.
    let batches = eval_sql_expr(state, tenant_id, trimmed, "assign").await?;
    for batch in &batches {
        if batch.num_rows() > 0 {
            let col = batch.column(0);
            return Ok(arrow_scalar_to_value(col, 0));
        }
    }

    Ok(nodedb_types::Value::Null)
}

/// Execute a SQL expression via DataFusion and return the result batches.
///
/// Wraps the expression in `SELECT (<expr>) as __result` and collects.
/// Uses a standalone DataFusion session for scalar evaluation only.
pub async fn eval_sql_expr(
    _state: &SharedState,
    _tenant_id: TenantId,
    expr: &str,
    context: &str,
) -> crate::Result<Vec<arrow::record_batch::RecordBatch>> {
    let session = datafusion::execution::context::SessionContext::new();
    let select_sql = format!("SELECT ({expr}) as __result");
    let df = session
        .sql(&select_sql)
        .await
        .map_err(|e| crate::Error::PlanError {
            detail: format!("{context} eval: {e}"),
        })?;
    df.collect().await.map_err(|e| crate::Error::PlanError {
        detail: format!("{context} eval collect: {e}"),
    })
}
