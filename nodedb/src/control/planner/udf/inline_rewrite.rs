//! User-defined function inlining and permission checking.
//!
//! Provides two operations on logical plans:
//! 1. **Extract** user UDF references (for EXECUTE permission checking).
//! 2. **Inline** user UDF calls (replace with body expressions).
//!
//! The pipeline is: parse SQL → extract UDF refs → check EXECUTE → inline → optimize.
//! This separation ensures EXECUTE permission is verified before inlining erases
//! the function name from the plan.
//!
//! **Volatility handling:** The `FunctionVolatility` classification is enforced
//! by DataFusion's optimizer. When the UDF is registered as a `ScalarUDF`, its
//! `Volatility` is set from the stored definition. DataFusion uses this to decide
//! constant-folding eligibility. After inlining replaces the UDF call with the body
//! expression, the constituent functions carry their own volatility.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::common::Result as DfResult;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::logical_expr::{Expr, LogicalPlan};

/// Stores user UDF definitions for inlining and permission checking.
///
/// Created per-session from the tenant's function catalog.
#[derive(Debug)]
pub struct InlineUserFunctions {
    /// function_name -> (param_names, body_sql).
    functions: HashMap<String, (Vec<String>, String)>,
}

impl Default for InlineUserFunctions {
    fn default() -> Self {
        Self::new()
    }
}

impl InlineUserFunctions {
    pub fn new() -> Self {
        Self {
            functions: HashMap::new(),
        }
    }

    /// Register a function for inlining.
    pub fn add(&mut self, name: String, param_names: Vec<String>, body_sql: String) {
        self.functions.insert(name, (param_names, body_sql));
    }

    /// Returns true if there are no user functions registered.
    pub fn is_empty(&self) -> bool {
        self.functions.is_empty()
    }

    /// Check if a function name is a user-defined function.
    pub fn is_user_function(&self, name: &str) -> bool {
        self.functions.contains_key(name)
    }

    /// Extract all user UDF names referenced in a logical plan.
    ///
    /// Walks the plan's expression tree looking for `ScalarFunction` calls
    /// whose names match registered user UDFs. Returns the set of function
    /// names found. This is called BEFORE inlining to check EXECUTE permissions.
    pub fn extract_udf_references(&self, plan: &LogicalPlan) -> HashSet<String> {
        let mut refs = HashSet::new();
        if self.functions.is_empty() {
            return refs;
        }
        extract_udf_refs_from_plan(plan, &self.functions, &mut refs);
        refs
    }

    /// Inline all user UDF calls in the logical plan.
    ///
    /// Replaces `ScalarFunction` calls matching registered user UDFs with their
    /// body expressions, substituting parameter references with actual arguments.
    /// Must be called AFTER EXECUTE permission check.
    pub fn inline(&self, plan: LogicalPlan) -> DfResult<LogicalPlan> {
        if self.functions.is_empty() {
            return Ok(plan);
        }

        // Build a minimal SessionContext for parsing body expressions.
        let ctx = Arc::new({
            let config = datafusion::prelude::SessionConfig::new()
                .with_information_schema(false)
                .with_default_catalog_and_schema("nodedb", "public");
            let session = datafusion::execution::context::SessionContext::new_with_config(config);
            crate::control::planner::context::register_udfs_on(&session);
            session
        });

        let functions = &self.functions;
        let ctx_ref = &ctx;

        plan.transform_up(|node| node.map_expressions(|expr| inline_expr(expr, functions, ctx_ref)))
            .map(|t| t.data)
    }
}

// ─── UDF reference extraction ────────────────────────────────────────────────

/// Recursively walk a logical plan and collect user UDF names from expressions.
fn extract_udf_refs_from_plan(
    plan: &LogicalPlan,
    functions: &HashMap<String, (Vec<String>, String)>,
    refs: &mut HashSet<String>,
) {
    // Check all expressions in this plan node.
    for expr in plan.expressions() {
        extract_udf_refs_from_expr(&expr, functions, refs);
    }
    // Recurse into child plans.
    for input in plan.inputs() {
        extract_udf_refs_from_plan(input, functions, refs);
    }
}

/// Recursively walk an expression and collect user UDF names.
fn extract_udf_refs_from_expr(
    expr: &Expr,
    functions: &HashMap<String, (Vec<String>, String)>,
    refs: &mut HashSet<String>,
) {
    if let Expr::ScalarFunction(sf) = expr {
        let name = sf.name().to_lowercase();
        if functions.contains_key(&name) {
            refs.insert(name);
        }
    }
    // Use DataFusion's Expr children traversal — handles all expression variants.
    let _ = expr.apply(|e| {
        if let Expr::ScalarFunction(sf) = e {
            let name = sf.name().to_lowercase();
            if functions.contains_key(&name) {
                refs.insert(name);
            }
        }
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    });
}

// ─── Inlining ────────────────────────────────────────────────────────────────

type SessionContextRef = Arc<datafusion::execution::context::SessionContext>;

/// Parse a SQL expression body, substituting parameter references with actual args.
fn parse_body_expr(
    body_sql: &str,
    param_names: &[String],
    args: &[Expr],
    ctx: &SessionContextRef,
) -> DfResult<Expr> {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::DFSchema;

    let fields: Vec<Field> = param_names
        .iter()
        .map(|name| Field::new(name, DataType::Utf8, true))
        .collect();
    let schema = Schema::new(fields);
    let df_schema = DFSchema::try_from(schema)?;

    let expr_sql = body_sql.trim();
    let expr_sql = if expr_sql.to_uppercase().starts_with("SELECT ") {
        &expr_sql["SELECT ".len()..]
    } else {
        expr_sql
    };

    let state = ctx.state();
    let body_expr = state.create_logical_expr(expr_sql, &df_schema)?;

    let substituted = body_expr.transform_up(|e| {
        if let Expr::Column(col) = &e
            && let Some(idx) = param_names.iter().position(|n| n == col.name())
            && let Some(arg) = args.get(idx)
        {
            return Ok(Transformed::yes(arg.clone()));
        }
        Ok(Transformed::no(e))
    })?;

    Ok(substituted.data)
}

/// Recursively inline user UDF calls within an expression tree.
fn inline_expr(
    expr: Expr,
    functions: &HashMap<String, (Vec<String>, String)>,
    ctx: &SessionContextRef,
) -> DfResult<Transformed<Expr>> {
    expr.transform_up(|e| {
        if let Expr::ScalarFunction(ref sf) = e {
            let func_name = sf.name().to_lowercase();
            if let Some((param_names, body_sql)) = functions.get(&func_name) {
                let inlined = parse_body_expr(body_sql, param_names, &sf.args, ctx)?;
                return Ok(Transformed::yes(inlined));
            }
        }
        Ok(Transformed::no(e))
    })
}
