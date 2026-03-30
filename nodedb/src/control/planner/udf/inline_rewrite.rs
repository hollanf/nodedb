//! AnalyzerRule that inlines user-defined SQL expression functions.
//!
//! When DataFusion encounters a `ScalarFunction` call matching a user UDF,
//! this rule replaces it with the function's body expression, substituting
//! parameter references with the actual argument expressions.
//!
//! This makes the optimizer see through user functions: predicate pushdown,
//! constant folding, and projection elimination all work on the inlined body.
//!
//! **Volatility handling:** The `FunctionVolatility` classification is enforced
//! by DataFusion's optimizer *before* inlining occurs. When the UDF is registered
//! as a `ScalarUDF`, its `Volatility` is set from the stored definition. DataFusion
//! uses this to decide constant-folding eligibility: `Immutable` UDFs with all-literal
//! inputs are folded; `Stable`/`Volatile` are not. After inlining replaces the UDF
//! call with the body expression, the constituent functions (e.g. `LOWER`, `TRIM`)
//! carry their own volatility, so the optimizer continues to respect the semantics.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::common::Result as DfResult;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::optimizer::AnalyzerRule;

/// Holds the parsed body expression and parameter names for one user UDF.
pub struct InlinableFunction {
    /// Parameter names in declaration order.
    pub param_names: Vec<String>,
    /// The body SQL expression text (sans leading SELECT).
    pub body_sql: String,
}

/// Analyzer rule that inlines user SQL expression UDFs into the logical plan.
///
/// Registered per-session with the functions available for that tenant.
#[derive(Debug)]
pub struct InlineUserFunctions {
    /// function_name -> inlinable definition.
    /// Stored as body_sql + param_names; parsed lazily on first match.
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

    /// Returns true if there are no functions to inline (skip the rule).
    pub fn is_empty(&self) -> bool {
        self.functions.is_empty()
    }
}

/// Minimal reference to session context for expression parsing.
type SessionContextRef = Arc<datafusion::execution::context::SessionContext>;

/// Parse a SQL expression body, substituting parameter references with actual args.
///
/// Strategy: parse the body SQL into an Expr using a schema built from param names
/// (typed to match the actual argument expressions), then replace Column references
/// matching param names with the corresponding argument expressions.
fn parse_body_expr(
    body_sql: &str,
    param_names: &[String],
    args: &[Expr],
    ctx: &SessionContextRef,
) -> DfResult<Expr> {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::DFSchema;

    // Build a schema from parameter names. We use Utf8 as a placeholder type —
    // the actual type coercion happens when we substitute with real args.
    let fields: Vec<Field> = param_names
        .iter()
        .map(|name| Field::new(name, DataType::Utf8, true))
        .collect();
    let schema = Schema::new(fields);
    let df_schema = DFSchema::try_from(schema)?;

    // Strip leading SELECT if present.
    let expr_sql = body_sql.trim();
    let expr_sql = if expr_sql.to_uppercase().starts_with("SELECT ") {
        &expr_sql["SELECT ".len()..]
    } else {
        expr_sql
    };

    let state = ctx.state();
    let body_expr = state.create_logical_expr(expr_sql, &df_schema)?;

    // Substitute parameter Column references with actual argument expressions.
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

impl AnalyzerRule for InlineUserFunctions {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> DfResult<LogicalPlan> {
        if self.functions.is_empty() {
            return Ok(plan);
        }

        // We need a SessionContext for parsing body expressions.
        // Build a minimal one with system UDFs registered.
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

        // Transform the plan bottom-up, rewriting expressions that match user UDFs.
        plan.transform_up(|node| node.map_expressions(|expr| inline_expr(expr, functions, ctx_ref)))
            .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "inline_user_functions"
    }
}

/// Recursively inline user UDF calls within an expression tree.
fn inline_expr(
    expr: Expr,
    functions: &HashMap<String, (Vec<String>, String)>,
    ctx: &SessionContextRef,
) -> DfResult<Transformed<Expr>> {
    // Transform bottom-up so nested UDF calls get inlined first.
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
