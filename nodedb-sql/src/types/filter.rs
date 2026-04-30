//! Filter predicate types for SQL plan IR.

use crate::types_expr::{SqlExpr, SqlValue};

/// A filter predicate.
#[derive(Debug, Clone)]
pub struct Filter {
    pub expr: FilterExpr,
}

/// Filter expression tree.
#[derive(Debug, Clone)]
pub enum FilterExpr {
    Comparison {
        field: String,
        op: CompareOp,
        value: SqlValue,
    },
    InList {
        field: String,
        values: Vec<SqlValue>,
    },
    Between {
        field: String,
        low: SqlValue,
        high: SqlValue,
    },
    IsNull {
        field: String,
    },
    IsNotNull {
        field: String,
    },
    And(Vec<Filter>),
    Or(Vec<Filter>),
    Not(Box<Filter>),
    /// Raw expression filter (for complex predicates that don't fit simple patterns).
    Expr(SqlExpr),
}

/// Comparison operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
}
