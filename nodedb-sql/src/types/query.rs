//! Query structure types: projections, sort keys, aggregates, windows, engine/join/spatial enums.

use crate::types_expr::SqlExpr;

/// Database engine type for a collection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineType {
    DocumentSchemaless,
    DocumentStrict,
    KeyValue,
    Columnar,
    Timeseries,
    Spatial,
    /// ND sparse array engine — `CREATE ARRAY ...`. Routes through
    /// `ArrayRules` for SQL-level validation, but most table-shaped
    /// operations are unsupported on this engine (DML happens via
    /// dedicated `INSERT INTO ARRAY` / `DELETE FROM ARRAY` syntax).
    Array,
}

/// SQL join type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Semi,
    Anti,
    Cross,
}

impl JoinType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Inner => "inner",
            Self::Left => "left",
            Self::Right => "right",
            Self::Full => "full",
            Self::Semi => "semi",
            Self::Anti => "anti",
            Self::Cross => "cross",
        }
    }
}

/// Spatial predicate types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpatialPredicate {
    DWithin,
    Contains,
    Intersects,
    Within,
}

/// Projection item in SELECT.
#[derive(Debug, Clone)]
pub enum Projection {
    /// Simple column reference: `SELECT name`
    Column(String),
    /// All columns: `SELECT *`
    Star,
    /// Qualified star: `SELECT t.*`
    QualifiedStar(String),
    /// Computed expression: `SELECT price * qty AS total`
    Computed { expr: SqlExpr, alias: String },
}

/// Sort key for ORDER BY.
#[derive(Debug, Clone)]
pub struct SortKey {
    pub expr: SqlExpr,
    pub ascending: bool,
    pub nulls_first: bool,
}

/// Aggregate expression: `COUNT(*)`, `SUM(amount)`, etc.
#[derive(Debug, Clone)]
pub struct AggregateExpr {
    pub function: String,
    pub args: Vec<SqlExpr>,
    pub alias: String,
    pub distinct: bool,
}

/// Window function specification.
#[derive(Debug, Clone)]
pub struct WindowSpec {
    pub function: String,
    pub args: Vec<SqlExpr>,
    pub partition_by: Vec<SqlExpr>,
    pub order_by: Vec<SortKey>,
    pub alias: String,
}
