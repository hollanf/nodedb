//! Unit tests for NDARRAY_* function planning.

use crate::catalog::{ArrayCatalogView, SqlCatalogError};
use crate::error::Result;
use crate::functions::registry::FunctionRegistry;
use crate::parser::statement::parse_sql;
use crate::types::{CollectionInfo, SqlCatalog, SqlPlan};
use crate::types_array::{
    ArrayAttrAst, ArrayAttrType, ArrayDimAst, ArrayDimType, ArrayDomainBound, ArrayReducerAst,
};

struct StubCatalog {
    view: Option<ArrayCatalogView>,
    right_view: Option<ArrayCatalogView>,
}
impl SqlCatalog for StubCatalog {
    fn get_collection(
        &self,
        _name: &str,
    ) -> std::result::Result<Option<CollectionInfo>, SqlCatalogError> {
        Ok(None)
    }
    fn lookup_array(&self, name: &str) -> Option<ArrayCatalogView> {
        if name == "g" || name == "left" {
            self.view.clone()
        } else if name == "right" {
            self.right_view.clone()
        } else {
            None
        }
    }
}

fn view() -> ArrayCatalogView {
    ArrayCatalogView {
        name: "g".into(),
        dims: vec![
            ArrayDimAst {
                name: "chrom".into(),
                dtype: ArrayDimType::Int64,
                lo: ArrayDomainBound::Int64(1),
                hi: ArrayDomainBound::Int64(23),
            },
            ArrayDimAst {
                name: "pos".into(),
                dtype: ArrayDimType::Int64,
                lo: ArrayDomainBound::Int64(0),
                hi: ArrayDomainBound::Int64(1_000_000),
            },
        ],
        attrs: vec![
            ArrayAttrAst {
                name: "variant".into(),
                dtype: ArrayAttrType::String,
                nullable: true,
            },
            ArrayAttrAst {
                name: "qual".into(),
                dtype: ArrayAttrType::Float64,
                nullable: true,
            },
        ],
        tile_extents: vec![1, 1_000_000],
    }
}

fn cat() -> StubCatalog {
    StubCatalog {
        view: Some(view()),
        right_view: Some(view()),
    }
}

fn plan_one(sql: &str) -> Result<SqlPlan> {
    let stmts = parse_sql(sql)?;
    let q = match &stmts[0] {
        sqlparser::ast::Statement::Query(q) => q,
        _ => panic!("not a query"),
    };
    crate::planner::select::plan_query(
        q,
        &cat(),
        &FunctionRegistry::new(),
        crate::TemporalScope::default(),
    )
}

#[test]
fn slice_happy() {
    let p =
        plan_one("SELECT * FROM NDARRAY_SLICE('g', '{chrom: [1,1], pos: [0, 100]}', ['qual'], 50)")
            .unwrap();
    match p {
        SqlPlan::NdArraySlice {
            name,
            slice,
            attr_projection,
            limit,
            ..
        } => {
            assert_eq!(name, "g");
            assert_eq!(slice.dim_ranges.len(), 2);
            assert_eq!(attr_projection, vec!["qual".to_string()]);
            assert_eq!(limit, 50);
        }
        other => panic!("expected NdArraySlice, got {other:?}"),
    }
}

#[test]
fn slice_unknown_dim_rejected() {
    let err = plan_one("SELECT * FROM NDARRAY_SLICE('g', '{nope: [1, 2]}')")
        .err()
        .unwrap();
    assert!(format!("{err}").contains("no dim"));
}

#[test]
fn project_happy() {
    let p = plan_one("SELECT * FROM NDARRAY_PROJECT('g', ['qual', 'variant'])").unwrap();
    match p {
        SqlPlan::NdArrayProject {
            name,
            attr_projection,
        } => {
            assert_eq!(name, "g");
            assert_eq!(attr_projection, vec!["qual".to_string(), "variant".into()]);
        }
        other => panic!("expected NdArrayProject, got {other:?}"),
    }
}

#[test]
fn project_empty_rejected() {
    assert!(plan_one("SELECT * FROM NDARRAY_PROJECT('g', ARRAY[])").is_err());
}

#[test]
fn agg_scalar() {
    let p = plan_one("SELECT * FROM NDARRAY_AGG('g', 'qual', 'sum')").unwrap();
    match p {
        SqlPlan::NdArrayAgg {
            name,
            attr,
            reducer,
            group_by_dim,
            ..
        } => {
            assert_eq!(name, "g");
            assert_eq!(attr, "qual");
            assert_eq!(reducer, ArrayReducerAst::Sum);
            assert!(group_by_dim.is_none());
        }
        other => panic!("expected NdArrayAgg, got {other:?}"),
    }
}

#[test]
fn agg_grouped() {
    let p = plan_one("SELECT * FROM NDARRAY_AGG('g', 'qual', 'mean', 'chrom')").unwrap();
    match p {
        SqlPlan::NdArrayAgg {
            reducer,
            group_by_dim,
            ..
        } => {
            assert_eq!(reducer, ArrayReducerAst::Mean);
            assert_eq!(group_by_dim, Some("chrom".into()));
        }
        other => panic!("expected NdArrayAgg, got {other:?}"),
    }
}

#[test]
fn agg_unknown_reducer_rejected() {
    assert!(plan_one("SELECT * FROM NDARRAY_AGG('g', 'qual', 'bogus')").is_err());
}

#[test]
fn elementwise_happy() {
    let p = plan_one("SELECT * FROM NDARRAY_ELEMENTWISE('left', 'right', 'add', 'qual')").unwrap();
    assert!(matches!(p, SqlPlan::NdArrayElementwise { .. }));
}

#[test]
fn elementwise_unknown_op_rejected() {
    assert!(plan_one("SELECT * FROM NDARRAY_ELEMENTWISE('left', 'right', 'wat', 'qual')").is_err());
}

#[test]
fn flush_happy() {
    let p = plan_one("SELECT NDARRAY_FLUSH('g')").unwrap();
    assert!(matches!(p, SqlPlan::NdArrayFlush { .. }));
}

#[test]
fn compact_happy() {
    let p = plan_one("SELECT NDARRAY_COMPACT('g')").unwrap();
    assert!(matches!(p, SqlPlan::NdArrayCompact { .. }));
}

#[test]
fn flush_unknown_array_rejected() {
    assert!(plan_one("SELECT NDARRAY_FLUSH('nope')").is_err());
}
