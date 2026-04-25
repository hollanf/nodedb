//! Unit tests for SELECT query planning.

use super::*;
use crate::functions::registry::FunctionRegistry;
use crate::parser::statement::parse_sql;
use crate::types::*;
use sqlparser::ast::Statement;

struct TestCatalog;

impl SqlCatalog for TestCatalog {
    fn get_collection(
        &self,
        name: &str,
    ) -> std::result::Result<Option<CollectionInfo>, SqlCatalogError> {
        let info = match name {
            "products" => Some(CollectionInfo {
                name: "products".into(),
                engine: EngineType::DocumentSchemaless,
                columns: Vec::new(),
                primary_key: Some("id".into()),
                has_auto_tier: false,
                indexes: Vec::new(),
                bitemporal: false,
            }),
            "users" => Some(CollectionInfo {
                name: "users".into(),
                engine: EngineType::DocumentSchemaless,
                columns: Vec::new(),
                primary_key: Some("id".into()),
                has_auto_tier: false,
                indexes: Vec::new(),
                bitemporal: false,
            }),
            "orders" => Some(CollectionInfo {
                name: "orders".into(),
                engine: EngineType::DocumentSchemaless,
                columns: Vec::new(),
                primary_key: Some("id".into()),
                has_auto_tier: false,
                indexes: Vec::new(),
                bitemporal: false,
            }),
            "docs" => Some(CollectionInfo {
                name: "docs".into(),
                engine: EngineType::DocumentSchemaless,
                columns: Vec::new(),
                primary_key: Some("id".into()),
                has_auto_tier: false,
                indexes: Vec::new(),
                bitemporal: false,
            }),
            "tags" => Some(CollectionInfo {
                name: "tags".into(),
                engine: EngineType::DocumentSchemaless,
                columns: Vec::new(),
                primary_key: Some("id".into()),
                has_auto_tier: false,
                indexes: Vec::new(),
                bitemporal: false,
            }),
            "user_prefs" => Some(CollectionInfo {
                name: "user_prefs".into(),
                engine: EngineType::KeyValue,
                columns: Vec::new(),
                primary_key: Some("key".into()),
                has_auto_tier: false,
                indexes: Vec::new(),
                bitemporal: false,
            }),
            _ => None,
        };
        Ok(info)
    }
}

fn plan_select_sql(sql: &str) -> SqlPlan {
    let statements = parse_sql(sql).unwrap();
    let Statement::Query(query) = &statements[0] else {
        panic!("expected query statement");
    };
    plan_query(
        query,
        &TestCatalog,
        &FunctionRegistry::new(),
        crate::TemporalScope::default(),
    )
    .unwrap()
}

#[test]
fn aggregate_subquery_join_filters_input_before_aggregation() {
    let plan = plan_select_sql(
        "SELECT AVG(price) FROM products WHERE category IN (SELECT DISTINCT category FROM products WHERE qty > 100)",
    );

    let SqlPlan::Aggregate { input, .. } = plan else {
        panic!("expected aggregate plan");
    };

    let SqlPlan::Join {
        left,
        join_type,
        on,
        ..
    } = *input
    else {
        panic!("expected semi-join below aggregate");
    };

    assert_eq!(join_type, JoinType::Semi);
    assert_eq!(on, vec![("category".into(), "category".into())]);
    assert!(matches!(*left, SqlPlan::Scan { .. }));
}

#[test]
fn scalar_subquery_defers_projection_until_after_join_filter() {
    let plan = plan_select_sql(
        "SELECT user_id FROM orders WHERE amount > (SELECT AVG(amount) FROM orders)",
    );

    let SqlPlan::Join {
        left,
        projection,
        filters,
        ..
    } = plan
    else {
        panic!("expected join plan");
    };

    let SqlPlan::Scan {
        projection: scan_projection,
        ..
    } = *left
    else {
        panic!("expected scan on join left");
    };

    assert!(scan_projection.is_empty(), "scan projected too early");
    assert_eq!(projection.len(), 1);
    match &projection[0] {
        Projection::Column(name) => assert_eq!(name, "user_id"),
        other => panic!("expected user_id projection, got {other:?}"),
    }
    assert!(
        !filters.is_empty(),
        "scalar comparison should stay post-join"
    );
}

#[test]
fn chained_join_preserves_qualified_on_keys() {
    let plan = plan_select_sql(
        "SELECT d.name, t.tag, p.theme \
         FROM docs d \
         LEFT JOIN tags t ON d.id = t.doc_id \
         INNER JOIN user_prefs p ON d.id = p.key",
    );

    let SqlPlan::Join { left, on, .. } = plan else {
        panic!("expected outer join plan");
    };
    assert_eq!(on, vec![("d.id".into(), "p.key".into())]);

    let SqlPlan::Join { on: inner_on, .. } = *left else {
        panic!("expected nested left join");
    };
    assert_eq!(inner_on, vec![("d.id".into(), "t.doc_id".into())]);
}
