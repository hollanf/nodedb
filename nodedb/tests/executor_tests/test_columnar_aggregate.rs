use crate::helpers::{make_ctx, payload_value, send_ok};
use nodedb::bridge::envelope::PhysicalPlan;
use nodedb::bridge::physical_plan::{ColumnarOp, QueryOp};

#[test]
fn aggregate_count_reads_plain_columnar_engine_rows() {
    let mut ctx = make_ctx();

    let rows = serde_json::json!([
        {"id": "r1", "city": "SF", "temp": 21},
        {"id": "r2", "city": "NYC", "temp": 18},
        {"id": "r3", "city": "SF", "temp": 25}
    ]);
    let payload = nodedb_types::json_to_msgpack(&rows).unwrap();

    send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Columnar(ColumnarOp::Insert {
            collection: "weather".into(),
            payload,
            format: "msgpack".into(),
        }),
    );

    let docs = ctx.core.scan_collection(1, "weather", 100).unwrap();
    assert_eq!(
        docs.len(),
        3,
        "scan_collection must see columnar engine rows"
    );

    let payload = send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Query(QueryOp::Aggregate {
            collection: "weather".into(),
            group_by: Vec::new(),
            aggregates: vec![("count".into(), "*".into())],
            filters: Vec::new(),
            having: Vec::new(),
            limit: 10,
            sub_group_by: Vec::new(),
            sub_aggregates: Vec::new(),
        }),
    );

    let result = payload_value(&payload);
    let rows = result
        .as_array()
        .unwrap_or_else(|| panic!("expected aggregate rows, got {result}"));
    assert_eq!(rows.len(), 1, "expected a single COUNT(*) row");
    assert_eq!(rows[0]["count_all"].as_u64(), Some(3));
}
