//! Arrow schema for change stream events and CdcEvent → RecordBatch conversion.

use std::sync::Arc;

use datafusion::arrow::array::{StringArray, UInt16Array, UInt32Array, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;

use crate::event::cdc::event::CdcEvent;

/// Arrow schema for change stream events.
///
/// Columns match CdcEvent fields, all as typed Arrow arrays.
pub fn stream_event_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("sequence", DataType::UInt64, false),
        Field::new("partition", DataType::UInt16, false),
        Field::new("collection", DataType::Utf8, false),
        Field::new("event_type", DataType::Utf8, false),
        Field::new("row_id", DataType::Utf8, false),
        Field::new("lsn", DataType::UInt64, false),
        Field::new("event_time", DataType::UInt64, false),
        Field::new("tenant_id", DataType::UInt32, false),
        Field::new("new_value", DataType::Utf8, true),
        Field::new("old_value", DataType::Utf8, true),
    ]))
}

/// Convert a slice of CdcEvents to an Arrow RecordBatch.
///
/// Returns None if the slice is empty.
pub fn events_to_record_batch(events: &[CdcEvent]) -> Option<RecordBatch> {
    if events.is_empty() {
        return None;
    }

    let schema = stream_event_schema();

    let sequences: Vec<u64> = events.iter().map(|e| e.sequence).collect();
    let partitions: Vec<u16> = events.iter().map(|e| e.partition).collect();
    let collections: Vec<&str> = events.iter().map(|e| e.collection.as_str()).collect();
    let event_types: Vec<&str> = events.iter().map(|e| e.op.as_str()).collect();
    let row_ids: Vec<&str> = events.iter().map(|e| e.row_id.as_str()).collect();
    let lsns: Vec<u64> = events.iter().map(|e| e.lsn).collect();
    let event_times: Vec<u64> = events.iter().map(|e| e.event_time).collect();
    let tenant_ids: Vec<u32> = events.iter().map(|e| e.tenant_id).collect();

    let new_values: Vec<Option<String>> = events
        .iter()
        .map(|e| {
            e.new_value
                .as_ref()
                .map(|v| serde_json::to_string(v).unwrap_or_default())
        })
        .collect();
    let old_values: Vec<Option<String>> = events
        .iter()
        .map(|e| {
            e.old_value
                .as_ref()
                .map(|v| serde_json::to_string(v).unwrap_or_default())
        })
        .collect();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from(sequences)),
            Arc::new(UInt16Array::from(partitions)),
            Arc::new(StringArray::from(collections)),
            Arc::new(StringArray::from(event_types)),
            Arc::new(StringArray::from(row_ids)),
            Arc::new(UInt64Array::from(lsns)),
            Arc::new(UInt64Array::from(event_times)),
            Arc::new(UInt32Array::from(tenant_ids)),
            Arc::new(StringArray::from(new_values)),
            Arc::new(StringArray::from(old_values)),
        ],
    )
    .ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_has_expected_fields() {
        let schema = stream_event_schema();
        assert_eq!(schema.fields().len(), 10);
        assert_eq!(schema.field(0).name(), "sequence");
        assert_eq!(schema.field(3).name(), "event_type");
        assert_eq!(schema.field(8).name(), "new_value");
        assert!(schema.field(8).is_nullable());
    }

    #[test]
    fn empty_events_returns_none() {
        assert!(events_to_record_batch(&[]).is_none());
    }

    #[test]
    fn events_to_batch_roundtrip() {
        let events = vec![
            CdcEvent {
                sequence: 1,
                partition: 0,
                collection: "orders".into(),
                op: "INSERT".into(),
                row_id: "o-1".into(),
                event_time: 1700000000000,
                lsn: 100,
                tenant_id: 1,
                new_value: Some(serde_json::json!({"total": 99})),
                old_value: None,
            },
            CdcEvent {
                sequence: 2,
                partition: 1,
                collection: "orders".into(),
                op: "UPDATE".into(),
                row_id: "o-2".into(),
                event_time: 1700000001000,
                lsn: 200,
                tenant_id: 1,
                new_value: Some(serde_json::json!({"total": 50})),
                old_value: Some(serde_json::json!({"total": 40})),
            },
        ];

        let batch = events_to_record_batch(&events).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 10);

        let sequences = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(sequences.value(0), 1);
        assert_eq!(sequences.value(1), 2);

        let event_types = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(event_types.value(0), "INSERT");
        assert_eq!(event_types.value(1), "UPDATE");
    }
}
